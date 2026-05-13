#!/usr/bin/env python3
"""
Actor-to-Actor Game API Server
Serves the pathfinding game with actor connections via REST API
"""

import os
import sqlite3
import json
import random
import html
import ipaddress
from datetime import date, datetime
from collections import deque
from flask import Flask, jsonify, request, send_from_directory, Response
from flask_cors import CORS

app = Flask(__name__, static_folder=None)
CORS(app)

DATABASE_PATH = "/app/data/actors.db"
STATIC_PATH = "/app/actor-game/build"

def init_daily_connections_table():
    """Ensure daily_connections table exists"""
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()
    c.execute('''
    CREATE TABLE IF NOT EXISTS daily_connections (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        target_date DATE UNIQUE NOT NULL,
        start_actor_id INTEGER NOT NULL,
        target_actor_id INTEGER NOT NULL,
        optimal_path TEXT,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
    ''')
    conn.commit()
    conn.close()

def is_local_request():
    """Check if request comes from local network (private IP or loopback)"""
    ip = request.remote_addr or ''
    try:
        addr = ipaddress.ip_address(ip)
        return addr.is_private or addr.is_loopback
    except ValueError:
        return ip in ('localhost',)

# Initialize DB on import
init_daily_connections_table()

@app.route('/health')
def health():
    """Health check endpoint - doesn't require database"""
    return "healthy\n"

@app.route('/')
def index():
    """Serve the main page"""
    return send_from_directory(STATIC_PATH, 'index.html')

@app.route('/api/stats')
def get_stats():
    """Get database statistics"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM actors')
        actor_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM movie_credits')
        movie_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM tv_credits')
        tv_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT MAX(last_updated) FROM actors')
        last_update = cursor.fetchone()[0]
        
        conn.close()
        
        return jsonify({
            "actors": actor_count,
            "movies": movie_count,
            "tv_shows": tv_count,
            "last_updated": last_update
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/daily-connection')
def get_daily_connection():
    """Get today's daily connection"""
    try:
        today = date.today().isoformat()
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('SELECT start_actor_id, target_actor_id, optimal_path FROM daily_connections WHERE target_date = ?', (today,))
        row = c.fetchone()
        conn.close()
        
        if not row:
            return jsonify({"available": False, "message": "No daily connection for today"}), 404
        
        start_id, target_id, optimal_path_json = row
        
        # Get actor details
        conn = sqlite3.connect(DATABASE_PATH)
        c = conn.cursor()
        c.execute('SELECT id, name, profile_path, popularity FROM actors WHERE id IN (?, ?)', (start_id, target_id))
        actors_db = {r[0]: {"id": r[0], "name": r[1], "profile_path": r[2], "popularity": r[3]} for r in c.fetchall()}
        conn.close()
        
        return jsonify({
            "available": True,
            "date": today,
            "start_actor": actors_db.get(start_id),
            "target_actor": actors_db.get(target_id),
            "optimal_path": json.loads(optimal_path_json) if optimal_path_json else None
        })
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/admin/daily-connection', methods=['GET', 'POST', 'DELETE'])
def admin_daily_connection():
    """Admin: Manage daily connections (local-only)"""
    if not is_local_request():
        return jsonify({"error": "Admin access restricted to local network"}), 403
    
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()
    
    if request.method == 'GET':
        target_date = request.args.get('date', date.today().isoformat())
        c.execute('SELECT id, target_date, start_actor_id, target_actor_id, optimal_path, created_at FROM daily_connections WHERE target_date = ?', (target_date,))
        row = c.fetchone()
        conn.close()
        if not row:
            return jsonify({"exists": False, "date": target_date})
        return jsonify({
            "exists": True, "id": row[0], "date": row[1],
            "start_actor_id": row[2], "target_actor_id": row[3],
            "optimal_path": json.loads(row[4]) if row[4] else None,
            "created_at": row[5]
        })
    
    elif request.method == 'POST':
        data = request.get_json()
        target_date = data.get('date', date.today().isoformat())
        start_id = data['start_actor_id']
        target_id = data['target_actor_id']
        optimal_path = json.dumps(data.get('optimal_path', []))
        
        c.execute('''
        INSERT OR REPLACE INTO daily_connections (target_date, start_actor_id, target_actor_id, optimal_path)
        VALUES (?, ?, ?, ?)
        ''', (target_date, start_id, target_id, optimal_path))
        conn.commit()
        conn.close()
        return jsonify({"success": True, "date": target_date})
    
    elif request.method == 'DELETE':
        dc_id = request.args.get('id')
        if dc_id:
            c.execute('DELETE FROM daily_connections WHERE id = ?', (dc_id,))
        else:
            c.execute('DELETE FROM daily_connections WHERE target_date = ?', (request.args.get('date', date.today().isoformat()),))
        conn.commit()
        conn.close()
        return jsonify({"success": True})

@app.route('/api/admin/daily-connections')
def admin_list_daily_connections():
    """Admin: List all scheduled daily connections (local-only)"""
    if not is_local_request():
        return jsonify({"error": "Admin access restricted to local network"}), 403
    
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()
    c.execute('SELECT id, target_date, start_actor_id, target_actor_id, created_at FROM daily_connections ORDER BY target_date DESC LIMIT 30')
    rows = c.fetchall()
    conn.close()
    
    result = []
    for row in rows:
        result.append({"id": row[0], "date": row[1], "start_actor_id": row[2], "target_actor_id": row[3], "created_at": row[4]})
    return jsonify({"connections": result})

@app.route('/api/admin/actors/search')
def admin_actor_search():
    """Admin: Search actors (local-only)"""
    if not is_local_request():
        return jsonify({"error": "Admin access restricted to local network"}), 403
    
    q = request.args.get('q', '').strip()
    if not q:
        return jsonify({"actors": []})
    
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()
    c.execute('SELECT id, name, popularity, profile_path FROM actors WHERE name LIKE ? ORDER BY popularity DESC LIMIT 20', (f'%{q}%',))
    actors = [{"id": r[0], "name": r[1], "popularity": r[2], "profile_path": r[3]} for r in c.fetchall()]
    conn.close()
    return jsonify({"actors": actors})

@app.route('/api/admin/find-path')
def admin_find_path():
    """Admin: Find optimal path between two actors (local-only)"""
    if not is_local_request():
        return jsonify({"error": "Admin access restricted to local network"}), 403
    
    start_id = request.args.get('start', type=int)
    target_id = request.args.get('target', type=int)
    if not start_id or not target_id:
        return jsonify({"error": "Missing start or target"}), 400
    
    # Reuse the BFS pathfinding logic
    conn = sqlite3.connect(DATABASE_PATH)
    c = conn.cursor()
    c.execute('''
    SELECT m1.actor_id, m2.actor_id, m1.id, m1.title
    FROM movie_credits m1
    JOIN movie_credits m2 ON m1.id = m2.id AND m1.actor_id < m2.actor_id
    ORDER BY m1.actor_id
    ''')
    graph = {}
    edge_movies = {}
    for a1, a2, mid, title in c.fetchall():
        graph.setdefault(a1, []).append(a2)
        graph.setdefault(a2, []).append(a1)
        key = (a1, a2) if a1 < a2 else (a2, a1)
        if key not in edge_movies:
            edge_movies[key] = (mid, title)
    
    visited = {start_id: None}
    q = deque([start_id])
    found = False
    while q and not found:
        cur = q.popleft()
        for nb in graph.get(cur, []):
            if nb not in visited:
                visited[nb] = cur
                if nb == target_id:
                    found = True
                    break
                q.append(nb)
    
    if not found:
        conn.close()
        return jsonify({"error": "No path found"}), 404
    
    path_ids = []
    node = target_id
    while node is not None:
        path_ids.append(node)
        node = visited[node]
    path_ids.reverse()
    
    path = []
    for i in range(len(path_ids) - 1):
        a1, a2 = path_ids[i], path_ids[i+1]
        key = (a1, a2) if a1 < a2 else (a2, a1)
        movie_id, movie_title = edge_movies.get(key, (None, "Unknown"))
        c.execute('SELECT name, profile_path FROM actors WHERE id=?', (a1,))
        actor = c.fetchone()
        path.append({"type": "actor", "id": a1, "name": actor[0] if actor else "Unknown", "profile_path": actor[1] if actor else None})
        path.append({"type": "movie", "id": movie_id, "title": movie_title})
    c.execute('SELECT name, profile_path FROM actors WHERE id=?', (target_id,))
    target = c.fetchone()
    path.append({"type": "actor", "id": target_id, "name": target[0] if target else "Unknown", "profile_path": target[1] if target else None})
    
    conn.close()
    return jsonify({"path": path, "length": len(path_ids) - 1})

@app.route('/api/admin')
def admin_panel():
    """Admin panel HTML page (local-only)"""
    if not is_local_request():
        return "<h1>403 Forbidden</h1><p>Admin access restricted to local network</p>", 403
    
    return ADMIN_HTML

ADMIN_HTML = '''<!DOCTYPE html>
<html lang="en">
<head><meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Actor-to-Actor Admin</title>
<style>
*{box-sizing:border-box;margin:0;padding:0}
body{font-family:-apple-system,BlinkMacSystemFont,sans-serif;background:#0a0a1a;color:#e0e0e0;padding:20px;max-width:900px;margin:0 auto}
h1{color:#4ade80;margin-bottom:20px}
h2{color:#94a3b8;margin:20px 0 10px;font-size:1.1em}
.card{background:#1a1a2e;border-radius:12px;padding:20px;margin-bottom:16px}
label{display:block;margin-bottom:4px;color:#94a3b8;font-size:0.85em}
input,select,button{background:#16213e;color:#e0e0e0;border:1px solid #2a2a4a;border-radius:6px;padding:8px 12px;font-size:0.95em;width:100%;margin-bottom:8px}
button{background:#4ade80;color:#0a0a1a;font-weight:600;cursor:pointer;border:none}
button:hover{background:#22c55e}
button.danger{background:#ef4444}
button.danger:hover{background:#dc2626}
.search-results{max-height:200px;overflow-y:auto;background:#16213e;border-radius:6px;padding:4px}
.search-item{padding:8px 12px;cursor:pointer;border-radius:4px}
.search-item:hover{background:#2a2a4a}
.search-item.selected{background:#4ade80;color:#0a0a1a}
.row{display:flex;gap:12px}
.row>*{flex:1}
.path-display{margin-top:12px;padding:12px;background:#0a0a1a;border-radius:6px;font-size:0.9em;line-height:1.6}
.connection-list{list-style:none}
.connection-list li{padding:8px 0;border-bottom:1px solid #2a2a4a;display:flex;justify-content:space-between}
.badge{background:#4ade80;color:#0a0a1a;padding:2px 8px;border-radius:4px;font-size:0.8em}
</style>
</head>
<body>
<h1>🎬 Actor-to-Actor Admin</h1>

<div class="card">
  <h2>Set Daily Connection</h2>
  <label>Date</label>
  <input type="date" id="dcDate">
  <div class="row">
    <div>
      <label>Start Actor</label>
      <input type="text" id="startSearch" placeholder="Search actors..." autocomplete="off">
      <div class="search-results" id="startResults"></div>
    </div>
    <div>
      <label>Target Actor</label>
      <input type="text" id="targetSearch" placeholder="Search actors..." autocomplete="off">
      <div class="search-results" id="targetResults"></div>
    </div>
  </div>
  <div style="margin-top:8px">
    <button onclick="findPath()">Find Optimal Path</button>
    <button onclick="saveDaily()" style="background:#818cf8">Save Daily Connection</button>
  </div>
  <div class="path-display" id="pathPreview">Select start and target actors, then click "Find Optimal Path"</div>
</div>

<div class="card">
  <h2>Upcoming Connections</h2>
  <ul class="connection-list" id="connectionList">Loading...</ul>
</div>

<script>
let selectedStart = null, selectedTarget = null, optimalPath = null;
let startData = [], targetData = [];

document.getElementById('dcDate').value = new Date().toISOString().split('T')[0];

function debounce(fn, ms) {
  let t; return (...a) => { clearTimeout(t); t = setTimeout(() => fn(...a), ms); };
}

// Use event delegation for search results (avoids inline onclick escaping hell)
document.getElementById('startResults').addEventListener('click', function(e) {
  const div = e.target.closest('.search-item');
  if (!div) return;
  const id = parseInt(div.dataset.id);
  const name = div.dataset.name;
  selectedStart = id;
  document.getElementById('startSearch').value = name;
  this.innerHTML = '';
  updatePreview();
});
document.getElementById('targetResults').addEventListener('click', function(e) {
  const div = e.target.closest('.search-item');
  if (!div) return;
  const id = parseInt(div.dataset.id);
  const name = div.dataset.name;
  selectedTarget = id;
  document.getElementById('targetSearch').value = name;
  this.innerHTML = '';
  updatePreview();
});

function updatePreview() {
  const parts = [];
  if (selectedStart) parts.push('Start ✓');
  if (selectedTarget) parts.push('Target ✓');
  document.getElementById('pathPreview').textContent = parts.length ? 'Selected: ' + parts.join(' ') + '. Click "Find Optimal Path".' : 'Select start and target actors, then click "Find Optimal Path"';
}

async function searchActors(inputId, resultsId, store) {
  const q = document.getElementById(inputId).value.trim();
  const results = document.getElementById(resultsId);
  if (q.length < 2) { results.innerHTML = ''; return; }
  try {
    const r = await fetch('/api/admin/actors/search?q=' + encodeURIComponent(q));
    const data = await r.json();
    if (!data.actors || data.actors.length === 0) { results.innerHTML = '<div class="search-item">No results</div>'; return; }
    store.length = 0;
    data.actors.forEach(a => store.push(a));
    results.innerHTML = data.actors.map(a =>
      '<div class="search-item" data-id="' + a.id + '" data-name="' + a.name.replace(/"/g,'&quot;') + '">' +
      a.name + ' <span style="color:#94a3b8;font-size:0.85em">(pop: ' + a.popularity.toFixed(1) + ')</span></div>'
    ).join('');
  } catch(e) {
    results.innerHTML = '<div class="search-item" style="color:#ef4444">Error searching</div>';
  }
}

document.getElementById('startSearch').addEventListener('input', debounce(() => searchActors('startSearch','startResults', startData), 300));
document.getElementById('targetSearch').addEventListener('input', debounce(() => searchActors('targetSearch','targetResults', targetData), 300));

async function findPath() {
  if (!selectedStart || !selectedTarget) { document.getElementById('pathPreview').textContent = 'Please select both start and target actors.'; return; }
  const el = document.getElementById('pathPreview');
  el.textContent = 'Finding optimal path...';
  try {
    const r = await fetch('/api/admin/find-path?start=' + selectedStart + '&target=' + selectedTarget);
    if (!r.ok) { el.textContent = 'No path found between these actors.'; return; }
    const data = await r.json();
    optimalPath = data.path;
    const html = optimalPath.map((item, i) => {
      const name = item.name || item.title;
      const color = item.type === 'actor' ? '#4ade80' : '#f59e0b';
      return '<span style="color:' + color + '">' + name + '</span>' +
        (i < optimalPath.length - 1 ? ' <span style="color:#94a3b8">→</span> ' : '');
    }).join('');
    el.innerHTML = '<strong>Optimal Path (' + data.length + ' connections):</strong><br>' + html;
  } catch(e) {
    el.textContent = 'Error finding path.';
  }
}

async function saveDaily() {
  if (!selectedStart || !selectedTarget) { alert('Select both actors first!'); return; }
  const date = document.getElementById('dcDate').value;
  try {
    const r = await fetch('/api/admin/daily-connection', {
      method: 'POST',
      headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({date, start_actor_id: selectedStart, target_actor_id: selectedTarget, optimal_path: optimalPath || []})
    });
    const data = await r.json();
    if (data.success) { alert('Saved!'); loadConnections(); }
    else alert('Error: ' + (data.error || 'unknown'));
  } catch(e) {
    alert('Failed to save connection.');
  }
}

async function loadConnections() {
  const list = document.getElementById('connectionList');
  try {
    const r = await fetch('/api/admin/daily-connections');
    const data = await r.json();
    if (!data.connections || data.connections.length === 0) { list.innerHTML = '<li style="color:#94a3b8">No connections scheduled</li>'; return; }
    list.innerHTML = data.connections.map(dc =>
      '<li><span><strong>' + dc.date + '</strong> — actor ' + dc.start_actor_id + ' → ' + dc.target_actor_id + '</span>' +
      '<button class="danger" onclick="deleteConnection(' + dc.id + ')" style="width:auto;padding:2px 10px;font-size:0.85em">X</button></li>'
    ).join('');
  } catch(e) {
    list.innerHTML = '<li style="color:#ef4444">Error loading connections</li>';
  }
}

async function deleteConnection(id) {
  if (!confirm('Delete this daily connection?')) return;
  try {
    await fetch('/api/admin/daily-connection?id=' + id, {method: 'DELETE'});
  } catch(e) {}
  loadConnections();
}

loadConnections();
</script>
</body>
</html>'''

@app.route('/api/actors')
def get_actors():
    """Get actors with pagination and search"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Parameters
        page = int(request.args.get('page', 1))
        limit = min(int(request.args.get('limit', 50)), 100)  # Max 100
        search = request.args.get('search', '').strip()
        
        offset = (page - 1) * limit
        
        # Build query
        if search:
            query = '''
            SELECT id, name, popularity, profile_path, place_of_birth, credits_count
            FROM actors 
            WHERE name LIKE ? 
            ORDER BY popularity DESC 
            LIMIT ? OFFSET ?
            '''
            params = (f'%{search}%', limit, offset)
            
            count_query = 'SELECT COUNT(*) FROM actors WHERE name LIKE ?'
            count_params = (f'%{search}%',)
        else:
            query = '''
            SELECT id, name, popularity, profile_path, place_of_birth, credit_count
            FROM actors 
            ORDER BY popularity DESC 
            LIMIT ? OFFSET ?
            '''
            params = (limit, offset)
            
            count_query = 'SELECT COUNT(*) FROM actors'
            count_params = ()
        
        # Get total count
        cursor.execute(count_query, count_params)
        total = cursor.fetchone()[0]
        
        # Get actors
        cursor.execute(query, params)
        actors = []
        
        for row in cursor.fetchall():
            actors.append({
                "id": row[0],
                "name": row[1],
                "popularity": row[2],
                "profile_path": row[3],
                "place_of_birth": row[4],
                "credit_count": row[5]
            })
        
        conn.close()
        
        return jsonify({
            "actors": actors,
            "total": total,
            "page": page,
            "limit": limit,
            "pages": (total + limit - 1) // limit
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/actor/<int:actor_id>')
def get_actor_details(actor_id):
    """Get detailed actor information with credits"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Get actor info
        cursor.execute('''
        SELECT id, name, popularity, profile_path, place_of_birth, credit_count
        FROM actors WHERE id = ?
        ''', (actor_id,))
        
        actor_row = cursor.fetchone()
        if not actor_row:
            return jsonify({"error": "Actor not found"}), 404
        
        actor = {
            "id": actor_row[0],
            "name": actor_row[1],
            "popularity": actor_row[2],
            "profile_path": actor_row[3],
            "place_of_birth": actor_row[4],
            "credit_count": actor_row[5]
        }
        
        # Get movie credits
        cursor.execute('''
        SELECT id, title, character, popularity, release_date
        FROM movie_credits 
        WHERE actor_id = ? 
        ORDER BY popularity DESC
        ''', (actor_id,))
        
        movies = []
        for row in cursor.fetchall():
            movies.append({
                "id": row[0],
                "title": row[1],
                "character": row[2],
                "popularity": row[3],
                "release_date": row[4]
            })
        
        # Get TV credits
        cursor.execute('''
        SELECT id, name, character, popularity, first_air_date
        FROM tv_credits 
        WHERE actor_id = ? 
        ORDER BY popularity DESC
        ''', (actor_id,))
        
        tv_shows = []
        for row in cursor.fetchall():
            tv_shows.append({
                "id": row[0],
                "name": row[1],
                "character": row[2],
                "popularity": row[3],
                "first_air_date": row[4]
            })
        
        conn.close()
        
        actor['movies'] = movies
        actor['tv_shows'] = tv_shows
        
        return jsonify(actor)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/game/start')
def start_game():
    """Start a new game using pre-computed optimal paths from actor_connections.db"""
    try:
        difficulty = request.args.get('difficulty', 'normal')
        exclude_mcu = request.args.get('exclude_mcu', 'false').lower() == 'true'
        
        # Try to get a pre-computed connection first
        connections_path = "/app/data/actor_connections.db"
        if os.path.exists(connections_path):
            conn = sqlite3.connect(connections_path)
            cursor = conn.cursor()
            
            # Get a random pre-computed connection for this difficulty
            cursor.execute('''
            SELECT start_id, target_id, optimal_path 
            FROM actor_connections 
            WHERE difficulty = ? 
            ORDER BY RANDOM() 
            LIMIT 1
            ''', (difficulty,))
            
            result = cursor.fetchone()
            if result:
                start_id, target_id, optimal_path_blob = result
                
                # Decompress the optimal path
                import gzip
                optimal_path_data = gzip.decompress(optimal_path_blob).decode('utf-8')
                optimal_path = json.loads(optimal_path_data)
                
                # Get actor details from main database
                main_conn = sqlite3.connect(DATABASE_PATH)
                main_cursor = main_conn.cursor()
                
                main_cursor.execute('SELECT id, name, profile_path FROM actors WHERE id IN (?, ?)', (start_id, target_id))
                actors = main_cursor.fetchall()
                
                if len(actors) == 2:
                    start_actor = next(a for a in actors if a[0] == int(start_id))
                    target_actor = next(a for a in actors if a[0] == int(target_id))
                    
                    main_conn.close()
                    conn.close()
                    
                    return jsonify({
                        "start_actor": {
                            "id": start_actor[0],
                            "name": start_actor[1],
                            "profile_path": start_actor[2]
                        },
                        "target_actor": {
                            "id": target_actor[0],
                            "name": target_actor[1],
                            "profile_path": target_actor[2]
                        },
                        "optimal_path": optimal_path,
                        "difficulty": difficulty
                    })
                
                main_conn.close()
            conn.close()
        
        # Fallback: random selection if no pre-computed paths
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Get actors based on difficulty using weighted popularity + credit count
        # Higher credit count threshold for easy = established careers only
        if difficulty == 'easy':
            min_popularity = 25
            min_credits = 8
        elif difficulty == 'hard':
            min_popularity = 10
            min_credits = 2
        else:  # normal
            min_popularity = 15
            min_credits = 4
        
        cursor.execute('''
        SELECT DISTINCT a.id, a.name, a.profile_path, COUNT(mc.id) as credit_count
        FROM actors a
        INNER JOIN movie_credits mc ON a.id = mc.actor_id
        WHERE a.popularity >= ? 
        AND (a.place_of_birth LIKE '%USA%' OR a.place_of_birth LIKE '%UK%' OR a.place_of_birth LIKE '%Canada%' OR a.place_of_birth LIKE '%Australia%' OR a.place_of_birth IS NULL)
        AND mc.character IS NOT NULL AND mc.character != ''
        AND mc.character NOT LIKE 'Self%' AND mc.character NOT LIKE 'Himself%'
        AND mc.character NOT LIKE 'Herself%' AND mc.character NOT LIKE '%Archive%'
        AND mc.character NOT LIKE '%Reader:%' AND mc.character NOT LIKE '%Narrator%'
        AND LENGTH(mc.character) > 2
        GROUP BY a.id, a.name, a.profile_path
        HAVING COUNT(mc.id) >= ?
        ORDER BY a.popularity DESC
        LIMIT 100
        ''', (min_popularity, min_credits))
        
        candidates = cursor.fetchall()
        if len(candidates) < 2:
            return jsonify({"error": "Not enough actors in database"}), 400
            
        start_actor = random.choice(candidates)
        target_actor = random.choice([a for a in candidates if a[0] != start_actor[0]])
        
        conn.close()
        
        return jsonify({
            "start_actor": {
                "id": start_actor[0],
                "name": start_actor[1],
                "profile_path": start_actor[2]
            },
            "target_actor": {
                "id": target_actor[0],
                "name": target_actor[1],
                "profile_path": target_actor[2]
            },
            "difficulty": difficulty
        })
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/actor/<int:actor_id>/connections')
def get_actor_connections(actor_id):
    """Get actors who appeared in movies with the specified actor"""
    try:
        search = request.args.get('search', '').strip()
        
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Find actors who shared movies with this actor
        query = '''
        SELECT DISTINCT a2.id, a2.name, a2.profile_path, a2.popularity,
               GROUP_CONCAT(DISTINCT m.title) as shared_movies
        FROM movie_credits m1
        JOIN movie_credits m2 ON m1.id = m2.id  
        JOIN actors a2 ON m2.actor_id = a2.id
        JOIN movie_credits m ON m.id = m1.id AND m.actor_id = a2.id
        WHERE m1.actor_id = ? AND a2.id != ?
        '''
        
        params = [actor_id, actor_id]
        
        if search:
            query += ' AND a2.name LIKE ?'
            params.append(f'%{search}%')
            
        query += '''
        GROUP BY a2.id
        ORDER BY a2.popularity DESC
        LIMIT 20
        '''
        
        cursor.execute(query, params)
        
        connections = []
        for row in cursor.fetchall():
            connections.append({
                "id": row[0],
                "name": row[1],
                "profile_path": row[2],
                "popularity": row[3],
                "shared_movies": row[4].split(',') if row[4] else []
            })
        
        conn.close()
        return jsonify({"connections": connections})
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/movies/shared/<int:actor1_id>/<int:actor2_id>')
def get_shared_movies(actor1_id, actor2_id):
    """Get movies shared between two actors"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT DISTINCT m1.id, m1.title, m1.popularity, m1.release_date
        FROM movie_credits m1
        JOIN movie_credits m2 ON m1.id = m2.id
        WHERE m1.actor_id = ? AND m2.actor_id = ?
        ORDER BY m1.popularity DESC
        LIMIT 10
        ''', (actor1_id, actor2_id))
        
        movies = []
        for row in cursor.fetchall():
            movies.append({
                "id": row[0],
                "title": row[1],
                "popularity": row[2],
                "release_date": row[3]
            })
        
        conn.close()
        return jsonify({"movies": movies})
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/movies/<int:movie_id>/cast')
def get_movie_cast(movie_id):
    """Get cast of a specific movie, with optional name search"""
    try:
        search = request.args.get('search', '').strip()
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        query = '''
        SELECT a.id, a.name, a.profile_path, a.popularity, mc.character
        FROM movie_credits mc
        JOIN actors a ON mc.actor_id = a.id
        WHERE mc.id = ?
        '''
        params = [movie_id]
        
        if search:
            query += ' AND a.name LIKE ?'
            params.append(f'%{search}%')
        
        query += ' ORDER BY mc.popularity DESC LIMIT 20'
        
        cursor.execute(query, params)
        cast = []
        for row in cursor.fetchall():
            cast.append({
                "id": row[0],
                "name": row[1],
                "profile_path": row[2],
                "popularity": row[3],
                "character": row[4]
            })
        
        conn.close()
        return jsonify({"cast": cast})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/movies/<int:movie_id>/has-actor/<int:actor_id>')
def movie_has_actor(movie_id, actor_id):
    """Check if an actor appears in a movie (for auto-complete)"""
    try:
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        cursor.execute('SELECT COUNT(*) FROM movie_credits WHERE id = ? AND actor_id = ?', (movie_id, actor_id))
        count = cursor.fetchone()[0]
        conn.close()
        return jsonify({"has_actor": count > 0})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/api/search')
def search_actors():
    """Search actors by name"""
    try:
        query = request.args.get('q', '').strip()
        if not query:
            return jsonify({"actors": []})
        
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute('''
        SELECT id, name, popularity, profile_path
        FROM actors 
        WHERE name LIKE ? 
        ORDER BY popularity DESC 
        LIMIT 10
        ''', (f'%{query}%',))
        
        actors = []
        for row in cursor.fetchall():
            actors.append({
                "id": row[0],
                "name": row[1],
                "popularity": row[2],
                "profile_path": row[3]
            })
        
        conn.close()
        return jsonify({"actors": actors})
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/actors/<int:actor_id>/movies')
def get_actor_movies(actor_id):
    """Get movies for a specific actor with MCU filtering"""
    try:
        exclude_mcu = request.args.get('exclude_mcu', 'false').lower() == 'true'
        
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Use movie_credits table for detailed movie information
        query = '''
        SELECT id, title, character, popularity, release_date
        FROM movie_credits
        WHERE actor_id = ?
        '''
        
        params = [actor_id]
        
        if exclude_mcu:
            # Exclude MCU movies
            mcu_titles = [
                '%avengers%', '%iron man%', '%thor%', '%captain america%',
                '%guardians of the galaxy%', '%ant-man%', '%doctor strange%',
                '%black panther%', '%spider-man%', '%captain marvel%',
                '%eternals%', '%shang-chi%', '%loki%', '%wandavision%',
                '%falcon and winter soldier%', '%hawkeye%', '%moon knight%',
                '%she-hulk%', '%ms. marvel%', '%what if%'
            ]
            
            for title in mcu_titles:
                query += f' AND title NOT LIKE ?'
                params.append(title)
        
        query += ' ORDER BY popularity DESC, release_date DESC'
        
        cursor.execute(query, params)
        
        movies = []
        for row in cursor.fetchall():
            movies.append({
                "id": row[0],
                "title": row[1],
                "character": row[2],
                "popularity": row[3],
                "release_date": row[4]
            })
        
        conn.close()
        return jsonify(movies)
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/api/game/validate-path', methods=['POST'])
def validate_path():
    """Validate a user's path attempt"""
    try:
        data = request.get_json()
        path = data.get('path', [])
        start_actor_id = data.get('start_actor_id')
        target_actor_id = data.get('target_actor_id')
        difficulty = data.get('difficulty', 'normal')
        exclude_mcu = data.get('exclude_mcu', False)
        
        if not path or len(path) < 3:
            return jsonify({"valid": False, "error": "Path too short"})
            
        # Validate path structure: actor -> movie -> actor -> movie -> ... -> actor
        if len(path) % 2 == 0:
            return jsonify({"valid": False, "error": "Path must end with an actor"})
            
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Validate each connection in the path
        for i in range(0, len(path) - 2, 2):
            actor_id = path[i]
            movie_id = path[i + 1]
            next_actor_id = path[i + 2]
            
            # Check if actor appears in movie (using movie_credits table)
            cursor.execute('''
            SELECT COUNT(*) FROM movie_credits 
            WHERE actor_id = ? AND id = ?
            ''', (actor_id, movie_id))
            
            if cursor.fetchone()[0] == 0:
                conn.close()
                return jsonify({"valid": False, "error": f"Actor {actor_id} not in movie {movie_id}"})
                
            # Check if next actor appears in same movie
            cursor.execute('''
            SELECT COUNT(*) FROM movie_credits 
            WHERE actor_id = ? AND id = ?
            ''', (next_actor_id, movie_id))
            
            if cursor.fetchone()[0] == 0:
                conn.close()
                return jsonify({"valid": False, "error": f"Actor {next_actor_id} not in movie {movie_id}"})
        
        # Check if path starts and ends correctly
        if path[0] != start_actor_id or path[-1] != target_actor_id:
            conn.close()
            return jsonify({"valid": False, "error": "Path doesn't connect start and target actors"})
            
        # Calculate path length (number of movie connections)
        path_length = (len(path) - 1) // 2
        
        # Check difficulty requirements
        if difficulty == 'easy' and path_length > 2:
            conn.close()
            return jsonify({"valid": False, "error": "Easy mode allows maximum 2 connections"})
        elif difficulty == 'normal' and path_length > 4:
            conn.close()
            return jsonify({"valid": False, "error": "Normal mode allows maximum 4 connections"})
        elif difficulty == 'hard' and path_length > 6:
            conn.close()
            return jsonify({"valid": False, "error": "Hard mode allows maximum 6 connections"})
            
        conn.close()
        
        return jsonify({
            "valid": True, 
            "path_length": path_length,
            "difficulty": difficulty
        })
        
    except Exception as e:
        return jsonify({"valid": False, "error": str(e)})


@app.route('/api/game/find-path')
def find_optimal_path():
    """Find the optimal path between two actors (for hints or solution)"""
    try:
        start_id = request.args.get('start_id', type=int)
        target_id = request.args.get('target_id', type=int)
        difficulty = request.args.get('difficulty', 'normal')
        exclude_mcu = request.args.get('exclude_mcu', 'false').lower() == 'true'
        
        if not start_id or not target_id:
            return jsonify({"error": "Missing start_id or target_id"}), 400
            
        # First try pre-computed paths
        connections_path = "/app/data/actor_connections.db"
        if os.path.exists(connections_path):
            conn = sqlite3.connect(connections_path)
            cursor = conn.cursor()
            
            cursor.execute('''
            SELECT optimal_path FROM actor_connections 
            WHERE start_id = ? AND target_id = ? AND difficulty = ?
            ''', (str(start_id), str(target_id), difficulty))
            
            result = cursor.fetchone()
            if result:
                import gzip
                optimal_path_data = gzip.decompress(result[0]).decode('utf-8')
                optimal_path = json.loads(optimal_path_data)
                conn.close()
                return jsonify({"path": optimal_path, "precomputed": True})
            conn.close()
        
        # Fallback: real-time BFS shortest path
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Build actor connections graph
        cursor.execute('''
        SELECT m1.actor_id, m2.actor_id, m1.id, m1.title, m2.character
        FROM movie_credits m1
        JOIN movie_credits m2 ON m1.id = m2.id AND m1.actor_id < m2.actor_id
        ORDER BY m1.actor_id
        ''')
        
        graph = {}
        edge_movies = {}
        for a1, a2, movie_id, title, char in cursor.fetchall():
            graph.setdefault(a1, []).append(a2)
            graph.setdefault(a2, []).append(a1)
            key = (a1, a2) if a1 < a2 else (a2, a1)
            if key not in edge_movies:
                edge_movies[key] = (movie_id, title)
        
        # BFS to find shortest path
        visited = {start_id: None}
        q = deque([start_id])
        found = False
        while q and not found:
            current = q.popleft()
            for neighbor in graph.get(current, []):
                if neighbor not in visited:
                    visited[neighbor] = current
                    if neighbor == target_id:
                        found = True
                        break
                    q.append(neighbor)
        
        if not found:
            conn.close()
            return jsonify({"error": "No path found between these actors"}), 404
        
        # Reconstruct path: actor -> movie -> actor -> movie -> ... -> actor
        path_ids = []
        node = target_id
        while node is not None:
            path_ids.append(node)
            node = visited[node]
        path_ids.reverse()
        
        # Build full path with movie details
        path = []
        for i in range(len(path_ids) - 1):
            a1, a2 = path_ids[i], path_ids[i+1]
            key = (a1, a2) if a1 < a2 else (a2, a1)
            movie_id, movie_title = edge_movies.get(key, (None, "Unknown"))
            
            cursor.execute('SELECT name, profile_path FROM actors WHERE id=?', (a1,))
            actor = cursor.fetchone()
            path.append({
                "type": "actor", "id": a1,
                "name": actor[0] if actor else "Unknown",
                "profile_path": actor[1] if actor else None
            })
            path.append({
                "type": "movie", "id": movie_id,
                "title": movie_title
            })
        
        cursor.execute('SELECT name, profile_path FROM actors WHERE id=?', (target_id,))
        target = cursor.fetchone()
        path.append({
            "type": "actor", "id": target_id,
            "name": target[0] if target else "Unknown",
            "profile_path": target[1] if target else None
        })
        
        conn.close()
        return jsonify({"path": path, "precomputed": False, "length": len(path_ids) - 1})
        
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.route('/<path:path>')
def static_files(path):
    """Serve static files"""
    try:
        return send_from_directory(STATIC_PATH, path)
    except:
        return send_from_directory(STATIC_PATH, 'index.html')  # SPA fallback

if __name__ == '__main__':
    import glob
    
    # Ensure data directory exists
    os.makedirs('/app/data', exist_ok=True)
    
    # Log startup info
    print(f"Starting server on 0.0.0.0:5000")
    print(f"Static path: {os.path.abspath(STATIC_PATH)}")
    print(f"Static path exists: {os.path.exists(os.path.abspath(STATIC_PATH))}")
    print(f"Index file exists: {os.path.exists(os.path.join(os.path.abspath(STATIC_PATH), 'index.html'))}")
    print(f"Static JS files: {glob.glob(os.path.join(os.path.abspath(STATIC_PATH), 'static', 'js', '*.js'))}")
    
    # Run the server - bind to 0.0.0.0 for Docker
    app.run(host='0.0.0.0', port=5000, debug=False)
