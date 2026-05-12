"""Simulate thousands of games to verify the DB is working correctly"""
import sqlite3
from collections import deque
import random
import time

DB_PATH = "/app/data/actors.db"

def load_graph():
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    
    c.execute('SELECT id, name, popularity FROM actors')
    actors = {row[0]: {"name": row[1], "popularity": row[2]} for row in c.fetchall()}
    
    c.execute('''
    SELECT m1.actor_id, m2.actor_id, m1.title
    FROM movie_credits m1
    JOIN movie_credits m2 ON m1.id = m2.id AND m1.actor_id < m2.actor_id
    GROUP BY m1.actor_id, m2.actor_id
    ''')
    
    graph = {aid: set() for aid in actors}
    edge_count = 0
    for a1, a2, title in c.fetchall():
        graph[a1].add(a2)
        graph[a2].add(a1)
        edge_count += 1
    
    conn.close()
    print(f"Graph: {len(actors)} nodes, {edge_count} edges")
    return actors, graph

def shortest_path(graph, start, target, max_depth=10):
    if start == target:
        return [start]
    visited = {start: None}
    q = deque([start])
    depth = 0
    nodes_visited = 0
    while q and depth <= max_depth:
        level_size = len(q)
        for _ in range(level_size):
            current = q.popleft()
            nodes_visited += 1
            if current == target:
                path = []
                node = target
                while node is not None:
                    path.append(node)
                    node = visited[node]
                path.reverse()
                return path, nodes_visited
            for neighbor in graph.get(current, set()):
                if neighbor not in visited:
                    visited[neighbor] = current
                    q.append(neighbor)
        depth += 1
    return None, nodes_visited

def simulate(actors, graph, difficulties, num_games=1000):
    results = {d: {"found": 0, "total": 0, "avg_len": 0, "avg_nodes": 0, "times": []} for d in difficulties}
    
    for difficulty, min_pop, max_trials in difficulties:
        print(f"\n=== {difficulty.upper()} (min_pop={min_pop}) ===")
        eligible = [aid for aid, info in actors.items() if info["popularity"] >= min_pop]
        print(f"Eligible actors: {len(eligible)}")
        
        if len(eligible) < 10:
            print(f"  SKIP - not enough eligible actors")
            continue
        
        found = 0
        total_len = 0
        total_nodes = 0
        times = []
        
        for i in range(num_games):
            start = random.choice(eligible)
            target = random.choice([a for a in eligible if a != start])
            
            t0 = time.time()
            path, nodes = shortest_path(graph, start, target, max_depth=6)
            elapsed = time.time() - t0
            
            if path:
                found += 1
                total_len += len(path) - 1
                total_nodes += nodes
            
            if (i + 1) % 1000 == 0:
                print(f"  {i+1}/{num_games}... found={found}/{i+1}")
        
        avg_len = total_len / found if found else 0
        avg_nodes = total_nodes / found if found else 0
        results[difficulty] = {
            "found": found, "total": num_games,
            "avg_len": avg_len, "avg_nodes": avg_nodes,
            "eligible": len(eligible)
        }
        print(f"  Found paths: {found}/{num_games} ({100*found/num_games:.1f}%)")
        print(f"  Avg path length: {avg_len:.2f} connections")
        print(f"  Avg BFS nodes: {avg_nodes:.0f}")
    
    return results

# Difficulty configs: (name, min_popularity, num_games)
DIFFICULTIES = [
    ("easy", 40, 2000),
    ("normal", 25, 5000),
    ("hard", 15, 3000),
]

if __name__ == "__main__":
    print("Loading graph from DB...")
    t0 = time.time()
    actors, graph = load_graph()
    print(f"Loaded in {time.time()-t0:.1f}s")
    
    results = simulate(actors, graph, DIFFICULTIES, num_games=2000)
    
    print("\n=== SUMMARY ===")
    for diff, r in results.items():
        rate = 100 * r["found"] / r["total"] if r["total"] else 0
        print(f"{diff:8s}: {r['found']:4d}/{r['total']:5d} ({rate:5.1f}%) found | avg len {r['avg_len']:.1f} | eligible {r['eligible']}")
