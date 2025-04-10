import os
import sqlite3
import networkx as nx
import json
import numpy as np
from tqdm import tqdm
import pandas as pd
import pickle
import gzip
import time

# Constants
REGIONS = ['GLOBAL', 'US', 'UK', 'CA', 'AU', 'FR', 'DE', 'IN', 'KR', 'JP', 'CN']
CONNECTION_DB_PATH = 'actor-game/public/actor_connections.db'  # Updated path
REGIONS_TO_PROCESS = ['GLOBAL', 'US', 'UK']  # Prioritize these regions for connections

# Difficulty settings
DIFFICULTY_CONFIG = {
    'easy': {'min_connections': 1, 'max_connections': 3, 'count': 1000},
    'normal': {'min_connections': 3, 'max_connections': 5, 'count': 1000},
    'hard': {'min_connections': 5, 'max_connections': 8, 'count': 1000}
}

def load_actor_data(region):
    """Load actor data from a single SQLite database filtered by region"""
    print(f"Loading actor data for {region}...")
    
    possible_paths = [
        "public/actors.db",
        "actor-game/public/actors.db",
        "./actors.db"
    ]
    
    db_path = None
    for path in possible_paths:
        if os.path.exists(path):
            db_path = path
            print(f"Found database at {path}")
            break
    
    if not db_path:
        print(f"Consolidated actors database not found in any location")
        
        legacy_paths = [
            f"public/actors_{region}.db",
            f"actor-game/public/actors_{region}.db",
            f"./actors_{region}.db"
        ]
        
        for path in legacy_paths:
            if os.path.exists(path):
                print(f"Found legacy database at {path}")
                db_path = path
                break
        
        if not db_path:
            print(f"No database found for {region}")
            return None
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='actor_regions'")
    has_region_table = cursor.fetchone() is not None
    
    if has_region_table:
        print(f"Using consolidated database with region flags")
        if region == "GLOBAL":
            actors_df = pd.read_sql("""
                SELECT a.id, a.name, a.popularity, a.profile_path 
                FROM actors a
                JOIN actor_regions ar ON a.id = ar.actor_id
                WHERE ar.region = 'GLOBAL'
            """, conn)
        else:
            actors_df = pd.read_sql(f"""
                SELECT a.id, a.name, a.popularity, a.profile_path 
                FROM actors a
                JOIN actor_regions ar ON a.id = ar.actor_id
                WHERE ar.region = '{region}'
            """, conn)
    else:
        print(f"Using legacy database format")
        actors_df = pd.read_sql("SELECT id, name, popularity, profile_path FROM actors", conn)
    
    actor_ids = actors_df['id'].tolist()
    if not actor_ids:
        print(f"No actors found for region {region}")
        conn.close()
        return {}
    
    actor_ids_str = ', '.join(map(str, actor_ids))
    
    movie_credits_df = pd.read_sql(
        f"SELECT actor_id, id, title, poster_path, popularity FROM movie_credits WHERE actor_id IN ({actor_ids_str})", 
        conn
    )
    
    # Now load TV credits and include the character field for filtering
    tv_credits_df = pd.read_sql(
        f"SELECT actor_id, id, name as title, poster_path, popularity, character FROM tv_credits WHERE actor_id IN ({actor_ids_str})", 
        conn
    )
    
    conn.close()
    
    # Convert to dictionaries for faster lookup
    actors = {}
    for _, row in actors_df.iterrows():
        actor_id = str(row['id'])
        actors[actor_id] = {
            'id': actor_id,
            'name': row['name'],
            'popularity': row['popularity'],
            'profile_path': row['profile_path'],
            'movie_credits': [],
            'tv_credits': []
        }
    
    for _, row in movie_credits_df.iterrows():
        actor_id = str(row['actor_id'])
        if actor_id in actors:
            actors[actor_id]['movie_credits'].append({
                'id': str(row['id']),
                'title': row['title'],
                'poster_path': row['poster_path'],
                'popularity': row['popularity']
            })
    
    for _, row in tv_credits_df.iterrows():
        actor_id = str(row['actor_id'])
        if actor_id in actors:
            actors[actor_id]['tv_credits'].append({
                'id': str(row['id']),
                'title': row['title'],
                'poster_path': row['poster_path'],
                'popularity': row['popularity'],
                'character': row.get('character')
            })
    
    print(f"Loaded {len(actors)} actors for {region}")
    return actors

def build_actor_graph(actors, include_tv=True):
    """Build a NetworkX graph of actor connections through movies/TV shows,
    filtering TV credits that likely don't represent scripted acting roles."""
    print("Building actor connection graph...")
    G = nx.Graph()
    
    # Add all actors as nodes.
    for actor_id, actor in actors.items():
        G.add_node(actor_id, 
                   type='actor',
                   name=actor['name'],
                   popularity=actor['popularity'],
                   profile_path=actor.get('profile_path'))
    
    # Create a dictionary mapping each credit ID to the list of actor IDs involved.
    credit_to_actors = {}
    
    # Process movie credits with similar filtering as TV
    for actor_id, actor in tqdm(actors.items(), desc="Processing movie credits"):
        for credit in actor.get('movie_credits', []):
            character = (credit.get('character') or "").strip().lower()
            # Skip if the actor is simply playing himself/herself
            if character in ['self', 'himself', 'herself']:
                continue
            # Skip documentaries and similar non-fiction formats
            movie_title = credit.get('title', '').lower()
            if any(keyword in movie_title for keyword in ['documentary', 'behind the scenes']):
                continue
            movie_id = credit['id']
            credit_to_actors.setdefault(movie_id, []).append(actor_id)
    
    # Process TV credits with filtering heuristics.
    if include_tv:
        excluded_keywords = ['talk', 'game', 'reality', 'news', 'award']
        for actor_id, actor in tqdm(actors.items(), desc="Processing TV credits"):
            for credit in actor.get('tv_credits', []):
                tv_title = credit.get('title', '').lower()
                character = (credit.get('character') or "").strip().lower()
                # Skip if the actor is simply playing himself/herself.
                if character in ['self', 'himself', 'herself']:
                    continue
                # Skip if the TV title contains keywords suggesting a non-scripted format.
                if any(keyword in tv_title for keyword in excluded_keywords):
                    continue
                show_id = credit['id']
                credit_to_actors.setdefault(show_id, []).append(actor_id)
    
    # Now create edges between any two actors who share a credit.
    edge_count = 0
    for credit_id, actor_list in tqdm(credit_to_actors.items(), desc="Creating actor connections"):
        if len(actor_list) < 2:
            continue
        for i in range(len(actor_list)):
            for j in range(i+1, len(actor_list)):
                actor1, actor2 = actor_list[i], actor_list[j]
                if G.has_edge(actor1, actor2):
                    G[actor1][actor2]['weight'] += 1
                    G[actor1][actor2]['credits'].append(credit_id)
                else:
                    G.add_edge(actor1, actor2, weight=1, credits=[credit_id])
                    edge_count += 1
    
    print(f"Built graph with {G.number_of_nodes()} actors and {edge_count} connections")
    return G

def find_paths_by_difficulty(G, actors, difficulty_config):
    """Find paths between actors for different difficulty levels"""
    print("Finding optimal paths by difficulty...")
    paths_by_difficulty = {
        'easy': [],
        'normal': [],
        'hard': []
    }
    
    # Determine actor popularity for potential start points.
    actor_popularity = [(actor_id, actors[actor_id]['popularity']) 
                        for actor_id in G.nodes() if actor_id in actors]
    actor_popularity.sort(key=lambda x: x[1], reverse=True)
    
    top_actors = [a[0] for a in actor_popularity[:int(len(actor_popularity) * 0.1)]]
    top_actors = top_actors[:100]  # Limit to 100 to reduce computation.
    
    print(f"Using {len(top_actors)} top actors as potential start points")
    processed_pairs = set()
    
    for difficulty, config in difficulty_config.items():
        print(f"Processing {difficulty} difficulty...")
        min_connections = config['min_connections']
        max_connections = config['max_connections']
        target_count = config['count']
        
        for start_actor_id in tqdm(top_actors, desc=f"Finding {difficulty} paths"):
            if difficulty == 'easy':
                target_pool = [a[0] for a in actor_popularity[:int(len(actor_popularity)*0.3)]]
            elif difficulty == 'normal':
                target_pool = [a[0] for a in actor_popularity[int(len(actor_popularity)*0.15):int(len(actor_popularity)*0.6)]]
            else:  # hard
                target_pool = [a[0] for a in actor_popularity[int(len(actor_popularity)*0.4):]]
            
            target_pool = [a for a in target_pool if a != start_actor_id]
            sampled_targets = np.random.choice(target_pool, min(20, len(target_pool)), replace=False)
            
            for target_actor_id in sampled_targets:
                pair_key = tuple(sorted([start_actor_id, target_actor_id]))
                if pair_key in processed_pairs:
                    continue
                processed_pairs.add(pair_key)
                try:
                    shortest_path = nx.shortest_path(G, start_actor_id, target_actor_id)
                    connection_length = len(shortest_path) - 1
                    if min_connections <= connection_length <= max_connections:
                        full_path = []
                        for i in range(len(shortest_path) - 1):
                            actor1 = shortest_path[i]
                            actor2 = shortest_path[i+1]
                            if i == 0:
                                full_path.append({
                                    'type': 'actor',
                                    'id': actor1,
                                    'name': actors[actor1]['name'],
                                    'profile_path': actors[actor1]['profile_path']
                                })
                            # Look for a credit (movie or TV) connecting these two actors.
                            credit_id = G[actor1][actor2]['credits'][0]
                            movie_data = None
                            for credit in actors[actor1]['movie_credits']:
                                if credit['id'] == credit_id:
                                    movie_data = credit
                                    break
                            if not movie_data:
                                for credit in actors[actor1]['tv_credits']:
                                    if credit['id'] == credit_id:
                                        movie_data = credit
                                        break
                            full_path.append({
                                'type': 'movie',
                                'id': credit_id,
                                'title': movie_data['title'] if movie_data else 'Unknown',
                                'poster_path': movie_data['poster_path'] if movie_data else None
                            })
                            full_path.append({
                                'type': 'actor',
                                'id': actor2,
                                'name': actors[actor2]['name'],
                                'profile_path': actors[actor2]['profile_path']
                            })
                        paths_by_difficulty[difficulty].append({
                            'start_id': start_actor_id,
                            'target_id': target_actor_id,
                            'connection_length': connection_length,
                            'path': full_path
                        })
                        if len(paths_by_difficulty[difficulty]) >= target_count:
                            break
                except nx.NetworkXNoPath:
                    continue
            if len(paths_by_difficulty[difficulty]) >= target_count:
                break
    
    for difficulty, paths in paths_by_difficulty.items():
        print(f"Found {len(paths)} paths for {difficulty} difficulty")
    
    return paths_by_difficulty

def compress_path(path):
    """Compress path data to save space"""
    minimal_path = []
    for item in path:
        compressed = {
            't': 'a' if item['type'] == 'actor' else 'm',
            'i': item['id']
        }
        if item['type'] == 'actor':
            compressed['n'] = item['name']
            if item['profile_path']:
                compressed['p'] = item['profile_path']
        else:
            compressed['n'] = item['title']
            if item['poster_path']:
                compressed['p'] = item['poster_path']
        minimal_path.append(compressed)
    json_str = json.dumps(minimal_path)
    return gzip.compress(json_str.encode('utf-8'))

def create_connection_database(paths_by_difficulty):
    """Create SQLite database with actor connections"""
    print("Creating connection database...")
    
    if not os.path.exists(os.path.dirname(CONNECTION_DB_PATH)):
        alt_path = os.path.join(os.getcwd(), "actor-game", "public", "actor_connections.db")
        print(f"Primary path {CONNECTION_DB_PATH} not found, using {alt_path} instead")
        connection_path = alt_path
    else:
        connection_path = CONNECTION_DB_PATH
        
    if os.path.exists(connection_path):
        os.remove(connection_path)
    
    os.makedirs(os.path.dirname(connection_path), exist_ok=True)
    
    conn = sqlite3.connect(connection_path)
    cursor = conn.cursor()
    
    cursor.execute('''
    CREATE TABLE actor_connections (
        start_id TEXT NOT NULL,
        target_id TEXT NOT NULL,
        connection_length INTEGER NOT NULL,
        optimal_path BLOB NOT NULL,
        difficulty TEXT NOT NULL,
        PRIMARY KEY (start_id, target_id)
    )
    ''')
    
    cursor.execute('CREATE INDEX idx_difficulty ON actor_connections(difficulty)')
    cursor.execute('CREATE INDEX idx_connection_length ON actor_connections(connection_length)')
    
    for difficulty, paths in paths_by_difficulty.items():
        for path_data in tqdm(paths, desc=f"Inserting {difficulty} paths"):
            compressed_path = compress_path(path_data['path'])
            cursor.execute(
                'INSERT INTO actor_connections VALUES (?, ?, ?, ?, ?)',
                (
                    path_data['start_id'],
                    path_data['target_id'],
                    path_data['connection_length'],
                    compressed_path,
                    difficulty
                )
            )
    conn.commit()
    conn.close()
    
    print(f"Connection database created at {connection_path}")

def main():
    start_time = time.time()
    
    all_actors = {}
    for region in REGIONS_TO_PROCESS:
        region_actors = load_actor_data(region)
        if region_actors:
            all_actors.update(region_actors)
    
    graph = build_actor_graph(all_actors)
    paths = find_paths_by_difficulty(graph, all_actors, DIFFICULTY_CONFIG)
    create_connection_database(paths)
    
    elapsed_time = time.time() - start_time
    print(f"Process completed in {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    main()
