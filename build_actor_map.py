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
CONNECTION_DB_PATH = 'public/actor_connections.db'
REGIONS_TO_PROCESS = ['GLOBAL', 'US', 'UK']  # Prioritize these regions for connections

# Difficulty settings
DIFFICULTY_CONFIG = {
    'easy': {'min_connections': 1, 'max_connections': 3, 'count': 1000},
    'normal': {'min_connections': 3, 'max_connections': 5, 'count': 1000},
    'hard': {'min_connections': 5, 'max_connections': 8, 'count': 1000}
}

def load_actor_data(region):
    """Load actor data from SQLite database for a specific region"""
    print(f"Loading actor data for {region}...")
    
    # Look in multiple possible locations
    possible_paths = [
        f"public/actors_{region}.db",
        f"actor-game/public/actors_{region}.db",
        f"./actors_{region}.db"
    ]
    
    db_path = None
    for path in possible_paths:
        if os.path.exists(path):
            db_path = path
            print(f"Found database at {path}")
            break
    
    if not db_path:
        print(f"Database for {region} not found in any location")
        return None
    
    conn = sqlite3.connect(db_path)
    
    # Load actors
    actors_df = pd.read_sql("SELECT id, name, popularity, profile_path FROM actors", conn)
    
    # Load movie credits
    movie_credits_df = pd.read_sql(
        "SELECT actor_id, id, title, poster_path, popularity FROM movie_credits", 
        conn
    )
    
    # Load TV credits if needed
    tv_credits_df = pd.read_sql(
        "SELECT actor_id, id, name as title, poster_path, popularity FROM tv_credits", 
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
    
    # Add movie credits to actors
    for _, row in movie_credits_df.iterrows():
        actor_id = str(row['actor_id'])
        if actor_id in actors:
            actors[actor_id]['movie_credits'].append({
                'id': str(row['id']),
                'title': row['title'],
                'poster_path': row['poster_path'],
                'popularity': row['popularity']
            })
    
    # Add TV credits to actors
    for _, row in tv_credits_df.iterrows():
        actor_id = str(row['actor_id'])
        if actor_id in actors:
            actors[actor_id]['tv_credits'].append({
                'id': str(row['id']),
                'title': row['title'],
                'poster_path': row['poster_path'],
                'popularity': row['popularity']
            })
    
    print(f"Loaded {len(actors)} actors for {region}")
    return actors

def build_actor_graph(actors, include_tv=True):
    """Build a NetworkX graph of actor connections through movies/TV shows"""
    print("Building actor connection graph...")
    G = nx.Graph()
    
    # Add all actors as nodes
    for actor_id, actor in actors.items():
        if actor['profile_path']:  # Only include actors with images
            G.add_node(actor_id, 
                       type='actor',
                       name=actor['name'],
                       popularity=actor['popularity'],
                       profile_path=actor['profile_path'])
    
    # Create connections through movies
    movie_to_actors = {}
    
    # Process movie credits
    for actor_id, actor in tqdm(actors.items(), desc="Processing movie credits"):
        if actor_id not in G: continue
        
        for credit in actor['movie_credits']:
            movie_id = credit['id']
            if movie_id not in movie_to_actors:
                movie_to_actors[movie_id] = []
            movie_to_actors[movie_id].append(actor_id)
    
    # Process TV credits if included
    if include_tv:
        for actor_id, actor in tqdm(actors.items(), desc="Processing TV credits"):
            if actor_id not in G: continue
            
            for credit in actor['tv_credits']:
                show_id = credit['id']
                if show_id not in movie_to_actors:
                    movie_to_actors[show_id] = []
                movie_to_actors[show_id].append(actor_id)
    
    # Add edges between actors who worked together
    edge_count = 0
    for movie_id, actor_list in tqdm(movie_to_actors.items(), desc="Creating actor connections"):
        for i in range(len(actor_list)):
            for j in range(i+1, len(actor_list)):
                actor1, actor2 = actor_list[i], actor_list[j]
                if G.has_edge(actor1, actor2):
                    # Edge already exists, just increment weight
                    G[actor1][actor2]['weight'] += 1
                    G[actor1][actor2]['movies'].append(movie_id)
                else:
                    # Create new edge
                    G.add_edge(actor1, actor2, weight=1, movies=[movie_id])
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
    
    # Get top actors by popularity for start points
    actor_popularity = [(actor_id, actors[actor_id]['popularity']) 
                        for actor_id in G.nodes() 
                        if actor_id in actors]
    actor_popularity.sort(key=lambda x: x[1], reverse=True)
    
    # Top 10% for start actors
    top_actors = [a[0] for a in actor_popularity[:int(len(actor_popularity) * 0.1)]]
    top_actors = top_actors[:100]  # Limit to at most 100 to prevent excessive computation
    
    print(f"Using {len(top_actors)} top actors as potential start points")
    
    # Track connections we've already processed
    processed_pairs = set()
    
    # For each difficulty level
    for difficulty, config in difficulty_config.items():
        print(f"Processing {difficulty} difficulty...")
        min_connections = config['min_connections']
        max_connections = config['max_connections']
        target_count = config['count']
        
        # For each start actor
        for start_actor_id in tqdm(top_actors, desc=f"Finding {difficulty} paths"):
            # Determine target actor pool based on difficulty
            if difficulty == 'easy':
                # Top 30% for easy mode
                target_pool = [a[0] for a in actor_popularity[:int(len(actor_popularity) * 0.3)]]
            elif difficulty == 'normal':
                # Middle 45% for normal mode
                target_pool = [a[0] for a in actor_popularity[int(len(actor_popularity) * 0.15):int(len(actor_popularity) * 0.6)]]
            else:  # hard
                # Bottom 60% for hard mode
                target_pool = [a[0] for a in actor_popularity[int(len(actor_popularity) * 0.4):]]
            
            # Exclude start actor from targets
            target_pool = [a for a in target_pool if a != start_actor_id]
            
            # Sample a subset of targets to try
            sampled_targets = np.random.choice(target_pool, 
                                              min(20, len(target_pool)), 
                                              replace=False)
            
            for target_actor_id in sampled_targets:
                # Skip if we've already processed this pair
                pair_key = tuple(sorted([start_actor_id, target_actor_id]))
                if pair_key in processed_pairs:
                    continue
                
                processed_pairs.add(pair_key)
                
                try:
                    # Find shortest path
                    shortest_path = nx.shortest_path(G, start_actor_id, target_actor_id)
                    connection_length = len(shortest_path) - 1  # Number of edges (connections)
                    
                    # Check if path matches our difficulty criteria
                    if min_connections <= connection_length <= max_connections:
                        # Convert path to full format with movie connections
                        full_path = []
                        
                        for i in range(len(shortest_path) - 1):
                            actor1 = shortest_path[i]
                            actor2 = shortest_path[i + 1]
                            
                            # Add first actor
                            if i == 0:
                                full_path.append({
                                    'type': 'actor',
                                    'id': actor1,
                                    'name': actors[actor1]['name'],
                                    'profile_path': actors[actor1]['profile_path']
                                })
                            
                            # Find a movie they were in together
                            movie_id = G[actor1][actor2]['movies'][0]  # Take first movie
                            
                            # Find movie details from either actor's credits
                            movie_data = None
                            for credit in actors[actor1]['movie_credits']:
                                if credit['id'] == movie_id:
                                    movie_data = credit
                                    break
                            
                            if not movie_data:
                                for credit in actors[actor1]['tv_credits']:
                                    if credit['id'] == movie_id:
                                        movie_data = credit
                                        break
                            
                            # Add movie to path
                            full_path.append({
                                'type': 'movie',
                                'id': movie_id,
                                'title': movie_data['title'] if movie_data else 'Unknown',
                                'poster_path': movie_data['poster_path'] if movie_data else None
                            })
                            
                            # Add second actor
                            full_path.append({
                                'type': 'actor',
                                'id': actor2,
                                'name': actors[actor2]['name'],
                                'profile_path': actors[actor2]['profile_path']
                            })
                        
                        # Store this path
                        paths_by_difficulty[difficulty].append({
                            'start_id': start_actor_id,
                            'target_id': target_actor_id,
                            'connection_length': connection_length,
                            'path': full_path
                        })
                        
                        # Break if we have enough paths for this difficulty
                        if len(paths_by_difficulty[difficulty]) >= target_count:
                            break
                
                except nx.NetworkXNoPath:
                    continue  # No path exists
            
            # Break if we have enough paths for this difficulty
            if len(paths_by_difficulty[difficulty]) >= target_count:
                break
    
    # Print stats
    for difficulty, paths in paths_by_difficulty.items():
        print(f"Found {len(paths)} paths for {difficulty} difficulty")
    
    return paths_by_difficulty

def compress_path(path):
    """Compress path data to save space"""
    # Convert to minimal representation
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
    
    # Serialize to JSON and compress
    json_str = json.dumps(minimal_path)
    return gzip.compress(json_str.encode('utf-8'))

def create_connection_database(paths_by_difficulty):
    """Create SQLite database with actor connections"""
    print("Creating connection database...")
    
    # Remove existing database if it exists
    if os.path.exists(CONNECTION_DB_PATH):
        os.remove(CONNECTION_DB_PATH)
    
    # Ensure directory exists
    os.makedirs(os.path.dirname(CONNECTION_DB_PATH), exist_ok=True)
    
    # Create database
    conn = sqlite3.connect(CONNECTION_DB_PATH)
    cursor = conn.cursor()
    
    # Create table
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
    
    # Create indices
    cursor.execute('CREATE INDEX idx_difficulty ON actor_connections(difficulty)')
    cursor.execute('CREATE INDEX idx_connection_length ON actor_connections(connection_length)')
    
    # Insert data
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
    
    # Commit changes and close
    conn.commit()
    conn.close()
    
    print(f"Connection database created at {CONNECTION_DB_PATH}")

def main():
    start_time = time.time()
    
    # Load actor data for main regions
    all_actors = {}
    for region in REGIONS_TO_PROCESS:
        region_actors = load_actor_data(region)
        if region_actors:
            all_actors.update(region_actors)
    
    # Build actor graph
    graph = build_actor_graph(all_actors)
    
    # Find paths by difficulty
    paths = find_paths_by_difficulty(graph, all_actors, DIFFICULTY_CONFIG)
    
    # Create connection database
    create_connection_database(paths)
    
    elapsed_time = time.time() - start_time
    print(f"Process completed in {elapsed_time:.2f} seconds")

if __name__ == "__main__":
    main()