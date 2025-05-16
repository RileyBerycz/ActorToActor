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
from datetime import datetime

# Constants
REGIONS = ['GLOBAL', 'US', 'UK', 'CA', 'AU', 'FR', 'DE', 'IN', 'KR', 'JP', 'CN']
CONNECTION_DB_PATH = 'actor-game/public/actor_connections.db'  # Updated path
REGIONS_TO_PROCESS = ['GLOBAL', 'US', 'UK', 'CA', 'AU', 'FR', 'DE', 'IN', 'KR', 'JP', 'CN']  # Prioritize these regions for connections

# Difficulty settings
DIFFICULTY_CONFIG = {
    'easy': {'min_connections': 1, 'max_connections': 2, 'count': 1000},
    'normal': {'min_connections': 3, 'max_connections': 4, 'count': 1000},
    'hard': {'min_connections': 4, 'max_connections': 20, 'count': 1000}  # 20 is arbitrary upper bound
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
    """Build NetworkX graph with stronger documentary/self-appearance filtering"""
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
        # Get current date to filter out future movies
        current_year = datetime.now().year
        current_month = datetime.now().month

        for credit in actor.get('movie_credits', []):
            # Skip future movies (those with release dates beyond current month of current year)
            if credit.get('release_date'):
                try:
                    release_year = int(credit.get('release_date', '0000')[:4])
                    release_month = int(credit.get('release_date', '00-00')[5:7]) if len(credit.get('release_date', '')) >= 7 else 0
                    
                    # Skip movies from the future
                    if release_year > current_year or (release_year == current_year and release_month > current_month):
                        continue
                except (ValueError, IndexError):
                    # If date parsing fails, proceed with the movie
                    pass
                    
            character = (credit.get('character') or "").strip().lower()
            # Stricter filtering for actors playing themselves
            if character in ['self', 'himself', 'herself'] or actor['name'].lower() in character.lower():
                continue
            # Skip documentaries and similar non-fiction formats
            movie_title = credit.get('title', '').lower()
            if any(keyword in movie_title for keyword in ['documentary', 'behind the scenes']):
                continue
            movie_id = credit['id']
            credit_to_actors.setdefault(movie_id, []).append((actor_id, character))
    
    # Process TV credits with much stronger filtering
    if include_tv:
        excluded_keywords = ['talk', 'game', 'reality', 'news', 'award', 'interview', 
                            'host', 'special', 'ceremony', 'documentary', 'behind', 'making of',
                            'tonight show', 'late night', 'late show', 'live with', 'the view',
                            'jimmy', 'conan', 'ellen', 'oprah', 'inside the actors studio']
        excluded_titles = ['basquiat', 'biography', 'portrait', 'story of']  # Known problematic titles
        
        for actor_id, actor in tqdm(actors.items(), desc="Processing TV credits"):
            for credit in actor.get('tv_credits', []):
                tv_title = credit.get('title', '').lower()
                character = (credit.get('character') or "").strip().lower()
                
                # Skip ANY title containing known documentary keywords
                if any(keyword in tv_title for keyword in excluded_keywords):
                    continue
                    
                # Skip known problematic titles
                if any(title in tv_title for title in excluded_titles):
                    continue
                
                # Much stricter self-appearance check
                if character in ['self', 'himself', 'herself', 'themselves'] or 'self' in character:
                    continue
                
                # Skip if character contains "interview" or similar words
                if any(word in character for word in ['interview', 'host', 'narrator', 'voice']):
                    continue
                
                show_id = credit['id']
                credit_to_actors.setdefault(show_id, []).append((actor_id, character))
    
    # Now create edges between any two actors who share a credit.
    edge_count = 0
    for credit_id, actor_list in tqdm(credit_to_actors.items(), desc="Creating actor connections"):
        if len(actor_list) < 2:
            continue
        for i in range(len(actor_list)):
            for j in range(i+1, len(actor_list)):
                actor1, char1 = actor_list[i]
                actor2, char2 = actor_list[j]
                
                # Skip if they're both playing themselves
                if "self" in char1 and "self" in char2:
                    continue
                    
                if G.has_edge(actor1, actor2):
                    G[actor1][actor2]['weight'] += 1
                    G[actor1][actor2]['credits'].append(credit_id)
                else:
                    G.add_edge(actor1, actor2, weight=1, credits=[credit_id])
                    edge_count += 1
    
    print(f"Built graph with {G.number_of_nodes()} actors and {edge_count} connections")
    return G

def find_paths_by_difficulty(G, actors, difficulty_config):
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

    # Define popularity threshold (e.g., top 20% for easy/normal, top 10% for hard)
    easy_normal_popular = [a[0] for a in actor_popularity[:max(1, int(len(actor_popularity) * 0.2))]]
    hard_popular = [a[0] for a in actor_popularity[:max(1, int(len(actor_popularity) * 0.1))]]

    processed_pairs = set()

    for difficulty, config in difficulty_config.items():
        print(f"Processing {difficulty} difficulty...")
        min_connections = config['min_connections']
        max_connections = config['max_connections']
        target_count = config['count']

        # Use only global actors for hard
        if difficulty == 'hard':
            start_pool = hard_popular
            target_pool = hard_popular
        else:
            start_pool = easy_normal_popular
            target_pool = easy_normal_popular

        for start_actor_id in tqdm(start_pool, desc=f"Finding {difficulty} paths"):
            sampled_targets = np.random.choice(
                [a for a in target_pool if a != start_actor_id], 
                min(20, len(target_pool)-1), 
                replace=False
            )
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

def create_connection_database(paths_by_difficulty, region="GLOBAL"):
    """Create/update SQLite database with actor connections"""
    print(f"Adding {region} connections to database...")
    
    # Always use the standard path and ensure the directory exists
    os.makedirs(os.path.dirname(CONNECTION_DB_PATH), exist_ok=True)
    connection_path = CONNECTION_DB_PATH
    
    # Check if database exists
    db_exists = os.path.exists(connection_path)
    
    conn = sqlite3.connect(connection_path)
    cursor = conn.cursor()
    
    # Create the tables if they don't exist
    if not db_exists:
        print(f"Creating new connection database at {connection_path}")
        cursor.execute('''
        CREATE TABLE actor_connections (
            start_id TEXT NOT NULL,
            target_id TEXT NOT NULL,
            connection_length INTEGER NOT NULL,
            optimal_path BLOB NOT NULL,
            difficulty TEXT NOT NULL,
            region TEXT NOT NULL,
            PRIMARY KEY (start_id, target_id, region)
        )
        ''')
        
        cursor.execute('CREATE INDEX idx_difficulty_region ON actor_connections(difficulty, region)')
        cursor.execute('CREATE INDEX idx_connection_length ON actor_connections(connection_length)')
    else:
        print(f"Appending to existing database at {connection_path}")
        # Remove previous entries for this region to allow regeneration
        cursor.execute('DELETE FROM actor_connections WHERE region = ?', (region,))
        print(f"Removed previous {region} connections")
    
    for difficulty, paths in paths_by_difficulty.items():
        for path_data in tqdm(paths, desc=f"Inserting {difficulty} paths for {region}"):
            compressed_path = compress_path(path_data['path'])
            cursor.execute(
                'INSERT INTO actor_connections VALUES (?, ?, ?, ?, ?, ?)',
                (
                    path_data['start_id'],
                    path_data['target_id'],
                    path_data['connection_length'],
                    compressed_path,
                    difficulty,
                    region
                )
            )
    conn.commit()
    conn.close()
    
    print(f"Connection database updated with {region} paths")

def main():
    # For each region, generate appropriate connections
    for region in REGIONS_TO_PROCESS:
        region_actors = load_actor_data(region)
        if region_actors:
            graph = build_actor_graph(region_actors)
            
            # Only generate hard paths for GLOBAL region
            if region == 'GLOBAL':
                paths = find_paths_by_difficulty(graph, region_actors, DIFFICULTY_CONFIG)
            else:
                # For non-GLOBAL regions, only generate easy and normal paths
                easy_normal_config = {k: v for k, v in DIFFICULTY_CONFIG.items() 
                                     if k in ['easy', 'normal']}
                paths = find_paths_by_difficulty(graph, region_actors, easy_normal_config)
                # Add empty list for hard to maintain structure
                paths['hard'] = []
                
            # Store with region information
            create_connection_database(paths, region)

if __name__ == "__main__":
    main()
