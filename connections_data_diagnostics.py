#!/usr/bin/env python
import os
import sqlite3
import networkx as nx
import json
import numpy as np
from tqdm import tqdm
import pandas as pd
import gzip
import time
import tkinter as tk
from tkinter import ttk, messagebox, filedialog
import threading
from collections import deque
from PIL import Image, ImageTk
import io
import requests
import webbrowser
from matplotlib.figure import Figure
from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg

############### PART 1: BUILD/UPDATE CONNECTION DATABASE ###############

# Constants for build part
REGIONS = ['GLOBAL', 'US', 'UK', 'CA', 'AU', 'FR', 'DE', 'IN', 'KR', 'JP', 'CN']
CONNECTION_DB_PATH = 'actor-game/public/actor_connections.db'  # Updated path
REGIONS_TO_PROCESS = ['GLOBAL', 'US', 'UK']
DIFFICULTY_CONFIG = {
    'easy': {'min_connections': 1, 'max_connections': 3, 'count': 1000},
    'normal': {'min_connections': 3, 'max_connections': 5, 'count': 1000},
    'hard': {'min_connections': 5, 'max_connections': 8, 'count': 1000}
}

def load_actor_data(region):
    """Load actor data from a single SQLite database filtered by region."""
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
            return {}
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='actor_regions'")
    has_region_table = cursor.fetchone() is not None
    if has_region_table:
        print("Using consolidated database with region flags")
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
        print("Using legacy database format")
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
    tv_credits_df = pd.read_sql(
        f"SELECT actor_id, id, name as title, poster_path, popularity, character FROM tv_credits WHERE actor_id IN ({actor_ids_str})", 
        conn
    )
    conn.close()
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
    """
    Build a NetworkX graph of actor connections through movies/TV shows.
    TV credits are filtered to exclude non-scripted roles using heuristics.
    """
    print("Building actor connection graph...")
    G = nx.Graph()
    for actor_id, actor in actors.items():
        # Add all actors as nodes (regardless of profile_path)
        G.add_node(actor_id, 
                   type='actor',
                   name=actor['name'],
                   popularity=actor['popularity'],
                   profile_path=actor.get('profile_path'))
    credit_to_actors = {}
    # Process movie credits (all movies are considered proper acting projects)
    for actor_id, actor in tqdm(actors.items(), desc="Processing movie credits"):
        for credit in actor.get('movie_credits', []):
            movie_id = credit['id']
            credit_to_actors.setdefault(movie_id, []).append(actor_id)
    # Process TV credits with filtering heuristics.
    if include_tv:
        excluded_keywords = ['talk', 'game', 'reality', 'news', 'award']
        for actor_id, actor in tqdm(actors.items(), desc="Processing TV credits"):
            for credit in actor.get('tv_credits', []):
                tv_title = credit.get('title', '').lower()
                character = (credit.get('character') or "").strip().lower()
                if character in ['self', 'himself', 'herself']:
                    continue
                if any(keyword in tv_title for keyword in excluded_keywords):
                    continue
                show_id = credit['id']
                credit_to_actors.setdefault(show_id, []).append(actor_id)
    # Create edges between actors sharing the same credit.
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
    """Find paths between actors that satisfy the difficulty criteria."""
    print("Finding optimal paths by difficulty...")
    paths_by_difficulty = {'easy': [], 'normal': [], 'hard': []}
    actor_popularity = [(actor_id, actors[actor_id]['popularity']) 
                        for actor_id in G.nodes() if actor_id in actors]
    actor_popularity.sort(key=lambda x: x[1], reverse=True)
    top_actors = [a[0] for a in actor_popularity[:int(len(actor_popularity)*0.1)]]
    top_actors = top_actors[:100]
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
            else:
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
                        for i in range(len(shortest_path)-1):
                            actor1 = shortest_path[i]
                            actor2 = shortest_path[i+1]
                            if i == 0:
                                full_path.append({
                                    'type': 'actor',
                                    'id': actor1,
                                    'name': actors[actor1]['name'],
                                    'profile_path': actors[actor1]['profile_path']
                                })
                            credit_id = G[actor1][actor2]['credits'][0]
                            credit_data = None
                            for credit in actors[actor1]['movie_credits']:
                                if credit['id'] == credit_id:
                                    credit_data = credit
                                    break
                            if not credit_data:
                                for credit in actors[actor1]['tv_credits']:
                                    if credit['id'] == credit_id:
                                        credit_data = credit
                                        break
                            full_path.append({
                                'type': 'movie',
                                'id': credit_id,
                                'title': credit_data['title'] if credit_data else 'Unknown',
                                'poster_path': credit_data['poster_path'] if credit_data else None
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
    for diff, paths in paths_by_difficulty.items():
        print(f"Found {len(paths)} paths for {diff} difficulty")
    return paths_by_difficulty

def compress_path(path):
    """Compress the given path data for storage."""
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

def create_connection_database(paths_by_difficulty, all_actors):
    """Create the connection database with actor records and connection paths."""
    print("Creating connection database...")
    if not os.path.exists(os.path.dirname(CONNECTION_DB_PATH)):
        alt_path = os.path.join(os.getcwd(), "actor-game", "public", "actor_connections.db")
        print(f"Primary path not found, using {alt_path}")
        connection_path = alt_path
    else:
        connection_path = CONNECTION_DB_PATH
    if os.path.exists(connection_path):
        os.remove(connection_path)
    os.makedirs(os.path.dirname(connection_path), exist_ok=True)
    conn = sqlite3.connect(connection_path)
    cursor = conn.cursor()
    # Create actors table
    cursor.execute('''
    CREATE TABLE actors (
        id TEXT PRIMARY KEY,
        name TEXT NOT NULL,
        profile_path TEXT,
        popularity REAL,
        place_of_birth TEXT
    )
    ''')
    for actor_id, actor in all_actors.items():
        cursor.execute('''
        INSERT OR REPLACE INTO actors (id, name, profile_path, popularity, place_of_birth)
        VALUES (?, ?, ?, ?, ?)
        ''', (
            actor_id,
            actor['name'],
            actor.get('profile_path'),
            actor.get('popularity'),
            actor.get('place_of_birth', '')
        ))
    # Create actor_connections table
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

def update_connection_database():
    """Build the graph and update the connection database."""
    start_time = time.time()
    all_actors = {}
    for region in REGIONS_TO_PROCESS:
        region_actors = load_actor_data(region)
        if region_actors:
            all_actors.update(region_actors)
    if not all_actors:
        print("No actor data loaded. Aborting update.")
        return
    graph = build_actor_graph(all_actors)
    paths = find_paths_by_difficulty(graph, all_actors, DIFFICULTY_CONFIG)
    create_connection_database(paths, all_actors)
    elapsed_time = time.time() - start_time
    print(f"Database update completed in {elapsed_time:.2f} seconds")

############### PART 2: GUI DIAGNOSTICS ###############
# The following class creates a Tkinter GUI to diagnose the connection database.
class ActorToActorApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Actor Connections Database Analyzer")
        self.root.geometry("1000x700")
        self.image_cache = {}
        self.current_actor_id = None
        self.create_menu()
        self.notebook = ttk.Notebook(root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        self.path_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.path_tab, text="Path Finder")
        self.setup_path_finder(self.path_tab)
        self.actor_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.actor_tab, text="Actor Explorer")
        self.setup_actor_explorer(self.actor_tab)
        self.stats_tab = ttk.Frame(self.notebook)
        self.notebook.add(self.stats_tab, text="Database Stats")
        self.setup_stats_tab(self.stats_tab)
        self.status_var = tk.StringVar()
        self.status_var.set("Ready")
        status_bar = ttk.Label(root, textvariable=self.status_var, relief=tk.SUNKEN, anchor=tk.W)
        status_bar.pack(side=tk.BOTTOM, fill=tk.X)
        self.graph = nx.Graph()
        self.load_database()
    
    def create_menu(self):
        menubar = tk.Menu(self.root)
        file_menu = tk.Menu(menubar, tearoff=0)
        file_menu.add_command(label="Open Database...", command=self.open_database)
        file_menu.add_command(label="Refresh Database", command=self.refresh_database)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.root.quit)
        menubar.add_cascade(label="File", menu=file_menu)
        tools_menu = tk.Menu(menubar, tearoff=0)
        tools_menu.add_command(label="Database Statistics", command=lambda: self.notebook.select(self.stats_tab))
        tools_menu.add_command(label="Export Current Path", command=self.export_path)
        menubar.add_cascade(label="Tools", menu=tools_menu)
        help_menu = tk.Menu(menubar, tearoff=0)
        help_menu.add_command(label="About", command=self.show_about)
        help_menu.add_command(label="How to Use", command=self.show_help)
        menubar.add_cascade(label="Help", menu=help_menu)
        self.root.config(menu=menubar)
    
    def setup_path_finder(self, parent):
        main_frame = ttk.Frame(parent)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        input_frame = ttk.LabelFrame(main_frame, text="Find Path Between Actors", padding="10")
        input_frame.pack(fill=tk.X, pady=10)
        ttk.Label(input_frame, text="Start Actor:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        self.start_actor_entry = ttk.Entry(input_frame, width=30)
        self.start_actor_entry.grid(row=0, column=1, padx=5, pady=5, sticky=tk.W)
        ttk.Button(input_frame, text="Search", command=lambda: self.search_actors_for("start")).grid(
            row=0, column=2, padx=5, pady=5)
        ttk.Label(input_frame, text="Target Actor:").grid(row=1, column=0, padx=5, pady=5, sticky=tk.W)
        self.target_actor_entry = ttk.Entry(input_frame, width=30)
        self.target_actor_entry.grid(row=1, column=1, padx=5, pady=5, sticky=tk.W)
        ttk.Button(input_frame, text="Search", command=lambda: self.search_actors_for("target")).grid(
            row=1, column=2, padx=5, pady=5)
        self.search_button = ttk.Button(input_frame, text="Find Path", command=self.find_path)
        self.search_button.grid(row=1, column=3, padx=5, pady=5)
        options_frame = ttk.LabelFrame(main_frame, text="Search Options", padding="10")
        options_frame.pack(fill=tk.X, pady=10)
        self.include_tv = tk.BooleanVar(value=True)
        ttk.Checkbutton(options_frame, text="Include TV Shows", variable=self.include_tv).grid(
            row=0, column=0, padx=5, pady=5, sticky=tk.W)
        self.exclude_mcu = tk.BooleanVar(value=False)
        ttk.Checkbutton(options_frame, text="Exclude MCU Movies", variable=self.exclude_mcu).grid(
            row=0, column=1, padx=5, pady=5, sticky=tk.W)
        ttk.Label(options_frame, text="Max Search Depth:").grid(row=0, column=2, padx=5, pady=5, sticky=tk.W)
        self.max_depth = tk.IntVar(value=6)
        depth_values = list(range(2, 11))
        depth_selector = ttk.Combobox(options_frame, textvariable=self.max_depth, values=depth_values, width=3)
        depth_selector.grid(row=0, column=3, padx=5, pady=5, sticky=tk.W)
        results_frame = ttk.LabelFrame(main_frame, text="Path Results", padding="10")
        results_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        results_paned = ttk.PanedWindow(results_frame, orient=tk.VERTICAL)
        results_paned.pack(fill=tk.BOTH, expand=True)
        text_frame = ttk.Frame(results_paned)
        results_paned.add(text_frame, weight=1)
        self.results_text = tk.Text(text_frame, wrap=tk.WORD, height=5)
        self.results_text.pack(fill=tk.BOTH, expand=True, side=tk.LEFT)
        text_scrollbar = ttk.Scrollbar(text_frame, orient=tk.VERTICAL, command=self.results_text.yview)
        text_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.results_text.configure(yscrollcommand=text_scrollbar.set)
        visual_frame = ttk.Frame(results_paned)
        results_paned.add(visual_frame, weight=3)
        canvas_frame = ttk.Frame(visual_frame)
        canvas_frame.pack(fill=tk.BOTH, expand=True)
        self.path_canvas = tk.Canvas(canvas_frame, bg="#f0f0f0")
        self.path_canvas.pack(fill=tk.BOTH, expand=True, side=tk.LEFT)
        canvas_scrollbar = ttk.Scrollbar(canvas_frame, orient=tk.HORIZONTAL, command=self.path_canvas.xview)
        canvas_scrollbar.pack(side=tk.BOTTOM, fill=tk.X)
        self.path_canvas.configure(xscrollcommand=canvas_scrollbar.set)
        self.path_frame = ttk.Frame(self.path_canvas, padding="10")
        self.path_canvas.create_window((0, 0), window=self.path_frame, anchor=tk.NW)
        self.path_frame.bind("<Configure>", self._on_frame_configure)
    
    def setup_actor_explorer(self, parent):
        main_frame = ttk.Frame(parent)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        search_frame = ttk.Frame(main_frame)
        search_frame.pack(fill=tk.X, pady=10)
        ttk.Label(search_frame, text="Actor Name:").pack(side=tk.LEFT, padx=5)
        self.actor_search_entry = ttk.Entry(search_frame, width=30)
        self.actor_search_entry.pack(side=tk.LEFT, padx=5)
        self.actor_search_entry.bind("<Return>", lambda e: self.search_actor())
        ttk.Button(search_frame, text="Search", command=self.search_actor).pack(side=tk.LEFT, padx=5)
        paned = ttk.PanedWindow(main_frame, orient=tk.HORIZONTAL)
        paned.pack(fill=tk.BOTH, expand=True)
        actor_list_frame = ttk.LabelFrame(paned, text="Search Results")
        paned.add(actor_list_frame, weight=1)
        actor_tree_frame = ttk.Frame(actor_list_frame)
        actor_tree_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        columns = ("id", "name", "popularity")
        self.actor_tree = ttk.Treeview(actor_tree_frame, columns=columns, show="headings", height=20)
        self.actor_tree.heading("id", text="ID")
        self.actor_tree.heading("name", text="Name")
        self.actor_tree.heading("popularity", text="Popularity")
        self.actor_tree.column("id", width=50, stretch=False)
        self.actor_tree.column("name", width=150)
        self.actor_tree.column("popularity", width=70, stretch=False)
        self.actor_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        actor_tree_scrollbar = ttk.Scrollbar(actor_tree_frame, orient=tk.VERTICAL, command=self.actor_tree.yview)
        actor_tree_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.actor_tree.configure(yscrollcommand=actor_tree_scrollbar.set)
        self.actor_tree.bind("<<TreeviewSelect>>", self.show_actor_details)
        details_frame = ttk.LabelFrame(paned, text="Actor Details")
        paned.add(details_frame, weight=2)
        self.details_notebook = ttk.Notebook(details_frame)
        self.details_notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        self.info_frame = ttk.Frame(self.details_notebook)
        self.details_notebook.add(self.info_frame, text="Info")
        self.profile_frame = ttk.Frame(self.info_frame)
        self.profile_frame.pack(fill=tk.X, pady=10)
        self.actor_image_label = ttk.Label(self.profile_frame)
        self.actor_image_label.pack(side=tk.LEFT, padx=10)
        self.actor_info_frame = ttk.Frame(self.profile_frame)
        self.actor_info_frame.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=10)
        self.actor_name_label = ttk.Label(self.actor_info_frame, text="", font=("TkDefaultFont", 14, "bold"))
        self.actor_name_label.pack(anchor=tk.W, pady=5)
        self.actor_id_label = ttk.Label(self.actor_info_frame, text="")
        self.actor_id_label.pack(anchor=tk.W)
        self.actor_popularity_label = ttk.Label(self.actor_info_frame, text="")
        self.actor_popularity_label.pack(anchor=tk.W)
        self.actor_birth_label = ttk.Label(self.actor_info_frame, text="")
        self.actor_birth_label.pack(anchor=tk.W)
        self.movie_frame = ttk.Frame(self.details_notebook)
        self.details_notebook.add(self.movie_frame, text="Movies")
        movie_tree_frame = ttk.Frame(self.movie_frame)
        movie_tree_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        movie_columns = ("id", "title", "character", "year", "popularity")
        self.movie_tree = ttk.Treeview(movie_tree_frame, columns=movie_columns, show="headings", height=20)
        self.movie_tree.heading("id", text="ID")
        self.movie_tree.heading("title", text="Title")
        self.movie_tree.heading("character", text="Character")
        self.movie_tree.heading("year", text="Year")
        self.movie_tree.heading("popularity", text="Popularity")
        self.movie_tree.column("id", width=50, stretch=False)
        self.movie_tree.column("title", width=200)
        self.movie_tree.column("character", width=150)
        self.movie_tree.column("year", width=50, stretch=False)
        self.movie_tree.column("popularity", width=70, stretch=False)
        self.movie_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        movie_tree_scrollbar = ttk.Scrollbar(movie_tree_frame, orient=tk.VERTICAL, command=self.movie_tree.yview)
        movie_tree_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.movie_tree.configure(yscrollcommand=movie_tree_scrollbar.set)
        self.tv_frame = ttk.Frame(self.details_notebook)
        self.details_notebook.add(self.tv_frame, text="TV Shows")
        tv_tree_frame = ttk.Frame(self.tv_frame)
        tv_tree_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        tv_columns = ("id", "name", "character", "year", "popularity")
        self.tv_tree = ttk.Treeview(tv_tree_frame, columns=tv_columns, show="headings", height=20)
        self.tv_tree.heading("id", text="ID")
        self.tv_tree.heading("name", text="Name")
        self.tv_tree.heading("character", text="Character")
        self.tv_tree.heading("year", text="Year")
        self.tv_tree.heading("popularity", text="Popularity")
        self.tv_tree.column("id", width=50, stretch=False)
        self.tv_tree.column("name", width=200)
        self.tv_tree.column("character", width=150)
        self.tv_tree.column("year", width=50, stretch=False)
        self.tv_tree.column("popularity", width=70, stretch=False)
        self.tv_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        tv_tree_scrollbar = ttk.Scrollbar(tv_tree_frame, orient=tk.VERTICAL, command=self.tv_tree.yview)
        tv_tree_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.tv_tree.configure(yscrollcommand=tv_tree_scrollbar.set)
        self.costars_frame = ttk.Frame(self.details_notebook)
        self.details_notebook.add(self.costars_frame, text="Co-stars")
        costars_tree_frame = ttk.Frame(self.costars_frame)
        costars_tree_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        costars_columns = ("id", "name", "shared_projects", "popularity")
        self.costars_tree = ttk.Treeview(costars_tree_frame, columns=costars_columns, show="headings", height=20)
        self.costars_tree.heading("id", text="ID")
        self.costars_tree.heading("name", text="Name")
        self.costars_tree.heading("shared_projects", text="Shared Projects")
        self.costars_tree.heading("popularity", text="Popularity")
        self.costars_tree.column("id", width=50, stretch=False)
        self.costars_tree.column("name", width=200)
        self.costars_tree.column("shared_projects", width=100, stretch=False)
        self.costars_tree.column("popularity", width=70, stretch=False)
        self.costars_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        costars_tree_scrollbar = ttk.Scrollbar(costars_tree_frame, orient=tk.VERTICAL, command=self.costars_tree.yview)
        costars_tree_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.costars_tree.configure(yscrollcommand=costars_tree_scrollbar.set)
        button_frame = ttk.Frame(details_frame)
        button_frame.pack(fill=tk.X, padx=5, pady=5)
        ttk.Button(button_frame, text="Set as Start Actor", command=lambda: self.set_path_actor("start")).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Set as Target Actor", command=lambda: self.set_path_actor("target")).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Find Paths to Other Actors", command=self.find_paths_to_selected).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Open TMDB Page", command=self.open_tmdb_page).pack(side=tk.RIGHT, padx=5)
    
    def setup_stats_tab(self, parent):
        main_frame = ttk.Frame(parent)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        info_frame = ttk.LabelFrame(main_frame, text="Database Information", padding="10")
        info_frame.pack(fill=tk.X, pady=10)
        self.db_path_var = tk.StringVar(value="Not loaded")
        ttk.Label(info_frame, text="Database Path:").grid(row=0, column=0, sticky=tk.W, padx=5, pady=2)
        ttk.Label(info_frame, textvariable=self.db_path_var).grid(row=0, column=1, sticky=tk.W, padx=5, pady=2)
        self.db_size_var = tk.StringVar(value="")
        ttk.Label(info_frame, text="Database Size:").grid(row=1, column=0, sticky=tk.W, padx=5, pady=2)
        ttk.Label(info_frame, textvariable=self.db_size_var).grid(row=1, column=1, sticky=tk.W, padx=5, pady=2)
        self.actor_count_var = tk.StringVar(value="")
        ttk.Label(info_frame, text="Actors:").grid(row=2, column=0, sticky=tk.W, padx=5, pady=2)
        ttk.Label(info_frame, textvariable=self.actor_count_var).grid(row=2, column=1, sticky=tk.W, padx=5, pady=2)
        self.movie_count_var = tk.StringVar(value="")
        ttk.Label(info_frame, text="Movies:").grid(row=3, column=0, sticky=tk.W, padx=5, pady=2)
        ttk.Label(info_frame, textvariable=self.movie_count_var).grid(row=3, column=1, sticky=tk.W, padx=5, pady=2)
        self.tv_count_var = tk.StringVar(value="")
        ttk.Label(info_frame, text="TV Shows:").grid(row=4, column=0, sticky=tk.W, padx=5, pady=2)
        ttk.Label(info_frame, textvariable=self.tv_count_var).grid(row=4, column=1, sticky=tk.W, padx=5, pady=2)
        top_frame = ttk.LabelFrame(main_frame, text="Popular Actors", padding="10")
        top_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        top_tree_frame = ttk.Frame(top_frame)
        top_tree_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        columns = ("rank", "id", "name", "popularity", "projects")
        self.top_actors_tree = ttk.Treeview(top_tree_frame, columns=columns, show="headings", height=20)
        self.top_actors_tree.heading("rank", text="#")
        self.top_actors_tree.heading("id", text="ID")
        self.top_actors_tree.heading("name", text="Name")
        self.top_actors_tree.heading("popularity", text="Popularity")
        self.top_actors_tree.heading("projects", text="Projects")
        self.top_actors_tree.column("rank", width=40, stretch=False)
        self.top_actors_tree.column("id", width=60, stretch=False)
        self.top_actors_tree.column("name", width=200)
        self.top_actors_tree.column("popularity", width=80, stretch=False)
        self.top_actors_tree.column("projects", width=80, stretch=False)
        self.top_actors_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        top_tree_scrollbar = ttk.Scrollbar(top_tree_frame, orient=tk.VERTICAL, command=self.top_actors_tree.yview)
        top_tree_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.top_actors_tree.configure(yscrollcommand=top_tree_scrollbar.set)
        ttk.Button(main_frame, text="Refresh Statistics", command=self.update_stats).pack(pady=10)
        viz_frame = ttk.LabelFrame(main_frame, text="Visualizations", padding="10")
        viz_frame.pack(fill=tk.X, pady=10)
        ttk.Button(viz_frame, text="Show Popularity Distribution", 
                  command=lambda: self.show_visualization("popularity")).pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Button(viz_frame, text="Show Projects Per Actor", 
                  command=lambda: self.show_visualization("projects")).pack(side=tk.LEFT, padx=5, pady=5)
        ttk.Button(viz_frame, text="Show MCU vs. Non-MCU Movies", 
                  command=lambda: self.show_visualization("mcu")).pack(side=tk.LEFT, padx=5, pady=5)
    
    def load_database(self, db_path=None):
        if not db_path:
            db_path = CONNECTION_DB_PATH
        if not os.path.exists(db_path):
            self.status_var.set(f"Database not found at {db_path}")
            return False
        try:
            self.status_var.set(f"Loading database from {db_path}...")
            self.root.update_idletasks()
            self.db_path = db_path
            self.db_path_var.set(db_path)
            size_bytes = os.path.getsize(db_path)
            size_mb = size_bytes / (1024 * 1024)
            self.db_size_var.set(f"{size_mb:.2f} MB")
            self.build_graph_from_database()
            self.update_stats()
            return True
        except Exception as e:
            self.status_var.set(f"Error loading database: {str(e)}")
            messagebox.showerror("Database Error", f"Could not load database: {str(e)}")
            return False
    
    def open_database(self):
        filename = filedialog.askopenfilename(
            title="Select Actor Database",
            filetypes=[("SQLite Database", "*.db"), ("All Files", "*.*")]
        )
        if filename:
            self.load_database(filename)
    
    def refresh_database(self):
        if hasattr(self, 'db_path'):
            self.load_database(self.db_path)
        else:
            self.load_database()
    
    def build_graph_from_database(self):
        self.graph.clear()
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            self.status_var.set("Loading actors from database...")
            self.root.update_idletasks()
            cursor.execute("SELECT id, name, profile_path, popularity, place_of_birth FROM actors")
            for actor_id, name, profile_path, popularity, place_of_birth in cursor.fetchall():
                self.graph.add_node(actor_id, 
                                    type="actor", 
                                    name=name, 
                                    profile_path=profile_path,
                                    popularity=popularity,
                                    place_of_birth=place_of_birth)
            self.status_var.set("Loading movies from database...")
            self.root.update_idletasks()
            cursor.execute("""
                SELECT m.id, m.title, m.actor_id, m.poster_path, m.release_date, m.popularity, m.character, m.is_mcu
                FROM movie_credits m
            """)
            for movie_id, title, actor_id, poster_path, release_date, popularity, character, is_mcu in cursor.fetchall():
                movie_node = f"movie-{movie_id}"
                if not self.graph.has_node(movie_node):
                    self.graph.add_node(movie_node, type="movie", title=title, poster_path=poster_path,
                                        release_date=release_date, popularity=popularity, is_mcu=bool(is_mcu))
                self.graph.add_edge(actor_id, movie_node, character=character)
            self.status_var.set("Loading TV shows from database...")
            self.root.update_idletasks()
            cursor.execute("""
                SELECT t.id, t.name, t.actor_id, t.poster_path, t.first_air_date, t.popularity, t.character, t.is_mcu
                FROM tv_credits t
            """)
            for tv_id, name, actor_id, poster_path, first_air_date, popularity, character, is_mcu in cursor.fetchall():
                tv_node = f"tv-{tv_id}"
                if not self.graph.has_node(tv_node):
                    self.graph.add_node(tv_node, type="tv", name=name, poster_path=poster_path,
                                        first_air_date=first_air_date, popularity=popularity, is_mcu=bool(is_mcu))
                self.graph.add_edge(actor_id, tv_node, character=character)
            conn.close()
            actor_count = len([n for n, d in self.graph.nodes(data=True) if d.get('type') == 'actor'])
            movie_count = len([n for n, d in self.graph.nodes(data=True) if d.get('type') == 'movie'])
            tv_count = len([n for n, d in self.graph.nodes(data=True) if d.get('type') == 'tv'])
            self.status_var.set(f"Loaded {actor_count} actors, {movie_count} movies, {tv_count} TV shows")
        except Exception as e:
            self.status_var.set(f"Error loading database: {str(e)}")
            messagebox.showerror("Database Error", f"Could not load database: {str(e)}")
    
    def update_stats(self):
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM actors")
            actor_count = cursor.fetchone()[0]
            self.actor_count_var.set(f"{actor_count:,}")
            cursor.execute("SELECT COUNT(DISTINCT id) FROM movie_credits")
            movie_count = cursor.fetchone()[0]
            self.movie_count_var.set(f"{movie_count:,}")
            cursor.execute("SELECT COUNT(DISTINCT id) FROM tv_credits")
            tv_count = cursor.fetchone()[0]
            self.tv_count_var.set(f"{tv_count:,}")
            cursor.execute("""
                SELECT a.id, a.name, a.popularity, 
                      (SELECT COUNT(*) FROM movie_credits m WHERE m.actor_id = a.id) +
                      (SELECT COUNT(*) FROM tv_credits t WHERE t.actor_id = a.id) as project_count
                FROM actors a
                ORDER BY a.popularity DESC
                LIMIT 100
            """)
            for item in self.top_actors_tree.get_children():
                self.top_actors_tree.delete(item)
            for i, (actor_id, name, popularity, project_count) in enumerate(cursor.fetchall(), 1):
                self.top_actors_tree.insert("", tk.END, values=(i, actor_id, name, f"{popularity:.1f}", project_count))
            conn.close()
        except Exception as e:
            self.status_var.set(f"Error updating statistics: {str(e)}")
            messagebox.showerror("Statistics Error", f"Could not update statistics: {str(e)}")
    
    def show_visualization(self, viz_type):
        try:
            viz_window = tk.Toplevel(self.root)
            viz_window.title(f"Visualization - {viz_type.title()}")
            viz_window.geometry("800x600")
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            fig = Figure(figsize=(10,6), dpi=100)
            ax = fig.add_subplot(111)
            if viz_type == "popularity":
                cursor.execute("SELECT popularity FROM actors WHERE popularity > 0 ORDER BY popularity DESC LIMIT 1000")
                popularity = [row[0] for row in cursor.fetchall()]
                ax.hist(popularity, bins=50, alpha=0.75, color='blue')
                ax.set_title('Actor Popularity Distribution')
                ax.set_xlabel('Popularity Score')
                ax.set_ylabel('Number of Actors')
            elif viz_type == "projects":
                cursor.execute("""
                    SELECT 
                      (SELECT COUNT(*) FROM movie_credits m WHERE m.actor_id = a.id) as movie_count,
                      (SELECT COUNT(*) FROM tv_credits t WHERE t.actor_id = a.id) as tv_count,
                      COUNT(*) as actor_count
                    FROM actors a
                    GROUP BY 
                      CASE 
                        WHEN (SELECT COUNT(*) FROM movie_credits m WHERE m.actor_id = a.id) + 
                             (SELECT COUNT(*) FROM tv_credits t WHERE t.actor_id = a.id) > 20 THEN 20
                        ELSE (SELECT COUNT(*) FROM movie_credits m WHERE m.actor_id = a.id) + 
                             (SELECT COUNT(*) FROM tv_credits t WHERE t.actor_id = a.id)
                      END
                    ORDER BY 
                      (SELECT COUNT(*) FROM movie_credits m WHERE m.actor_id = a.id) + 
                      (SELECT COUNT(*) FROM tv_credits t WHERE t.actor_id = a.id)
                """)
                data = cursor.fetchall()
                project_counts = {}
                for movie_count, tv_count, actor_count in data:
                    total = movie_count + tv_count
                    if total > 20:
                        total = "20+"
                    project_counts.setdefault(total, 0)
                    project_counts[total] += actor_count
                keys = sorted(project_counts.keys(), key=lambda x: 21 if x=="20+" else int(x))
                keys = [str(k) for k in keys]
                values = [project_counts[k] for k in keys]
                ax.bar(keys, values, color='green')
                ax.set_title('Projects Per Actor Distribution')
                ax.set_xlabel('Number of Projects')
                ax.set_ylabel('Number of Actors')
                ax.set_yscale('log')
            elif viz_type == "mcu":
                cursor.execute("""
                    SELECT is_mcu, COUNT(*) FROM movie_credits
                    GROUP BY is_mcu
                """)
                data = cursor.fetchall()
                mcu_count, non_mcu_count = 0, 0
                for is_mcu, count in data:
                    if is_mcu:
                        mcu_count = count
                    else:
                        non_mcu_count = count
                labels = ['MCU Movies', 'Other Movies']
                sizes = [mcu_count, non_mcu_count]
                ax.pie(sizes, labels=labels, autopct='%1.1f%%', startangle=90, colors=['red', 'blue'])
                ax.set_title('MCU vs. Non-MCU Movies')
                ax.axis('equal')
            conn.close()
            canvas = FigureCanvasTkAgg(fig, master=viz_window)
            canvas.draw()
            canvas.get_tk_widget().pack(fill=tk.BOTH, expand=True)
            ttk.Button(viz_window, text="Close", command=viz_window.destroy).pack(pady=10)
        except Exception as e:
            self.status_var.set(f"Error creating visualization: {str(e)}")
            messagebox.showerror("Visualization Error", f"Could not create visualization: {str(e)}")
    
    def search_actors_for(self, target_type):
        self.target_type = target_type
        self.search_actors()
    
    def search_actors(self):
        search_dialog = tk.Toplevel(self.root)
        search_dialog.title("Search Actors")
        search_dialog.geometry("600x500")
        search_dialog.transient(self.root)
        search_dialog.grab_set()
        search_frame = ttk.Frame(search_dialog, padding="10")
        search_frame.pack(fill=tk.X)
        ttk.Label(search_frame, text="Actor Name:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        search_entry = ttk.Entry(search_frame, width=40)
        search_entry.grid(row=0, column=1, padx=5, pady=5, sticky=tk.W)
        search_entry.focus_set()
        target_var = tk.StringVar(value=self.target_type if hasattr(self, 'target_type') else "start")
        ttk.Radiobutton(search_frame, text="Set as Start Actor", variable=target_var, value="start").grid(
            row=1, column=0, columnspan=2, sticky=tk.W, padx=5)
        ttk.Radiobutton(search_frame, text="Set as Target Actor", variable=target_var, value="target").grid(
            row=2, column=0, columnspan=2, sticky=tk.W, padx=5)
        results_frame = ttk.LabelFrame(search_dialog, text="Search Results", padding="10")
        results_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        columns = ("id", "name", "popularity", "movies", "tv_shows")
        results_tree = ttk.Treeview(results_frame, columns=columns, show="headings", height=15)
        results_tree.heading("id", text="ID")
        results_tree.heading("name", text="Name")
        results_tree.heading("popularity", text="Popularity")
        results_tree.heading("movies", text="Movies")
        results_tree.heading("tv_shows", text="TV Shows")
        results_tree.column("id", width=50, stretch=False)
        results_tree.column("name", width=250)
        results_tree.column("popularity", width=80, stretch=False)
        results_tree.column("movies", width=60, stretch=False)
        results_tree.column("tv_shows", width=60, stretch=False)
        results_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        scrollbar = ttk.Scrollbar(results_frame, orient=tk.VERTICAL, command=results_tree.yview)
        scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        results_tree.configure(yscrollcommand=scrollbar.set)
        detail_frame = ttk.LabelFrame(search_dialog, text="Actor Details", padding="10")
        detail_frame.pack(fill=tk.X, padx=10, pady=10)
        image_label = ttk.Label(detail_frame)
        image_label.pack(side=tk.LEFT, padx=10)
        info_frame = ttk.Frame(detail_frame)
        info_frame.pack(side=tk.LEFT, fill=tk.X, expand=True, padx=10)
        actor_name_label = ttk.Label(info_frame, text="", font=("TkDefaultFont", 12, "bold"))
        actor_name_label.pack(anchor=tk.W, pady=5)
        actor_detail_label = ttk.Label(info_frame, text="")
        actor_detail_label.pack(anchor=tk.W)
        status_var = tk.StringVar()
        status_var.set("Enter an actor name to search")
        status_label = ttk.Label(search_dialog, textvariable=status_var, anchor=tk.W)
        status_label.pack(fill=tk.X, padx=10, pady=5)
        button_frame = ttk.Frame(search_dialog, padding="10")
        button_frame.pack(fill=tk.X)
        def show_actor_details(event=None):
            selected_items = results_tree.selection()
            if not selected_items:
                return
            actor_id = results_tree.item(selected_items[0], "values")[0]
            try:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT name, profile_path, popularity, place_of_birth
                    FROM actors
                    WHERE id = ?
                """, (actor_id,))
                actor = cursor.fetchone()
                if actor:
                    name, profile_path, popularity, place_of_birth = actor
                    actor_name_label.config(text=name)
                    details = []
                    if popularity:
                        details.append(f"Popularity: {popularity:.1f}")
                    if place_of_birth:
                        details.append(f"Born: {place_of_birth}")
                    actor_detail_label.config(text="\n".join(details))
                    if profile_path:
                        try:
                            image_url = f"https://image.tmdb.org/t/p/w185{profile_path}"
                            response = requests.get(image_url)
                            if response.status_code == 200:
                                image_data = Image.open(io.BytesIO(response.content))
                                image_data = image_data.resize((100, 150), Image.LANCZOS)
                                photo = ImageTk.PhotoImage(image_data)
                                image_label.config(image=photo)
                                image_label.image = photo
                            else:
                                image_label.config(image=None)
                        except Exception as e:
                            image_label.config(image=None)
                    else:
                        image_label.config(image=None)
                conn.close()
            except Exception as e:
                status_var.set(f"Error loading actor details: {str(e)}")
        results_tree.bind("<<TreeviewSelect>>", show_actor_details)
        def perform_search():
            query = search_entry.get().strip()
            if not query:
                status_var.set("Please enter a search term")
                return
            status_var.set(f"Searching for '{query}'...")
            search_dialog.update_idletasks()
            for item in results_tree.get_children():
                results_tree.delete(item)
            try:
                conn = sqlite3.connect(self.db_path)
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT a.id, a.name, a.popularity,
                           (SELECT COUNT(*) FROM movie_credits m WHERE m.actor_id = a.id) AS movie_count,
                           (SELECT COUNT(*) FROM tv_credits t WHERE t.actor_id = a.id) AS tv_count
                    FROM actors a
                    WHERE a.name LIKE ?
                    ORDER BY a.popularity DESC
                    LIMIT 100
                """, (f'%{query}%',))
                results = cursor.fetchall()
                conn.close()
                if not results:
                    status_var.set(f"No actors found matching '{query}'")
                    return
                for actor_id, name, popularity, movie_count, tv_count in results:
                    results_tree.insert("", tk.END, values=(actor_id, name, f"{popularity:.1f}", movie_count, tv_count))
                status_var.set(f"Found {len(results)} actors matching '{query}'")
            except Exception as e:
                status_var.set(f"Error searching database: {str(e)}")
        def select_actor():
            selected_items = results_tree.selection()
            if not selected_items:
                status_var.set("Please select an actor")
                return
            actor_id = results_tree.item(selected_items[0], "values")[0]
            actor_name = results_tree.item(selected_items[0], "values")[1]
            if target_var.get() == "start":
                self.start_actor_entry.delete(0, tk.END)
                self.start_actor_entry.insert(0, actor_name)
                self.start_actor_entry.actor_id = actor_id
            else:
                self.target_actor_entry.delete(0, tk.END)
                self.target_actor_entry.insert(0, actor_name)
                self.target_actor_entry.actor_id = actor_id
            self.notebook.select(self.path_tab)
            search_dialog.destroy()
        search_button = ttk.Button(button_frame, text="Search", command=perform_search)
        search_button.pack(side=tk.LEFT, padx=5)
        select_button = ttk.Button(button_frame, text="Select", command=select_actor)
        select_button.pack(side=tk.LEFT, padx=5)
        cancel_button = ttk.Button(button_frame, text="Cancel", command=search_dialog.destroy)
        cancel_button.pack(side=tk.RIGHT, padx=5)
        view_credits_button = ttk.Button(button_frame, text="View Full Details", 
                                        command=lambda: self.view_actor_from_search(results_tree))
        view_credits_button.pack(side=tk.RIGHT, padx=5)
        search_entry.bind("<Return>", lambda event: perform_search())
        results_tree.bind("<Double-1>", lambda event: select_actor())
        if hasattr(self, 'target_type') and self.target_type in ('start', 'target'):
            entry = self.start_actor_entry if self.target_type == 'start' else self.target_actor_entry
            if entry.get():
                search_entry.insert(0, entry.get())
                perform_search()
    
    def view_actor_from_search(self, tree):
        selected_items = tree.selection()
        if not selected_items:
            return
        actor_id = tree.item(selected_items[0], "values")[0]
        self.notebook.select(self.actor_tab)
        self.load_actor_by_id(actor_id)
    
    def search_actor(self):
        query = self.actor_search_entry.get().strip()
        if not query:
            messagebox.showinfo("Search Error", "Please enter an actor name")
            return
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT id, name, popularity,
                       (SELECT COUNT(*) FROM movie_credits m WHERE m.actor_id = a.id) AS movie_count,
                       (SELECT COUNT(*) FROM tv_credits t WHERE t.actor_id = a.id) AS tv_count
                FROM actors a
                WHERE name LIKE ?
                ORDER BY popularity DESC
                LIMIT 100
            """, (f'%{query}%',))
            results = cursor.fetchall()
            conn.close()
            for item in self.actor_tree.get_children():
                self.actor_tree.delete(item)
            if not results:
                self.status_var.set(f"No actors found matching '{query}'")
                return
            for actor_id, name, popularity, movie_count, tv_count in results:
                self.actor_tree.insert("", tk.END, values=(actor_id, name, f"{popularity:.1f}"))
            self.status_var.set(f"Found {len(results)} actors matching '{query}'")
        except Exception as e:
            self.status_var.set(f"Error searching database: {str(e)}")
            messagebox.showerror("Search Error", f"Could not search: {str(e)}")
    
    def show_actor_details(self, event=None):
        selected_items = self.actor_tree.selection()
        if not selected_items:
            return
        actor_id = self.actor_tree.item(selected_items[0], "values")[0]
        self.load_actor_by_id(actor_id)
    
    def load_actor_by_id(self, actor_id):
        try:
            self.current_actor_id = actor_id
            conn = sqlite3.connect(self.db_path)
            cursor = conn.cursor()
            cursor.execute("""
                SELECT name, profile_path, popularity, place_of_birth
                FROM actors
                WHERE id = ?
            """, (actor_id,))
            actor = cursor.fetchone()
            if not actor:
                self.status_var.set(f"Actor with ID {actor_id} not found")
                conn.close()
                return
            name, profile_path, popularity, place_of_birth = actor
            self.actor_name_label.config(text=name)
            self.actor_id_label.config(text=f"ID: {actor_id}")
            self.actor_popularity_label.config(text=f"Popularity: {popularity:.1f}" if popularity else "Popularity: unknown")
            self.actor_birth_label.config(text=f"Born: {place_of_birth}" if place_of_birth else "")
            if profile_path:
                try:
                    image_url = f"https://image.tmdb.org/t/p/w185{profile_path}"
                    response = requests.get(image_url)
                    if response.status_code == 200:
                        image_data = Image.open(io.BytesIO(response.content))
                        image_data = image_data.resize((150,225), Image.LANCZOS)
                        photo = ImageTk.PhotoImage(image_data)
                        self.actor_image_label.config(image=photo)
                        self.image_cache[actor_id] = photo
                    else:
                        self.actor_image_label.config(image=None)
                except Exception as e:
                    self.actor_image_label.config(image=None)
            else:
                self.actor_image_label.config(image=None)
            self.status_var.set(f"Loading movie credits for {name}...")
            self.root.update_idletasks()
            for item in self.movie_tree.get_children():
                self.movie_tree.delete(item)
            cursor.execute("""
                SELECT id, title, character, release_date, popularity, is_mcu
                FROM movie_credits
                WHERE actor_id = ?
                ORDER BY popularity DESC
            """, (actor_id,))
            for movie_id, title, character, release_date, popularity, is_mcu in cursor.fetchall():
                year = release_date.split('-')[0] if release_date else ""
                mcu_text = "[MCU] " if is_mcu else ""
                self.movie_tree.insert("", tk.END, values=(movie_id, f"{mcu_text}{title}", character or "", year, f"{popularity:.1f}" if popularity else ""))
            movie_count = len(self.movie_tree.get_children())
            self.status_var.set(f"Loading TV credits for {name}...")
            self.root.update_idletasks()
            for item in self.tv_tree.get_children():
                self.tv_tree.delete(item)
            cursor.execute("""
                SELECT id, name, character, first_air_date, popularity, is_mcu
                FROM tv_credits
                WHERE actor_id = ?
                ORDER BY popularity DESC
            """, (actor_id,))
            for tv_id, tv_name, character, first_air_date, popularity, is_mcu in cursor.fetchall():
                year = first_air_date.split('-')[0] if first_air_date else ""
                mcu_text = "[MCU] " if is_mcu else ""
                self.tv_tree.insert("", tk.END, values=(tv_id, f"{mcu_text}{tv_name}", character or "", year, f"{popularity:.1f}" if popularity else ""))
            tv_count = len(self.tv_tree.get_children())
            self.status_var.set(f"Loading co-stars for {name}...")
            self.root.update_idletasks()
            for item in self.costars_tree.get_children():
                self.costars_tree.delete(item)
            cursor.execute("""
                WITH actor_media AS (
                    SELECT m.id as media_id, 'movie' as media_type
                    FROM movie_credits m WHERE m.actor_id = ?
                    UNION ALL
                    SELECT t.id as media_id, 'tv' as media_type
                    FROM tv_credits t WHERE t.actor_id = ?
                ),
                costars AS (
                    SELECT a.id, a.name, a.popularity, COUNT(*) as shared_count
                    FROM actors a
                    JOIN movie_credits m ON a.id = m.actor_id
                    JOIN actor_media am ON m.id = am.media_id AND am.media_type = 'movie'
                    WHERE a.id != ?
                    GROUP BY a.id, a.name
                    UNION ALL
                    SELECT a.id, a.name, a.popularity, COUNT(*) as shared_count
                    FROM actors a
                    JOIN tv_credits t ON a.id = t.actor_id
                    JOIN actor_media am ON t.id = am.media_id AND am.media_type = 'tv'
                    WHERE a.id != ?
                    GROUP BY a.id, a.name
                )
                SELECT id, name, SUM(shared_count) as total_shared, MAX(popularity) as popularity
                FROM costars
                GROUP BY id, name
                ORDER BY total_shared DESC, popularity DESC
                LIMIT 100
            """, (actor_id, actor_id, actor_id, actor_id))
            for costar_id, costar_name, shared_count, popularity in cursor.fetchall():
                self.costars_tree.insert("", tk.END, values=(costar_id, costar_name, shared_count, f"{popularity:.1f}" if popularity else ""))
            costar_count = len(self.costars_tree.get_children())
            self.status_var.set(f"Actor: {name} | {movie_count} movies | {tv_count} TV shows | {costar_count} co-stars")
            conn.close()
            self.details_notebook.tab(1, text=f"Movies ({movie_count})")
            self.details_notebook.tab(2, text=f"TV Shows ({tv_count})")
            self.details_notebook.tab(3, text=f"Co-stars ({costar_count})")
        except Exception as e:
            self.status_var.set(f"Error loading actor details: {str(e)}")
    
    def set_path_actor(self, actor_type):
        if not self.current_actor_id:
            return
        actor_name = self.actor_name_label.cget("text")
        if actor_type == "start":
            self.start_actor_entry.delete(0, tk.END)
            self.start_actor_entry.insert(0, actor_name)
            self.start_actor_entry.actor_id = self.current_actor_id
        else:
            self.target_actor_entry.delete(0, tk.END)
            self.target_actor_entry.insert(0, actor_name)
            self.target_actor_entry.actor_id = self.current_actor_id
        self.notebook.select(self.path_tab)
    
    def find_path(self):
        start_actor = self.start_actor_entry.get().strip()
        target_actor = self.target_actor_entry.get().strip()
        if not start_actor or not target_actor:
            messagebox.showwarning("Input Error", "Please enter both starting and target actor names")
            return
        self.status_var.set("Searching for path...")
        self.search_button.config(state=tk.DISABLED)
        self.results_text.delete(1.0, tk.END)
        for widget in self.path_frame.winfo_children():
            widget.destroy()
        threading.Thread(target=self.perform_search, args=(start_actor, target_actor), daemon=True).start()
    
    def perform_search(self, start_actor, target_actor):
        try:
            self.build_graph_from_database()
            if start_actor not in self.graph or target_actor not in self.graph:
                self.root.after(0, lambda: self.show_error("Actor not found", "One or both actors are not in the database."))
                return
            path = self.find_shortest_path(start_actor, target_actor)
            if path:
                self.root.after(0, lambda: self.display_path(path))
            else:
                self.root.after(0, lambda: self.show_error("No Path Found", "No connection found between these actors."))
        except Exception as e:
            self.root.after(0, lambda: self.show_error("Error", f"An error occurred: {str(e)}"))
        finally:
            self.root.after(0, lambda: self.search_button.config(state=tk.NORMAL))
            self.root.after(0, lambda: self.status_var.set("Ready"))
    
    def find_shortest_path(self, start_actor, target_actor):
        queue = deque([(start_actor, [start_actor])])
        visited = {start_actor}
        while queue:
            current, path = queue.popleft()
            for neighbor in self.graph.neighbors(current):
                if self.graph.nodes[neighbor].get('type') == 'movie':
                    for actor in self.graph.neighbors(neighbor):
                        if actor != current and self.graph.nodes[actor].get('type') == 'actor':
                            if actor == target_actor:
                                return path + [neighbor, actor]
                            if actor not in visited:
                                visited.add(actor)
                                queue.append((actor, path + [neighbor, actor]))
        return None
    
    def display_path(self, path):
        self.results_text.delete(1.0, tk.END)
        path_str = ""
        for i, node in enumerate(path):
            if i % 2 == 0:
                path_str += f"Actor: {node}"
            else:
                path_str += f" → Film: {node} → "
        self.results_text.insert(tk.END, path_str)
        for i, node in enumerate(path):
            frame = ttk.Frame(self.path_frame)
            frame.pack(side=tk.LEFT, padx=10, pady=10)
            label = ttk.Label(frame, text=node, wraplength=100, justify=tk.CENTER)
            label.pack()
            if i < len(path) - 1:
                arrow = ttk.Label(self.path_frame, text="→")
                arrow.pack(side=tk.LEFT)
    
    def show_error(self, title, message):
        messagebox.showerror(title, message)
        self.search_button.config(state=tk.NORMAL)
        self.status_var.set("Ready")
    
    def find_paths_to_selected(self):
        if not self.current_actor_id:
            return
        actor_name = self.actor_name_label.cget("text")
        self.start_actor_entry.delete(0, tk.END)
        self.start_actor_entry.insert(0, actor_name)
        self.start_actor_entry.actor_id = self.current_actor_id
        self.target_actor_entry.delete(0, tk.END)
        self.target_actor_entry.actor_id = None
        self.notebook.select(self.path_tab)
    
    def open_tmdb_page(self):
        if not self.current_actor_id:
            return
        url = f"https://www.themoviedb.org/person/{self.current_actor_id}"
        webbrowser.open(url)
    
    def _on_frame_configure(self, event):
        self.path_canvas.configure(scrollregion=self.path_canvas.bbox("all"))
    
    def export_path(self):
        if not self.results_text.get(1.0, tk.END).strip():
            messagebox.showinfo("Export Error", "No path to export")
            return
        file_path = filedialog.asksaveasfilename(
            title="Save Path As",
            defaultextension=".txt",
            filetypes=[("Text Files", "*.txt"), ("All Files", "*.*")]
        )
        if not file_path:
            return
        try:
            with open(file_path, "w") as f:
                f.write(self.results_text.get(1.0, tk.END).strip())
            messagebox.showinfo("Export Successful", f"Path exported to {file_path}")
        except Exception as e:
            messagebox.showerror("Export Error", f"Could not export path: {str(e)}")
    
    def show_about(self):
        messagebox.showinfo("About", "Actor Connections Database Analyzer\nVersion 1.0")
    
    def show_help(self):
        messagebox.showinfo("How to Use", "Instructions on how to use the application...")
    
if __name__ == "__main__":
    # First update the connection database.
    print("Updating connection database...")
    update_connection_database()
    # Then launch the GUI.
    root = tk.Tk()
    app = ActorToActorApp(root)
    root.mainloop()
