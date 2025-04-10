#!/usr/bin/env python
import os
import sqlite3
import networkx as nx
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

class ActorToActorApp:
    def __init__(self, root):
        self.root = root
        self.root.title("Actor Connections Database Analyzer")
        self.root.geometry("1000x700")
        self.image_cache = {}
        self.current_actor_id = None
        self.db_connections = {}
        self.table_schemas = {}
        self.graph = nx.Graph()
        
        # Setup UI components
        self._create_menu()
        self._create_notebook()
        self._create_status_bar()
        
        # Load database
        self.load_database()

    def _create_menu(self):
        menubar = tk.Menu(self.root)
        
        # File menu
        file_menu = tk.Menu(menubar, tearoff=0)
        file_menu.add_command(label="Open Database...", command=self.open_database)
        file_menu.add_command(label="Refresh Database", command=self.refresh_database)
        file_menu.add_separator()
        file_menu.add_command(label="Exit", command=self.root.quit)
        menubar.add_cascade(label="File", menu=file_menu)
        
        # Tools menu
        tools_menu = tk.Menu(menubar, tearoff=0)
        tools_menu.add_command(label="Database Statistics", 
                             command=lambda: self.notebook.select(self.stats_tab))
        tools_menu.add_command(label="Database Explorer", command=self.explore_database)
        tools_menu.add_command(label="Export Current Path", command=self.export_path)
        menubar.add_cascade(label="Tools", menu=tools_menu)
        
        # Help menu
        help_menu = tk.Menu(menubar, tearoff=0)
        help_menu.add_command(label="About", command=self.show_about)
        help_menu.add_command(label="How to Use", command=self.show_help)
        menubar.add_cascade(label="Help", menu=help_menu)
        
        self.root.config(menu=menubar)

    def _create_notebook(self):
        self.notebook = ttk.Notebook(self.root)
        self.notebook.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Create tabs
        self.path_tab = ttk.Frame(self.notebook)
        self.actor_tab = ttk.Frame(self.notebook)
        self.stats_tab = ttk.Frame(self.notebook)
        
        self.notebook.add(self.path_tab, text="Path Finder")
        self.notebook.add(self.actor_tab, text="Actor Explorer")
        self.notebook.add(self.stats_tab, text="Database Stats")
        
        # Setup tab contents
        self.setup_path_finder(self.path_tab)
        self.setup_actor_explorer(self.actor_tab)
        self.setup_stats_tab(self.stats_tab)

    def _create_status_bar(self):
        self.status_var = tk.StringVar()
        self.status_var.set("Ready")
        status_bar = ttk.Label(self.root, textvariable=self.status_var, 
                             relief=tk.SUNKEN, anchor=tk.W)
        status_bar.pack(side=tk.BOTTOM, fill=tk.X)

    def setup_path_finder(self, parent):
        main_frame = ttk.Frame(parent)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Input frame
        input_frame = ttk.LabelFrame(main_frame, text="Find Path Between Actors", padding="10")
        input_frame.pack(fill=tk.X, pady=10)
        
        # Start actor row
        ttk.Label(input_frame, text="Start Actor:").grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        self.start_actor_entry = ttk.Entry(input_frame, width=30)
        self.start_actor_entry.grid(row=0, column=1, padx=5, pady=5, sticky=tk.W)
        ttk.Button(input_frame, text="Search", 
                 command=lambda: self.search_actors_for("start")).grid(row=0, column=2, padx=5, pady=5)
        
        # Target actor row
        ttk.Label(input_frame, text="Target Actor:").grid(row=1, column=0, padx=5, pady=5, sticky=tk.W)
        self.target_actor_entry = ttk.Entry(input_frame, width=30)
        self.target_actor_entry.grid(row=1, column=1, padx=5, pady=5, sticky=tk.W)
        ttk.Button(input_frame, text="Search", 
                 command=lambda: self.search_actors_for("target")).grid(row=1, column=2, padx=5, pady=5)
        
        # Search button
        self.search_button = ttk.Button(input_frame, text="Find Path", command=self.find_path)
        self.search_button.grid(row=1, column=3, padx=5, pady=5)
        
        # Options frame
        options_frame = ttk.LabelFrame(main_frame, text="Search Options", padding="10")
        options_frame.pack(fill=tk.X, pady=10)
        
        # Option checkboxes
        self.include_tv = tk.BooleanVar(value=True)
        ttk.Checkbutton(options_frame, text="Include TV Shows", 
                      variable=self.include_tv).grid(row=0, column=0, padx=5, pady=5, sticky=tk.W)
        
        self.exclude_mcu = tk.BooleanVar(value=False)
        ttk.Checkbutton(options_frame, text="Exclude MCU Movies", 
                      variable=self.exclude_mcu).grid(row=0, column=1, padx=5, pady=5, sticky=tk.W)
        
        # Depth selector
        ttk.Label(options_frame, text="Max Search Depth:").grid(row=0, column=2, padx=5, pady=5, sticky=tk.W)
        self.max_depth = tk.IntVar(value=6)
        depth_values = list(range(2, 11))
        depth_selector = ttk.Combobox(options_frame, textvariable=self.max_depth, 
                                    values=depth_values, width=3)
        depth_selector.grid(row=0, column=3, padx=5, pady=5, sticky=tk.W)
        
        # Results area
        results_frame = ttk.LabelFrame(main_frame, text="Path Results", padding="10")
        results_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        # Paned window for results
        results_paned = ttk.PanedWindow(results_frame, orient=tk.VERTICAL)
        results_paned.pack(fill=tk.BOTH, expand=True)
        
        # Text area for path
        text_frame = ttk.Frame(results_paned)
        results_paned.add(text_frame, weight=1)
        
        self.results_text = tk.Text(text_frame, wrap=tk.WORD, height=5)
        self.results_text.pack(fill=tk.BOTH, expand=True, side=tk.LEFT)
        
        text_scrollbar = ttk.Scrollbar(text_frame, orient=tk.VERTICAL, command=self.results_text.yview)
        text_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.results_text.configure(yscrollcommand=text_scrollbar.set)
        
        # Visual representation
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
        # Implementation simplified but core functionality maintained
        main_frame = ttk.Frame(parent)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Search area
        search_frame = ttk.Frame(main_frame)
        search_frame.pack(fill=tk.X, pady=10)
        
        ttk.Label(search_frame, text="Actor Name:").pack(side=tk.LEFT, padx=5)
        self.actor_search_entry = ttk.Entry(search_frame, width=30)
        self.actor_search_entry.pack(side=tk.LEFT, padx=5)
        self.actor_search_entry.bind("<Return>", lambda e: self.search_actors())
        
        ttk.Button(search_frame, text="Search", command=self.search_actors).pack(side=tk.LEFT, padx=5)
        
        # Split view
        paned = ttk.PanedWindow(main_frame, orient=tk.HORIZONTAL)
        paned.pack(fill=tk.BOTH, expand=True)
        
        # Actor list
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
        
        # Details area
        details_frame = ttk.LabelFrame(paned, text="Actor Details")
        paned.add(details_frame, weight=2)
        
        self.details_notebook = ttk.Notebook(details_frame)
        self.details_notebook.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        # Info tab
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
        
        # Movies tab
        self.movies_frame = ttk.Frame(self.details_notebook)
        self.details_notebook.add(self.movies_frame, text="Movies")
        
        movie_tree_frame = ttk.Frame(self.movies_frame)
        movie_tree_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        columns = ("id", "title", "character", "year")
        self.movies_tree = ttk.Treeview(movie_tree_frame, columns=columns, show="headings", height=15)
        self.movies_tree.heading("id", text="ID")
        self.movies_tree.heading("title", text="Title")
        self.movies_tree.heading("character", text="Character")
        self.movies_tree.heading("year", text="Year")
        
        self.movies_tree.column("id", width=50, stretch=False)
        self.movies_tree.column("title", width=200)
        self.movies_tree.column("character", width=150)
        self.movies_tree.column("year", width=50, stretch=False)
        
        self.movies_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        movies_scrollbar = ttk.Scrollbar(movie_tree_frame, orient=tk.VERTICAL, command=self.movies_tree.yview)
        movies_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.movies_tree.configure(yscrollcommand=movies_scrollbar.set)
        
        # TV Shows tab
        self.tv_frame = ttk.Frame(self.details_notebook)
        self.details_notebook.add(self.tv_frame, text="TV Shows")
        
        tv_tree_frame = ttk.Frame(self.tv_frame)
        tv_tree_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        columns = ("id", "title", "character", "years")
        self.tv_tree = ttk.Treeview(tv_tree_frame, columns=columns, show="headings", height=15)
        self.tv_tree.heading("id", text="ID")
        self.tv_tree.heading("title", text="Title")
        self.tv_tree.heading("character", text="Character")
        self.tv_tree.heading("years", text="Years")
        
        self.tv_tree.column("id", width=50, stretch=False)
        self.tv_tree.column("title", width=200)
        self.tv_tree.column("character", width=150)
        self.tv_tree.column("years", width=80, stretch=False)
        
        self.tv_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        tv_scrollbar = ttk.Scrollbar(tv_tree_frame, orient=tk.VERTICAL, command=self.tv_tree.yview)
        tv_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.tv_tree.configure(yscrollcommand=tv_scrollbar.set)
        
        # Co-stars tab
        self.costars_frame = ttk.Frame(self.details_notebook)
        self.details_notebook.add(self.costars_frame, text="Co-stars")
        
        costar_tree_frame = ttk.Frame(self.costars_frame)
        costar_tree_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        columns = ("id", "name", "movies", "popularity")
        self.costars_tree = ttk.Treeview(costar_tree_frame, columns=columns, show="headings", height=15)
        self.costars_tree.heading("id", text="ID")
        self.costars_tree.heading("name", text="Name")
        self.costars_tree.heading("movies", text="Movies Together")
        self.costars_tree.heading("popularity", text="Popularity")
        
        self.costars_tree.column("id", width=50, stretch=False)
        self.costars_tree.column("name", width=200)
        self.costars_tree.column("movies", width=100)
        self.costars_tree.column("popularity", width=70, stretch=False)
        
        self.costars_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        costars_scrollbar = ttk.Scrollbar(costar_tree_frame, orient=tk.VERTICAL, command=self.costars_tree.yview)
        costars_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.costars_tree.configure(yscrollcommand=costars_scrollbar.set)

        # Action buttons
        button_frame = ttk.Frame(details_frame)
        button_frame.pack(fill=tk.X, padx=5, pady=5)
        
        ttk.Button(button_frame, text="Set as Start Actor", 
                 command=lambda: self.set_path_actor("start")).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Set as Target Actor", 
                 command=lambda: self.set_path_actor("target")).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Find Paths", 
                 command=self.find_paths_to_selected).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Open TMDB Page", 
                 command=self.open_tmdb_page).pack(side=tk.RIGHT, padx=5)

    def _create_placeholder_tab(self, title, notebook):
        frame = ttk.Frame(notebook)
        notebook.add(frame, text=title)
        ttk.Label(frame, text=f"{title} not available in this database.").pack(padx=10, pady=10)
        return frame

    def setup_stats_tab(self, parent):
        main_frame = ttk.Frame(parent)
        main_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Database info section
        info_frame = ttk.LabelFrame(main_frame, text="Database Information", padding="10")
        info_frame.pack(fill=tk.X, pady=10)
        
        self.db_path_var = tk.StringVar(value="Not loaded")
        self.db_size_var = tk.StringVar(value="")
        self.actor_count_var = tk.StringVar(value="")
        self.movie_count_var = tk.StringVar(value="N/A")
        self.tv_count_var = tk.StringVar(value="N/A")
        
        # Create labels for database info
        fields = [
            ("Database Path:", self.db_path_var),
            ("Database Size:", self.db_size_var),
            ("Actors:", self.actor_count_var),
            ("Movies:", self.movie_count_var),
            ("TV Shows:", self.tv_count_var)
        ]
        
        for i, (label_text, var) in enumerate(fields):
            ttk.Label(info_frame, text=label_text).grid(row=i, column=0, sticky=tk.W, padx=5, pady=2)
            ttk.Label(info_frame, textvariable=var).grid(row=i, column=1, sticky=tk.W, padx=5, pady=2)
        
        # Popular actors section
        top_frame = ttk.LabelFrame(main_frame, text="Popular Actors", padding="10")
        top_frame.pack(fill=tk.BOTH, expand=True, pady=10)
        
        top_tree_frame = ttk.Frame(top_frame)
        top_tree_frame.pack(fill=tk.BOTH, expand=True, padx=5, pady=5)
        
        columns = ("rank", "id", "name", "popularity")
        self.top_actors_tree = ttk.Treeview(top_tree_frame, columns=columns, show="headings", height=20)
        
        # Configure columns
        self.top_actors_tree.heading("rank", text="#")
        self.top_actors_tree.heading("id", text="ID")
        self.top_actors_tree.heading("name", text="Name")
        self.top_actors_tree.heading("popularity", text="Popularity")
        
        self.top_actors_tree.column("rank", width=40, stretch=False)
        self.top_actors_tree.column("id", width=60, stretch=False)
        self.top_actors_tree.column("name", width=200)
        self.top_actors_tree.column("popularity", width=80, stretch=False)
        
        self.top_actors_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        top_tree_scrollbar = ttk.Scrollbar(top_tree_frame, orient=tk.VERTICAL, command=self.top_actors_tree.yview)
        top_tree_scrollbar.pack(side=tk.RIGHT, fill=tk.Y)
        self.top_actors_tree.configure(yscrollcommand=top_tree_scrollbar.set)
        
        # Refresh button
        ttk.Button(main_frame, text="Refresh Statistics", command=self.update_stats).pack(pady=10)

    def check_all_databases(self):
        """Try to access both databases and inspect their schemas"""
        self.status_var.set("Checking available databases...")
        
        # Display current working directory for debugging
        cwd = os.getcwd()
        self.status_var.set(f"Current working directory: {cwd}")
        
        # Possible database locations with expanded search paths
        db_locations = [
            # Absolute paths
            os.path.join(cwd, 'actor_connections.db'),
            os.path.join(cwd, 'actors.db'),
            
            # Try parent directory
            os.path.join(os.path.dirname(cwd), 'actor_connections.db'),
            os.path.join(os.path.dirname(cwd), 'actors.db'),
            
            # Common relative paths
            'actor_connections.db',
            'actors.db',
            '../actor_connections.db',
            '../actors.db',
            './actor_connections.db',
            './actors.db',
            
            # Project-specific paths
            'actor-game/public/actor_connections.db',
            'actor-game/public/actors.db'
        ]
        
        # Store connections to both databases
        self.db_connections = {}
        self.table_schemas = {}
        
        # Add diagnostics to show all paths being checked
        all_paths_checked = []
        
        for db_path in db_locations:
            all_paths_checked.append(db_path)
            abs_path = os.path.abspath(db_path)
            
            if os.path.exists(abs_path):
                try:
                    conn = sqlite3.connect(abs_path)
                    cursor = conn.cursor()
                    
                    # Get all tables
                    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
                    tables = [table[0] for table in cursor.fetchall()]
                    
                    # Get file size
                    file_size = os.path.getsize(abs_path)
                    size_display = f"{file_size / (1024*1024):.2f} MB"
                    
                    # Store db info
                    db_key = os.path.basename(abs_path).replace('.db', '')
                    self.db_connections[db_key] = {
                        'path': abs_path,
                        'size': size_display,
                        'tables': tables
                    }
                    self.table_schemas[db_key] = {}
                    
                    # Get schema for each table
                    for table in tables:
                        cursor.execute(f"PRAGMA table_info({table})")
                        columns = [col[1] for col in cursor.fetchall()]
                        self.table_schemas[db_key][table] = columns
                    
                    conn.close()
                    self.status_var.set(f"Found database: {abs_path} with {len(tables)} tables")
                except sqlite3.Error as e:
                    self.status_var.set(f"Error inspecting {abs_path}: {str(e)}")
        
        if not self.db_connections:
            # If no databases found, show detailed diagnostic error
            error_msg = f"No usable databases found. Checked paths:\n\n"
            for path in all_paths_checked:
                abs_path = os.path.abspath(path)
                exists = "✓" if os.path.exists(abs_path) else "✗"
                error_msg += f"{exists} {abs_path}\n"
            
            messagebox.showerror("Database Error", error_msg)
            return False
        
        # Show summary of found databases
        db_summary = "Found databases:\n"
        for db_name, db_info in self.db_connections.items():
            db_summary += f"- {db_name}: {db_info['path']} ({db_info['size']})\n"
            db_summary += f"  Tables: {', '.join(db_info['tables'])}\n"
        
        self.status_var.set(db_summary)
        return True

    def load_database(self, db_path=None):
        if not self.check_all_databases():
            self.status_var.set("Failed to find any usable databases")
            return False
        
        try:
            if self.build_graph_from_database():
                self.update_stats()
                self.status_var.set(f"Database(s) loaded successfully")
                return True
            else:
                self.status_var.set("No data loaded from databases")
                return False
        except Exception as e:
            self.status_var.set(f"Error loading database: {str(e)}")
            messagebox.showerror("Database Error", f"Error loading database: {str(e)}")
            return False

    def build_graph_from_database(self):
        """Build the graph using the actors and actor_connections tables."""
        self.graph.clear()
        actors_loaded = False
        connections_loaded = False
        
        try:
            # First load actors from actors.db
            if 'actors' in self.db_connections:
                actor_db_path = self.db_connections['actors']['path']
                self.status_var.set(f"Loading actors from {actor_db_path}...")
                self.root.update_idletasks()
                
                conn = sqlite3.connect(actor_db_path)
                cursor = conn.cursor()
                
                # Check if 'actors' table exists
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='actors'")
                if cursor.fetchone():
                    cursor.execute("SELECT id, name, profile_path, popularity, place_of_birth FROM actors")
                    for actor_id, name, profile_path, popularity, place_of_birth in cursor.fetchall():
                        self.graph.add_node(actor_id, 
                                            type="actor", 
                                            name=name, 
                                            profile_path=profile_path,
                                            popularity=popularity,
                                            place_of_birth=place_of_birth)
                    actors_loaded = True
                    self.status_var.set(f"Loaded actor data from {actor_db_path}")
                else:
                    self.status_var.set(f"No 'actors' table found in {actor_db_path}")
                
                conn.close()
            else:
                self.status_var.set("No actors database found")
            
            # Then load connections from actor_connections.db
            if 'actor_connections' in self.db_connections:
                conn_db_path = self.db_connections['actor_connections']['path']
                self.status_var.set(f"Loading actor connections from {conn_db_path}...")
                self.root.update_idletasks()
                
                conn = sqlite3.connect(conn_db_path)
                cursor = conn.cursor()
                
                # First try the expected table name
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='actor_connections'")
                if cursor.fetchone():
                    cursor.execute("SELECT start_id, target_id, connection_length, optimal_path, difficulty FROM actor_connections")
                    for start_id, target_id, connection_length, optimal_path, difficulty in cursor.fetchall():
                        if self.graph.has_node(int(start_id)) and self.graph.has_node(int(target_id)):
                            self.graph.add_edge(int(start_id), int(target_id), 
                                              connection_length=connection_length,
                                              optimal_path=optimal_path, 
                                              difficulty=difficulty)
                    connections_loaded = True
                    self.status_var.set(f"Loaded connection data from {conn_db_path}")
                else:
                    # If no actor_connections table, try alternatives
                    alt_tables = [t for t in self.db_connections['actor_connections']['tables'] 
                                 if 'connection' in t.lower() or 'edge' in t.lower()]
                    
                    if alt_tables:
                        alt_table = alt_tables[0]
                        self.status_var.set(f"Trying alternative connections table: {alt_table}")
                        
                        # Get columns for the table to adapt the query
                        cursor.execute(f"PRAGMA table_info({alt_table})")
                        columns = [col[1] for col in cursor.fetchall()]
                        
                        # Try to map required columns to available columns
                        req_cols = ['start_id', 'target_id', 'connection_length', 'optimal_path', 'difficulty']
                        col_map = {}
                        
                        for req in req_cols:
                            # Try exact match or partial match
                            if req in columns:
                                col_map[req] = req
                            else:
                                matches = [c for c in columns if req.lower() in c.lower()]
                                if matches:
                                    col_map[req] = matches[0]
                                else:
                                    # Default values if column not found
                                    col_map[req] = None
                        
                        # Must have at least start_id and target_id
                        if col_map['start_id'] and col_map['target_id']:
                            query = f"SELECT {col_map['start_id']}, {col_map['target_id']}"
                            if col_map['connection_length']:
                                query += f", {col_map['connection_length']}"
                            else:
                                query += ", 1"  # Default length
                                
                            if col_map['optimal_path']:
                                query += f", {col_map['optimal_path']}"
                            else:
                                query += ", NULL"  # Default path
                                
                            if col_map['difficulty']:
                                query += f", {col_map['difficulty']}"
                            else:
                                query += ", 1"  # Default difficulty
                                
                            query += f" FROM {alt_table}"
                            
                            cursor.execute(query)
                            for row in cursor.fetchall():
                                if self.graph.has_node(int(row[0])) and self.graph.has_node(int(row[1])):
                                    self.graph.add_edge(int(row[0]), int(row[1]),
                                                      connection_length=row[2] if len(row) > 2 else 1,
                                                      optimal_path=row[3] if len(row) > 3 else None,
                                                      difficulty=row[4] if len(row) > 4 else 1)
                            
                            connections_loaded = True
                            self.status_var.set(f"Loaded connection data from alternative table {alt_table}")
                        else:
                            self.status_var.set(f"Could not map required columns in {alt_table}")
                    else:
                        self.status_var.set(f"No suitable connections table found in {conn_db_path}")
                
                conn.close()
            else:
                self.status_var.set("No connections database found")
            
            # Update status with count information
            actor_count = len([n for n, d in self.graph.nodes(data=True) if d.get('type') == 'actor'])
            edge_count = self.graph.number_of_edges()
            
            status_msg = []
            if actors_loaded:
                status_msg.append(f"{actor_count} actors")
            if connections_loaded:
                status_msg.append(f"{edge_count} connections")
            
            if status_msg:
                self.status_var.set(f"Loaded {' and '.join(status_msg)}")
            else:
                self.status_var.set("No data loaded - check database structure")
            
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            self.status_var.set(f"Error loading database: {str(e)}")
            messagebox.showerror("Database Error", f"Could not load database: {str(e)}\n\n{tb}")
            return False
        
        return actors_loaded or connections_loaded

    # Core functionality methods
    def search_actors_for(self, target_type):
        if target_type == "start":
            name = self.start_actor_entry.get().strip()
        else:
            name = self.target_actor_entry.get().strip()
        
        if not name:
            messagebox.showwarning("Missing Input", "Please enter an actor name")
            return

        self._show_actor_search_dialog(name, target_type)
    
    def search_actors(self):
        # Creates a dialog for searching actors
        threading.Thread(target=self._perform_actor_search, daemon=True).start()
    
    def _perform_actor_search(self):
        """Search for actors by name and display results in the tree view"""
        search_term = self.actor_search_entry.get().strip()
        if not search_term:
            self.root.after(0, lambda: self.status_var.set("Please enter a search term"))
            return
        
        self.root.after(0, lambda: self.status_var.set(f"Searching for '{search_term}'..."))
        self.root.after(0, lambda: self.actor_tree.delete(*self.actor_tree.get_children()))
        
        try:
            # Find database with actors table
            db_path = None
            if 'actors' in self.db_connections:
                db_path = self.db_connections['actors']['path']
            else:
                # Try any database that might have an actors table
                for db_info in self.db_connections.values():
                    if 'actors' in db_info['tables']:
                        db_path = db_info['path']
                        break
            
            if not db_path:
                self.root.after(0, lambda: self.status_var.set("No database with actors table found"))
                return
                
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Use LIKE for case-insensitive partial matching
            cursor.execute("""
                SELECT id, name, popularity 
                FROM actors 
                WHERE name LIKE ? 
                ORDER BY popularity DESC
                LIMIT 100
            """, (f"%{search_term}%",))
            
            results = cursor.fetchall()
            conn.close()
            
            # Update UI in the main thread
            def update_ui():
                if not results:
                    self.status_var.set(f"No actors found matching '{search_term}'")
                    return
                    
                for actor_id, name, popularity in results:
                    self.actor_tree.insert("", "end", values=(actor_id, name, f"{popularity:.1f}"))
                    
                self.status_var.set(f"Found {len(results)} actors matching '{search_term}'")
            
            self.root.after(0, update_ui)
            
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            print(f"Error searching actors: {str(e)}\n{tb}")
            self.root.after(0, lambda: self.status_var.set(f"Error searching: {str(e)}"))

    def show_actor_details(self, event=None):
        selected_items = self.actor_tree.selection()
        if not selected_items:
            return
        actor_id = self.actor_tree.item(selected_items[0], "values")[0]
        self.load_actor_by_id(actor_id)
    
    def load_actor_by_id(self, actor_id):
        """Load actor details and credits from database"""
        self.current_actor_id = actor_id
        
        try:
            # Get actor details from graph
            if self.graph.has_node(int(actor_id)):
                actor_data = self.graph.nodes[int(actor_id)]
                self.actor_name_label.config(text=actor_data.get('name', 'Unknown'))
                self.actor_id_label.config(text=f"ID: {actor_id}")
                self.actor_popularity_label.config(text=f"Popularity: {actor_data.get('popularity', 'N/A')}")
                
                if actor_data.get('place_of_birth'):
                    self.actor_birth_label.config(text=f"Born: {actor_data.get('place_of_birth', 'N/A')}")
                else:
                    self.actor_birth_label.config(text="")
                
                # Try to load image
                profile_path = actor_data.get('profile_path')
                self.load_actor_image(profile_path)
                
                # Clear existing credits
                self.movies_tree.delete(*self.movies_tree.get_children())
                self.tv_tree.delete(*self.tv_tree.get_children())
                self.costars_tree.delete(*self.costars_tree.get_children())
                
                # Get database path
                if 'actors' in self.db_connections:
                    db_path = self.db_connections['actors']['path']
                else:
                    db_paths = list(self.db_connections.values())
                    if db_paths:
                        db_path = db_paths[0]['path']
                    else:
                        self.status_var.set("No database found for actor credits")
                        return
                
                # Load credits in background thread
                threading.Thread(
                    target=self._load_actor_credits, 
                    args=(actor_id, db_path),
                    daemon=True
                ).start()
            else:
                messagebox.showerror("Actor Not Found", f"Actor with ID {actor_id} not found in database")
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            self.status_var.set(f"Error loading actor: {str(e)}")
            print(f"Error loading actor: {str(e)}\n{tb}")

    def load_actor_image(self, profile_path, size=(185, 278)):
        """Load actor profile image from TMDB and display it"""
        if not profile_path:
            # Set a placeholder image for actors without photos
            self.actor_image_label.config(image="")
            return
            
        # Check if image is already cached
        if profile_path in self.image_cache:
            self.actor_image_label.config(image=self.image_cache[profile_path])
            return
            
        try:
            # TMDB image base URL
            image_base_url = "https://image.tmdb.org/t/p/w185"
            if not profile_path.startswith('http'):
                # Construct full URL if just a path is provided
                image_url = f"{image_base_url}{profile_path}"
            else:
                image_url = profile_path
                
            # Download image in a background thread
            threading.Thread(target=self._download_and_display_image, 
                           args=(image_url, profile_path, size),
                           daemon=True).start()
        except Exception as e:
            print(f"Error loading image: {str(e)}")
            self.actor_image_label.config(image="")

    def _download_and_display_image(self, url, profile_path, size):
        """Background task to download and process actor image"""
        try:
            # Download image
            response = requests.get(url, timeout=10)
            if response.status_code == 200:
                # Load image from response data
                img_data = response.content
                img = Image.open(io.BytesIO(img_data))
                
                # Resize image while preserving aspect ratio
                img.thumbnail(size)
                
                # Convert to Tkinter PhotoImage
                tk_img = ImageTk.PhotoImage(img)
                
                # Cache the image and update the UI in the main thread
                self.image_cache[profile_path] = tk_img
                self.root.after(0, lambda: self.actor_image_label.config(image=tk_img))
            else:
                self.root.after(0, lambda: self.actor_image_label.config(image=""))
        except Exception as e:
            print(f"Error processing image: {str(e)}")
            self.root.after(0, lambda: self.actor_image_label.config(image=""))

    def _load_actor_credits(self, actor_id, db_path):
        """Load movie and TV credits for an actor"""
        self.status_var.set(f"Loading credits for actor {actor_id}...")
        
        try:
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Load movie credits
            cursor.execute("""
                SELECT id, title, character, release_date 
                FROM movie_credits 
                WHERE actor_id = ? 
                ORDER BY release_date DESC
            """, (actor_id,))
            movies = cursor.fetchall()
            
            # Add movies to tree - FIXED LAMBDA ISSUE
            for movie in movies:
                movie_id, title, character, release_date = movie
                year = release_date[:4] if release_date and len(release_date) >= 4 else "N/A"
                
                # Create copies of the values to avoid lambda capture issue
                def add_movie(mid=movie_id, mtitle=title, mchar=character, myear=year):
                    self.movies_tree.insert("", "end", values=(mid, mtitle, mchar, myear))
                
                self.root.after(0, add_movie)
            
            # Load TV credits
            cursor.execute("""
                SELECT id, name, character, first_air_date 
                FROM tv_credits 
                WHERE actor_id = ? 
                ORDER BY first_air_date DESC
            """, (actor_id,))
            tv_shows = cursor.fetchall()
            
            # Add TV shows to tree - FIXED LAMBDA ISSUE
            for show in tv_shows:
                show_id, name, character, first_air_date = show
                year = first_air_date[:4] if first_air_date and len(first_air_date) >= 4 else "N/A"
                
                # Create copies of the values
                def add_show(sid=show_id, sname=name, schar=character, syear=year):
                    self.tv_tree.insert("", "end", values=(sid, sname, schar, syear))
                
                self.root.after(0, add_show)
            
            # Load co-stars (optional)
            self._load_costars(actor_id, conn)
            
            conn.close()
            self.root.after(0, lambda: self.status_var.set(
                f"Loaded {len(movies)} movies and {len(tv_shows)} TV shows for {actor_id}"))
            
        except Exception as e:
            import traceback
            tb = traceback.format_exc()
            self.root.after(0, lambda: self.status_var.set(f"Error loading credits: {str(e)}"))
            print(f"Error loading credits: {str(e)}\n{tb}")

    def _load_costars(self, actor_id, conn):
        """Load co-stars for an actor"""
        try:
            # Find all actors who appeared in same movies
            cursor = conn.cursor()
            cursor.execute("""
                WITH actor_movies AS (
                    SELECT id FROM movie_credits WHERE actor_id = ?
                )
                SELECT a.id, a.name, COUNT(*) as movie_count, a.popularity 
                FROM actors a
                JOIN movie_credits mc ON a.id = mc.actor_id
                WHERE mc.id IN (SELECT id FROM actor_movies)
                AND a.id != ?
                GROUP BY a.id
                ORDER BY movie_count DESC, a.popularity DESC
                LIMIT 100
            """, (actor_id, actor_id))
            
            costars = cursor.fetchall()
            
            # Add co-stars to tree - FIXED LAMBDA ISSUE
            for costar in costars:
                costar_id, name, movie_count, popularity = costar
                
                # Create copies of the values
                def add_costar(cid=costar_id, cname=name, count=movie_count, pop=popularity):
                    self.costars_tree.insert("", "end", values=(cid, cname, count, f"{pop:.1f}"))
                
                self.root.after(0, add_costar)
                
        except Exception as e:
            print(f"Error loading co-stars: {str(e)}")

    def set_path_actor(self, actor_type):
        """Set the selected actor as either start or target actor"""
        selected_items = self.actor_tree.selection()
        if not selected_items:
            messagebox.showinfo("Selection Required", "Please select an actor first")
            return
        
        actor_id = self.actor_tree.item(selected_items[0], "values")[0]
        actor_name = self.actor_tree.item(selected_items[0], "values")[1]
        
        if actor_type == "start":
            self.start_actor_id = actor_id
            self.start_actor_entry.delete(0, tk.END)
            self.start_actor_entry.insert(0, actor_name)
            self.status_var.set(f"Set {actor_name} as start actor")
        else:  # target
            self.target_actor_id = actor_id
            self.target_actor_entry.delete(0, tk.END)
            self.target_actor_entry.insert(0, actor_name)
            self.status_var.set(f"Set {actor_name} as target actor")
        
        # Switch to path finder tab
        self.notebook.select(self.path_tab)

    def find_path(self):
        """Find a path between the start and target actors"""
        start_name = self.start_actor_entry.get().strip()
        target_name = self.target_actor_entry.get().strip()
        
        if not start_name or not target_name:
            messagebox.showwarning("Missing Input", "Please enter both start and target actor names")
            return
        
        # Clear previous results
        self.results_text.delete("1.0", tk.END)
        for widget in self.path_frame.winfo_children():
            widget.destroy()
            
        self.status_var.set(f"Finding path between '{start_name}' and '{target_name}'...")
        
        # Get actor IDs if needed
        start_id = getattr(self, "start_actor_id", None)
        target_id = getattr(self, "target_actor_id", None)
        
        if not start_id:
            # Search for start actor
            start_id = self._find_actor_by_name(start_name)
            if not start_id:
                self.status_var.set(f"Could not find actor '{start_name}'")
                return
        
        if not target_id:
            # Search for target actor
            target_id = self._find_actor_by_name(target_name)
            if not target_id:
                self.status_var.set(f"Could not find actor '{target_name}'")
                return
        
        # Start path finding in a background thread
        threading.Thread(
            target=self._find_shortest_path,
            args=(int(start_id), int(target_id)),
            daemon=True
        ).start()

    def _find_actor_by_name(self, name):
        """Search for an actor by name and return their ID"""
        try:
            if 'actors' in self.db_connections:
                db_path = self.db_connections['actors']['path']
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                
                # Try exact match first
                cursor.execute("SELECT id FROM actors WHERE name = ? LIMIT 1", (name,))
                result = cursor.fetchone()
                
                # If no exact match, try LIKE query
                if not result:
                    cursor.execute("SELECT id FROM actors WHERE name LIKE ? ORDER BY popularity DESC LIMIT 1", 
                                  (f"%{name}%",))
                    result = cursor.fetchone()
                
                conn.close()
                
                if result:
                    return result[0]
                else:
                    # If still not found, show dialog with potential matches
                    self._show_actor_search_dialog(name, "start" if name == self.start_actor_entry.get() else "target")
                    return None
            return None
        except Exception as e:
            print(f"Error finding actor by name: {str(e)}")
            return None

    def _find_shortest_path(self, start_id, target_id):
        """Find the shortest path between two actors"""
        try:
            path = nx.shortest_path(self.graph, source=start_id, target=target_id)
            self.root.after(0, lambda: self._display_path(path))
        except nx.NetworkXNoPath:
            self.root.after(0, lambda: self.status_var.set("No path found"))
        except Exception as e:
            print(f"Error finding shortest path: {str(e)}")
            self.root.after(0, lambda: self.status_var.set(f"Error finding path: {str(e)}"))

    def _display_path(self, path):
        """Display the found actor path in the UI"""
        if not path or len(path) < 2:
            self.status_var.set("Invalid path found")
            return
        
        # Get actor names for the path
        actor_names = []
        for actor_id in path:
            if self.graph.has_node(actor_id):
                name = self.graph.nodes[actor_id].get('name', f"Actor {actor_id}")
                actor_names.append(name)
            else:
                actor_names.append(f"Unknown Actor {actor_id}")
        
        # Create text description
        path_text = " → ".join(actor_names)
        self.results_text.delete("1.0", tk.END)
        self.results_text.insert("1.0", f"Path found with {len(path)-1} connections:\n\n{path_text}")
        
        # Update status
        self.status_var.set(f"Found path with {len(path)-1} connections")
        
        # Clear previous visual path
        for widget in self.path_frame.winfo_children():
            widget.destroy()
        
        # Add visual representation of the path
        for i, actor_id in enumerate(path):
            if i > 0:
                # Add arrow between actors
                arrow_label = ttk.Label(self.path_frame, text="→")
                arrow_label.pack(side=tk.LEFT, padx=5)
                
                # Try to find movie that connects these actors
                actor1 = path[i-1]
                actor2 = actor_id
                
                # Get movie connections if they exist in the graph
                if self.graph.has_edge(actor1, actor2):
                    movie_frame = ttk.Frame(self.path_frame)
                    movie_frame.pack(side=tk.LEFT, padx=5)
                    movie_title = "Unknown Movie"
                    
                    # Check if we have movie data
                    if 'credits' in self.graph[actor1][actor2]:
                        credit_id = self.graph[actor1][actor2]['credits'][0]
                        # Find the movie name from actor credits
                        if 'actors' in self.db_connections:
                            try:
                                conn = sqlite3.connect(self.db_connections['actors']['path'])
                                cursor = conn.cursor()
                                cursor.execute("SELECT title FROM movie_credits WHERE id = ? LIMIT 1", (credit_id,))
                                result = cursor.fetchone()
                                if result:
                                    movie_title = result[0]
                                conn.close()
                            except:
                                pass
                    
                    ttk.Label(movie_frame, text=f"in\n{movie_title}", font=("TkDefaultFont", 8), 
                             justify=tk.CENTER, wraplength=100).pack()
            
            # Add actor box
            actor_frame = ttk.Frame(self.path_frame, borderwidth=2, relief=tk.GROOVE, padding=5)
            actor_frame.pack(side=tk.LEFT, padx=5, pady=10)
            
            if self.graph.has_node(actor_id):
                name = self.graph.nodes[actor_id].get('name', f"Actor {actor_id}")
                
                # Try to load image if available
                profile_path = self.graph.nodes[actor_id].get('profile_path')
                if profile_path:
                    image_url = f"https://image.tmdb.org/t/p/w92{profile_path}"
                    try:
                        response = requests.get(image_url, timeout=3)
                        if response.status_code == 200:
                            img = Image.open(io.BytesIO(response.content))
                            img.thumbnail((50, 75))
                            photo = ImageTk.PhotoImage(img)
                            img_label = ttk.Label(actor_frame, image=photo)
                            img_label.image = photo  # Keep a reference
                            img_label.pack(padx=5, pady=5)
                    except:
                        pass
            else:
                name = f"Unknown Actor {actor_id}"
                
            ttk.Label(actor_frame, text=name, font=("TkDefaultFont", 9, "bold"), 
                     wraplength=120, justify=tk.CENTER).pack(padx=5, pady=5)
        
        # Update the canvas scroll region
        self.path_frame.update_idletasks()
        self.path_canvas.configure(scrollregion=self.path_canvas.bbox("all"))
        
        # Scroll to the beginning
        self.path_canvas.xview_moveto(0)

    def update_stats(self):
        """Update the statistics displayed in the stats tab with current database information"""
        # Update database path info
        if 'actors' in self.db_connections:
            self.db_path_var.set(self.db_connections['actors']['path'])
            self.db_size_var.set(self.db_connections['actors']['size'])
        elif self.db_connections:
            # Use the first available database if actors.db not found
            first_db = next(iter(self.db_connections.values()))
            self.db_path_var.set(first_db['path'])
            self.db_size_var.set(first_db['size'])
        else:
            self.db_path_var.set("Not loaded")
            self.db_size_var.set("")
        
        # Update actor count
        actor_count = len([n for n, d in self.graph.nodes(data=True) if d.get('type') == 'actor'])
        if actor_count > 0:
            self.actor_count_var.set(f"{actor_count:,}")
        else:
            self.actor_count_var.set("0")
        
        # Update movie and TV show counts if available
        try:
            if 'actors' in self.db_connections:
                conn = sqlite3.connect(self.db_connections['actors']['path'])
                cursor = conn.cursor()
                
                # Try to get movie count - NOTE: Changed table name from 'movies' to 'movie_credits'
                if 'movie_credits' in self.db_connections['actors']['tables']:
                    cursor.execute("SELECT COUNT(DISTINCT id) FROM movie_credits")
                    movie_count = cursor.fetchone()[0]
                    self.movie_count_var.set(f"{movie_count:,}")
                else:
                    self.movie_count_var.set("N/A (no movie_credits table)")
                
                # Try to get TV show count - NOTE: Changed table name from 'tv_shows' to 'tv_credits'
                if 'tv_credits' in self.db_connections['actors']['tables']:
                    cursor.execute("SELECT COUNT(DISTINCT id) FROM tv_credits")
                    tv_count = cursor.fetchone()[0]
                    self.tv_count_var.set(f"{tv_count:,}")
                else:
                    self.tv_count_var.set("N/A (no tv_credits table)")
                
                conn.close()
            else:
                self.movie_count_var.set("N/A (actors database not found)")
                self.tv_count_var.set("N/A (actors database not found)")
        except Exception as e:
            self.movie_count_var.set("Error querying")
            self.tv_count_var.set("Error querying")
            print(f"Error updating media counts: {str(e)}")
        
        # Update popular actors list
        self.top_actors_tree.delete(*self.top_actors_tree.get_children())
        
        # Get all actors with popularity info
        actors_with_pop = [(n, d.get('name', 'Unknown'), d.get('popularity', 0))
                          for n, d in self.graph.nodes(data=True) 
                          if d.get('type') == 'actor' and d.get('popularity')]
        
        # Sort by popularity (descending)
        actors_with_pop.sort(key=lambda x: x[2], reverse=True)
        
        # Add top actors to tree
        for i, (actor_id, name, popularity) in enumerate(actors_with_pop[:100], 1):
            self.top_actors_tree.insert("", "end", values=(i, actor_id, name, f"{popularity:.1f}"))
        
        # Display diagnostic info in status bar
        self.status_var.set(f"Stats updated. {actor_count} actors loaded in graph.")

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
    
    def export_path(self):
        # Export the current path to a file
        pass
    
    def _on_frame_configure(self, event):
        self.path_canvas.configure(scrollregion=self.path_canvas.bbox("all"))
    
    def show_about(self):
        messagebox.showinfo("About", "Actor Connections Database Analyzer\nVersion 1.0")
    
    def show_help(self):
        messagebox.showinfo("How to Use", 
                          "Use the provided tabs to search for actors, view details, and find paths between actors.")
    
    def open_tmdb_page(self):
        if not self.current_actor_id:
            return
        url = f"https://www.themoviedb.org/person/{self.current_actor_id}"
        webbrowser.open(url)
    
    def find_paths_to_selected(self):
        """Find paths to the currently selected actor from others"""
        selected_items = self.actor_tree.selection()
        if not selected_items:
            messagebox.showinfo("Selection Required", "Please select an actor first")
            return
            
        actor_id = self.actor_tree.item(selected_items[0], "values")[0]
        actor_name = self.actor_tree.item(selected_items[0], "values")[1]
        
        # Set as target actor
        self.target_actor_id = actor_id
        self.target_actor_entry.delete(0, tk.END)
        self.target_actor_entry.insert(0, actor_name)
        
        # Switch to path finder tab
        self.notebook.select(self.path_tab)
        
        # Show message to select start actor if not already selected
        if not hasattr(self, "start_actor_id"):
            messagebox.showinfo("Select Start Actor", 
                            "Now please search for and select a start actor, then click 'Find Path'")
    
    def threaded_load_actor_credits(self, actor_id, selected_db, db_path, credits_tree, status_var):
        # Method to load actor credits in a background thread
        threading.Thread(target=self._load_credits_task, 
                       args=(actor_id, selected_db, db_path, credits_tree, status_var),
                       daemon=True).start()
    
    def _load_credits_task(self, actor_id, selected_db, db_path, credits_tree, status_var):
        # Background task implementation
        pass
    
    def explore_database(self):
        """Interactive tool to explore database structure and content"""
        dialog = tk.Toplevel(self.root)
        dialog.title("Database Explorer")
        dialog.geometry("800x600")
        
        # Database selection
        selection_frame = ttk.Frame(dialog, padding="10")
        selection_frame.pack(fill=tk.X)
        
        ttk.Label(selection_frame, text="Database:").pack(side=tk.LEFT, padx=5)
        db_var = tk.StringVar()
        db_list = list(self.db_connections.keys())
        if db_list:
            db_var.set(db_list[0])
        db_combo = ttk.Combobox(selection_frame, textvariable=db_var, values=db_list, width=20)
        db_combo.pack(side=tk.LEFT, padx=5)
        
        ttk.Label(selection_frame, text="Table:").pack(side=tk.LEFT, padx=5)
        table_var = tk.StringVar()
        table_combo = ttk.Combobox(selection_frame, textvariable=table_var, width=20)
        table_combo.pack(side=tk.LEFT, padx=5)
        
        # Query editor
        query_frame = ttk.LabelFrame(dialog, text="SQL Query", padding="10")
        query_frame.pack(fill=tk.X, padx=10, pady=5)
        
        query_text = tk.Text(query_frame, height=5, width=50)
        query_text.pack(fill=tk.X, expand=True)
        query_text.insert("1.0", "SELECT * FROM table_name LIMIT 100")
        
        # Results area
        results_frame = ttk.LabelFrame(dialog, text="Results", padding="10")
        results_frame.pack(fill=tk.BOTH, expand=True, padx=10, pady=5)
        
        # Treeview for results
        tree_frame = ttk.Frame(results_frame)
        tree_frame.pack(fill=tk.BOTH, expand=True)
        
        results_tree = ttk.Treeview(tree_frame)
        results_tree.pack(side=tk.LEFT, fill=tk.BOTH, expand=True)
        
        tree_scroll = ttk.Scrollbar(tree_frame, orient=tk.VERTICAL, command=results_tree.yview)
        tree_scroll.pack(side=tk.RIGHT, fill=tk.Y)
        results_tree.configure(yscrollcommand=tree_scroll.set)
        
        # Button actions
        def update_tables(*args):
            if not db_var.get() in self.db_connections:
                return
            selected_db = db_var.get()
            tables = self.db_connections[selected_db]['tables']
            table_combo['values'] = tables
            if tables:
                table_var.set(tables[0])
        
        def run_query():
            if not db_var.get() in self.db_connections:
                return
            selected_db = db_var.get()
            sql = query_text.get("1.0", tk.END).strip()
            
            # Clear existing results
            for col in results_tree['columns']:
                results_tree.heading(col, text="")
            results_tree['columns'] = ()
            results_tree.delete(*results_tree.get_children())
            
            try:
                db_path = self.db_connections[selected_db]['path']
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                cursor.execute(sql)
                
                # Get column names
                col_names = [desc[0] for desc in cursor.description]
                results_tree['columns'] = col_names
                # Set column headings
                for col in col_names:
                    results_tree.heading(col, text=col)
                    results_tree.column(col, width=100)
                
                # Add data rows
                for row in cursor.fetchall():
                    results_tree.insert("", "end", values=row)
                
                conn.close()
            except Exception as e:
                messagebox.showerror("Query Error", str(e))
        
        def insert_table_name():
            selected_table = table_var.get()
            query_text.delete("1.0", tk.END)
            query_text.insert("1.0", f"SELECT * FROM {selected_table} LIMIT 100")
        
        # Connect events
        db_var.trace("w", update_tables)
        table_combo.bind("<<ComboboxSelected>>", lambda e: insert_table_name())
        
        # Buttons
        button_frame = ttk.Frame(dialog)
        button_frame.pack(fill=tk.X, padx=10, pady=10)
        
        ttk.Button(button_frame, text="Run Query", command=run_query).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Insert Table Name", 
                command=insert_table_name).pack(side=tk.LEFT, padx=5)
        ttk.Button(button_frame, text="Close", 
                command=dialog.destroy).pack(side=tk.RIGHT, padx=5)
        
        # Initialize table list
        update_tables()

    def _show_actor_search_dialog(self, name, actor_type):
        """Show a dialog with actor search results for path finding"""
        dialog = tk.Toplevel(self.root)
        dialog.title(f"Select {actor_type.title()} Actor")
        dialog.geometry("500x400")
        dialog.transient(self.root)
        dialog.grab_set()
        
        ttk.Label(dialog, text=f"Select an actor for '{name}':", 
                 font=("TkDefaultFont", 10, "bold")).pack(pady=10, padx=10, anchor=tk.W)
        
        # Create treeview for results
        columns = ("id", "name", "popularity")
        tree = ttk.Treeview(dialog, columns=columns, show="headings", height=15)
        tree.heading("id", text="ID")
        tree.heading("name", text="Name")
        tree.heading("popularity", text="Popularity")
        
        tree.column("id", width=50, stretch=False)
        tree.column("name", width=250)
        tree.column("popularity", width=70, stretch=False)
        
        tree.pack(fill=tk.BOTH, expand=True, padx=10, pady=10)
        
        # Buttons
        button_frame = ttk.Frame(dialog)
        button_frame.pack(fill=tk.X, padx=10, pady=10)
        
        def select_actor():
            selected = tree.selection()
            if not selected:
                return
                
            actor_id = tree.item(selected[0], "values")[0]
            actor_name = tree.item(selected[0], "values")[1]
            
            if actor_type == "start":
                self.start_actor_id = actor_id
                self.start_actor_entry.delete(0, tk.END)
                self.start_actor_entry.insert(0, actor_name)
            else:
                self.target_actor_id = actor_id
                self.target_actor_entry.delete(0, tk.END)
                self.target_actor_entry.insert(0, actor_name)
            
            dialog.destroy()
        
        ttk.Button(button_frame, text="Select", command=select_actor).pack(side=tk.RIGHT, padx=5)
        ttk.Button(button_frame, text="Cancel", command=dialog.destroy).pack(side=tk.RIGHT, padx=5)
        
        # Populate with search results
        conn = sqlite3.connect(self.db_connections['actors']['path'])
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, name, popularity 
            FROM actors 
            WHERE name LIKE ? 
            ORDER BY popularity DESC
            LIMIT 100
        """, (f"%{name}%",))
        
        results = cursor.fetchall()
        conn.close()
        
        for actor_id, name, popularity in results:
            tree.insert("", "end", values=(actor_id, name, f"{popularity:.1f}"))
            
        # If results found, select the first one
        if tree.get_children():
            tree.selection_set(tree.get_children()[0])

if __name__ == "__main__":
    root = tk.Tk()
    app = ActorToActorApp(root)
    root.mainloop()