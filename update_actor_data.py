import os
import requests
import json
import time
import sqlite3
import random
from requests.exceptions import ConnectionError, Timeout, RequestException
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

# Retrieve your TMDB API key from environment variables
TMDB_API_KEY = os.environ.get("TMDB_API_KEY")
if not TMDB_API_KEY:
    raise Exception("TMDB_API_KEY not set in environment variables.")

# Initialize Firebase (using service account from GitHub secrets)
firebase_key_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT")
if firebase_key_json:
    # Parse JSON string from environment variable
    firebase_key_dict = json.loads(firebase_key_json)
    
    # Initialize with the parsed credentials
    cred = credentials.Certificate(firebase_key_dict)
    firebase_admin.initialize_app(cred)
    db = firestore.client()
    print("Firebase initialized successfully")
else:
    print("FIREBASE_SERVICE_ACCOUNT not set, skipping Firebase upload")
    db = None

# Firebase operation counters for free tier limits
firebase_writes = 0
firebase_reads = 0
FIREBASE_DAILY_WRITE_LIMIT = 18000  # Set below the 20k limit to be safe
FIREBASE_DAILY_READ_LIMIT = 45000   # Set below the 50k limit to be safe

# Flag to enable/disable Firebase upload (can be controlled via command line)
ENABLE_FIREBASE = os.environ.get("ENABLE_FIREBASE", "true").lower() == "true"
if not ENABLE_FIREBASE:
    print("Firebase upload disabled via environment variable")
    db = None

# Constants
BASE_URL = "https://api.themoviedb.org/3"
POPULAR_ACTORS_URL = f"{BASE_URL}/person/popular"
ACTOR_MOVIE_CREDITS_URL_TEMPLATE = f"{BASE_URL}/person/{{}}/movie_credits"
ACTOR_TV_CREDITS_URL_TEMPLATE = f"{BASE_URL}/person/{{}}/tv_credits"
ACTOR_DETAILS_URL_TEMPLATE = f"{BASE_URL}/person/{{}}"

# Image base URL
IMAGE_BASE_URL = "https://image.tmdb.org/t/p/"
PROFILE_SIZE = "w185"
POSTER_SIZE = "w342"

# How many pages of popular actors to fetch
TOTAL_PAGES = 100

# Define regions
REGIONS = {
    "US": "United States",
    "UK": "United Kingdom",
    "CA": "Canada",
    "AU": "Australia",
    "KR": "South Korea",
    "CN": "China",
    "JP": "Japan",
    "IN": "India",
    "FR": "France",
    "DE": "Germany",
    "OTHER": "Other"
}

# Popularity threshold for global recognition
GLOBAL_POPULARITY_THRESHOLD = 15

# Robust API request function with exponential backoff
def make_api_request(url, params, max_retries=5):
    """Make API request with retry logic and exponential backoff"""
    retries = 0
    while retries < max_retries:
        try:
            response = requests.get(url, params=params, timeout=10)
            
            # Check for rate limiting (429 status code)
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 10))
                print(f"Rate limited. Waiting for {retry_after} seconds...")
                time.sleep(retry_after + 1)  # Add 1 second buffer
                retries += 1
                continue
                
            # Return successful response
            if response.status_code == 200:
                return response.json()
            
            # Handle other errors
            print(f"API error: {response.status_code} - {response.text}")
            return None
            
        except (ConnectionError, Timeout, RequestException) as e:
            wait_time = 2 ** retries + random.uniform(0, 1)
            print(f"Request failed: {e}. Retrying in {wait_time:.2f} seconds...")
            time.sleep(wait_time)
            retries += 1
    
    print(f"Failed after {max_retries} retries. Skipping this request.")
    return None

# Function to upload actor data to Firebase
def upload_to_firebase(actor_id, actor_data, movie_credits, tv_credits, regions):
    """Upload actor data to Firebase Firestore with existence checks and batching"""
    global firebase_writes, firebase_reads
    
    if db is None or firebase_writes >= FIREBASE_DAILY_WRITE_LIMIT:
        return  # Skip if Firebase not initialized or approaching limits
    
    try:
        # Create a batch for efficient writes
        batch = db.batch()
        actor_id_str = str(actor_id)
        writes_in_batch = 0
        
        # Check if actor already exists first
        actor_ref = db.collection('actors').document(actor_id_str)
        actor_doc = actor_ref.get()
        firebase_reads += 1
        
        if firebase_reads >= FIREBASE_DAILY_READ_LIMIT:
            print(f"Approaching Firebase read limit ({firebase_reads}), stopping Firebase operations")
            return
        
        actor_exists = actor_doc.exists
        actor_data_changed = False
        
        if actor_exists:
            existing_data = actor_doc.to_dict()
            # Check if data needs updating (compare fields)
            if (existing_data.get('name') != actor_data.get('name', '') or
                existing_data.get('popularity') != actor_data.get('popularity', 0) or
                existing_data.get('profile_path') != actor_data.get('profile_path', '') or
                existing_data.get('place_of_birth') != actor_data.get('place_of_birth', 'Unknown') or
                set(existing_data.get('regions', [])) != set(regions)):
                actor_data_changed = True
        
        # Only update actor if it doesn't exist or data changed
        if not actor_exists or actor_data_changed:
            batch.set(actor_ref, {
                'name': actor_data.get('name', ''),
                'popularity': actor_data.get('popularity', 0),
                'profile_path': actor_data.get('profile_path', ''),
                'place_of_birth': actor_data.get('place_of_birth', 'Unknown'),
                'regions': regions
            })
            writes_in_batch += 1
        
        # Handle movie credits - use a separate batch to avoid exceeding batch size limits
        if len(movie_credits) > 0:
            movie_batch = db.batch()
            movie_writes = 0
            
            # Get existing movie credits to avoid unnecessary writes
            if actor_exists:
                movie_refs_snapshot = actor_ref.collection('movie_credits').limit(500).get()
                firebase_reads += 1
                
                # Create a map of existing movies for quick lookup
                existing_movies = {doc.id: doc.to_dict() for doc in movie_refs_snapshot}
            else:
                existing_movies = {}
            
            for movie in movie_credits:
                movie_id_str = str(movie['id'])
                movie_ref = actor_ref.collection('movie_credits').document(movie_id_str)
                
                # Check if movie credit exists and needs updating
                if movie_id_str in existing_movies:
                    existing = existing_movies[movie_id_str]
                    if (existing.get('title') == movie.get('title', '') and
                        existing.get('character') == movie.get('character', '') and
                        existing.get('popularity') == movie.get('popularity', 0) and
                        existing.get('release_date') == movie.get('release_date', '') and
                        existing.get('poster_path') == movie.get('poster_path', '') and
                        existing.get('is_mcu') == movie.get('is_mcu', False)):
                        continue  # Skip if no changes
                
                movie_batch.set(movie_ref, {
                    'title': movie.get('title', ''),
                    'character': movie.get('character', ''),
                    'popularity': movie.get('popularity', 0),
                    'release_date': movie.get('release_date', ''),
                    'poster_path': movie.get('poster_path', ''),
                    'is_mcu': movie.get('is_mcu', False)
                })
                movie_writes += 1
                
                # Commit batch if getting large and create a new one
                if movie_writes >= 400:  # Firestore batch limit is 500
                    movie_batch.commit()
                    firebase_writes += movie_writes
                    if firebase_writes >= FIREBASE_DAILY_WRITE_LIMIT:
                        print(f"Approaching Firebase write limit ({firebase_writes}), stopping Firebase operations")
                        return
                    movie_batch = db.batch()
                    movie_writes = 0
            
            # Commit any remaining movie writes
            if movie_writes > 0:
                movie_batch.commit()
                firebase_writes += movie_writes
        
        # Handle TV credits - similar approach as movies
        if len(tv_credits) > 0:
            tv_batch = db.batch()
            tv_writes = 0
            
            # Get existing TV credits
            if actor_exists:
                tv_refs_snapshot = actor_ref.collection('tv_credits').limit(500).get()
                firebase_reads += 1
                existing_tv = {doc.id: doc.to_dict() for doc in tv_refs_snapshot}
            else:
                existing_tv = {}
            
            for tv in tv_credits:
                tv_id_str = str(tv['id'])
                tv_ref = actor_ref.collection('tv_credits').document(tv_id_str)
                
                # Check if TV credit exists and needs updating
                if tv_id_str in existing_tv:
                    existing = existing_tv[tv_id_str]
                    if (existing.get('name') == tv.get('name', '') and
                        existing.get('character') == tv.get('character', '') and
                        existing.get('popularity') == tv.get('popularity', 0) and
                        existing.get('first_air_date') == tv.get('first_air_date', '') and
                        existing.get('poster_path') == tv.get('poster_path', '') and
                        existing.get('is_mcu') == tv.get('is_mcu', False)):
                        continue  # Skip if no changes
                
                tv_batch.set(tv_ref, {
                    'name': tv.get('name', ''),
                    'character': tv.get('character', ''),
                    'popularity': tv.get('popularity', 0),
                    'first_air_date': tv.get('first_air_date', ''),
                    'poster_path': tv.get('poster_path', ''),
                    'is_mcu': tv.get('is_mcu', False)
                })
                tv_writes += 1
                
                # Commit batch if getting large
                if tv_writes >= 400:
                    tv_batch.commit()
                    firebase_writes += tv_writes
                    if firebase_writes >= FIREBASE_DAILY_WRITE_LIMIT:
                        print(f"Approaching Firebase write limit ({firebase_writes}), stopping Firebase operations")
                        return
                    tv_batch = db.batch()
                    tv_writes = 0
            
            # Commit any remaining TV writes
            if tv_writes > 0:
                tv_batch.commit()
                firebase_writes += tv_writes
        
        # Commit the main actor batch if it has any operations
        if writes_in_batch > 0:
            batch.commit()
            firebase_writes += writes_in_batch
        
        # Print status update with operation counts
        print(f"Firebase: Actor {actor_id} ({actor_data.get('name')}) processed. Total ops: {firebase_reads} reads, {firebase_writes} writes")
        
        # Check if we're approaching limits
        if firebase_writes >= FIREBASE_DAILY_WRITE_LIMIT or firebase_reads >= FIREBASE_DAILY_READ_LIMIT:
            print(f"Approaching Firebase limits (writes: {firebase_writes}, reads: {firebase_reads}), stopping Firebase operations")
            global db
            db = None  # Disable further Firebase operations
            
    except Exception as e:
        print(f"Error uploading actor {actor_id} to Firebase: {e}")

# Database setup function
def setup_database(region):
    """Create SQLite database for a specific region"""
    db_path = f"actor-game/public/actors_{region}.db"
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Remove existing database if any
    if os.path.exists(db_path):
        os.remove(db_path)
    
    # Create new database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create tables
    cursor.execute('''
    CREATE TABLE actors (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        popularity REAL,
        profile_path TEXT,
        place_of_birth TEXT
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE actor_regions (
        actor_id INTEGER,
        region TEXT,
        PRIMARY KEY (actor_id, region),
        FOREIGN KEY (actor_id) REFERENCES actors (id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE movie_credits (
        id INTEGER,
        actor_id INTEGER,
        title TEXT,
        character TEXT,
        popularity REAL,
        release_date TEXT,
        poster_path TEXT,
        is_mcu BOOLEAN,
        PRIMARY KEY (id, actor_id),
        FOREIGN KEY (actor_id) REFERENCES actors (id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE tv_credits (
        id INTEGER,
        actor_id INTEGER,
        name TEXT,
        character TEXT,
        popularity REAL,
        first_air_date TEXT,
        poster_path TEXT,
        is_mcu BOOLEAN,
        PRIMARY KEY (id, actor_id),
        FOREIGN KEY (actor_id) REFERENCES actors (id)
    )
    ''')
    
    conn.commit()
    return conn, cursor

# Set up databases for all regions
databases = {}
for region in list(REGIONS.keys()) + ["GLOBAL"]:
    databases[region] = setup_database(region)

# Track processed actors to avoid duplicates
processed_actors = set()

# Cache for movie/TV MCU status to reduce API calls
mcu_cache = {
    'movie': {},  # movie_id -> is_mcu
    'tv': {}      # tv_id -> is_mcu
}

# Track progress for resuming on future runs
progress_file = "firebase_progress.json"
last_processed_page = 1
last_processed_actor = None

# Load progress if available
if os.path.exists(progress_file) and ENABLE_FIREBASE:
    try:
        with open(progress_file, "r") as f:
            progress = json.load(f)
            last_processed_page = progress.get("page", 1)
            last_processed_actor = progress.get("actor_id")
            firebase_writes = progress.get("writes", 0)
            firebase_reads = progress.get("reads", 0)
            print(f"Resuming Firebase upload from page {last_processed_page}, actor {last_processed_actor}")
    except Exception as e:
        print(f"Error loading progress: {e}")

# Main data fetching loop
for page in range(last_processed_page, TOTAL_PAGES + 1):
    print(f"Processing page {page}/{TOTAL_PAGES}")
    
    # Get page of popular actors
    params = {
        "api_key": TMDB_API_KEY,
        "page": page
    }
    data = make_api_request(POPULAR_ACTORS_URL, params)
    
    if not data:
        print(f"Failed to fetch page {page}. Trying again later.")
        time.sleep(30)  # Wait longer before retrying the page
        continue
    
    for person in data.get("results", []):
        actor_id = person["id"]
        
        # Skip previously processed actors
        if actor_id in processed_actors:
            continue
            
        processed_actors.add(actor_id)
        
        actor_name = person["name"]
        popularity = person.get("popularity", 0)
        profile_path = person.get("profile_path", "")
        
        print(f"Fetching data for {actor_name} (ID: {actor_id})")
        
        # Step 1: Get detailed person info
        details_params = {"api_key": TMDB_API_KEY}
        details_data = make_api_request(ACTOR_DETAILS_URL_TEMPLATE.format(actor_id), details_params)
        
        place_of_birth = "Unknown"
        known_regions = []
        
        if details_data:
            place_of_birth = details_data.get("place_of_birth", "Unknown")
            
            # Update profile_path if missing from popular actors list
            if not profile_path and details_data.get("profile_path"):
                profile_path = details_data.get("profile_path")
            
            # Handle None values
            if place_of_birth is None:
                place_of_birth = "Unknown"
            
            # Determine regions based on place of birth
            for region_code, region_name in REGIONS.items():
                if place_of_birth != "Unknown" and region_name in place_of_birth:
                    known_regions.append(region_code)
            
            # If no specific region matched, mark as OTHER
            if not known_regions and place_of_birth != "Unknown":
                known_regions.append("OTHER")
            
            # If popularity is above threshold, mark as globally recognized
            if popularity >= GLOBAL_POPULARITY_THRESHOLD:
                known_regions.append("GLOBAL")
        
        # Step 2: Get movie credits
        credits_params = {"api_key": TMDB_API_KEY}
        credits_data = make_api_request(ACTOR_MOVIE_CREDITS_URL_TEMPLATE.format(actor_id), credits_params)
        
        movie_credits = []
        
        if credits_data:
            for credit in credits_data.get("cast", []):
                # Only add movies above popularity threshold
                if credit.get("popularity", 0) > 1.5:
                    movie_id = credit["id"]
                    poster_path = credit.get("poster_path", "")
                    
                    # Check MCU status from cache first
                    is_mcu = False
                    if movie_id in mcu_cache['movie']:
                        is_mcu = mcu_cache['movie'][movie_id]
                    else:
                        # Get individual movie details to check production companies
                        movie_params = {"api_key": TMDB_API_KEY}
                        movie_data = make_api_request(f"{BASE_URL}/movie/{movie_id}", movie_params)
                        
                        if movie_data:
                            production_companies = movie_data.get("production_companies", [])
                            
                            # Check if Marvel Studios is a production company
                            for company in production_companies:
                                if "Marvel Studios" in company.get("name", ""):
                                    is_mcu = True
                                    break
                            
                            # Save to cache
                            mcu_cache['movie'][movie_id] = is_mcu
                    
                    # Add to movie credits with MCU flag
                    movie_credits.append({
                        "id": movie_id,
                        "title": credit.get("title", ""),
                        "character": credit.get("character", ""),
                        "popularity": credit.get("popularity", 0),
                        "release_date": credit.get("release_date", ""),
                        "poster_path": poster_path,
                        "is_mcu": is_mcu
                    })
                    
                    # More controlled rate limiting
                    if movie_id not in mcu_cache['movie']:
                        time.sleep(0.25)  # 250ms between new movie lookups
        
        # Step 3: Get TV credits
        tv_credits_params = {"api_key": TMDB_API_KEY}
        tv_credits_data = make_api_request(ACTOR_TV_CREDITS_URL_TEMPLATE.format(actor_id), tv_credits_params)
        
        tv_credits = []
        if tv_credits_data:
            for credit in tv_credits_data.get("cast", []):
                if credit.get("popularity", 0) > 1.5:
                    tv_id = credit["id"]
                    poster_path = credit.get("poster_path", "")
                    
                    # Check MCU status from cache first
                    is_mcu = False
                    if tv_id in mcu_cache['tv']:
                        is_mcu = mcu_cache['tv'][tv_id]
                    else:
                        # Get TV show details to check production companies
                        tv_params = {"api_key": TMDB_API_KEY}
                        tv_data = make_api_request(f"{BASE_URL}/tv/{tv_id}", tv_params)
                        
                        if tv_data:
                            production_companies = tv_data.get("production_companies", [])
                            
                            # Check if Marvel Studios is a production company
                            for company in production_companies:
                                if "Marvel Studios" in company.get("name", ""):
                                    is_mcu = True
                                    break
                                
                                # Also check for Marvel Television
                                if "Marvel Television" in company.get("name", ""):
                                    is_mcu = True
                                    break
                            
                            # Save to cache
                            mcu_cache['tv'][tv_id] = is_mcu
                    
                    tv_credits.append({
                        "id": tv_id,
                        "name": credit.get("name", ""),
                        "character": credit.get("character", ""),
                        "popularity": credit.get("popularity", 0),
                        "first_air_date": credit.get("first_air_date", ""),
                        "poster_path": poster_path,
                        "is_mcu": is_mcu
                    })
                    
                    # More controlled rate limiting
                    if tv_id not in mcu_cache['tv']:
                        time.sleep(0.25)  # 250ms between new TV lookups
        
        # Create actor data object for Firebase
        actor_data = {
            "name": actor_name,
            "popularity": popularity,
            "profile_path": profile_path,
            "place_of_birth": place_of_birth
        }
        
        # Upload to Firebase (if enabled)
        upload_to_firebase(actor_id, actor_data, movie_credits, tv_credits, known_regions)
            
        # Insert data into appropriate region databases
        for region in known_regions:
            if region not in databases:
                continue
                
            conn, cursor = databases[region]
            
            # Clean text for SQL
            safe_name = actor_name.replace("'", "''")
            safe_place = place_of_birth.replace("'", "''") if place_of_birth else "Unknown"
            
            # Insert actor data (no is_mcu flag here anymore)
            cursor.execute(f'''
            INSERT OR REPLACE INTO actors 
            (id, name, popularity, profile_path, place_of_birth)
            VALUES (
                {actor_id}, 
                '{safe_name}', 
                {popularity}, 
                '{profile_path}', 
                '{safe_place}'
            )
            ''')
            
            # Insert region data for this actor
            for r in known_regions:
                cursor.execute(f'''
                INSERT OR REPLACE INTO actor_regions (actor_id, region)
                VALUES ({actor_id}, '{r}')
                ''')
            
            # Insert movie credits (with is_mcu flag)
            for movie in movie_credits:
                safe_title = movie["title"].replace("'", "''")
                safe_character = movie["character"].replace("'", "''")
                
                cursor.execute(f'''
                INSERT OR REPLACE INTO movie_credits 
                (id, actor_id, title, character, popularity, release_date, poster_path, is_mcu)
                VALUES (
                    {movie["id"]}, 
                    {actor_id}, 
                    '{safe_title}', 
                    '{safe_character}', 
                    {movie["popularity"]}, 
                    '{movie["release_date"]}', 
                    '{movie["poster_path"]}',
                    {1 if movie["is_mcu"] else 0}
                )
                ''')
            
            # Insert TV credits (with is_mcu flag)
            for tv in tv_credits:
                safe_name = tv["name"].replace("'", "''")
                safe_character = tv["character"].replace("'", "''")
                
                cursor.execute(f'''
                INSERT OR REPLACE INTO tv_credits 
                (id, actor_id, name, character, popularity, first_air_date, poster_path, is_mcu)
                VALUES (
                    {tv["id"]}, 
                    {actor_id}, 
                    '{safe_name}', 
                    '{safe_character}', 
                    {tv["popularity"]}, 
                    '{tv["first_air_date"]}', 
                    '{tv["poster_path"]}',
                    {1 if tv["is_mcu"] else 0}
                )
                ''')
            
            conn.commit()
        
        # Save progress information
        if ENABLE_FIREBASE:
            with open(progress_file, "w") as f:
                json.dump({
                    "page": page,
                    "actor_id": actor_id,
                    "writes": firebase_writes,
                    "reads": firebase_reads
                }, f)
        
        # Delay between actors
        time.sleep(0.5)
    
    # Delay between pages
    time.sleep(1)
    print(f"Completed page {page}/{TOTAL_PAGES}")

# Optimize databases and close connections
for region, (conn, cursor) in databases.items():
    # Create indexes for better performance
    cursor.execute("CREATE INDEX idx_movie_credits_actor ON movie_credits (actor_id)")
    cursor.execute("CREATE INDEX idx_movie_credits_mcu ON movie_credits (is_mcu)")
    cursor.execute("CREATE INDEX idx_tv_credits_actor ON tv_credits (actor_id)")
    cursor.execute("CREATE INDEX idx_tv_credits_mcu ON tv_credits (is_mcu)")
    cursor.execute("CREATE INDEX idx_actor_regions ON actor_regions (region)")
    
    # Optimize database
    cursor.execute("VACUUM")
    conn.commit()
    conn.close()
    
    print(f"Database for region {region} saved successfully")

print("All data successfully updated and written to both SQLite databases and Firebase")
