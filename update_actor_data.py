#!/usr/bin/env python
import os
import requests
import json
import time
import sqlite3
import random
import pycountry
import sys
import datetime
from requests.exceptions import ConnectionError, Timeout, RequestException

# Retrieve your TMDB API key from environment variables
TMDB_API_KEY = os.environ.get("TMDB_API_KEY")
if not TMDB_API_KEY:
    raise Exception("TMDB_API_KEY not set in environment variables.")

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
TOTAL_PAGES = 1000

# Minimum popularity for movie/TV credits
MIN_CREDIT_POPULARITY = 1.0

# Add after other constants
CHECKPOINT_FILE = "actor-game/public/checkpoint.json"
MAX_RUNTIME_HOURS = 4  # Exit after this many hours to allow clean completion

# Expanded region approach
# Create regions dictionary dynamically with all countries
REGIONS = {}

# Add global region
REGIONS["GLOBAL"] = {"name": "Global", "countries": [], "threshold": 25}

# Add continental/regional groups
CONTINENTS = {
    "NAMERICA": {"name": "North America", "countries": ["US", "CA", "MX"], "threshold": 15},
    "EUROPE": {"name": "Europe", "threshold": 15},
    "ASIA": {"name": "Asia", "threshold": 15},
    "SAMERICA": {"name": "South America", "threshold": 15},
    "AFRICA": {"name": "Africa", "threshold": 15},
    "OCEANIA": {"name": "Oceania", "threshold": 15}
}

# Add all these regions
REGIONS.update(CONTINENTS)

# Add individual countries
for country in pycountry.countries:
    code = country.alpha_2
    REGIONS[code] = {
        "name": country.name,
        "countries": [code],
        "threshold": 8,  # Lower threshold for individual countries
        "continent": None  # Placeholder for continent mapping
    }

# Helper function to determine which continent a country belongs to
def get_continent(country_code):
    """Determine which continent a country belongs to"""
    # Basic mapping of some common countries to continents
    europe_codes = ['GB', 'FR', 'DE', 'IT', 'ES', 'PT', 'BE', 'NL', 'CH', 'AT', 'SE', 'NO', 'DK', 'FI', 'PL']
    namerica_codes = ['US', 'CA', 'MX']
    samerica_codes = ['BR', 'AR', 'CO', 'PE', 'CL', 'VE']
    asia_codes = ['CN', 'JP', 'KR', 'IN', 'TH', 'VN', 'MY', 'ID', 'PH', 'SG']
    oceania_codes = ['AU', 'NZ']
    africa_codes = ['ZA', 'NG', 'EG', 'MA', 'KE']
    
    if country_code in europe_codes:
        return 'EUROPE'
    elif country_code in namerica_codes:
        return 'NAMERICA'
    elif country_code in samerica_codes:
        return 'SAMERICA'
    elif country_code in asia_codes:
        return 'ASIA'
    elif country_code in oceania_codes:
        return 'OCEANIA'
    elif country_code in africa_codes:
        return 'AFRICA'
    else:
        return 'OTHER'

# Helper function to get threshold value for a country
def get_country_threshold(country_code):
    """Get the threshold value for a specific country"""
    if country_code in REGIONS:
        return REGIONS[country_code].get("threshold", 8)  # Default to 8 if not specified
    else:
        return 8  # Default threshold for countries not in REGIONS

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

def calculate_years_active(movie_credits, tv_credits):
    """Calculate how many years an actor has been active based on their credits"""
    all_dates = []
    
    # Extract movie dates
    for movie in movie_credits:
        date_str = movie.get("release_date")
        if date_str and len(date_str) >= 4:
            try:
                year = int(date_str[:4])
                if 1900 <= year <= 2030:  # Basic validation
                    all_dates.append(year)
            except ValueError:
                pass
    
    # Extract TV dates
    for tv in tv_credits:
        date_str = tv.get("first_air_date")
        if date_str and len(date_str) >= 4:
            try:
                year = int(date_str[:4])
                if 1900 <= year <= 2030:  # Basic validation
                    all_dates.append(year)
            except ValueError:
                pass
    
    if not all_dates:
        return 1  # Default to 1 year if no valid dates
    
    current_year = time.localtime().tm_year
    earliest_year = min(all_dates)
    latest_year = max(all_dates)
    
    # Calculate years active (minimum 1 year)
    years_active = max(1, latest_year - earliest_year + 1)
    
    # Cap at reasonable maximum (e.g., 60 years)
    return min(years_active, 60)

def calculate_credit_popularity(movie_credits, tv_credits):
    """Calculate average popularity of an actor's credits"""
    all_popularity_scores = []
    
    for movie in movie_credits:
        pop = movie.get("popularity", 0)
        if pop > 0:
            all_popularity_scores.append(pop)
    
    for tv in tv_credits:
        pop = tv.get("popularity", 0)
        if pop > 0:
            all_popularity_scores.append(pop)
    
    if not all_popularity_scores:
        return 0
    
    return sum(all_popularity_scores) / len(all_popularity_scores)

def calculate_custom_popularity(tmdb_popularity, num_credits, years_active, avg_credit_popularity):
    """Calculate a more balanced popularity score"""
    longevity_factor = min(years_active / 10, 1.0)  # Cap at 10 years
    credits_factor = min(num_credits / 20, 1.0)     # Cap at 20 credits
    
    # Balanced formula giving weight to all factors
    custom_score = (
        tmdb_popularity * 0.3 +                # Recent popularity (30%)
        avg_credit_popularity * 0.2 +          # Quality of work (20%) 
        credits_factor * 25 +                  # Quantity of work (25%)
        longevity_factor * 25                  # Career longevity (25%)
    )
    
    return custom_score

def setup_database():
    """Create a single SQLite database with region flags"""
    db_path = "actor-game/public/actors.db"
    
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
        tmdb_popularity REAL,  -- Keep for reference
        profile_path TEXT,
        place_of_birth TEXT,
        years_active INTEGER,
        credit_count INTEGER
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE actor_regions (
        actor_id INTEGER,
        region TEXT,
        popularity_score REAL,
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

def normalize_image_path(path):
    """Ensure image path has leading slash if it exists"""
    if not path:
        return ""
    return path if path.startswith('/') else f"/{path}"

def load_checkpoint():
    """Load previous execution progress from checkpoint file"""
    if not os.path.exists(CHECKPOINT_FILE):
        print("No checkpoint file found, starting fresh")
        return {
            "last_page": 0,
            "processed_actors": [],
            "last_update": None,
            "completed": False
        }
    
    try:
        with open(CHECKPOINT_FILE, 'r') as f:
            checkpoint = json.load(f)
            print(f"Resuming from page {checkpoint['last_page'] + 1}")
            return checkpoint
    except Exception as e:
        print(f"Error loading checkpoint: {e}")
        return {
            "last_page": 0,
            "processed_actors": [],
            "last_update": None,
            "completed": False
        }

def save_checkpoint(page, processed_actors, completed=False):
    """Save current progress to checkpoint file"""
    os.makedirs(os.path.dirname(CHECKPOINT_FILE), exist_ok=True)
    
    checkpoint = {
        "last_page": page,
        "processed_actors": list(processed_actors),
        "last_update": time.strftime("%Y-%m-%d %H:%M:%S"),
        "completed": completed
    }
    
    with open(CHECKPOINT_FILE, 'w') as f:
        json.dump(checkpoint, f)
    
    print(f"Checkpoint saved at page {page}")

# Set up single database
conn, cursor = setup_database()

# Load checkpoint information
checkpoint = load_checkpoint()
start_page = checkpoint.get("last_page", 0) + 1
processed_actors = set(checkpoint.get("processed_actors", []))

# Track start time for runtime limit
start_time = time.time()
max_runtime_seconds = MAX_RUNTIME_HOURS * 60 * 60

print(f"Starting data collection from page {start_page}/{TOTAL_PAGES}")
print(f"Already processed {len(processed_actors)} actors")

# Add this near the top of the script where other data structures are initialized
mcu_cache = {'movie': {}, 'tv': {}, 'person': {}}

# Make sure to load it from file if it exists
try:
    with open('mcu_cache.json', 'r') as f:
        mcu_data = json.load(f)
        # Convert to dictionaries with proper type conversion for keys
        mcu_cache = {
            'movie': {int(k): v for k, v in mcu_data.get('movie', {}).items()},
            'tv': {int(k): v for k, v in mcu_data.get('tv', {}).items()},
            'person': {int(k): v for k, v in mcu_data.get('person', {}).items()}
        }
    print("Loaded MCU cache")
except FileNotFoundError:
    print("No MCU cache found, starting with empty cache")

# Main data fetching loop
for page in range(start_page, TOTAL_PAGES + 1):
    print(f"Processing page {page}/{TOTAL_PAGES}")
    
    # Check runtime - exit gracefully if we're approaching limit
    elapsed_seconds = time.time() - start_time
    if elapsed_seconds > max_runtime_seconds:
        print(f"Approaching maximum runtime of {MAX_RUNTIME_HOURS} hours. Saving checkpoint and exiting.")
        save_checkpoint(page - 1, processed_actors)
        print("Execution will continue in the next workflow run")
        # Early exit - database will remain valid with partial data
        sys.exit(0)
    
    # Get page of popular actors
    params = {
        "api_key": TMDB_API_KEY,
        "page": page
    }
    data = make_api_request(POPULAR_ACTORS_URL, params)
    
    if not data:
        print(f"Failed to fetch page {page}. Trying again later.")
        time.sleep(30)
        continue
    
    for person in data.get("results", []):
        actor_id = person["id"]
        
        # Skip previously processed actors
        if actor_id in processed_actors:
            continue
            
        processed_actors.add(actor_id)
        
        actor_name = person["name"]
        tmdb_popularity = person.get("popularity", 0)
        profile_path = normalize_image_path(person.get("profile_path", ""))
        
        print(f"Fetching data for {actor_name} (ID: {actor_id})")
        
        # Step 1: Get detailed person info
        details_params = {"api_key": TMDB_API_KEY}
        details_data = make_api_request(ACTOR_DETAILS_URL_TEMPLATE.format(actor_id), details_params)
        
        place_of_birth = "Unknown"
        
        if details_data:
            place_of_birth = details_data.get("place_of_birth", "Unknown")
            
            # Update profile_path if missing from popular actors list
            if not profile_path and details_data.get("profile_path"):
                profile_path = normalize_image_path(details_data.get("profile_path"))
            
            # Handle None values
            if place_of_birth is None:
                place_of_birth = "Unknown"
        
        # Step 2: Get movie credits - THRESHOLD CHANGED TO 1.0
        credits_params = {"api_key": TMDB_API_KEY}
        credits_data = make_api_request(ACTOR_MOVIE_CREDITS_URL_TEMPLATE.format(actor_id), credits_params)
        
        movie_credits = []
        
        if credits_data:
            for credit in credits_data.get("cast", []):
                # Only add movies above popularity threshold - CHANGED FROM 1.5 TO 1.0
                if credit.get("popularity", 0) >= MIN_CREDIT_POPULARITY:
                    movie_id = credit["id"]
                    poster_path = normalize_image_path(credit.get("poster_path", ""))
                    
                    # Get character info
                    character = credit.get("character", "")
                    
                    # Check if character is self-appearance
                    if character.lower() in ['self', 'himself', 'herself']:
                        # Skip self appearances
                        continue
                        
                    # Skip documentaries
                    title = credit.get("title", "").lower()
                    if any(keyword in title for keyword in ['documentary', 'behind the scenes']):
                        continue
                    
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
                        "character": character,
                        "popularity": credit.get("popularity", 0),
                        "release_date": credit.get("release_date", ""),
                        "poster_path": poster_path,
                        "is_mcu": is_mcu
                    })
                    
                    # Rate limiting
                    if movie_id not in mcu_cache['movie']:
                        time.sleep(0.25)
        
        # Step 3: Get TV credits - THRESHOLD CHANGED TO 1.0
        tv_credits_params = {"api_key": TMDB_API_KEY}
        tv_credits_data = make_api_request(ACTOR_TV_CREDITS_URL_TEMPLATE.format(actor_id), tv_credits_params)
        
        tv_credits = []
        if tv_credits_data:
            for credit in tv_credits_data.get("cast", []):
                if credit.get("popularity", 0) >= MIN_CREDIT_POPULARITY:
                    tv_id = credit["id"]
                    poster_path = normalize_image_path(credit.get("poster_path", ""))
                    
                    # Get character info
                    character = credit.get("character", "")
                    
                    # Skip if the actor is playing themselves
                    if character.lower() in ['self', 'himself', 'herself']:
                        continue
                    
                    # Skip if the TV title contains keywords suggesting a non-scripted format
                    tv_name = credit.get("name", "").lower()
                    excluded_keywords = ['talk', 'game', 'reality', 'news', 'award']
                    if any(keyword in tv_name for keyword in excluded_keywords):
                        continue
                    
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
                            
                            # Check for Marvel studios or television
                            for company in production_companies:
                                if "Marvel" in company.get("name", ""):
                                    is_mcu = True
                                    break
                            
                            # Save to cache
                            mcu_cache['tv'][tv_id] = is_mcu
                    
                    tv_credits.append({
                        "id": tv_id,
                        "name": credit.get("name", ""),
                        "character": character,
                        "popularity": credit.get("popularity", 0),
                        "first_air_date": credit.get("first_air_date", ""),
                        "poster_path": poster_path,
                        "is_mcu": is_mcu
                    })
                    
                    # Rate limiting
                    if tv_id not in mcu_cache['tv']:
                        time.sleep(0.25)
        
        # Calculate number of credits
        num_credits = len(movie_credits) + len(tv_credits)
        
        # Calculate years active
        years_active = calculate_years_active(movie_credits, tv_credits)
        
        # Calculate average credit popularity
        avg_credit_popularity = calculate_credit_popularity(movie_credits, tv_credits)
        
        # Calculate custom popularity score
        custom_popularity = calculate_custom_popularity(
            tmdb_popularity, 
            num_credits,
            years_active,
            avg_credit_popularity
        )
        
        print(f"  TMDB Popularity: {tmdb_popularity:.2f}, Custom Popularity: {custom_popularity:.2f}")
        
        # Use custom_popularity for all further operations
        actor_regions, avg_scores = assign_actor_to_regions(
            {"id": actor_id, "name": actor_name, "popularity": custom_popularity},
            movie_credits,
            tv_credits
        )
        
        print(f"  Assigned {actor_name} to regions: {', '.join(actor_regions)}")
        
        # Insert data into the database with custom popularity as primary metric
        for region in actor_regions:
            # Clean text for SQL
            safe_name = actor_name.replace("'", "''")
            safe_place = place_of_birth.replace("'", "''") if place_of_birth else "Unknown"
            
            # Insert actor data - using custom_popularity as the main popularity field
            cursor.execute(f'''
            INSERT OR REPLACE INTO actors 
            (id, name, popularity, tmdb_popularity, profile_path, place_of_birth, years_active, credit_count)
            VALUES (
                {actor_id}, 
                '{safe_name}', 
                {custom_popularity},
                {tmdb_popularity}, 
                '{profile_path}', 
                '{safe_place}',
                {years_active},
                {num_credits}
            )
            ''')
            
            # Insert region data for this actor - using custom popularity
            cursor.execute(f"""
            INSERT OR REPLACE INTO actor_regions (actor_id, region, popularity_score)
            VALUES ({actor_id}, '{region}', {custom_popularity})
            """)
            
            # Insert movie credits
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
            
            # Insert TV credits
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
        
        # Delay between actors
        time.sleep(0.5)
    
    # Save checkpoint after each page
    save_checkpoint(page, processed_actors)
    
    # Delay between pages
    time.sleep(1)
    print(f"Completed page {page}/{TOTAL_PAGES}")

# Check if we've completed all pages before finalizing
checkpoint = load_checkpoint()
if not checkpoint.get('completed', False):
    print("Data collection is not complete. Will continue in next run.")
    sys.exit(0)

# Only perform database optimization and final steps when completed
print("All pages processed. Finalizing database...")

# Optimize database and create indexes
print("Creating indexes and optimizing database...")
cursor.execute("CREATE INDEX idx_actors_popularity ON actors (popularity DESC)")
cursor.execute("CREATE INDEX idx_movie_credits_actor ON movie_credits (actor_id)")
cursor.execute("CREATE INDEX idx_movie_credits_mcu ON movie_credits (is_mcu)")
cursor.execute("CREATE INDEX idx_tv_credits_actor ON tv_credits (actor_id)")
cursor.execute("CREATE INDEX idx_tv_credits_mcu ON tv_credits (is_mcu)")
cursor.execute("CREATE INDEX idx_actor_regions ON actor_regions (region)")

# Optimize database
cursor.execute("VACUUM")
conn.commit()
conn.close()

print("Database saved successfully")

# Add a flag file to indicate data status
with open("actor-game/public/data_source_info.json", "w") as f:
    json.dump({
        "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "sqlite_complete": True,
        "popularity_metric": "custom"
    }, f)

print("""
All data successfully updated:
- SQLite database saved to GitHub repository
- Using CUSTOM popularity metric instead of TMDB popularity
- Filtering out self-appearances in both movies and TV shows
- Including credits with popularity >= 1.0
""")