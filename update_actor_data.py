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

# =============================================================================
# ACTOR TO ACTOR GAME - DATA COLLECTION SCRIPT
# =============================================================================
# This script collects actor data from TMDB API, processes it, and creates an
# SQLite database to power the Actor-to-Actor game. It collects:
# - Actor details and popularity metrics
# - Movie and TV credits with character information
# - Regional assignments for actors based on their work and origin
# - MCU (Marvel Cinematic Universe) flags for special game modes
# 
# The script supports checkpointing to resume data collection across
# multiple executions, making it suitable for GitHub Actions workflows.
# =============================================================================

# Retrieve TMDB API key from environment variables
TMDB_API_KEY = os.environ.get("TMDB_API_KEY")
if not TMDB_API_KEY:
    raise Exception("TMDB_API_KEY not set in environment variables.")

# =============================================================================
# API CONFIGURATION
# =============================================================================
BASE_URL = "https://api.themoviedb.org/3"
POPULAR_ACTORS_URL = f"{BASE_URL}/person/popular"
ACTOR_MOVIE_CREDITS_URL_TEMPLATE = f"{BASE_URL}/person/{{}}/movie_credits"
ACTOR_TV_CREDITS_URL_TEMPLATE = f"{BASE_URL}/person/{{}}/tv_credits"
ACTOR_DETAILS_URL_TEMPLATE = f"{BASE_URL}/person/{{}}"

# Image configuration
IMAGE_BASE_URL = "https://image.tmdb.org/t/p/"
PROFILE_SIZE = "w185"
POSTER_SIZE = "w342"

# =============================================================================
# DATA COLLECTION SETTINGS
# =============================================================================
TOTAL_PAGES = 1000                # Number of pages of popular actors to fetch
MIN_CREDIT_POPULARITY = 1.0       # Minimum popularity for movie/TV credits to include
CHECKPOINT_FILE = "actor-game/public/checkpoint.json"
MAX_RUNTIME_HOURS = 4             # Exit after this many hours to allow clean completion

# =============================================================================
# REGION CONFIGURATION
# =============================================================================
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

# Add individual countries from pycountry library
for country in pycountry.countries:
    code = country.alpha_2
    REGIONS[code] = {
        "name": country.name,
        "countries": [code],
        "threshold": 8,  # Lower threshold for individual countries
        "continent": None  # Placeholder for continent mapping
    }

# =============================================================================
# UTILITY FUNCTIONS - REGION MAPPING
# =============================================================================
def get_continent(country_code):
    """
    Determine which continent a country belongs to based on its code
    
    Args:
        country_code: ISO 2-letter country code
        
    Returns:
        String continent identifier or 'OTHER'
    """
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

def get_country_threshold(country_code):
    """
    Get the threshold value for a specific country
    
    Args:
        country_code: ISO 2-letter country code
        
    Returns:
        Integer threshold value
    """
    if country_code in REGIONS:
        return REGIONS[country_code].get("threshold", 8)  # Default to 8 if not specified
    else:
        return 8  # Default threshold for countries not in REGIONS

# =============================================================================
# ACTOR REGION ASSIGNMENT
# =============================================================================
def assign_actor_to_regions(actor, movie_credits, tv_credits, details_data):
    """
    Improved regional assignment considering birth country and actual production countries
    
    This function determines which regions an actor should be associated with based on:
    1. Their birth place/country of origin
    2. Production countries of their movies
    3. Overall popularity (globally significant actors are added to major regions)
    
    Args:
        actor: Dictionary with actor details
        movie_credits: List of movie credits
        tv_credits: List of TV credits
        details_data: Dictionary with additional actor details
        
    Returns:
        tuple: (assigned_regions, region_scores)
    """
    assigned_regions = ["GLOBAL"]  # Always include global
    region_scores = {"GLOBAL": actor["popularity"]}
    
    # 1. Assign based on place of birth (highest priority)
    if details_data and details_data.get("place_of_birth"):
        birth_place = details_data["place_of_birth"]
        
        # Check for common countries in birth place
        if any(country in birth_place for country in ["United Kingdom", "England", "Scotland", "Wales"]):
            assigned_regions.append("UK")
            region_scores["UK"] = actor["popularity"] * 1.2  # Boost for home country
        
        elif "United States" in birth_place:
            assigned_regions.append("US") 
            region_scores["US"] = actor["popularity"] * 1.2
            
        # Add more countries as needed
    
    # 2. Fetch ACTUAL production countries for movies
    production_countries = {}
    for movie in movie_credits:
        # Need to fetch full movie details to get production_countries
        movie_data = make_api_request(f"{BASE_URL}/movie/{movie['id']}", {"api_key": TMDB_API_KEY})
        if movie_data and "production_countries" in movie_data:
            for country in movie_data["production_countries"]:
                code = country["iso_3166_1"]
                production_countries[code] = production_countries.get(code, 0) + 1
    
    # 3. Assign to countries where they've worked extensively
    for country_code, count in production_countries.items():
        if count >= 3:  # Actor has 3+ productions in country
            if country_code not in assigned_regions:
                assigned_regions.append(country_code)
                region_scores[country_code] = actor["popularity"]
    
    # 4. Keep global popularity logic for extremely popular actors
    # Ensures A-list actors appear in key regional databases
    if actor["popularity"] > 25:
        for region in ["US", "UK", "CA", "AU", "FR", "DE"]:
            if region not in assigned_regions:
                assigned_regions.append(region)
                region_scores[region] = actor["popularity"]
    
    return assigned_regions, region_scores

# =============================================================================
# API INTERACTION
# =============================================================================
def make_api_request(url, params, max_retries=5):
    """
    Make API request with retry logic and exponential backoff
    
    Handles various error conditions including rate limiting,
    connection issues, and timeouts with smart retries.
    
    Args:
        url: API endpoint URL
        params: Dictionary of query parameters
        max_retries: Maximum number of retry attempts
        
    Returns:
        Dictionary with API response or None if failed
    """
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
            # Implement exponential backoff with jitter
            wait_time = 2 ** retries + random.uniform(0, 1)
            print(f"Request failed: {e}. Retrying in {wait_time:.2f} seconds...")
            time.sleep(wait_time)
            retries += 1
    
    print(f"Failed after {max_retries} retries. Skipping this request.")
    return None

# =============================================================================
# POPULARITY METRICS CALCULATION
# =============================================================================
def calculate_years_active(movie_credits, tv_credits):
    """
    Calculate how many years an actor has been active based on their credits
    
    Args:
        movie_credits: List of movie credits
        tv_credits: List of TV credits
        
    Returns:
        Integer representing years active (1-60)
    """
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
    """
    Calculate average popularity of an actor's credits
    
    Args:
        movie_credits: List of movie credits
        tv_credits: List of TV credits
        
    Returns:
        Float representing average popularity
    """
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
    """
    Calculate a more balanced popularity score that considers:
    - Recent TMDB popularity (trending status)
    - Number of credits (prolific career)
    - Years active (longevity)
    - Average credit popularity (quality of work)
    
    Args:
        tmdb_popularity: Raw popularity from TMDB
        num_credits: Number of credits
        years_active: Years active in industry
        avg_credit_popularity: Average popularity of credits
        
    Returns:
        Float representing custom balanced popularity score
    """
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

# =============================================================================
# DATABASE SETUP
# =============================================================================
def setup_database():
    """
    Create a single SQLite database with all required tables
    
    Returns:
        tuple: (connection, cursor)
    """
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

# =============================================================================
# UTILITY FUNCTIONS
# =============================================================================
def normalize_image_path(path):
    """
    Ensure image path has leading slash if it exists
    
    Args:
        path: Image path string
        
    Returns:
        Normalized path string
    """
    if not path:
        return ""
    return path if path.startswith('/') else f"/{path}"

# =============================================================================
# CHECKPOINT MANAGEMENT
# =============================================================================
def load_checkpoint():
    """
    Load previous execution progress from checkpoint file
    
    Returns:
        Dictionary with checkpoint data
    """
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
    """
    Save current progress to checkpoint file
    
    Args:
        page: Current page number
        processed_actors: Set of processed actor IDs
        completed: Whether data collection is complete
    """
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

# =============================================================================
# INITIALIZATION
# =============================================================================
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

# Initialize MCU cache to avoid repeat API calls for MCU detection
mcu_cache = {'movie': {}, 'tv': {}, 'person': {}}

# Make sure to load MCU cache from file if it exists
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

# =============================================================================
# MAIN DATA COLLECTION LOOP
# =============================================================================
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
                    
                    # Skip self-appearances which aren't useful for the game
                    if character.lower() in ['self', 'himself', 'herself']:
                        continue
                        
                    # Skip documentaries which aren't useful for the game
                    title = credit.get("title", "").lower()
                    if any(keyword in title for keyword in ['documentary', 'behind the scenes']):
                        continue
                    
                    # Check MCU status from cache first (for "exclude MCU" game mode)
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
                            
                            # Save to cache to avoid redundant API calls
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
                    
                    # Rate limiting for new API calls
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
                    
                    # Skip non-scripted TV formats
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
        
        # Calculate metrics for custom popularity score
        num_credits = len(movie_credits) + len(tv_credits)
        years_active = calculate_years_active(movie_credits, tv_credits)
        avg_credit_popularity = calculate_credit_popularity(movie_credits, tv_credits)
        
        # Calculate custom popularity score based on multiple factors
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
            tv_credits,
            details_data  # Pass in the details data you fetched earlier
        )
        
        print(f"  Assigned {actor_name} to regions: {', '.join(actor_regions)}")
        
        # =============================================================================
        # DATABASE INSERTION
        # =============================================================================
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
        
        # Delay between actors to respect API rate limits
        time.sleep(0.5)
    
    # Save checkpoint after each page
    save_checkpoint(page, processed_actors)
    
    # Delay between pages
    time.sleep(1)
    print(f"Completed page {page}/{TOTAL_PAGES}")

# =============================================================================
# FINALIZATION
# =============================================================================
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