import os
import requests
import json
import time
import sqlite3
import random
import pycountry
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
TOTAL_PAGES = 100

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

# Database setup function - create a single database
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
        profile_path TEXT,
        place_of_birth TEXT
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE actor_regions (
        actor_id INTEGER,
        region TEXT,
        popularity_score REAL,  # Add this column
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

# Add this helper function:
def normalize_image_path(path):
    """Ensure image path has leading slash if it exists"""
    if not path:
        return ""
    return path if path.startswith('/') else f"/{path}"

# Set up single database instead of multiple region-specific ones
conn, cursor = setup_database()

# Track processed actors to avoid duplicates
processed_actors = set()

# Cache for movie/TV MCU status to reduce API calls
mcu_cache = {
    'movie': {},  # movie_id -> is_mcu
    'tv': {}      # tv_id -> is_mcu
}

# Cache for release/availability data to reduce API calls
release_cache = {
    'movie': {},  # movie_id -> countries
    'tv': {}      # tv_id -> countries
}

def calculate_regional_popularity(actor_id, movie_credits, tv_credits):
    """Calculate an actor's popularity in different regions based on their work"""
    region_scores = {region: 0 for region in REGIONS.keys()}
    region_release_counts = {region: 0 for region in REGIONS.keys()}
    
    # Process movie credits
    for movie in movie_credits:
        movie_id = movie["id"]
        movie_popularity = movie["popularity"]
        
        # Get regional release data for this movie
        release_data = get_movie_release_data(movie_id)
        
        # For each region, check if the movie was released there and add to popularity
        for region, info in REGIONS.items():
            if region == "GLOBAL":
                # Global score uses overall movie popularity
                region_scores["GLOBAL"] += movie_popularity
                region_release_counts["GLOBAL"] += 1
                continue
                
            # Check if movie was released in any of the region's countries
            released_in_region = False
            for country in info.get("countries", []):
                if country in release_data:
                    released_in_region = True
                    break
            
            if released_in_region:
                region_scores[region] += movie_popularity
                region_release_counts[region] += 1
    
    # Process TV credits similarly
    for tv in tv_credits:
        tv_id = tv["id"]
        tv_popularity = tv["popularity"]
        
        # Get regional availability data for this TV show
        availability_data = get_tv_availability_data(tv_id)
        
        # For each region, check if the TV show was available there
        for region, info in REGIONS.items():
            if region == "GLOBAL":
                # Global score uses overall TV popularity
                region_scores["GLOBAL"] += tv_popularity
                region_release_counts["GLOBAL"] += 1
                continue
                
            # Check if TV show was available in any of the region's countries
            available_in_region = False
            for country in info.get("countries", []):
                if country in availability_data:
                    available_in_region = True
                    break
            
            if available_in_region:
                region_scores[region] += tv_popularity
                region_release_counts[region] += 1
    
    # Calculate average popularity scores per region
    avg_scores = {}
    for region in REGIONS.keys():
        if region_release_counts[region] > 0:
            avg_scores[region] = region_scores[region] / region_release_counts[region]
        else:
            avg_scores[region] = 0
    
    # Determine regions where actor exceeds popularity threshold
    qualified_regions = []
    for region, info in REGIONS.items():
        # For GLOBAL, need a higher threshold and also consider count of credits
        if region == "GLOBAL":
            if avg_scores[region] >= info["threshold"] and region_release_counts[region] >= 5:
                qualified_regions.append(region)
        # For other regions, meet threshold and have at least 3 releases there
        elif avg_scores[region] >= info["threshold"] and region_release_counts[region] >= 3:
            qualified_regions.append(region)
    
    # If no regions qualify, add region with highest score
    if not qualified_regions and region_release_counts:
        best_region = max(avg_scores.items(), key=lambda x: x[1])[0]
        qualified_regions.append(best_region)
    
    # Always ensure actor is in at least one region
    if not qualified_regions:
        qualified_regions.append("OTHER")
    
    return qualified_regions, avg_scores

def assign_countries_to_actor(release_data):
    """Maps all countries where an actor's work was released"""
    country_scores = {}
    
    # Process all countries in release data
    for movie_id, countries in release_data.items():
        for country_code in countries:
            if country_code not in country_scores:
                country_scores[country_code] = 0
            country_scores[country_code] += 1
    
    # Apply thresholds based on country size/market
    qualified_countries = []
    for country, score in country_scores.items():
        # Get country-specific threshold from configuration
        threshold = get_country_threshold(country)
        if score >= threshold:
            qualified_countries.append(country)
    
    return qualified_countries

def assign_actor_to_regions(actor_data, movie_credits, tv_credits):
    """Determine which regions an actor is popular in with tiered approach"""
    # Calculate base regional scores
    regional_scores, avg_scores = calculate_regional_popularity(actor_data["id"], movie_credits, tv_credits)
    
    # Get overall popularity from TMDB
    overall_popularity = actor_data.get("popularity", 0)
    
    # Global tier assignments based on overall popularity
    if overall_popularity >= 25:  # Mega stars
        if "GLOBAL" not in regional_scores:
            regional_scores.append("GLOBAL")
    
    # Franchise analysis - actors in major franchises get wider recognition
    us_franchise_keywords = ["Marvel", "Star Wars", "Harry Potter", "Mission", 
                            "Fast & Furious", "Jurassic", "Batman", "Superman"]
    
    mcu_credits = sum(1 for m in movie_credits if m.get("is_mcu", False))
    franchise_count = mcu_credits
    
    # Check for other franchises
    for movie in movie_credits:
        title = movie.get("title", "")
        for keyword in us_franchise_keywords:
            if keyword in title:
                franchise_count += 1
                break
    
    # If they're in multiple franchises, add key regions
    if franchise_count >= 2:
        if "US" not in regional_scores:
            regional_scores.append("US")
        if "UK" not in regional_scores:
            regional_scores.append("UK")
    
    # Special case for actors in Asian entertainment industries
    asian_keywords = ["K-drama", "C-drama", "Bollywood", "anime", "manga"]
    asian_content = False
    
    for tv in tv_credits:
        tv_name = tv.get("name", "").lower()
        for keyword in asian_keywords:
            if keyword.lower() in tv_name:
                asian_content = True
                break
    
    if asian_content and "ASIA" not in regional_scores:
        regional_scores.append("ASIA")
    
    # Make sure actor is assigned to at least one region
    if not regional_scores:
        # Fallback to region with most credits
        regional_scores.append("OTHER")
    
    return regional_scores, avg_scores

def get_movie_release_data(movie_id):
    """Get release data for a movie by country with caching"""
    if movie_id in release_cache['movie']:
        return release_cache['movie'][movie_id]
    
    countries = {}
    release_url = f"{BASE_URL}/movie/{movie_id}/release_dates"
    params = {"api_key": TMDB_API_KEY}
    
    data = make_api_request(release_url, params)
    if data and "results" in data:
        for result in data["results"]:
            country_code = result.get("iso_3166_1")
            if country_code:
                countries[country_code] = True
    
    # Cache the result
    release_cache['movie'][movie_id] = countries
    
    # Add delay to avoid rate limiting
    time.sleep(0.25)
    
    return countries

def get_tv_availability_data(tv_id):
    """Get countries where a TV show was available with caching"""
    if tv_id in release_cache['tv']:
        return release_cache['tv'][tv_id]
    
    countries = {}
    
    # First try content ratings which includes country info
    ratings_url = f"{BASE_URL}/tv/{tv_id}/content_ratings"
    params = {"api_key": TMDB_API_KEY}
    
    data = make_api_request(ratings_url, params)
    if data and "results" in data:
        for result in data["results"]:
            country_code = result.get("iso_3166_1")
            if country_code:
                countries[country_code] = True
    
    # If no countries found, try alternative data
    if not countries:
        # Check translated info as a proxy for availability
        translations_url = f"{BASE_URL}/tv/{tv_id}/translations"
        data = make_api_request(translations_url, params)
        
        if data and "translations" in data:
            for translation in data["translations"]:
                country = translation.get("iso_3166_1")
                if country:
                    countries[country] = True
    
    # Cache the result
    release_cache['tv'][tv_id] = countries
    
    # Add delay to avoid rate limiting
    time.sleep(0.25)
    
    return countries

# Main data fetching loop
for page in range(1, TOTAL_PAGES + 1):
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
        profile_path = normalize_image_path(person.get("profile_path", ""))
        
        # Construct profile image URL
        profile_image_url = f"{IMAGE_BASE_URL}{PROFILE_SIZE}{profile_path}" if profile_path else ""
        
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
        
        # Step 2: Get movie credits
        credits_params = {"api_key": TMDB_API_KEY}
        credits_data = make_api_request(ACTOR_MOVIE_CREDITS_URL_TEMPLATE.format(actor_id), credits_params)
        
        movie_credits = []
        
        if credits_data:
            for credit in credits_data.get("cast", []):
                # Only add movies above popularity threshold
                if credit.get("popularity", 0) > 1.5:
                    movie_id = credit["id"]
                    poster_path = normalize_image_path(credit.get("poster_path", ""))
                    
                    # Construct poster image URL
                    poster_image_url = f"{IMAGE_BASE_URL}{POSTER_SIZE}{poster_path}" if poster_path else ""
                    
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
                    poster_path = normalize_image_path(credit.get("poster_path", ""))
                    
                    # Construct poster image URL
                    poster_image_url = f"{IMAGE_BASE_URL}{POSTER_SIZE}{poster_path}" if poster_path else ""
                    
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
        
        # Calculate regional popularity
        actor_regions, avg_scores = assign_actor_to_regions(
            {"id": actor_id, "name": actor_name, "popularity": popularity},
            movie_credits,
            tv_credits
        )
        
        # Log region assignments for debugging
        print(f"Assigned {actor_name} to regions: {', '.join(actor_regions)}")
        
        # Insert data into the database
        for region in actor_regions:
            # Clean text for SQL
            safe_name = actor_name.replace("'", "''")
            safe_place = place_of_birth.replace("'", "''") if place_of_birth else "Unknown"
            
            # Insert actor data
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
            cursor.execute(f"""
            INSERT OR REPLACE INTO actor_regions (actor_id, region, popularity_score)
            VALUES ({actor_id}, '{region}', {avg_scores[region]})
            """)
            
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
        
        # Delay between actors
        time.sleep(0.5)
    
    # Delay between pages
    time.sleep(1)
    print(f"Completed page {page}/{TOTAL_PAGES}")

# Optimize database and close connection
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

print("Database saved successfully")

# Add a flag file to indicate data status
with open("actor-game/public/data_source_info.json", "w") as f:
    json.dump({
        "last_updated": time.strftime("%Y-%m-%d %H:%M:%S"),
        "sqlite_complete": True,
    }, f)

print("""
All data successfully updated:
- SQLite database saved to GitHub repository
""")
