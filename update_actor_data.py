import os
import requests
import json
import time
import sqlite3

# Retrieve your TMDB API key from environment variables
TMDB_API_KEY = os.environ.get("TMDB_API_KEY")
if not TMDB_API_KEY:
    raise Exception("TMDB_API_KEY not set in environment variables.")

BASE_URL = "https://api.themoviedb.org/3"
POPULAR_ACTORS_URL = f"{BASE_URL}/person/popular"
ACTOR_MOVIE_CREDITS_URL_TEMPLATE = f"{BASE_URL}/person/{{}}/movie_credits"
ACTOR_TV_CREDITS_URL_TEMPLATE = f"{BASE_URL}/person/{{}}/tv_credits"
ACTOR_DETAILS_URL_TEMPLATE = f"{BASE_URL}/person/{{}}"

# Image base URL
IMAGE_BASE_URL = "https://image.tmdb.org/t/p/"
PROFILE_SIZE = "w185"  # Actor image size
POSTER_SIZE = "w342"   # Movie/TV poster size

# How many pages of popular actors to fetch
TOTAL_PAGES = 100  # adjust as needed

# Define regions/zones for actor categorization
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
    # Actors table
    cursor.execute('''
    CREATE TABLE actors (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        popularity REAL,
        profile_path TEXT,
        place_of_birth TEXT
    )
    ''')
    
    # Actor regions table (for future filtering)
    cursor.execute('''
    CREATE TABLE actor_regions (
        actor_id INTEGER,
        region TEXT,
        PRIMARY KEY (actor_id, region),
        FOREIGN KEY (actor_id) REFERENCES actors (id)
    )
    ''')
    
    # Movie credits table - note the is_mcu flag here on the movie
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
    
    # TV credits table - also adding is_mcu flag here
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

# Main data fetching loop
for page in range(1, TOTAL_PAGES + 1):
    params = {
        "api_key": TMDB_API_KEY,
        "page": page
    }
    response = requests.get(POPULAR_ACTORS_URL, params=params)
    if response.status_code != 200:
        print(f"Error fetching popular actors page {page}: {response.text}")
        continue
    
    data = response.json()
    for person in data.get("results", []):
        actor_id = person["id"]
        
        # Skip if already processed
        if actor_id in processed_actors:
            continue
            
        processed_actors.add(actor_id)
        
        actor_name = person["name"]
        popularity = person.get("popularity", 0)
        profile_path = person.get("profile_path", "")
        
        print(f"Fetching data for {actor_name} (ID: {actor_id})")

        # Step 1: Get detailed person info
        details_url = ACTOR_DETAILS_URL_TEMPLATE.format(actor_id)
        details_params = {"api_key": TMDB_API_KEY}
        details_response = requests.get(details_url, params=details_params)

        place_of_birth = "Unknown"
        known_regions = []

        if details_response.status_code == 200:
            details_data = details_response.json()
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
        else:
            print(f"Error fetching details for {actor_name}: {details_response.text}")

        # Step 2: Get movie credits
        credits_url = ACTOR_MOVIE_CREDITS_URL_TEMPLATE.format(actor_id)
        credits_params = {"api_key": TMDB_API_KEY}
        credits_response = requests.get(credits_url, params=credits_params)

        movie_credits = []
        
        if credits_response.status_code == 200:
            credits_data = credits_response.json()

            for credit in credits_data.get("cast", []):
                # Only add movies above popularity threshold
                if credit.get("popularity", 0) > 1.5:
                    movie_id = credit["id"]
                    poster_path = credit.get("poster_path", "")
                    
                    # Get individual movie details to check production companies
                    is_mcu = False
                    movie_url = f"{BASE_URL}/movie/{movie_id}"
                    movie_params = {"api_key": TMDB_API_KEY}
                    movie_response = requests.get(movie_url, params=movie_params)

                    if movie_response.status_code == 200:
                        movie_data = movie_response.json()
                        production_companies = movie_data.get("production_companies", [])

                        # Check if Marvel Studios is a production company
                        for company in production_companies:
                            if "Marvel Studios" in company.get("name", ""):
                                is_mcu = True
                                break
                    
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

                    # Small delay to avoid rate limits
                    time.sleep(0.2)
        else:
            print(f"Error fetching movie credits for {actor_name}: {credits_response.text}")

        # Step 3: Get TV credits
        tv_credits_url = ACTOR_TV_CREDITS_URL_TEMPLATE.format(actor_id)
        tv_credits_params = {"api_key": TMDB_API_KEY}
        tv_credits_response = requests.get(tv_credits_url, params=tv_credits_params)

        tv_credits = []
        if tv_credits_response.status_code == 200:
            tv_credits_data = tv_credits_response.json()

            for credit in tv_credits_data.get("cast", []):
                if credit.get("popularity", 0) > 1.5:
                    tv_id = credit["id"]
                    poster_path = credit.get("poster_path", "")
                    
                    # Get TV show details to check production companies
                    is_mcu = False
                    tv_url = f"{BASE_URL}/tv/{tv_id}"
                    tv_params = {"api_key": TMDB_API_KEY}
                    tv_response = requests.get(tv_url, params=tv_params)

                    if tv_response.status_code == 200:
                        tv_data = tv_response.json()
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
                    
                    tv_credits.append({
                        "id": tv_id,
                        "name": credit.get("name", ""),
                        "character": credit.get("character", ""),
                        "popularity": credit.get("popularity", 0),
                        "first_air_date": credit.get("first_air_date", ""),
                        "poster_path": poster_path,
                        "is_mcu": is_mcu
                    })
                    
                    # Small delay
                    time.sleep(0.2)
        else:
            print(f"Error fetching TV credits for {actor_name}: {tv_credits_response.text}")

        # Insert data into appropriate region databases
        for region in known_regions:
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

        # Delay to avoid rate limits
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

print("All data successfully updated and written to SQLite databases")
