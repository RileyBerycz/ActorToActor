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
import re
from requests.exceptions import ConnectionError, Timeout, RequestException
from bs4 import BeautifulSoup
from datetime import datetime, timezone, timedelta
import pandas as pd
pd.set_option('future.no_silent_downcasting', True)

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
# Get configurable page limit from environment variable with default and hard cap
# Hard cap is set to 200000 based on TMDB API limitations (reported 202017 but 400 error)
MAX_POSSIBLE_PAGES = 200000
DEFAULT_PAGES_FILE = "actor-game/public/default_pages.txt"

# Try to load custom default from file
try:
    with open(DEFAULT_PAGES_FILE, 'r') as f:
        DEFAULT_PAGES = int(f.read().strip())
        print(f"Using saved default page count: {DEFAULT_PAGES}")
except (FileNotFoundError, ValueError):
    DEFAULT_PAGES = 1000
    print(f"Using built-in default page count: {DEFAULT_PAGES}")

# Get user-specified value for current run
user_value = os.environ.get("TMDB_MAX_PAGES", str(DEFAULT_PAGES))
try:
    requested_pages = int(user_value)
    if requested_pages > MAX_POSSIBLE_PAGES:
        print(f"⚠️ Requested {requested_pages} pages exceeds maximum of {MAX_POSSIBLE_PAGES}. Capping at maximum.")
        TOTAL_PAGES = MAX_POSSIBLE_PAGES
    else:
        TOTAL_PAGES = requested_pages
except ValueError:
    print(f"⚠️ Invalid page count '{user_value}'. Using default of {DEFAULT_PAGES}.")
    TOTAL_PAGES = DEFAULT_PAGES

# Check if we should update the default value for future runs
update_default = os.environ.get("UPDATE_DEFAULT", "false").lower() == "true"
if update_default and TOTAL_PAGES != DEFAULT_PAGES:
    try:
        os.makedirs(os.path.dirname(DEFAULT_PAGES_FILE), exist_ok=True)
        with open(DEFAULT_PAGES_FILE, 'w') as f:
            f.write(str(TOTAL_PAGES))
        print(f"✅ Updated default page count to {TOTAL_PAGES} for future runs")
    except Exception as e:
        print(f"⚠️ Failed to update default page count: {e}")

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
    Calculate average popularity of an actor's credits with enhanced metrics
    including quality metrics based on TMDB ratings
    """
    all_popularity_scores = []
    quality_scores = []  # Track quality scores separately
    
    # Process movie credits with enhanced scoring
    for movie in movie_credits:
        # Base TMDB popularity
        base_pop = movie.get("popularity", 0)
        if base_pop <= 0:
            continue
            
        movie_id = movie["id"]
        
        # REMOVED: Google Trends search interest code
        # Just use the TMDB popularity directly
        all_popularity_scores.append(base_pop)
        
        # Quality metrics - Get movie details for rating data
        # Cache movie quality data to avoid duplicate API calls
        quality_key = f"quality_movie_{movie_id}"
        if quality_key in _popularity_cache:
            quality_score = _popularity_cache[quality_key]
            if quality_score > 0:
                quality_scores.append(quality_score)
        else:
            # Fetch movie details to get rating data
            movie_params = {"api_key": TMDB_API_KEY, "append_to_response": "credits,reviews"}
            movie_data = make_api_request(f"{BASE_URL}/movie/{movie_id}", movie_params)
            
            if movie_data:
                vote_avg = movie_data.get('vote_average', 0)
                vote_count = movie_data.get('vote_count', 0)
                
                # Only consider movies with sufficient votes
                if vote_avg > 0 and vote_count > 20:
                    # Normalize vote_average from 0-10 to 0-1
                    normalized_score = vote_avg / 10.0
                    # Weight by number of votes (more votes = more confidence)
                    confidence = min(vote_count / 1000, 1.0)  
                    weighted_score = normalized_score * confidence
                    quality_scores.append(weighted_score)
                    _popularity_cache[quality_key] = weighted_score
                else:
                    _popularity_cache[quality_key] = 0
    
    # Process TV credits similarly
    for tv in tv_credits:
        base_pop = tv.get("popularity", 0)
        if base_pop <= 0:
            continue
        
        tv_id = tv.get("id", 0)
        
        # REMOVED: Google Trends search interest code
        # Just use TMDB popularity directly
        all_popularity_scores.append(base_pop)
        
        # Quality metrics for TV shows
        quality_key = f"quality_tv_{tv_id}"
        if quality_key in _popularity_cache:
            quality_score = _popularity_cache[quality_key]
            if quality_score > 0:
                quality_scores.append(quality_score)
        else:
            # Get TV show details to fetch rating data
            tv_params = {"api_key": TMDB_API_KEY}
            tv_data = make_api_request(f"{BASE_URL}/tv/{tv_id}", tv_params)
            
            if tv_data:
                vote_avg = tv_data.get('vote_average', 0)
                vote_count = tv_data.get('vote_count', 0)
                
                if vote_avg > 0 and vote_count > 20:
                    normalized_score = vote_avg / 10.0
                    confidence = min(vote_count / 1000, 1.0)
                    weighted_score = normalized_score * confidence
                    quality_scores.append(weighted_score)
                    _popularity_cache[quality_key] = weighted_score
                else:
                    _popularity_cache[quality_key] = 0
    
    # Calculate combined score from both popularity and quality
    if not all_popularity_scores:
        popularity_avg = 0
    else:
        popularity_avg = sum(all_popularity_scores) / len(all_popularity_scores)
        
    # Get top 10 quality scores for their best work
    quality_scores.sort(reverse=True)
    top_scores = quality_scores[:10]
    
    if not top_scores:
        quality_avg = 0
    else:
        quality_avg = sum(top_scores) / len(top_scores)
    
    # Combine popularity and quality (70/30 split)
    return (popularity_avg * 0.7) + (quality_avg * 0.3)

def calculate_custom_popularity(tmdb_popularity, num_credits, years_active, avg_credit_popularity, actor_name="", actor_id=None):
    """Calculate enhanced popularity score on a 0-100 scale"""
    # Basic factors
    longevity_factor = min(years_active / 15, 1.0)  # Cap at 15 years
    credits_factor = min(num_credits / 25, 1.0)     # Cap at 25 credits
    
    # Initialize external metrics
    wiki_pageviews = 0
    wiki_importance = 0
    awards_score = 0
    
    if actor_name:
        # Get Wikipedia metrics
        wiki_metrics = get_wiki_metrics(actor_name)
        wiki_pageviews = wiki_metrics['pageviews']
        wiki_importance = (wiki_metrics.get('revisions', 0) * 0.6) + (wiki_metrics.get('links', 0) * 0.4)
        
        # Get awards data
        awards_score = fetch_awards_score(actor_name)
    
    # Scale TMDB popularity (0-100 scale)
    normalized_tmdb = min(tmdb_popularity / 30.0, 1.0) * 100.0
    
    # Scale credits popularity (0-100 scale)
    normalized_credits = min(avg_credit_popularity, 25) * 4.0
    
    # Scale other factors to 0-100
    wiki_views_scaled = wiki_pageviews * 100
    wiki_imp_scaled = wiki_importance * 100
    awards_scaled = awards_score * 100
    credits_scaled = credits_factor * 100
    longevity_scaled = longevity_factor * 100
    
    # Enhanced scoring formula with all components on 0-100 scale
    enhanced_score = (
        normalized_tmdb * 0.30 +       # TMDB popularity (30%)
        normalized_credits * 0.25 +    # Quality of work (25%)
        wiki_views_scaled * 0.20 +     # Wikipedia popularity (20%)
        wiki_imp_scaled * 0.15 +       # Wikipedia importance (15%) 
        awards_scaled * 0.07 +         # Awards recognition (7%)
        credits_scaled * 0.02 +        # Quantity of work (2%)
        longevity_scaled * 0.01        # Career longevity (1%)
    )
    
    print(f"  Metrics: Wiki views={wiki_pageviews:.2f}, Wiki imp={wiki_importance:.2f}")
    
    return enhanced_score

# =============================================================================
# DATABASE SETUP
# =============================================================================
def setup_database():
    """
    Create or open the SQLite database with all required tables
    
    Returns:
        tuple: (connection, cursor)
    """
    db_path = "actor-game/public/actors.db"
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    # Check if we should force a clean database based on environment variable
    force_clean = os.environ.get("FORCE_CLEAN_DB", "false").lower() == "true"
    
    if force_clean and os.path.exists(db_path):
        print("Forced clean database requested. Removing existing database.")
        os.remove(db_path)
    
    # Connect to existing database or create a new one
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Create tables if they don't exist (using IF NOT EXISTS)
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS actors (
        id INTEGER PRIMARY KEY,
        name TEXT NOT NULL,
        popularity REAL,
        tmdb_popularity REAL,
        profile_path TEXT,
        place_of_birth TEXT,
        years_active INTEGER,
        credit_count INTEGER
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS actor_regions (
        actor_id INTEGER,
        region TEXT,
        popularity_score REAL,
        PRIMARY KEY (actor_id, region),
        FOREIGN KEY (actor_id) REFERENCES actors (id)
    )
    ''')
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS movie_credits (
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
    CREATE TABLE IF NOT EXISTS tv_credits (
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
    
    cursor.execute('''
    CREATE TABLE IF NOT EXISTS metrics_timestamps (
        actor_name TEXT,
        metric_type TEXT,
        value REAL,
        last_updated TEXT,
        PRIMARY KEY (actor_name, metric_type)
    )
    ''')
    
    conn.commit()
    return conn, cursor

# Set up metrics database for API caching
def setup_metrics_db():
    """
    Create or verify metrics caching database
    """
    metrics_db_path = "actor-game/public/metrics_cache.db"
    
    # Create directory if it doesn't exist
    os.makedirs(os.path.dirname(metrics_db_path), exist_ok=True)
    
    # Create database if it doesn't exist
    conn = sqlite3.connect(metrics_db_path)
    
    # Create table if it doesn't exist
    conn.execute('''
    CREATE TABLE IF NOT EXISTS metrics_timestamps (
        keyword TEXT,
        metric_type TEXT,
        value REAL,
        last_updated TEXT,
        PRIMARY KEY (keyword, metric_type)
    )
    ''')
    
    conn.commit()
    return conn

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

# Normalize follower counts to a 0–1 scale
def normalize_followers(followers):
    """Normalize follower counts to a 0–1 scale"""
    max_followers = 1_000_000_000  # Assume 1 billion as the upper limit
    normalized = {platform: min(count / max_followers, 1.0) for platform, count in followers.items()}
    return normalized

# =============================================================================
# CHECKPOINT MANAGEMENT
# =============================================================================
def load_checkpoint():
    """
    Load previous execution progress from checkpoint file
    
    Returns:
        Dictionary with checkpoint data
    """
    # Check if we're doing a force clean - in that case, always start fresh
    force_clean = os.environ.get("FORCE_CLEAN_DB", "false").lower() == "true"
    
    if force_clean and os.path.exists(CHECKPOINT_FILE):
        print("Forced clean database requested. Removing existing checkpoint.")
        os.remove(CHECKPOINT_FILE)
        return {
            "last_page": 0,
            "processed_actors": [],
            "last_update": None,
            "completed": False
        }
    
    # Rest of the function remains the same
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
print(f"Collection configured for maximum {TOTAL_PAGES} pages (of {MAX_POSSIBLE_PAGES} available)")

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


def should_update_metric(keyword, metric_type, conn, refresh_days=90):
    """
    Determines if we should make a new API call for this metric
    with longer refresh period (90 days by default)
    
    Args:
        keyword: Search term (actor name, movie title)
        metric_type: Type of metric ('trends', 'wiki', 'awards')
        conn: Database connection
        refresh_days: Number of days before refreshing data (default: 30)
        
    Returns:
        tuple: (should_update, cached_value)
    """
    try:
        # Create the metrics tracking table if it doesn't exist
        conn.execute('''
        CREATE TABLE IF NOT EXISTS metrics_timestamps (
            keyword TEXT,
            metric_type TEXT,
            value REAL,
            last_updated TEXT,
            PRIMARY KEY (keyword, metric_type)
        )
        ''')
        conn.commit()
        
        # Check when this metric was last updated
        cursor = conn.cursor()
        cursor.execute(
            "SELECT value, last_updated FROM metrics_timestamps WHERE keyword = ? AND metric_type = ?", 
            (keyword, metric_type)
        )
        result = cursor.fetchone()
        
        if result:
            value, timestamp_str = result
            last_update = datetime.fromisoformat(timestamp_str)
            now = datetime.now(timezone.utc)
            
            # If data is newer than refresh_days, use cached value
            if (now - last_update) < timedelta(days=refresh_days):
                return False, value
                
        # Data doesn't exist or is too old
        return True, None
            
    except Exception as e:
        print(f"Error checking metric timestamp: {e}")
        # If in doubt, fetch fresh data
        return True, None

def save_metric_value(keyword, metric_type, value, conn):
    """
    Save a metric value with current timestamp
    
    Args:
        keyword: Search term (actor name, movie title)
        metric_type: Type of metric ('trends', 'wiki', 'awards')
        value: Numeric value to save
        conn: Database connection
    """
    try:
        now = datetime.now(timezone.utc).isoformat()
        conn.execute(
            "INSERT OR REPLACE INTO metrics_timestamps (keyword, metric_type, value, last_updated) VALUES (?, ?, ?, ?)",
            (keyword, metric_type, value, now)
        )
        conn.commit()
    except Exception as e:
        print(f"Error saving metric value: {e}")

# Google Trends - Search interest
def fetch_trends_csv(keyword: str) -> float:
    """Get Google Trends data directly via their CSV API"""
    if not keyword:
        return 0.0
        
    try:
        # Step 1: Get token
        response = requests.get(
            "https://trends.google.com/trends/api/explore",
            params={
                "hl": "en-US", "tz": 0,
                "req": json.dumps({
                    "comparisonItem": [{"keyword": keyword, "geo": "", "time": "today 3-m"}],
                    "category": 0, "property": ""
                })
            },
            timeout=10
        )
        
        # The first line is garbage, skip it
        if not response.text or "\n" not in response.text:
            return 0.0
        
        content = response.text.split('\n', 1)[1]
        try:
            data = json.loads(content)
            token = data["widgets"][0]["token"]
        except (json.JSONDecodeError, KeyError, IndexError):
            return 0.0
        
        # Step 2: Fetch CSV
        csv_url = (
            "https://trends.google.com/trends/api/widgetdata/multiline/csv"
            f"?hl=en-US&req={{\"token\":\"{token}\"}}"
        )
        csv_response = requests.get(csv_url, timeout=10)
        
        # Parse CSV content safely
        lines = csv_response.text.strip().split("\n")
        if len(lines) <= 2:
            return 0.0
        
        # Check if we have enough data
        values = []
        try:
            for line in lines[2:]:
                parts = line.split(",")
                if len(parts) >= 2:
                    try:
                        value = float(parts[1])
                        values.append(value)
                    except ValueError:
                        pass
        except Exception:
            return 0.0
        
        # Calculate average and normalize
        if not values:
            return 0.0
            
        avg_value = sum(values) / len(values)
        return avg_value / 100.0
        
    except Exception as e:
        print(f"Google Trends CSV error for '{keyword}': {e}")
        return 0.0

# Replace the existing function with this improved version
def fetch_search_interest(keyword: str, conn=None) -> float:
    """Get search interest with better caching and direct CSV method"""
    if not keyword:
        return 0.0
    
    # Check database cache first
    if conn:
        should_update, cached_value = should_update_metric(keyword, 'trends', conn, refresh_days=180)
        if not should_update and cached_value is not None:
            return cached_value
    
    # Add rate limiting
    global _last_trends_call
    now = time.time()
    if _last_trends_call > 0:
        time_since_last = now - _last_trends_call
        if time_since_last < 5.0:  # 5-second delay between calls
            wait_time = 5.0 - time_since_last
            print(f"Waiting {wait_time:.1f}s for Google Trends rate limit...")
            time.sleep(wait_time)
    
    # Use direct CSV method instead of pytrends
    _last_trends_call = time.time()
    score = fetch_trends_csv(keyword)
    
    # Cache the successful result
    if conn:
        save_metric_value(keyword, 'trends', score, conn)
        
    return score

# Wikipedia pageviews
def fetch_wiki_pageviews(page_title: str) -> float:
    """Get Wikipedia pageviews over the last 90 days"""
    if not page_title:
        return 0.0
    try:
        # Add proper User-Agent header
        headers = {
            "User-Agent": "ActorToActor/1.0 (https://github.com/yourusername/ActorToActor; contact@example.com)"
        }
        
        end = datetime.now(timezone.utc).date() - timedelta(days=1)
        start = end - timedelta(days=89)
        url = (
            f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/"
            f"en.wikipedia/all-access/user/{requests.utils.quote(page_title)}/daily/"
            f"{start.strftime('%Y%m%d')}/{end.strftime('%Y%m%d')}"
        )
        
        # Use rate-limited request
        r = make_wiki_request(url, {}, headers)
        
        if r.status_code != 200:
            return 0.0
        data = r.json().get("items", [])
        total = sum(item.get("views", 0) for item in data)
        # Normalize against 1M views (cap at 1.0)
        return min(total / 1000000.0, 1.0)
    except Exception as e:
        print(f"Wikipedia pageviews error for '{page_title}': {e}")
        return 0.0

# Awards and nominations from Wikipedia
def fetch_awards_score(actor_name: str) -> float:
    """Get awards and nominations data from Wikipedia"""
    if not actor_name:
        return 0.0
    try:
        # Add proper User-Agent header
        headers = {
            "User-Agent": "ActorToActor/1.0 (https://github.com/yourusername/ActorToActor; contact@example.com)"
        }
        
        # Resolve actor page via search API
        search_url = "https://en.wikipedia.org/w/api.php"
        search_params = {
            "action": "query",
            "list": "search",
            "srsearch": actor_name,
            "format": "json"
        }
        
        # Use rate-limited request
        search_response = make_wiki_request(search_url, search_params, headers)
        s = search_response.json()
        
        if not s["query"]["search"]:
            return 0.0
            
        title = s["query"]["search"][0]["title"]
        
        # Fetch page HTML
        page_url = f"https://en.wikipedia.org/wiki/{requests.utils.quote(title)}"
        
        # Use rate-limited request for HTML
        page_response = make_wiki_request(page_url, {}, headers)
        html = page_response.text
        
        soup = BeautifulSoup(html, "html.parser")
        
        # Look for an infobox row containing awards
        infobox = soup.find("table", {"class": "infobox"})
        wins = noms = 0
        if infobox:
            for row in infobox.find_all("tr"):
                hdr = row.find("th")
                if hdr and "awards" in hdr.text.lower():
                    txt = row.get_text(" ", strip=True)
                    # find numbers before 'win' and 'nom'
                    wins += sum(int(m.group(1)) for m in re.finditer(r"(\d+)\s+win", txt, re.I))
                    noms += sum(int(m.group(1)) for m in re.finditer(r"(\d+)\s+nom", txt, re.I))
                    break
        raw = (wins * 0.7 + noms * 0.3) / 20.0  # Normalize against 20 awards
        return min(raw, 1.0)
    except Exception as e:
        print(f"Wikipedia awards error for '{actor_name}': {e}")
        return 0.0

def get_wiki_metrics(actor_name):
    """Get Wikipedia metrics for an actor (pageviews, revisions, links)"""
    if not actor_name:
        return {"pageviews": 0, "revisions": 0, "links": 0}
    
    try:
        # Add proper User-Agent header
        headers = {
            "User-Agent": "ActorToActor/1.0 (https://github.com/yourusername/ActorToActor; contact@example.com)"
        }
        
        # First find the correct Wikipedia page
        search_url = "https://en.wikipedia.org/w/api.php"
        search_params = {
            "action": "query",
            "list": "search",
            "srsearch": actor_name,
            "format": "json"
        }
        
        # Use rate-limited request
        search_response = make_wiki_request(search_url, search_params, headers)
        if search_response.status_code != 200:
            print(f"Wikipedia API error {search_response.status_code} for '{actor_name}'")
            return {"pageviews": 0, "revisions": 0, "links": 0}
            
        search_data = search_response.json()
        
        # Get the page title from search results
        page_title = search_data['query']['search'][0]['title']
        
        # Get pageviews
        pageviews = fetch_wiki_pageviews(page_title)
        
        # Get page info including revisions and links
        info_params = {
            "action": "query",
            "prop": "info|links",
            "titles": page_title,
            "inprop": "protection|talkid|watched|watchers|visitingwatchers|notificationtimestamp|subjectid|url|readable|preload|displaytitle|varianttitles",
            "format": "json"
        }
        
        # Use rate-limited request
        info_response = make_wiki_request(search_url, info_params, headers)
        info_data = info_response.json()
        
        # Process response
        pages = info_data.get('query', {}).get('pages', {})
        if not pages:
            return {"pageviews": pageviews, "revisions": 0, "links": 0}
            
        # Get first page (should be the only one)
        page_id = list(pages.keys())[0]
        page_info = pages[page_id]
        
        # Count revisions
        revisions_params = {
            "action": "query",
            "prop": "revisions",
            "titles": page_title,
            "rvlimit": "500",  # Maximum allowed without bot permissions
            "format": "json"
        }
        
        # Use rate-limited request
        revisions_response = make_wiki_request(search_url, revisions_params, headers)
        revisions_data = revisions_response.json()
        rev_pages = revisions_data.get('query', {}).get('pages', {})
        
        if rev_pages and page_id in rev_pages:
            revisions_count = len(rev_pages[page_id].get('revisions', []))
        else:
            revisions_count = 0
            
        # Count links
        links_count = len(page_info.get('links', []))
        
        # Normalize values
        normalized_revisions = min(revisions_count / 300, 1.0)  # Normalize against 300 revisions
        normalized_links = min(links_count / 200, 1.0)  # Normalize against 200 links
        
        return {
            "pageviews": pageviews,
            "revisions": normalized_revisions,
            "links": normalized_links
        }
        
    except Exception as e:
        print(f"Error getting Wikipedia metrics for {actor_name}: {e}")
        return {"pageviews": 0, "revisions": 0, "links": 0}
def get_social_media_followers_from_wikipedia(actor_name):
    """Scrape social media follower counts from Wikipedia"""
    try:
        # Add proper User-Agent header
        headers = {
            "User-Agent": "ActorToActor/1.0 (https://github.com/yourusername/ActorToActor; contact@example.com)"
        }
        
        # Search for the actor's page with proper headers
        search_url = "https://en.wikipedia.org/w/api.php"
        search_params = {
            "action": "query",
            "list": "search",
            "srsearch": actor_name,
            "format": "json"
        }
        
        # Use rate-limited request
        search_response = make_wiki_request(search_url, search_params, headers)
        search_data = search_response.json()
        
        if not search_data["query"]["search"]:
            return {}

        # Get the title of the first search result
        title = search_data["query"]["search"][0]["title"]

        # Fetch the Wikipedia page HTML
        page_url = f"https://en.wikipedia.org/wiki/{requests.utils.quote(title)}"
        
        # Use rate-limited request for HTML
        page_response = make_wiki_request(page_url, {}, headers)
        page_html = page_response.text
        
        soup = BeautifulSoup(page_html, "html.parser")

        # Look for social media follower counts in the infobox
        infobox = soup.find("table", {"class": "infobox"})
        followers = {}
        if infobox:
            for row in infobox.find_all("tr"):
                header = row.find("th")
                if header and "followers" in header.text.lower():
                    text = row.get_text(" ", strip=True)
                    # Extract follower counts for each platform
                    for platform in ["Twitter", "Instagram", "Facebook", "TikTok"]:
                        match = re.search(rf"{platform}.*?([\d,]+)", text, re.I)
                        if match:
                            followers[platform.lower()] = int(match.group(1).replace(",", ""))
        return followers
    except Exception as e:
        print(f"Error fetching social media followers for '{actor_name}': {e}")
        return {}
def get_wikidata_metrics(actor_name):
    """Get actor metrics from Wikidata"""
    # Get Wikidata ID from name
    url = f"https://www.wikidata.org/w/api.php?action=wbsearchentities&search={requests.utils.quote(actor_name)}&language=en&format=json"
    data = requests.get(url).json()
    if not data.get('search'):
        return 0.0
        
    wikidata_id = data['search'][0]['id']
    
    # Get statements count (more statements = more notable)
    url = f"https://www.wikidata.org/wiki/Special:EntityData/{wikidata_id}.json"
    entity_data = requests.get(url).json()
    statements_count = len(entity_data['entities'][wikidata_id].get('claims', {}))
    
    return min(statements_count / 50, 1.0)  # Normalize

# Initialize Wikipedia API rate limiting tracking
_last_wiki_call = 0

def make_wiki_request(url, params, headers):
    """Make Wikipedia API request with rate limiting"""
    global _last_wiki_call
    
    # Enforce 1-second delay between Wikipedia API calls
    now = time.time()
    if _last_wiki_call > 0:
        time_since_last = now - _last_wiki_call
        if time_since_last < 1.0:
            sleep_time = 1.0 - time_since_last
            time.sleep(sleep_time)
    
    _last_wiki_call = time.time()
    return requests.get(url, params=params, headers=headers, timeout=10)

# Cache for API responses to avoid duplicate requests
_popularity_cache = {
    'search_interest': {},  
    'wiki_pageviews': {},
    'wiki_metrics': {},
    'wikidata': {},
    'awards': {},
    'social': {}
}

# =============================================================================
# NEWS MENTIONS - GDELT
# =============================================================================
def get_gdelt_news_mentions(actor_name):
    """Get frequency of news mentions from GDELT Project"""
    # Format: YYYY-MM-DD
    end_date = datetime.now().strftime("%Y-%m-%d")
    start_date = (datetime.now() - timedelta(days=90)).strftime("%Y-%m-%d")
    
    url = f"https://api.gdeltproject.org/api/v2/doc/doc?query={requests.utils.quote(actor_name)}&mode=artlist&format=json&startdatetime={start_date}&enddatetime={end_date}"
    try:
        response = requests.get(url, timeout=10)
        data = response.json()
        articles_count = len(data.get('articles', []))
        return min(articles_count / 200, 1.0)  # Normalize
    except:
        return 0.0

# =============================================================================
# MAIN DATA COLLECTION LOOP
# =============================================================================
# Create metrics database connection
metrics_db_path = "actor-game/public/metrics_cache.db"
metrics_conn = sqlite3.connect(metrics_db_path)

print("Script starting...")
print(f"Python version: {sys.version}")

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
        
        # Apply this after calculating avg_credit_popularity
        avg_credit_popularity = min(avg_credit_popularity, 100) / 4  # Scale to max ~25
        
        # Calculate custom popularity score based on multiple factors
        custom_popularity = calculate_custom_popularity(
            tmdb_popularity, 
            num_credits,
            years_active,
            avg_credit_popularity,
            actor_name,
            actor_id  # Add actor ID parameter
        )
        
        # Normalize TMDB popularity to reduce extreme values
        normalized_tmdb = min(tmdb_popularity / 50.0, 1.0)
        
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