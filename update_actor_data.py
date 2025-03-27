import os
import requests
import json
import time

# Retrieve your TMDB API key from environment variables
TMDB_API_KEY = os.environ.get("TMDB_API_KEY")
if not TMDB_API_KEY:
    raise Exception("TMDB_API_KEY not set in environment variables.")

BASE_URL = "https://api.themoviedb.org/3"
POPULAR_ACTORS_URL = f"{BASE_URL}/person/popular"
# URL templates for fetching actor data
ACTOR_MOVIE_CREDITS_URL_TEMPLATE = f"{BASE_URL}/person/{{}}/movie_credits"
ACTOR_TV_CREDITS_URL_TEMPLATE = f"{BASE_URL}/person/{{}}/tv_credits"
ACTOR_DETAILS_URL_TEMPLATE = f"{BASE_URL}/person/{{}}"

# How many pages of popular actors to fetch (each page returns ~20 actors)
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

# Define popularity thresholds for global recognition
GLOBAL_POPULARITY_THRESHOLD = 15  # Actors above this are considered globally known

actors_data = {}

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
        actor_name = person["name"]
        popularity = person.get("popularity", 0)
        print(f"Fetching data for {actor_name} (ID: {actor_id})")
        
        # Step 1: Get detailed person info for country/region
        details_url = ACTOR_DETAILS_URL_TEMPLATE.format(actor_id)
        details_params = {"api_key": TMDB_API_KEY}
        details_response = requests.get(details_url, params=details_params)
        
        place_of_birth = "Unknown"
        known_regions = []
        
        if details_response.status_code == 200:
            details_data = details_response.json()
            place_of_birth = details_data.get("place_of_birth", "Unknown")
            
            # Determine regions based on place of birth
            for region_code, region_name in REGIONS.items():
                if region_name in place_of_birth:
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
            movie_credits = credits_data.get("cast", [])
        else:
            print(f"Error fetching movie credits for {actor_name}: {credits_response.text}")
        
        # Step 3: Get TV credits
        tv_credits_url = ACTOR_TV_CREDITS_URL_TEMPLATE.format(actor_id)
        tv_credits_params = {"api_key": TMDB_API_KEY}
        tv_credits_response = requests.get(tv_credits_url, params=tv_credits_params)
        
        tv_credits = []
        if tv_credits_response.status_code == 200:
            tv_credits_data = tv_credits_response.json()
            tv_credits = tv_credits_data.get("cast", [])
        else:
            print(f"Error fetching TV credits for {actor_name}: {tv_credits_response.text}")
            
        # Store the actor's data with all the new information
        actors_data[actor_id] = {
            "name": actor_name,
            "popularity": popularity,
            "place_of_birth": place_of_birth,
            "regions": known_regions,
            "movie_credits": movie_credits,
            "tv_credits": tv_credits
        }
        
        # Brief delay to avoid hitting rate limits
        time.sleep(0.5)  # Increased delay due to multiple API calls per actor
    
    # Delay between pages
    time.sleep(1)
    print(f"Completed page {page}/{TOTAL_PAGES}")

# Write the fetched data to a JSON file
output_file = "actors_data.json"
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(actors_data, f, indent=2, ensure_ascii=False)

print(f"Data successfully updated and written to {output_file}")
