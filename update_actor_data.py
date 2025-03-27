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
# URL template for fetching an actor's movie credits
ACTOR_MOVIE_CREDITS_URL_TEMPLATE = f"{BASE_URL}/person/{{}}/movie_credits"

# How many pages of popular actors to fetch (each page returns ~20 actors)
TOTAL_PAGES = 5  # adjust as needed

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
        print(f"Fetching movie credits for {actor_name} (ID: {actor_id})")
        
        credits_url = ACTOR_MOVIE_CREDITS_URL_TEMPLATE.format(actor_id)
        credits_params = {"api_key": TMDB_API_KEY}
        credits_response = requests.get(credits_url, params=credits_params)
        if credits_response.status_code != 200:
            print(f"Error fetching movie credits for {actor_name}: {credits_response.text}")
            continue
        credits_data = credits_response.json()
        
        # Store the actor's name and their movie credits (the 'cast' array)
        actors_data[actor_id] = {
            "name": actor_name,
            "movie_credits": credits_data.get("cast", [])
        }
        # Brief delay to avoid hitting rate limits
        time.sleep(0.25)
    
    # Delay between pages
    time.sleep(1)

# Write the fetched data to a JSON file
output_file = "actors_data.json"
with open(output_file, "w", encoding="utf-8") as f:
    json.dump(actors_data, f, indent=2, ensure_ascii=False)

print(f"Data successfully updated and written to {output_file}")
