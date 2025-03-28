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

# Load existing data from regional files
actors_by_region = {region: {} for region in list(REGIONS.keys())}
actors_by_region["GLOBAL"] = {}  # Add global category

for region in list(REGIONS.keys()) + ["GLOBAL"]:
    file_name = f"actors_data_{region}.json"
    if os.path.exists(file_name):
        try:
            with open(file_name, "r", encoding="utf-8") as f:
                actors_by_region[region] = json.load(f)
            print(f"Loaded existing data from {file_name}")

            # Merge regional data into the main actors_data dictionary
            for actor_id, actor_data in actors_by_region[region].items():
                actors_data[str(actor_id)] = actor_data  # Ensure actor_id is a string
        except Exception as e:
            print(f"Error loading existing data from {file_name}: {e}")

# Create a set of existing actor IDs for quick lookup
existing_actor_ids = set(actors_data.keys())

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

        actor_id_str = str(actor_id)  # Ensure actor_id is a string

        # Always fetch details if the actor is new or already exists
        fetch_details = actor_id_str not in existing_actor_ids

        # Step 1: Get detailed person info for country/region
        details_url = ACTOR_DETAILS_URL_TEMPLATE.format(actor_id)
        details_params = {"api_key": TMDB_API_KEY}
        details_response = requests.get(details_url, params=details_params)

        place_of_birth = "Unknown"
        known_regions = []

        if details_response.status_code == 200:
            details_data = details_response.json()
            place_of_birth = details_data.get("place_of_birth", "Unknown")

            # Fix: Handle None values for place_of_birth
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

        # When processing movie credits, check for Marvel Studios
        if credits_response.status_code == 200:
            credits_data = credits_response.json()

            # Initialize Marvel flag
            is_mcu_actor = False

            # Process movie credits and check for Marvel involvement
            movie_credits = []
            for credit in credits_data.get("cast", []):
                # Only keep essential credit info to reduce file size
                compact_credit = {
                    "id": credit["id"],
                    "title": credit.get("title", ""),
                    "character": credit.get("character", ""),
                    "popularity": credit.get("popularity", 0),
                    "release_date": credit.get("release_date", "")
                }

                # Only add movies above a certain popularity threshold
                if credit.get("popularity", 0) > 1.5:
                    movie_credits.append(compact_credit)

                    # If we haven't identified them as MCU yet, check this movie
                    if not is_mcu_actor:
                        # Get movie details to check production companies
                        movie_id = credit["id"]
                        movie_url = f"{BASE_URL}/movie/{movie_id}"
                        movie_params = {"api_key": TMDB_API_KEY}
                        movie_response = requests.get(movie_url, params=movie_params)

                        if movie_response.status_code == 200:
                            movie_data = movie_response.json()
                            production_companies = movie_data.get("production_companies", [])

                            # Check if Marvel Studios is in production companies
                            for company in production_companies:
                                if "Marvel Studios" in company.get("name", ""):
                                    is_mcu_actor = True
                                    break

                        # Limit API calls by adding a small delay
                        time.sleep(0.2)
        else:
            print(f"Error fetching movie credits for {actor_name}: {credits_response.text}")
            is_mcu_actor = False

        # Step 3: Get TV credits
        tv_credits_url = ACTOR_TV_CREDITS_URL_TEMPLATE.format(actor_id)
        tv_credits_params = {"api_key": TMDB_API_KEY}
        tv_credits_response = requests.get(tv_credits_url, params=tv_credits_params)

        # Process TV credits similar to how we process movie credits
        tv_credits = []
        if tv_credits_response.status_code == 200:
            tv_credits_data = tv_credits_response.json()

            # Only keep essential fields from TV credits and filter by popularity
            for credit in tv_credits_data.get("cast", []):
                if credit.get("popularity", 0) > 1.5:  # Same threshold as movies
                    compact_tv_credit = {
                        "id": credit["id"],
                        "name": credit.get("name", ""),
                        "character": credit.get("character", ""),
                        "popularity": credit.get("popularity", 0),
                        "first_air_date": credit.get("first_air_date", "")
                    }
                    tv_credits.append(compact_tv_credit)
        else:
            print(f"Error fetching TV credits for {actor_name}: {tv_credits_response.text}")

        # Store only what we need in the actor's data
        actors_data[actor_id_str] = {
            "name": actor_name,
            "popularity": popularity,
            "place_of_birth": place_of_birth,
            "regions": known_regions,
            "is_mcu": is_mcu_actor
        }

        # Only include credits if they exist
        if movie_credits:
            actors_data[actor_id_str]["movie_credits"] = movie_credits
        if tv_credits:
            actors_data[actor_id_str]["tv_credits"] = tv_credits

        # Brief delay to avoid hitting rate limits
        time.sleep(0.5)  # Increased delay due to multiple API calls per actor

    # Delay between pages
    time.sleep(1)
    print(f"Completed page {page}/{TOTAL_PAGES}")

# Split data by region for smaller files
actors_by_region = {region: {} for region in list(REGIONS.keys())}
actors_by_region["GLOBAL"] = {}  # Add global category

for actor_id, actor_data in actors_data.items():
    # Assign to each region the actor belongs to
    for region in actor_data["regions"]:
        actors_by_region[region][actor_id] = actor_data

# Save separate files for each region
for region, actors in actors_by_region.items():
    if actors:  # Skip empty regions
        output_file = f"actors_data_{region}.json"
        with open(output_file, "w", encoding="utf-8") as f:
            # No indentation for smaller file size
            json.dump(actors, f, ensure_ascii=False)
        print(f"Data for region {region} written to {output_file}")

# Write the complete data file without indentation to reduce size
with open("actors_data.json", "w", encoding="utf-8") as f:
    json.dump(actors_data, f, ensure_ascii=False)  # No indent

print(f"Data successfully updated and written to multiple files")
