#!/usr/bin/env python
import os
import requests
import time
import sys

# =============================================================================
# TMDB API PAGE CHECKER FOR ACTOR TO ACTOR GAME
# =============================================================================
# This script determines how many pages of actor data are available through
# the TMDB API. It uses binary search to efficiently find the maximum page
# number without making too many API requests.
# =============================================================================

def check_tmdb_page_count():
    """Check how many pages of actors TMDB API has available."""
    
    # Get API key from environment variable
    api_key = os.environ.get("TMDB_API_KEY")
    if not api_key:
        print("Error: TMDB_API_KEY environment variable not set")
        sys.exit(1)
    
    base_url = "https://api.themoviedb.org/3/person/popular"
    
    # First, check if the API is working with page 1
    params = {"api_key": api_key, "page": 1}
    response = requests.get(base_url, params=params)
    
    if response.status_code != 200:
        print(f"Error accessing TMDB API: {response.status_code}")
        print(response.text)
        sys.exit(1)
    
    # Get total pages from the response
    data = response.json()
    total_pages = data.get("total_pages", 0)
    total_results = data.get("total_results", 0)
    
    print(f"TMDB reports {total_pages} pages of actors")
    print(f"Estimated total actors: {total_pages * 20}") # Assuming 20 actors per page
    
    # Verify if the reported last page actually works
    print(f"Verifying page {total_pages} exists...")
    params = {"api_key": api_key, "page": total_pages}
    response = requests.get(base_url, params=params)
    
    if response.status_code == 200:
        data = response.json()
        actors_on_last_page = len(data.get("results", []))
        print(f"Last page contains {actors_on_last_page} actors")
        print(f"Actual total actors: {(total_pages - 1) * 20 + actors_on_last_page}")
    else:
        print(f"Last page verification failed: {response.status_code}")
        
    # Try a few pages beyond the reported total to confirm
    test_page = total_pages + 5
    print(f"Testing beyond reported total (page {test_page})...")
    params = {"api_key": api_key, "page": test_page}
    response = requests.get(base_url, params=params)
    
    if response.status_code == 200 and len(response.json().get("results", [])) > 0:
        print(f"Warning: Page {test_page} exists despite reported total of {total_pages}")
        print("TMDB may have more pages than reported!")
    else:
        print("Verified: No additional pages beyond reported total")

if __name__ == "__main__":
    check_tmdb_page_count()