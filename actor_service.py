#!/usr/bin/env python3
"""
Simplified Actor Database Service
Core functionality for maintaining an up-to-date database of actors with popularity weighting
"""

import os
import requests
import json
import time
import sqlite3
import sys
from datetime import datetime
from tqdm import tqdm

# Configuration
TMDB_API_KEY = os.environ.get("TMDB_API_KEY")
if not TMDB_API_KEY:
    print("ERROR: TMDB_API_KEY environment variable not set")
    sys.exit(1)

BASE_URL = "https://api.themoviedb.org/3"
DATABASE_PATH = "/app/data/actors.db"

class ActorDatabaseService:
    def __init__(self):
        self.api_key = TMDB_API_KEY
        self.setup_database()
        
    def setup_database(self):
        """Create the database and tables"""
        os.makedirs(os.path.dirname(DATABASE_PATH), exist_ok=True)
        
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        # Simple, focused schema
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS actors (
            id INTEGER PRIMARY KEY,
            name TEXT NOT NULL,
            popularity REAL,
            profile_path TEXT,
            place_of_birth TEXT,
            credits_count INTEGER DEFAULT 0,
            last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
            PRIMARY KEY (id, actor_id),
            FOREIGN KEY (actor_id) REFERENCES actors (id)
        )
        ''')
        
        # Indexes for performance
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_actors_popularity ON actors (popularity DESC)')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_actors_name ON actors (name)')
        
        conn.commit()
        conn.close()
        print("Database initialized")
    
    def make_api_request(self, url, params):
        """Make API request with retry logic"""
        for attempt in range(3):
            try:
                response = requests.get(url, params=params, timeout=10)
                if response.status_code == 200:
                    return response.json()
                elif response.status_code == 429:  # Rate limit
                    time.sleep(2 ** attempt)
                else:
                    print(f"API Error: {response.status_code}")
                    return None
            except Exception as e:
                print(f"Request failed (attempt {attempt + 1}): {e}")
                time.sleep(1)
        return None
    
    def calculate_weighted_popularity(self, tmdb_popularity, movie_credits, tv_credits):
        """Calculate weighted popularity based on TMDB data and credit quality"""
        if not movie_credits and not tv_credits:
            return tmdb_popularity * 0.5  # Reduce if no credits
        
        # Count significant credits (popularity > 5.0)
        significant_movies = len([m for m in movie_credits if m.get('popularity', 0) > 5.0])
        significant_tv = len([t for t in tv_credits if t.get('popularity', 0) > 5.0])
        
        # Simple weighting formula
        credit_boost = min((significant_movies + significant_tv) * 0.1, 2.0)  # Max 2x boost
        
        return min(tmdb_popularity * (1 + credit_boost), 100.0)  # Cap at 100
    
    def update_actor_data(self, max_pages=10):
        """Update actor database from TMDB"""
        print(f"Updating actor database (max {max_pages} pages)...")
        
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        processed_count = 0
        
        for page in range(1, max_pages + 1):
            print(f"Processing page {page}/{max_pages}")
            
            # Get popular actors
            params = {"api_key": self.api_key, "page": page}
            data = self.make_api_request(f"{BASE_URL}/person/popular", params)
            
            if not data or 'results' not in data:
                print(f"Failed to fetch page {page}")
                continue
            
            for person in tqdm(data['results'], desc=f"Page {page}"):
                actor_id = person['id']
                name = person['name']
                tmdb_popularity = person.get('popularity', 0)
                profile_path = person.get('profile_path', '')
                
                # Get additional details
                details = self.make_api_request(
                    f"{BASE_URL}/person/{actor_id}", 
                    {"api_key": self.api_key}
                )
                
                place_of_birth = "Unknown"
                if details:
                    place_of_birth = details.get('place_of_birth', 'Unknown') or 'Unknown'
                
                # Get movie credits
                movies = self.make_api_request(
                    f"{BASE_URL}/person/{actor_id}/movie_credits",
                    {"api_key": self.api_key}
                )
                movie_credits = movies.get('cast', []) if movies else []
                
                # Get TV credits  
                tv_shows = self.make_api_request(
                    f"{BASE_URL}/person/{actor_id}/tv_credits",
                    {"api_key": self.api_key}
                )
                tv_credits = tv_shows.get('cast', []) if tv_shows else []
                
                # Calculate weighted popularity
                weighted_popularity = self.calculate_weighted_popularity(
                    tmdb_popularity, movie_credits, tv_credits
                )
                
                # Filter credits to significant ones, sorted by popularity
                filtered_movies = [
                    m for m in movie_credits 
                    if m.get('popularity', 0) > 1.0 and m.get('character', '').lower() != 'self'
                ]
                significant_movies = sorted(filtered_movies, key=lambda x: x.get('popularity', 0), reverse=True)[:50]
                
                filtered_tv = [
                    t for t in tv_credits 
                    if t.get('popularity', 0) > 1.0 and t.get('character', '').lower() != 'self'
                ]
                significant_tv = sorted(filtered_tv, key=lambda x: x.get('popularity', 0), reverse=True)[:50]
                
                # Insert/update actor
                cursor.execute('''
                INSERT OR REPLACE INTO actors 
                (id, name, popularity, profile_path, place_of_birth, credits_count, last_updated)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP)
                ''', (
                    actor_id, name, weighted_popularity, profile_path, 
                    place_of_birth, len(significant_movies) + len(significant_tv)
                ))
                
                # Clear old credits for this actor
                cursor.execute('DELETE FROM movie_credits WHERE actor_id = ?', (actor_id,))
                cursor.execute('DELETE FROM tv_credits WHERE actor_id = ?', (actor_id,))
                
                # Insert movie credits
                for movie in significant_movies:
                    cursor.execute('''
                    INSERT OR REPLACE INTO movie_credits 
                    (id, actor_id, title, character, popularity, release_date)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        movie['id'], actor_id, movie.get('title', ''),
                        movie.get('character', ''), movie.get('popularity', 0),
                        movie.get('release_date', '')
                    ))
                
                # Insert TV credits
                for tv in significant_tv:
                    cursor.execute('''
                    INSERT OR REPLACE INTO tv_credits 
                    (id, actor_id, name, character, popularity, first_air_date)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        tv['id'], actor_id, tv.get('name', ''),
                        tv.get('character', ''), tv.get('popularity', 0),
                        tv.get('first_air_date', '')
                    ))
                
                processed_count += 1
                
                # Rate limiting
                time.sleep(0.1)
            
            # Commit after each page
            conn.commit()
            print(f"Page {page} completed ({len(data['results'])} actors)")
        
        conn.close()
        print(f"Database update completed! Processed {processed_count} actors")
        
        # Update status file
        with open("/app/data/status.json", "w") as f:
            json.dump({
                "last_updated": datetime.now().isoformat(),
                "total_actors": processed_count,
                "pages_processed": max_pages
            }, f)
    
    def reindex_credits(self):
        """Re-fetch credits for all existing actors (preserves actor list, refreshes movie/TV credits)"""
        import time as _time
        
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        cursor.execute('SELECT id, name FROM actors')
        actors = cursor.fetchall()
        total = len(actors)
        print(f"Reindexing credits for {total} actors...")
        
        processed = 0
        for actor_id, name in actors:
            # Re-fetch credits from TMDB
            movies = self.make_api_request(
                f"{BASE_URL}/person/{actor_id}/movie_credits",
                {"api_key": self.api_key}
            )
            movie_credits = movies.get('cast', []) if movies else []
            
            tv_shows = self.make_api_request(
                f"{BASE_URL}/person/{actor_id}/tv_credits",
                {"api_key": self.api_key}
            )
            tv_credits = tv_shows.get('cast', []) if tv_shows else []
            
            # Apply new filtering (50 cap, sorted by popularity)
            filtered_movies = [
                m for m in movie_credits
                if m.get('popularity', 0) > 1.0 and m.get('character', '').lower() != 'self'
            ]
            significant_movies = sorted(filtered_movies, key=lambda x: x.get('popularity', 0), reverse=True)[:50]
            
            filtered_tv = [
                t for t in tv_credits
                if t.get('popularity', 0) > 1.0 and t.get('character', '').lower() != 'self'
            ]
            significant_tv = sorted(filtered_tv, key=lambda x: x.get('popularity', 0), reverse=True)[:50]
            
            # Clear old credits
            cursor.execute('DELETE FROM movie_credits WHERE actor_id = ?', (actor_id,))
            cursor.execute('DELETE FROM tv_credits WHERE actor_id = ?', (actor_id,))
            
            # Insert new credits
            for movie in significant_movies:
                cursor.execute('''
                INSERT OR REPLACE INTO movie_credits
                (id, actor_id, title, character, popularity, release_date)
                VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    movie['id'], actor_id, movie.get('title', ''),
                    movie.get('character', ''), movie.get('popularity', 0),
                    movie.get('release_date', '')
                ))
            
            for tv in significant_tv:
                cursor.execute('''
                INSERT OR REPLACE INTO tv_credits
                (id, actor_id, name, character, popularity, first_air_date)
                VALUES (?, ?, ?, ?, ?, ?)
                ''', (
                    tv['id'], actor_id, tv.get('name', ''),
                    tv.get('character', ''), tv.get('popularity', 0),
                    tv.get('first_air_date', '')
                ))
            
            processed += 1
            if processed % 100 == 0:
                conn.commit()
                print(f"  Reindexed {processed}/{total} actors")
            
            _time.sleep(0.1)
        
        conn.commit()
        conn.close()
        print(f"Reindex complete! {processed} actors updated")
    
    def get_database_stats(self):
        """Get current database statistics"""
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        
        cursor.execute('SELECT COUNT(*) FROM actors')
        actor_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM movie_credits')
        movie_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM tv_credits')
        tv_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT MAX(last_updated) FROM actors')
        last_update = cursor.fetchone()[0]
        
        conn.close()
        
        return {
            "actors": actor_count,
            "movies": movie_count,
            "tv_shows": tv_count,
            "last_updated": last_update
        }

def main():
    """Main entry point"""
    service = ActorDatabaseService()
    
    if len(sys.argv) > 1:
        if sys.argv[1] == "update":
            max_pages = int(sys.argv[2]) if len(sys.argv) > 2 else 10
            service.update_actor_data(max_pages)
        elif sys.argv[1] == "reindex":
            service.reindex_credits()
        elif sys.argv[1] == "stats":
            stats = service.get_database_stats()
            print(json.dumps(stats, indent=2))
    else:
        print("Usage: python actor_service.py [update|stats|reindex] [max_pages]")

if __name__ == "__main__":
    main()
