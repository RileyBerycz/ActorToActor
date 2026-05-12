#!/usr/bin/env python3
"""
Simplified Actor Database Service
Core functionality for maintaining an up-to-date database of actors with popularity weighting
"""

import os
import requests
import json
import time as _time
from concurrent.futures import ThreadPoolExecutor, as_completed
import sqlite3
import sys
from datetime import datetime

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
            name TEXT,
            popularity REAL,
            profile_path TEXT,
            place_of_birth TEXT,
            credits_count INTEGER,
            last_updated TEXT,
            raw_popularity REAL DEFAULT 0
        )
        ''')
        
        # Add raw_popularity column if it doesn't exist (for existing DBs)
        try:
            cursor.execute('ALTER TABLE actors ADD COLUMN raw_popularity REAL DEFAULT 0')
        except:
            pass
        
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
                    _time.sleep(2 ** attempt)
                else:
                    print(f"API Error: {response.status_code}")
                    return None
            except Exception as e:
                print(f"Request failed (attempt {attempt + 1}): {e}")
                _time.sleep(1)
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
    
    def _process_actor_page_item(self, person):
        """Process a single actor from a page listing. Thread-safe."""
        delay = 0.03
        actor_id = person['id']
        name = person['name']
        tmdb_popularity = person.get('popularity', 0)
        profile_path = person.get('profile_path', '')
        
        details = self.make_api_request(
            f"{BASE_URL}/person/{actor_id}", 
            {"api_key": self.api_key}
        )
        _time.sleep(delay)
        
        place_of_birth = "Unknown"
        if details:
            place_of_birth = details.get('place_of_birth', 'Unknown') or 'Unknown'
        
        movies = self.make_api_request(
            f"{BASE_URL}/person/{actor_id}/movie_credits",
            {"api_key": self.api_key}
        )
        _time.sleep(delay)
        movie_credits = movies.get('cast', []) if movies else []
        
        tv_shows = self.make_api_request(
            f"{BASE_URL}/person/{actor_id}/tv_credits",
            {"api_key": self.api_key}
        )
        _time.sleep(delay)
        tv_credits = tv_shows.get('cast', []) if tv_shows else []
        
        weighted_popularity = self.calculate_weighted_popularity(
            tmdb_popularity, movie_credits, tv_credits
        )
        
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
        
        return {
            'actor': (actor_id, name, weighted_popularity, profile_path,
                      place_of_birth, len(significant_movies) + len(significant_tv),
                      tmdb_popularity),
            'movies': [(m['id'], actor_id, m.get('title', ''), m.get('character', ''),
                        m.get('popularity', 0), m.get('release_date', '')) for m in significant_movies],
            'tv': [(t['id'], actor_id, t.get('name', ''), t.get('character', ''),
                    t.get('popularity', 0), t.get('first_air_date', '')) for t in significant_tv]
        }
    
    def _process_reindex_item(self, actor_id, name):
        """Re-fetch credits for one actor. Thread-safe."""
        delay = 0.03
        
        person = self.make_api_request(
            f"{BASE_URL}/person/{actor_id}",
            {"api_key": self.api_key}
        )
        _time.sleep(delay)
        tmdb_popularity = person.get('popularity', 0) if person else 0
        
        movies = self.make_api_request(
            f"{BASE_URL}/person/{actor_id}/movie_credits",
            {"api_key": self.api_key}
        )
        _time.sleep(delay)
        movie_credits = movies.get('cast', []) if movies else []
        
        tv_shows = self.make_api_request(
            f"{BASE_URL}/person/{actor_id}/tv_credits",
            {"api_key": self.api_key}
        )
        _time.sleep(delay)
        tv_credits = tv_shows.get('cast', []) if tv_shows else []
        
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
        
        return {
            'actor_id': actor_id,
            'tmdb_popularity': tmdb_popularity,
            'movies': [(m['id'], actor_id, m.get('title', ''), m.get('character', ''),
                        m.get('popularity', 0), m.get('release_date', '')) for m in significant_movies],
            'tv': [(t['id'], actor_id, t.get('name', ''), t.get('character', ''),
                    t.get('popularity', 0), t.get('first_air_date', '')) for t in significant_tv]
        }
    
    def update_actor_data(self, max_pages=10):
        """Update actor database from TMDB using parallel credit fetching"""
        print(f"Updating actor database (max {max_pages} pages)...")
        
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        processed_count = 0
        
        for page in range(1, max_pages + 1):
            print(f"Fetching page {page}/{max_pages}...")
            params = {"api_key": self.api_key, "page": page}
            data = self.make_api_request(f"{BASE_URL}/person/popular", params)
            
            if not data or 'results' not in data:
                print(f"Failed to fetch page {page}")
                continue
            
            people = data['results']
            print(f"Fetching credits for {len(people)} actors on page {page} (5 workers)...")
            
            results = []
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [executor.submit(self._process_actor_page_item, p) for p in people]
                for i, future in enumerate(as_completed(futures)):
                    try:
                        result = future.result()
                        if result:
                            results.append(result)
                    except Exception as e:
                        print(f"  Error on page {page}: {e}")
                    if (i + 1) % 10 == 0 or (i + 1) == len(people):
                        print(f"  Fetched {i+1}/{len(people)}")
            
            for result in results:
                actor_id = result['actor'][0]
                cursor.execute('''
                INSERT OR REPLACE INTO actors 
                (id, name, popularity, profile_path, place_of_birth, credits_count, last_updated, raw_popularity)
                VALUES (?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP, ?)
                ''', result['actor'])
                
                cursor.execute('DELETE FROM movie_credits WHERE actor_id = ?', (actor_id,))
                cursor.execute('DELETE FROM tv_credits WHERE actor_id = ?', (actor_id,))
                
                for movie in result['movies']:
                    cursor.execute('''
                    INSERT OR REPLACE INTO movie_credits 
                    (id, actor_id, title, character, popularity, release_date)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ''', movie)
                
                for tv in result['tv']:
                    cursor.execute('''
                    INSERT OR REPLACE INTO tv_credits 
                    (id, actor_id, name, character, popularity, first_air_date)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ''', tv)
                
                processed_count += 1
            
            conn.commit()
            print(f"Page {page} completed ({len(results)} actors)")
        
        conn.close()
        print(f"Database update completed! Processed {processed_count} actors")
        
        with open("/app/data/status.json", "w") as f:
            json.dump({
                "last_updated": datetime.now().isoformat(),
                "total_actors": processed_count,
                "pages_processed": max_pages
            }, f)
    
    def reindex_credits(self):
        """Re-fetch credits for all existing actors (preserves actor list, refreshes movie/TV credits)"""
        conn = sqlite3.connect(DATABASE_PATH)
        cursor = conn.cursor()
        cursor.execute('SELECT id, name FROM actors')
        actors = cursor.fetchall()
        total = len(actors)
        print(f"Reindexing credits for {total} actors (5 workers, batches of 50)...")
        
        batch_size = 50
        processed = 0
        
        for start in range(0, total, batch_size):
            batch = actors[start:start + batch_size]
            results = []
            
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = {executor.submit(self._process_reindex_item, aid, nm): (aid, nm) for aid, nm in batch}
                for future in as_completed(futures):
                    try:
                        result = future.result()
                        if result:
                            results.append(result)
                    except Exception as e:
                        print(f"  Error reindexing: {e}")
            
            for result in results:
                aid = result['actor_id']
                cursor.execute('DELETE FROM movie_credits WHERE actor_id = ?', (aid,))
                cursor.execute('DELETE FROM tv_credits WHERE actor_id = ?', (aid,))
                
                for movie in result['movies']:
                    cursor.execute('''
                    INSERT OR REPLACE INTO movie_credits
                    (id, actor_id, title, character, popularity, release_date)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ''', movie)
                
                for tv in result['tv']:
                    cursor.execute('''
                    INSERT OR REPLACE INTO tv_credits
                    (id, actor_id, name, character, popularity, first_air_date)
                    VALUES (?, ?, ?, ?, ?, ?)
                    ''', tv)
                
                cursor.execute('UPDATE actors SET raw_popularity = ? WHERE id = ?',
                               (result['tmdb_popularity'], aid))
                processed += 1
            
            conn.commit()
            print(f"  Reindexed {processed}/{total} actors")
        
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
