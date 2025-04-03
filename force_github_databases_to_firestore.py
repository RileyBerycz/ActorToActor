import os
import json
import sqlite3
import time
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore

# Initialize Firebase from environment
firebase_key_json = os.environ.get("FIREBASE_SERVICE_ACCOUNT")
if not firebase_key_json:
    raise Exception("FIREBASE_SERVICE_ACCOUNT not set in environment variables.")

# Parse JSON string from environment variable
firebase_key_dict = json.loads(firebase_key_json)

# Initialize with the parsed credentials
cred = credentials.Certificate(firebase_key_dict)
firebase_admin.initialize_app(cred)
db = firestore.client()
print("Firebase initialized successfully")

# Firebase operation counters for free tier limits
firebase_writes = 0
firebase_reads = 0
FIREBASE_DAILY_WRITE_LIMIT = 18000  # Set below the 20k limit to be safe

# Regions to process
REGIONS = ["GLOBAL", "US", "UK", "CA", "AU", "KR", "CN", "JP", "IN", "FR", "DE", "OTHER"]

# Track progress for resuming on future runs
progress_file = "firebase_upload_progress.json"
last_region = None
last_actor_id = None
skip_to_actor = False

# Load progress if available
if os.path.exists(progress_file):
    try:
        with open(progress_file, "r") as f:
            progress = json.load(f)
            last_region = progress.get("region")
            last_actor_id = progress.get("actor_id")
            firebase_writes = progress.get("writes", 0)
            firebase_reads = progress.get("reads", 0)
            
            if last_region and last_actor_id:
                print(f"Resuming upload from region {last_region}, after actor ID {last_actor_id}")
                skip_to_actor = True
                
                # Find the index of the last region to start from there
                if last_region in REGIONS:
                    region_index = REGIONS.index(last_region)
                    REGIONS = REGIONS[region_index:]
    except Exception as e:
        print(f"Error loading progress: {e}")

def save_progress(region, actor_id):
    """Save current progress to allow resuming later"""
    with open(progress_file, "w") as f:
        json.dump({
            "region": region,
            "actor_id": actor_id,
            "writes": firebase_writes,
            "reads": firebase_reads
        }, f)

def batch_upload_to_firestore(actor_id, actor_data, movie_credits, tv_credits, regions):
    """Upload actor data to Firestore in batches"""
    global firebase_writes
    
    if firebase_writes >= FIREBASE_DAILY_WRITE_LIMIT:
        raise Exception(f"Approaching Firebase write limit ({firebase_writes}), stopping upload")
    
    try:
        # Create a batch for actor data
        batch = db.batch()
        actor_id_str = str(actor_id)
        writes_in_batch = 0
        
        # Set actor document
        actor_ref = db.collection('actors').document(actor_id_str)
        batch.set(actor_ref, {
            'name': actor_data.get('name', ''),
            'popularity': actor_data.get('popularity', 0),
            'profile_path': actor_data.get('profile_path', ''),
            'place_of_birth': actor_data.get('place_of_birth', 'Unknown'),
            'regions': regions
        })
        writes_in_batch += 1
        
        # Commit actor data batch
        batch.commit()
        firebase_writes += writes_in_batch
        
        # Process movie credits in batches
        if movie_credits:
            movie_batch = db.batch()
            movie_writes = 0
            
            for movie in movie_credits:
                movie_id = movie.get('id')
                if not movie_id:
                    continue
                    
                movie_ref = actor_ref.collection('movie_credits').document(str(movie_id))
                movie_batch.set(movie_ref, {
                    'title': movie.get('title', ''),
                    'character': movie.get('character', ''),
                    'popularity': movie.get('popularity', 0),
                    'release_date': movie.get('release_date', ''),
                    'poster_path': movie.get('poster_path', ''),
                    'is_mcu': bool(movie.get('is_mcu', 0))
                })
                movie_writes += 1
                
                # Commit batch if approaching size limit
                if movie_writes >= 400:  # Firestore batch limit is 500
                    movie_batch.commit()
                    firebase_writes += movie_writes
                    
                    if firebase_writes >= FIREBASE_DAILY_WRITE_LIMIT:
                        print(f"Approaching write limit ({firebase_writes}), pausing upload")
                        return False
                        
                    movie_batch = db.batch()
                    movie_writes = 0
            
            # Commit any remaining movie writes
            if movie_writes > 0:
                movie_batch.commit()
                firebase_writes += movie_writes
        
        # Process TV credits in batches
        if tv_credits:
            tv_batch = db.batch()
            tv_writes = 0
            
            for tv in tv_credits:
                tv_id = tv.get('id')
                if not tv_id:
                    continue
                    
                tv_ref = actor_ref.collection('tv_credits').document(str(tv_id))
                tv_batch.set(tv_ref, {
                    'name': tv.get('name', ''),
                    'character': tv.get('character', ''),
                    'popularity': tv.get('popularity', 0),
                    'first_air_date': tv.get('first_air_date', ''),
                    'poster_path': tv.get('poster_path', ''),
                    'is_mcu': bool(tv.get('is_mcu', 0))
                })
                tv_writes += 1
                
                # Commit batch if approaching size limit
                if tv_writes >= 400:
                    tv_batch.commit()
                    firebase_writes += tv_writes
                    
                    if firebase_writes >= FIREBASE_DAILY_WRITE_LIMIT:
                        print(f"Approaching write limit ({firebase_writes}), pausing upload")
                        return False
                        
                    tv_batch = db.batch()
                    tv_writes = 0
            
            # Commit any remaining TV writes
            if tv_writes > 0:
                tv_batch.commit()
                firebase_writes += tv_writes
        
        print(f"Actor {actor_id} uploaded with {len(movie_credits)} movies and {len(tv_credits)} TV shows")
        return True
        
    except Exception as e:
        print(f"Error uploading actor {actor_id}: {e}")
        return False

def process_region(region):
    """Process a specific region's database"""
    global firebase_writes, skip_to_actor, last_actor_id
    
    db_path = f"actor-game/public/actors_{region}.db"
    
    if not os.path.exists(db_path):
        print(f"Database for region {region} not found: {db_path}")
        return True  # Continue with next region
    
    print(f"Processing database for region {region}: {db_path}")
    
    try:
        # Connect to SQLite database
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # Enable row factory to access columns by name
        cursor = conn.cursor()
        
        # Get actors for this region
        cursor.execute('''
            SELECT a.*, GROUP_CONCAT(ar.region) as regions 
            FROM actors a
            JOIN actor_regions ar ON a.id = ar.actor_id
            GROUP BY a.id
            ORDER BY a.id
        ''')
        
        actors = cursor.fetchall()
        total_actors = len(actors)
        print(f"Found {total_actors} actors in region {region}")
        
        # Process each actor
        for i, actor in enumerate(actors):
            actor_id = actor['id']
            
            # Skip actors until we reach the last processed one (for resuming)
            if skip_to_actor and last_actor_id and actor_id <= last_actor_id:
                continue
            
            # We've reached or passed the last processed actor, stop skipping
            if skip_to_actor and last_actor_id and actor_id > last_actor_id:
                skip_to_actor = False
            
            # Get movie credits
            cursor.execute('SELECT * FROM movie_credits WHERE actor_id = ?', (actor_id,))
            movie_credits = [dict(row) for row in cursor.fetchall()]
            
            # Get TV credits
            cursor.execute('SELECT * FROM tv_credits WHERE actor_id = ?', (actor_id,))
            tv_credits = [dict(row) for row in cursor.fetchall()]
            
            # Extract regions
            regions_str = actor['regions'] if 'regions' in actor else None
            regions = regions_str.split(',') if regions_str else [region]
            
            # Create actor data dictionary
            actor_data = {
                'name': actor['name'],
                'popularity': actor['popularity'],
                'profile_path': actor['profile_path'],
                'place_of_birth': actor['place_of_birth'],
            }
            
            # Upload to Firestore
            success = batch_upload_to_firestore(actor_id, actor_data, movie_credits, tv_credits, regions)
            
            # Save progress every 10 actors or when we hit a limit
            if i % 10 == 0 or not success:
                save_progress(region, actor_id)
                
            # If we hit the write limit, stop processing
            if not success or firebase_writes >= FIREBASE_DAILY_WRITE_LIMIT:
                conn.close()
                return False
            
            # Add a small delay to avoid overwhelming Firestore
            if i % 20 == 0:
                time.sleep(1)
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"Error processing region {region}: {e}")
        return False

# Main execution loop
try:
    for region in REGIONS:
        print(f"Starting upload for region: {region}")
        
        # Process this region's database
        success = process_region(region)
        
        # If unsuccessful (likely hit a limit), stop processing
        if not success:
            print(f"Pausing upload due to limits or errors. Will resume from region {region}")
            break
        
        # Mark completion of this region
        print(f"Completed upload for region: {region}")
        last_actor_id = None  # Reset actor ID when moving to new region
        save_progress(region, None)
        
    # If we completed all regions, clean up progress file
    if os.path.exists(progress_file):
        os.remove(progress_file)
        print("Upload completed for all regions")
        
except Exception as e:
    print(f"Error during upload process: {e}")
    
print(f"Firebase operations completed with {firebase_writes} writes")