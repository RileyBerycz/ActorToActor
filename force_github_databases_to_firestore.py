import os
import json
import sqlite3
import time
import firebase_admin
from firebase_admin import credentials, firestore
from tqdm import tqdm
import re

print("Starting Firestore data migration script")

# Extract Firebase config from the firebase.js file with improved parsing
def extract_firebase_config():
    try:
        firebase_js_path = "actor-game/src/firebase.js"
        
        if not os.path.exists(firebase_js_path):
            print(f"Firebase config file not found at {firebase_js_path}")
            # Try alternative paths
            alt_paths = ["./firebase.js", "src/firebase.js", "../actor-game/src/firebase.js"]
            for path in alt_paths:
                if os.path.exists(path):
                    firebase_js_path = path
                    print(f"Found Firebase config at {path}")
                    break
            else:
                raise Exception("Could not find Firebase config file")
        
        with open(firebase_js_path, 'r') as f:
            content = f.read()
        
        # Find the Firebase configuration object
        config_match = re.search(r'(?:const|var|let)\s+firebaseConfig\s*=\s*({[^;]*})', content, re.DOTALL)
        if not config_match:
            print("Regular expression failed to match firebaseConfig")
            config_match = re.search(r'firebase\.initializeApp\(({[^;]*})\)', content, re.DOTALL)
            if not config_match:
                raise Exception("Could not find firebaseConfig in firebase.js")
        
        config_str = config_match.group(1).strip()
        
        # Manual parsing of the configuration object for maximum reliability
        firebase_config = {}
        
        # Extract common Firebase config keys
        for key in ['apiKey', 'authDomain', 'projectId', 'databaseURL', 
                    'storageBucket', 'messagingSenderId', 'appId', 'measurementId']:
            # Look for key: "value" or key: 'value' patterns
            pattern = fr'["\']?{key}["\']?\s*:\s*["\']([^"\']*)["\']'
            match = re.search(pattern, config_str)
            if match:
                firebase_config[key] = match.group(1)
        
        if 'projectId' not in firebase_config:
            # Fallback to project_id if projectId not found
            match = re.search(r'["\']?project_id["\']?\s*:\s*["\']([^"\']*)["\']', config_str)
            if match:
                firebase_config['projectId'] = match.group(1)
        
        if not firebase_config or 'projectId' not in firebase_config:
            raise Exception("Could not extract required Firebase configuration values")
            
        return firebase_config
        
    except Exception as e:
        print(f"Error extracting Firebase config: {e}")
        # Print detailed instructions for manual configuration
        print("\nPlease manually specify your Firebase configuration:")
        print("1. Create a file named 'firebase_config.json' with your Firebase settings")
        print("2. Include at minimum: {\"projectId\": \"your-project-id\"}")
        print("3. Run this script again\n")
        
        # Check if manual config exists
        if os.path.exists('firebase_config.json'):
            try:
                with open('firebase_config.json', 'r') as f:
                    return json.load(f)
            except Exception as e:
                print(f"Error reading manual config: {e}")
        
        return None

# Use service account from environment or file
def get_firebase_credentials(project_id):
    """Get Firebase credentials from the /firebase folder or other locations"""
    # First, check the /firebase folder (new location)
    firebase_folder_paths = [
        'firebase/serviceAccountKey.json',
        'firebase/firebase-credentials.json',
        'firebase/firebase-admin-key.json',
        f'firebase/{project_id}-firebase-adminsdk.json',
        # Look for any JSON file in the firebase folder
        'firebase/*.json'
    ]
    
    # Check if we have any JSON files in the firebase folder
    for pattern in firebase_folder_paths:
        if '*' in pattern:
            # Handle wildcard pattern
            import glob
            matching_files = glob.glob(pattern)
            if matching_files:
                first_match = matching_files[0]
                print(f"Found service account key at {first_match}")
                return credentials.Certificate(first_match)
        elif os.path.exists(pattern):
            print(f"Found service account key at {pattern}")
            return credentials.Certificate(pattern)
    
    # Next, try to use environment variable
    service_account_path = os.environ.get('GOOGLE_APPLICATION_CREDENTIALS')
    if service_account_path and os.path.exists(service_account_path):
        print(f"Using service account from GOOGLE_APPLICATION_CREDENTIALS: {service_account_path}")
        return credentials.Certificate(service_account_path)
    
    # Look for service account files in other common locations
    possible_paths = [
        'serviceAccountKey.json',
        'firebase-admin-key.json',
        f'{project_id}-firebase-adminsdk.json',
        'actor-game/serviceAccountKey.json',
        '../firebase/serviceAccountKey.json',  # Check parent directory too
        './firebase/serviceAccountKey.json'    # Explicit relative path
    ]
    
    for path in possible_paths:
        if os.path.exists(path):
            print(f"Found service account key at {path}")
            return credentials.Certificate(path)
    
    # If no service account found, ask user for credentials
    print("\n⚠️ No service account file found! You'll need to enter credentials manually.")
    print("You can find these in the Firebase Console > Project Settings > Service Accounts")
    print("NOTE: These credentials will ONLY be stored in memory during script execution.\n")
    
    # Get credentials from user input
    client_email = input("Enter the client_email from your service account: ")
    
    # For the private key, explain how to format it properly
    print("\nFor the private key, copy the ENTIRE private key including BEGIN/END lines.")
    print("Example: -----BEGIN PRIVATE KEY-----\\nMIIEvgIBAD...\\n-----END PRIVATE KEY-----\\n")
    private_key = input("Enter your private key: ")
    
    # Create a credentials dictionary with user input
    credentials_dict = {
        "type": "service_account",
        "project_id": project_id,
        "private_key_id": "manual_entry_" + str(int(time.time())),
        "private_key": private_key,
        "client_email": client_email,
        "client_id": "",
        "auth_uri": "https://accounts.google.com/o/oauth2/auth",
        "token_uri": "https://oauth2.googleapis.com/token",
        "auth_provider_x509_cert_url": "https://www.googleapis.com/oauth2/v1/certs",
        "client_x509_cert_url": f"https://www.googleapis.com/robot/v1/metadata/x509/{client_email.replace('@', '%40')}"
    }
    
    return credentials.Certificate(credentials_dict)

# Initialize Firebase with more robust error handling
print("Extracting Firebase configuration...")
firebase_config = extract_firebase_config()

if not firebase_config:
    print("Creating a service account key file is REQUIRED for this script to work.")
    print("Follow these steps:")
    print("1. Go to Firebase Console > Project Settings > Service Accounts")
    print("2. Click 'Generate new private key'")
    print("3. Save the file in this directory as 'serviceAccountKey.json'")
    print("4. Run this script again")
    exit(1)

project_id = firebase_config.get('projectId')
print(f"Using project ID: {project_id}")

# Get Firebase credentials
cred = get_firebase_credentials(project_id)

try:
    # Initialize Firebase with the credentials
    firebase_admin.initialize_app(cred, {
        'projectId': project_id,
    })
    db = firestore.client()
    print("Firebase initialized successfully")
    
    # Test connection
    try:
        test_doc = db.collection('_test_connection').document('test')
        test_doc.set({'timestamp': firestore.SERVER_TIMESTAMP})
        test_doc.delete()
        print("✅ Firestore connection verified")
    except Exception as e:
        print(f"⚠️ Firestore connection test failed: {e}")
        print("The script may not have full access to your Firestore database.")
        
except Exception as e:
    print(f"❌ Error initializing Firebase: {e}")
    print("\nThis script REQUIRES a valid service account key.")
    print("Please follow these steps:")
    print("1. Go to Firebase Console > Project Settings > Service Accounts")
    print("2. Click 'Generate new private key'")
    print("3. Save the file in this directory as 'serviceAccountKey.json'")
    exit(1)

# Firebase operation counters for free tier limits
firebase_writes = 0
firebase_deletes = 0
firebase_reads = 0
FIREBASE_DAILY_WRITE_LIMIT = 18000  # Set below the 20k limit to be safe

# Track progress for resuming on future runs
progress_file = "firebase_upload_progress.json"
last_actor_id = None
skip_to_actor = False

# Load progress if available
if os.path.exists(progress_file):
    try:
        with open(progress_file, "r") as f:
            progress = json.load(f)
            last_actor_id = progress.get("actor_id")
            firebase_writes = progress.get("writes", 0)
            firebase_reads = progress.get("reads", 0)
            
            if last_actor_id:
                print(f"Resuming upload after actor ID {last_actor_id}")
                skip_to_actor = True
    except Exception as e:
        print(f"Error loading progress: {e}")

def save_progress(actor_id):
    """Save current progress to allow resuming later"""
    with open(progress_file, "w") as f:
        json.dump({
            "actor_id": actor_id,
            "writes": firebase_writes,
            "reads": firebase_reads
        }, f)

def delete_all_firestore_data():
    """Delete all existing data from Firestore"""
    global firebase_deletes, firebase_reads
    
    # Ask for confirmation
    confirm = input("Are you sure you want to delete all existing Firestore data? (yes/no): ")
    if confirm.lower() != "yes":
        print("Deletion cancelled")
        return
    
    try:
        print("Retrieving actor documents...")
        actors_ref = db.collection('actors')
        actors = actors_ref.stream()
        
        # Count documents first
        actors_list = list(actors)
        total_actors = len(actors_list)
        firebase_reads += total_actors
        
        print(f"Deleting {total_actors} actor documents and their subcollections...")
        
        # Delete each actor document and its subcollections
        deleted = 0
        for actor_doc in tqdm(actors_list, desc="Deleting actors"):
            actor_id = actor_doc.id
            
            # Delete movie_credits subcollection
            movie_credits = actors_ref.document(actor_id).collection('movie_credits').stream()
            for credit in movie_credits:
                credit.reference.delete()
                firebase_deletes += 1
                deleted += 1
                
                # Check if we're approaching the limit
                if firebase_deletes % 1000 == 0:
                    print(f"Deleted {firebase_deletes} documents so far...")
                    time.sleep(1)  # Prevent rate limiting
            
            # Delete connections subcollection
            connections = actors_ref.document(actor_id).collection('connections').stream()
            for connection in connections:
                connection.reference.delete()
                firebase_deletes += 1
                deleted += 1
            
            # Delete the actor document itself
            actor_doc.reference.delete()
            firebase_deletes += 1
            deleted += 1
        
        print(f"Deleted {deleted} documents in total")
        
    except Exception as e:
        print(f"Error during deletion: {e}")

def upload_actor_to_firestore(actor_id, actor_data, movie_credits, connection_data=None):
    """Upload actor data to Firestore in batches"""
    global firebase_writes
    
    if firebase_writes >= FIREBASE_DAILY_WRITE_LIMIT:
        print(f"Approaching Firebase write limit ({firebase_writes}), stopping upload")
        return False
    
    try:
        # Create a batch for actor data
        batch = db.batch()
        actor_id_str = str(actor_id)
        
        # Set actor document with optimized structure for React queries
        actor_ref = db.collection('actors').document(actor_id_str)
        batch.set(actor_ref, {
            'id': actor_id,
            'name': actor_data.get('name', ''),
            'popularity': actor_data.get('popularity', 0),
            'profile_path': actor_data.get('profile_path', ''),
            'searchable_name': actor_data.get('name', '').lower(),  # For case-insensitive search
            'connection_count': len(connection_data) if connection_data else 0
        })
        
        # Commit actor data batch
        batch.commit()
        firebase_writes += 1
        
        # Process movie credits in smaller batches
        if movie_credits:
            for i in range(0, len(movie_credits), 400):
                movie_batch = db.batch()
                batch_count = 0
                
                for movie in movie_credits[i:i+400]:
                    movie_id = movie.get('id')
                    if not movie_id:
                        continue
                        
                    movie_ref = actor_ref.collection('movie_credits').document(str(movie_id))
                    movie_batch.set(movie_ref, {
                        'id': movie_id,
                        'title': movie.get('title', ''),
                        'character': movie.get('character', ''),
                        'popularity': movie.get('popularity', 0),
                        'release_date': movie.get('release_date', ''),
                        'poster_path': movie.get('poster_path', '')
                    })
                    batch_count += 1
                
                if batch_count > 0:
                    movie_batch.commit()
                    firebase_writes += batch_count
                    
                    if firebase_writes >= FIREBASE_DAILY_WRITE_LIMIT:
                        print(f"Approaching write limit ({firebase_writes}), pausing upload")
                        return False
        
        # Process actor connections if provided
        if connection_data:
            for i in range(0, len(connection_data), 400):
                connection_batch = db.batch()
                batch_count = 0
                
                # Get a slice of connections to process
                connection_items = list(connection_data.items())[i:i+400]
                
                for connected_actor_id, connection_info in connection_items:
                    connection_ref = actor_ref.collection('connections').document(str(connected_actor_id))
                    
                    # Find the most popular movie they worked on together
                    projects = connection_info.get('projects', [])
                    top_project = None
                    if projects:
                        projects.sort(key=lambda x: x.get('popularity', 0), reverse=True)
                        top_project = projects[0]
                    
                    connection_batch.set(connection_ref, {
                        'actor_id': int(connected_actor_id),
                        'projects_count': len(projects),
                        'top_project_id': top_project.get('id') if top_project else None,
                        'top_project_title': top_project.get('title') if top_project else None
                    })
                    batch_count += 1
                
                if batch_count > 0:
                    connection_batch.commit()
                    firebase_writes += batch_count
                    
                    if firebase_writes >= FIREBASE_DAILY_WRITE_LIMIT:
                        print(f"Approaching write limit ({firebase_writes}), pausing upload")
                        return False
        
        print(f"Actor {actor_id} uploaded with {len(movie_credits)} movies and {len(connection_data or {})} connections")
        return True
        
    except Exception as e:
        print(f"Error uploading actor {actor_id}: {e}")
        return False

def find_actor_connections(conn, actor_id):
    """Find all actors who have appeared in the same movies as this actor"""
    cursor = conn.cursor()
    cursor.execute('''
        SELECT mc1.actor_id as connected_actor_id, 
               mc1.id as movie_id, 
               mc1.title as movie_title,
               mc1.popularity as movie_popularity
        FROM movie_credits mc1
        JOIN movie_credits mc2 ON mc1.id = mc2.id
        WHERE mc2.actor_id = ? AND mc1.actor_id != ?
    ''', (actor_id, actor_id))
    
    connections = {}
    for row in cursor.fetchall():
        connected_actor_id = str(row[0])  # Use index since row factory might not be enabled
        
        if connected_actor_id not in connections:
            connections[connected_actor_id] = {
                'projects': []
            }
        
        connections[connected_actor_id]['projects'].append({
            'id': row[1],
            'title': row[2],
            'popularity': row[3]
        })
    
    return connections

def process_actors_database(db_path="actor-game/public/actors.db", limit=None):
    """Process the actors database"""
    global firebase_writes, skip_to_actor, last_actor_id
    
    if not os.path.exists(db_path):
        alt_paths = [
            "./actors.db",
            "public/actors.db",
            "../actor-game/public/actors.db"
        ]
        
        for path in alt_paths:
            if os.path.exists(path):
                db_path = path
                break
        
        if not os.path.exists(db_path):
            print(f"Database not found at {db_path} or alternative locations")
            return False
    
    print(f"Processing database: {db_path}")
    
    try:
        # Connect to SQLite database
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get actor count for progress tracking
        cursor.execute("SELECT COUNT(*) FROM actors")
        total_actors = cursor.fetchone()[0]
        
        if limit:
            print(f"Limited to processing {limit} actors out of {total_actors}")
            total_actors = min(limit, total_actors)
        
        # Get all actors
        if limit:
            cursor.execute('''
                SELECT id, name, popularity, profile_path, place_of_birth 
                FROM actors
                ORDER BY id
                LIMIT ?
            ''', (limit,))
        else:
            cursor.execute('''
                SELECT id, name, popularity, profile_path, place_of_birth 
                FROM actors
                ORDER BY id
            ''')
        
        # Process each actor with a progress bar
        for actor in tqdm(cursor.fetchall(), total=total_actors, desc="Processing actors"):
            actor_id = actor[0]
            
            # Skip actors until we reach the last processed one (for resuming)
            if skip_to_actor and last_actor_id and actor_id <= last_actor_id:
                continue
            
            # We've reached or passed the last processed actor, stop skipping
            if skip_to_actor and last_actor_id and actor_id > last_actor_id:
                skip_to_actor = False
            
            # Get movie credits
            movie_cursor = conn.cursor()
            movie_cursor.execute('''
                SELECT id, title, character, popularity, release_date, poster_path 
                FROM movie_credits 
                WHERE actor_id = ?
            ''', (actor_id,))
            
            movie_credits = []
            for mc in movie_cursor.fetchall():
                movie_credits.append({
                    'id': mc[0],
                    'title': mc[1],
                    'character': mc[2],
                    'popularity': mc[3],
                    'release_date': mc[4],
                    'poster_path': mc[5]
                })
            
            # Get actor connections
            connections = find_actor_connections(conn, actor_id)
            
            # Create actor data dictionary
            actor_data = {
                'id': actor_id,
                'name': actor[1],
                'popularity': actor[2],
                'profile_path': actor[3],
                'place_of_birth': actor[4] if len(actor) > 4 else 'Unknown',
            }
            
            # Upload to Firestore
            success = upload_actor_to_firestore(
                actor_id, 
                actor_data, 
                movie_credits,
                connections
            )
            
            # Save progress every 10 actors or when we hit a limit
            if actor_id % 10 == 0 or not success:
                save_progress(actor_id)
                
            # If we hit the write limit, stop processing
            if not success or firebase_writes >= FIREBASE_DAILY_WRITE_LIMIT:
                conn.close()
                return False
            
            # Add a small delay to avoid overwhelming Firestore
            if actor_id % 50 == 0:
                time.sleep(1)
        
        conn.close()
        return True
        
    except Exception as e:
        print(f"Error processing database: {e}")
        return False

# Main execution
if __name__ == "__main__":
    try:
        print("\n✨ ActorToActor Database to Firestore Migration Tool ✨\n")
        
        # Get processing options
        limit_option = input("How many actors to process? (Enter a number or 'all' for all actors): ")
        actor_limit = None if limit_option.lower() == 'all' else int(limit_option)
        
        # Clear existing data if needed
        if firebase_writes == 0 and not skip_to_actor:
            delete_choice = input("\nDo you want to delete existing Firestore data before uploading? (yes/no): ")
            if delete_choice.lower() == "yes":
                delete_all_firestore_data()
        
        # Process the actors database
        print("\nStarting database processing...")
        success = process_actors_database(limit=actor_limit)
        
        # If unsuccessful (likely hit a limit), inform about resuming
        if not success:
            print(f"\n⚠️ Pausing upload due to limits or errors. Will resume from actor ID {last_actor_id} next time.")
            print("Run this script again later to continue.")
        else:
            # If we completed all uploads, clean up progress file
            if os.path.exists(progress_file):
                os.remove(progress_file)
                print("\n✅ Upload completed for all actors!")
            
    except Exception as e:
        print(f"\n❌ Error during upload process: {e}")
        
    print(f"\nFirebase operations summary:")
    print(f"- Writes: {firebase_writes}")
    print(f"- Deletes: {firebase_deletes}")
    print(f"- Reads: {firebase_reads}")
    print("\nNote: Firebase free tier has a limit of 50K reads, 20K writes, and 20K deletes per day.")