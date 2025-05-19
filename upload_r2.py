import os
import requests
import time
import urllib.request
from datetime import datetime
import re

# Configuration - will be overridden by environment variables in GitHub Actions
DATABASE_URLS = {
    'actors': 'https://raw.githubusercontent.com/RileyBerycz/ActorToActor/main/actor-game/public/actors.db',
    'connections': 'https://raw.githubusercontent.com/RileyBerycz/ActorToActor/main/actor-game/public/actor_connections.db'
}
BUCKET_NAME = os.environ.get("CF_R2_BUCKET_NAME", "actor-to-actor-db-storage")
CF_ACCOUNT_ID = os.environ.get("CF_ACCOUNT_ID", "")  # Will be set in GitHub Actions
CF_API_TOKEN = os.environ.get("CF_API_TOKEN", "")    # Will be set in GitHub Actions
MAX_VERSIONS = 3  # Maximum number of backup versions to keep (excluding latest)

def download_file(url, target_path):
    """Download a file from URL to target path"""
    print(f"Downloading {url} to {target_path}...")
    os.makedirs(os.path.dirname(target_path), exist_ok=True)
    urllib.request.urlretrieve(url, target_path)
    print(f"Downloaded to {target_path} ({os.path.getsize(target_path) / 1024:.1f} KB)")
    return target_path

def upload_to_r2(file_path, key):
    """Upload file to R2 storage using direct API instead of CLI"""
    print(f"Uploading {file_path} to R2 as {key}...")
    
    if not CF_ACCOUNT_ID or not CF_API_TOKEN:
        raise ValueError("Cloudflare account ID and API token are required")
    
    # Direct API upload
    api_url = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/r2/buckets/{BUCKET_NAME}/objects/{key}"
    
    with open(file_path, 'rb') as file:
        headers = {
            "Authorization": f"Bearer {CF_API_TOKEN}",
            "Content-Type": "application/octet-stream"
        }
        
        response = requests.put(api_url, headers=headers, data=file)
        
    if response.status_code >= 200 and response.status_code < 300:
        print(f"Successfully uploaded {key} to R2")
        return key
    else:
        print(f"Error uploading to R2: {response.status_code}")
        print(response.text)
        raise Exception(f"R2 upload failed for {key}: {response.status_code}")

def list_r2_objects(prefix=""):
    """List objects in R2 bucket with the given prefix"""
    if not CF_ACCOUNT_ID or not CF_API_TOKEN:
        raise ValueError("Cloudflare account ID and API token are required")
    
    api_url = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/r2/buckets/{BUCKET_NAME}/objects"
    
    if prefix:
        api_url += f"?prefix={prefix}"
    
    headers = {
        "Authorization": f"Bearer {CF_API_TOKEN}"
    }
    
    response = requests.get(api_url, headers=headers)
    
    if response.status_code >= 200 and response.status_code < 300:
        return response.json().get('result', {}).get('objects', [])
    else:
        print(f"Error listing R2 objects: {response.status_code}")
        print(response.text)
        return []

def delete_r2_object(key):
    """Delete an object from R2 bucket"""
    if not CF_ACCOUNT_ID or not CF_API_TOKEN:
        raise ValueError("Cloudflare account ID and API token are required")
    
    api_url = f"https://api.cloudflare.com/client/v4/accounts/{CF_ACCOUNT_ID}/r2/buckets/{BUCKET_NAME}/objects/{key}"
    
    headers = {
        "Authorization": f"Bearer {CF_API_TOKEN}"
    }
    
    response = requests.delete(api_url, headers=headers)
    
    if response.status_code >= 200 and response.status_code < 300:
        print(f"Successfully deleted {key} from R2")
        return True
    else:
        print(f"Error deleting from R2: {response.status_code}")
        print(response.text)
        return False

def cleanup_old_versions(db_name):
    """Keep only the specified number of most recent versions"""
    print(f"Cleaning up old versions of {db_name}...")
    
    # List all objects with the db_name prefix
    objects = list_r2_objects(prefix=f"{db_name}/")
    
    # Filter to only get timestamped versions (not latest.db)
    timestamp_pattern = re.compile(r'^' + db_name + r'/(\d{8}_\d{6})_' + db_name + r'\.db$')
    timestamped_objects = []
    
    for obj in objects:
        key = obj.get('key', '')
        match = timestamp_pattern.match(key)
        if match:
            timestamp = match.group(1)
            timestamped_objects.append({
                'key': key,
                'timestamp': timestamp,
                'uploaded': obj.get('uploaded', '')
            })
    
    # Sort by timestamp, newest first
    timestamped_objects.sort(key=lambda x: x['timestamp'], reverse=True)
    
    # If we have more than MAX_VERSIONS, delete the oldest ones
    if len(timestamped_objects) > MAX_VERSIONS:
        for obj in timestamped_objects[MAX_VERSIONS:]:
            print(f"Deleting old version: {obj['key']} (uploaded on {obj['uploaded']})")
            delete_r2_object(obj['key'])
    
    print(f"Cleanup complete. Keeping {min(MAX_VERSIONS, len(timestamped_objects))} versions of {db_name}")

def main():
    """Download and upload database files to R2"""
    start_time = time.time()
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create data directory
    data_dir = "data"
    os.makedirs(data_dir, exist_ok=True)
    
    try:
        # Download databases
        local_dbs = {}
        for name, url in DATABASE_URLS.items():
            output_path = os.path.join(data_dir, f"{name}.db")
            local_dbs[name] = download_file(url, output_path)
        
        # Upload to R2 with both timestamped and latest versions
        for name, path in local_dbs.items():
            # Upload with timestamp (for versioning/history)
            versioned_key = f"{name}/{timestamp}_{name}.db"
            upload_to_r2(path, versioned_key)
            
            # Upload as latest version (for API use)
            latest_key = f"{name}/latest.db"
            upload_to_r2(path, latest_key)
            
            # Clean up old versions to maintain MAX_VERSIONS
            cleanup_old_versions(name)
        
        print(f"\nCompleted in {time.time() - start_time:.2f} seconds")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    main()