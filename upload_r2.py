import os
import requests
import time
import urllib.request
from datetime import datetime

# Configuration - will be overridden by environment variables in GitHub Actions
DATABASE_URLS = {
    'actors': 'https://raw.githubusercontent.com/RileyBerycz/ActorToActor/main/actor-game/public/actors.db',
    'connections': 'https://raw.githubusercontent.com/RileyBerycz/ActorToActor/main/actor-game/public/actor_connections.db'
}
BUCKET_NAME = os.environ.get("CF_R2_BUCKET_NAME", "actor-to-actor-db-storage")
CF_ACCOUNT_ID = os.environ.get("CF_ACCOUNT_ID", "")  # Will be set in GitHub Actions
CF_API_TOKEN = os.environ.get("CF_API_TOKEN", "")    # Will be set in GitHub Actions

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
        
        print(f"\nCompleted in {time.time() - start_time:.2f} seconds")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        raise

if __name__ == "__main__":
    main()