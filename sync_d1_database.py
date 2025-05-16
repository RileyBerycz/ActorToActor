#!/usr/bin/env python3
import os
import sqlite3
import subprocess
import requests
import time
import tempfile
import shutil
from datetime import datetime

# Configuration
DATABASE_URLS = {
    'actors': 'https://raw.githubusercontent.com/RileyBerycz/ActorToActor/main/actor-game/public/actors.db',
    'connections': 'https://raw.githubusercontent.com/RileyBerycz/ActorToActor/main/actor-game/public/actor_connections.db'
}
D1_DATABASE_NAME = "actor-to-actor-db"
CLOUDFLARE_API_TOKEN = os.environ.get("CLOUDFLARE_API_TOKEN")
SQLITE_BUSY_TIMEOUT = 60000  # 60 seconds

def download_database(url, output_path):
    """Download database file from URL"""
    print(f"Downloading database from {url}...")
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    with open(output_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    print(f"Database downloaded to {output_path}")
    return output_path

def extract_table(conn, table_name, output_file):
    """Extract table schema and data to SQL file"""
    print(f"Extracting {table_name}...")
    cursor = conn.cursor()
    
    try:
        # Get table schema
        cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        schema = cursor.fetchone()
        
        if not schema:
            print(f"Table {table_name} not found")
            return
        
        create_statement = schema[0]
        
        # Write DROP and CREATE statements
        with open(output_file, 'w', encoding='utf-8') as f:
            f.write(f"DROP TABLE IF EXISTS {table_name};\n")
            f.write(f"{create_statement};\n\n")
            
            # Get table data
            cursor.execute(f"SELECT * FROM {table_name}")
            rows = cursor.fetchall()
            column_names = [description[0] for description in cursor.description]
            
            # Generate INSERT statements in batches
            batch_size = 500
            for i in range(0, len(rows), batch_size):
                batch = rows[i:i+batch_size]
                if batch:
                    values_list = []
                    for row in batch:
                        # Convert row values to proper SQL format
                        formatted_values = []
                        for val in row:
                            if val is None:
                                formatted_values.append("NULL")
                            elif isinstance(val, (int, float)):
                                formatted_values.append(str(val))
                            elif isinstance(val, bytes):
                                # Handle binary data (like gzipped data in actor_connections)
                                hex_data = val.hex()
                                formatted_values.append(f"X'{hex_data}'")
                            else:
                                # FIX: Use double quotes outside and single quotes inside
                                # This avoids the f-string backslash escape issue
                                escaped_val = str(val).replace("'", "''")
                                formatted_values.append(f"'{escaped_val}'")
                        
                        values_list.append(f"({', '.join(formatted_values)})")
                    
                    column_str = ', '.join(column_names)
                    values_str = ',\n'.join(values_list)
                    f.write(f"INSERT INTO {table_name} ({column_str}) VALUES\n{values_str};\n\n")
        
        print(f"Extracted {len(rows)} rows from {table_name}")
    except Exception as e:
        print(f"Error extracting table {table_name}: {str(e)}")
        raise

def sync_database():
    """Orchestrate the entire sync process"""
    start_time = time.time()
    print(f"Starting database sync at {datetime.now().isoformat()}")
    
    # Create temp directory
    temp_dir = tempfile.mkdtemp()
    sql_dir = os.path.join(temp_dir, 'sql')
    os.makedirs(sql_dir, exist_ok=True)
    
    try:
        # Download databases
        local_dbs = {}
        for name, url in DATABASE_URLS.items():
            output_path = os.path.join(temp_dir, f"{name}.db")
            local_dbs[name] = download_database(url, output_path)
        
        # Extract actors tables
        actors_conn = sqlite3.connect(local_dbs['actors'])
        actors_conn.row_factory = sqlite3.Row
        
        # Get all tables from actors.db
        cursor = actors_conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        actor_tables = [row[0] for row in cursor.fetchall()]
        
        for table in actor_tables:
            output_file = os.path.join(sql_dir, f"{table}.sql")
            extract_table(actors_conn, table, output_file)
        
        actors_conn.close()
        
        # Extract connections tables
        connections_conn = sqlite3.connect(local_dbs['connections'])
        connections_conn.row_factory = sqlite3.Row
        
        # Get all tables from connections.db
        cursor = connections_conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
        connection_tables = [row[0] for row in cursor.fetchall()]
        
        for table in connection_tables:
            output_file = os.path.join(sql_dir, f"{table}.sql")
            extract_table(connections_conn, table, output_file)
        
        connections_conn.close()
        
        # Upload to D1 using wrangler
        print("\nUploading to D1...")
        for sql_file in os.listdir(sql_dir):
            file_path = os.path.join(sql_dir, sql_file)
            file_size = os.path.getsize(file_path) / (1024 * 1024)  # Size in MB
            
            # If the file is large, split it
            if file_size > 5:  # 5MB as a threshold
                print(f"{sql_file} is {file_size:.2f}MB, splitting into smaller files...")
                split_files = split_sql_file(file_path)
                files_to_process = split_files
            else:
                files_to_process = [file_path]
            
            # Process each file (original or split)
            for process_file in files_to_process:
                file_name = os.path.basename(process_file)
                print(f"Executing {file_name}...")
                
                # Add better error handling and debugging
                try:
                    # Add timeout to the command
                    result = subprocess.run(
                        ["wrangler", "d1", "execute", D1_DATABASE_NAME, 
                         "--file", process_file, "--remote"],
                        capture_output=True,
                        text=True,
                        env=dict(os.environ, CLOUDFLARE_API_TOKEN=CLOUDFLARE_API_TOKEN),
                        timeout=300  # 5 minute timeout
                    )
                    
                    if result.returncode != 0:
                        print(f"Error executing {file_name}:")
                        print(f"STDERR: {result.stderr}")
                        print(f"STDOUT: {result.stdout}")
                        raise Exception(f"D1 upload failed for {file_name}")
                    else:
                        print(f"Successfully executed {file_name}")
                        
                except subprocess.TimeoutExpired:
                    print(f"Command timed out when executing {file_name}")
                    raise Exception(f"D1 upload timed out for {file_name}")
                except Exception as e:
                    print(f"Exception when executing {file_name}: {str(e)}")
                    raise
        
        print(f"\nSync completed in {time.time() - start_time:.2f} seconds")
        
    except Exception as e:
        print(f"Sync error: {str(e)}")
        raise
    finally:
        # Clean up temp files
        shutil.rmtree(temp_dir, ignore_errors=True)

def ensure_latest_wrangler():
    """Ensure latest Wrangler is installed"""
    print("Checking Wrangler installation...")
    # First uninstall old wrangler if it exists
    try:
        subprocess.run(["npm", "uninstall", "-g", "@cloudflare/wrangler"], capture_output=True)
    except:
        pass  # Ignore if not installed
        
    # Install latest wrangler
    print("Installing latest Wrangler...")
    result = subprocess.run(
        ["npm", "install", "-g", "wrangler"], 
        capture_output=True, 
        text=True
    )
    
    if result.returncode != 0:
        print(f"Error installing Wrangler: {result.stderr}")
        raise Exception("Failed to install latest Wrangler")
    
    # Verify installation
    result = subprocess.run(
        ["wrangler", "--version"],
        capture_output=True,
        text=True
    )
    
    if result.returncode == 0:
        print(f"Using Wrangler version: {result.stdout.strip()}")
    else:
        raise Exception("Failed to verify Wrangler installation")

def create_d1_database_if_not_exists():
    """Create the D1 database if it doesn't exist"""
    print(f"Checking if D1 database '{D1_DATABASE_NAME}' exists...")
    
    # List existing D1 databases
    result = subprocess.run(
        ["wrangler", "d1", "list"],
        capture_output=True,
        text=True,
        env=dict(os.environ, CLOUDFLARE_API_TOKEN=CLOUDFLARE_API_TOKEN)
    )
    
    if result.returncode != 0:
        print(f"Error listing D1 databases: {result.stderr}")
        raise Exception("Failed to list D1 databases")
    
    # Check if our database exists in the output
    if D1_DATABASE_NAME not in result.stdout:
        print(f"Creating D1 database '{D1_DATABASE_NAME}'...")
        create_result = subprocess.run(
            ["wrangler", "d1", "create", D1_DATABASE_NAME],
            capture_output=True,
            text=True,
            env=dict(os.environ, CLOUDFLARE_API_TOKEN=CLOUDFLARE_API_TOKEN)
        )
        
        if create_result.returncode != 0:
            print(f"Error creating D1 database: {create_result.stderr}")
            raise Exception("Failed to create D1 database")
        
        print(f"D1 database '{D1_DATABASE_NAME}' created successfully")
    else:
        print(f"D1 database '{D1_DATABASE_NAME}' already exists")

def split_sql_file(file_path, max_statements=50):
    """Split a large SQL file into smaller files with fewer statements"""
    print(f"Splitting {file_path} into smaller files...")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Keep the schema (DROP and CREATE statements)
    lines = content.split('\n')
    schema_lines = []
    data_lines = []
    in_schema = True
    
    for line in lines:
        if in_schema and line.startswith("INSERT INTO"):
            in_schema = False
            
        if in_schema:
            schema_lines.append(line)
        else:
            data_lines.append(line)
    
    schema = '\n'.join(schema_lines)
    
    # Split INSERT statements
    inserts = []
    current_insert = []
    insert_count = 0
    
    for line in data_lines:
        if line.startswith("INSERT INTO") and insert_count > 0:
            inserts.append('\n'.join(current_insert))
            current_insert = [line]
            insert_count += 1
        else:
            current_insert.append(line)
            if "INSERT INTO" in line:
                insert_count += 1
    
    if current_insert:
        inserts.append('\n'.join(current_insert))
    
    # Create split files
    base_name = os.path.splitext(file_path)[0]
    output_files = []
    
    # Create batches of insert statements
    for i in range(0, len(inserts), max_statements):
        batch = inserts[i:i+max_statements]
        if batch:
            output_file = f"{base_name}_{i//max_statements+1}.sql"
            with open(output_file, 'w', encoding='utf-8') as f:
                f.write(f"{schema}\n\n")
                f.write('\n\n'.join(batch))
            output_files.append(output_file)
    
    return output_files

def verify_environment():
    """Verify all required environment variables are set"""
    print("Verifying environment...")
    
    # Check Node.js version
    node_result = subprocess.run(
        ["node", "--version"],
        capture_output=True,
        text=True
    )
    
    if node_result.returncode == 0:
        node_version = node_result.stdout.strip()
        print(f"Node.js version: {node_version}")
        
        # Check if version is at least 20.0.0
        version_parts = node_version.lstrip('v').split('.')
        major = int(version_parts[0])
        if major < 20:
            print(f"ERROR: Node.js version must be at least 20.0.0. Found {node_version}")
            print("Please update your Node.js installation.")
            return False
    else:
        print("Error checking Node.js version")
        return False
    
    # Check for CLOUDFLARE_API_TOKEN
    if not CLOUDFLARE_API_TOKEN:
        print("ERROR: CLOUDFLARE_API_TOKEN environment variable not set")
        return False
    
    print("Environment verification complete.")
    return True

if __name__ == "__main__":
    # Verify environment first
    if not verify_environment():
        print("Environment verification failed. Exiting.")
        exit(1)
    
    # Ensure latest wrangler is installed
    ensure_latest_wrangler()
    
    # Create D1 database if it doesn't exist
    create_d1_database_if_not_exists()
    
    # Run the sync
    sync_database()