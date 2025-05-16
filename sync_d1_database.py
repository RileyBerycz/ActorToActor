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
CLOUDFLARE_TOKEN = os.environ.get("CLOUDFLARE_API_TOKEN")
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
            print(f"Executing {sql_file}...")
            
            # Add timeout parameter for large files
            result = subprocess.run(
                ["npx", "wrangler", "d1", "execute", D1_DATABASE_NAME, "--remote", 
                 "--json=false", "--command-timeout=300", f"--file={file_path}"],
                capture_output=True,
                text=True
            )
            
            if result.returncode != 0:
                print(f"Error executing {sql_file}: {result.stderr}")
                raise Exception(f"D1 upload failed for {sql_file}")
            else:
                print(f"Successfully executed {sql_file}")
        
        print(f"\nSync completed in {time.time() - start_time:.2f} seconds")
        
    except Exception as e:
        print(f"Sync error: {str(e)}")
        raise
    finally:
        # Clean up temp files
        shutil.rmtree(temp_dir, ignore_errors=True)

if __name__ == "__main__":
    if not CLOUDFLARE_API_TOKEN:
        print("Error: CLOUDFLARE_API_TOKEN environment variable not set")
        exit(1)
    
    sync_database()