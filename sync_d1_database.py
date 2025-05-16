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

def download_database(url, output_path, max_retries=5):
    """Download database file from URL with retry logic for rate limits"""
    print(f"Downloading database from {url}...")
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, stream=True)
            
            # If we hit a rate limit, wait and retry
            if response.status_code == 429:
                retry_after = int(response.headers.get('Retry-After', 60))
                print(f"Rate limit hit. Waiting {retry_after} seconds before retry {attempt+1}/{max_retries}...")
                time.sleep(retry_after)
                continue
                
            response.raise_for_status()
            
            with open(output_path, 'wb') as f:
                for chunk in response.iter_content(chunk_size=8192):
                    f.write(chunk)
            
            print(f"Database downloaded to {output_path}")
            return output_path
            
        except requests.exceptions.HTTPError as e:
            if response.status_code == 429 and attempt < max_retries - 1:
                # GitHub doesn't always include Retry-After header, use exponential backoff
                wait_time = 60 * (2 ** attempt)
                print(f"Rate limit hit. Waiting {wait_time} seconds before retry {attempt+1}/{max_retries}...")
                time.sleep(wait_time)
            else:
                print(f"HTTP error downloading {url}: {str(e)}")
                if attempt < max_retries - 1:
                    wait_time = 30 * (attempt + 1)
                    print(f"Retrying in {wait_time} seconds... (Attempt {attempt+1}/{max_retries})")
                    time.sleep(wait_time)
                else:
                    raise
                    
        except requests.exceptions.RequestException as e:
            print(f"Error downloading {url}: {str(e)}")
            if attempt < max_retries - 1:
                wait_time = 30 * (attempt + 1)
                print(f"Retrying in {wait_time} seconds... (Attempt {attempt+1}/{max_retries})")
                time.sleep(wait_time)
            else:
                raise
    
    raise Exception(f"Failed to download {url} after {max_retries} attempts")

def extract_table(conn, table_name, output_file, batch_size=100):  # Reduced batch size
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
            batch_size = 10  # Only 10 rows per INSERT statement
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
        priority_tables = ['actors', 'actor_connections']  # Tables to process first
        remaining_tables = []

        for sql_file in os.listdir(sql_dir):
            # Add priority tables to the front of the queue
            file_path = os.path.join(sql_dir, sql_file)
            if any(table in sql_file for table in priority_tables):
                try:
                    print(f"Processing priority table: {sql_file}")
                    execute_sql_in_batches(file_path)
                except Exception as e:
                    print(f"Error processing priority table {sql_file}: {str(e)}")
                    # Continue with other tables
            else:
                remaining_tables.append(sql_file)

        # Process remaining tables
        for sql_file in remaining_tables:
            try:
                file_path = os.path.join(sql_dir, sql_file)
                print(f"Processing table: {sql_file}")
                execute_sql_in_batches(file_path)
            except Exception as e:
                print(f"Error processing table {sql_file}: {str(e)}")
                # Continue with other tables
        
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

def reset_d1_database():
    """Reset the D1 database to clean state"""
    print(f"Resetting D1 database '{D1_DATABASE_NAME}'...")
    
    # Start with a fresh database by deleting and recreating it
    # List the database to get its ID
    result = subprocess.run(
        ["wrangler", "d1", "list", "--json"],
        capture_output=True,
        text=True,
        env=dict(os.environ, CLOUDFLARE_API_TOKEN=CLOUDFLARE_API_TOKEN)
    )
    
    if result.returncode != 0:
        print(f"Error listing D1 databases: {result.stderr}")
        raise Exception("Failed to list D1 databases")
    
    # Try to parse the JSON output to get the database ID
    try:
        import json
        databases = json.loads(result.stdout)
        db_id = None
        for db in databases:
            if db.get('name') == D1_DATABASE_NAME:
                db_id = db.get('uuid')
                break
        
        if not db_id:
            print(f"Database {D1_DATABASE_NAME} not found")
            return False
        
        # Delete the database
        print(f"Deleting database {D1_DATABASE_NAME} (ID: {db_id})...")
        delete_result = subprocess.run(
            ["wrangler", "d1", "delete", D1_DATABASE_NAME, "--yes"],
            capture_output=True,
            text=True,
            env=dict(os.environ, CLOUDFLARE_API_TOKEN=CLOUDFLARE_API_TOKEN)
        )
        
        if delete_result.returncode != 0:
            print(f"Error deleting database: {delete_result.stderr}")
            print(f"Output: {delete_result.stdout}")
        
        # Recreate the database
        print(f"Creating new database {D1_DATABASE_NAME}...")
        create_result = subprocess.run(
            ["wrangler", "d1", "create", D1_DATABASE_NAME],
            capture_output=True,
            text=True,
            env=dict(os.environ, CLOUDFLARE_API_TOKEN=CLOUDFLARE_API_TOKEN)
        )
        
        if create_result.returncode != 0:
            print(f"Error creating database: {create_result.stderr}")
            print(f"Output: {create_result.stdout}")
            raise Exception("Failed to recreate D1 database")
        
        print(f"Database {D1_DATABASE_NAME} reset successfully")
        return True
    
    except Exception as e:
        print(f"Error resetting database: {str(e)}")
        raise

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

def execute_sql_in_batches(file_path, batch_size=2):  # Even smaller batch size
    """Execute SQL file in small batches instead of all at once"""
    print(f"Executing {file_path} in batches...")
    
    with open(file_path, 'r', encoding='utf-8') as f:
        sql_content = f.read()
    
    # Split by semicolon to get individual SQL statements
    statements = sql_content.split(';')
    statements = [s.strip() for s in statements if s.strip()]
    total_statements = len(statements)
    
    print(f"Found {total_statements} SQL statements to execute")
    
    # Create a temp directory for batch files
    batch_dir = os.path.dirname(file_path)
    base_name = os.path.basename(file_path)
    
    # Add a success counter
    success_count = 0
    
    # Execute in small batches
    for i in range(0, total_statements, batch_size):
        batch = statements[i:i+batch_size]
        if not batch:
            continue
            
        # Create a temporary file for this batch
        batch_sql = ';\n'.join(batch) + ';'
        batch_file = os.path.join(batch_dir, f"{base_name}_batch_{i}.sql")
        
        with open(batch_file, 'w', encoding='utf-8') as f:
            f.write(batch_sql)
        
        print(f"Executing batch {i//batch_size + 1}/{(total_statements + batch_size - 1)//batch_size}...")
        
        # Use --file instead of --command for large SQL
        result = subprocess.run(
            ["wrangler", "d1", "execute", D1_DATABASE_NAME, 
             "--file", batch_file, "--remote"],
            capture_output=True,
            text=True,
            env=dict(os.environ, CLOUDFLARE_API_TOKEN=CLOUDFLARE_API_TOKEN),
            timeout=120
        )
        
        if result.returncode != 0:
            print(f"Error executing batch {i//batch_size + 1}:")
            print(f"STDERR: {result.stderr}")
            print(f"STDOUT: {result.stdout}")
            
            # Try an even smaller batch as fallback
            if batch_size > 1 and "argument list too long" in result.stderr.lower():
                print("Command line too long, trying with smaller batches...")
                for statement in batch:
                    single_batch_file = os.path.join(batch_dir, f"{base_name}_single_{i}.sql")
                    with open(single_batch_file, 'w', encoding='utf-8') as f:
                        f.write(statement + ";")
                        
                    single_result = subprocess.run(
                        ["wrangler", "d1", "execute", D1_DATABASE_NAME, 
                         "--file", single_batch_file, "--remote"],
                        capture_output=True,
                        text=True,
                        env=dict(os.environ, CLOUDFLARE_API_TOKEN=CLOUDFLARE_API_TOKEN),
                        timeout=120
                    )
                    
                    if single_result.returncode != 0:
                        print(f"Error executing single statement:")
                        print(f"STDERR: {single_result.stderr}")
                    else:
                        print("Successfully executed single statement")
            else:
                # Don't fail immediately, try to continue with next batch
                print("Continuing with next batch...")
        else:
            success_count += len(batch)
            print(f"Successfully executed batch {i//batch_size + 1}/{(total_statements + batch_size - 1)//batch_size}")
            print(f"Progress: {success_count}/{total_statements} statements executed")

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

def clear_tables_in_database():
    """Clear all tables in the database instead of recreating it"""
    print(f"Clearing tables in D1 database '{D1_DATABASE_NAME}'...")
    
    # Get list of tables in the database
    list_tables_cmd = "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
    
    result = subprocess.run(
        ["wrangler", "d1", "execute", D1_DATABASE_NAME, 
         "--command", list_tables_cmd, "--remote", "--json"],
        capture_output=True,
        text=True,
        env=dict(os.environ, CLOUDFLARE_API_TOKEN=CLOUDFLARE_API_TOKEN)
    )
    
    if result.returncode != 0:
        print(f"Error listing tables: {result.stderr}")
        print("Continuing with upload anyway...")
        return
    
    try:
        import json
        result_json = json.loads(result.stdout)
        if 'results' in result_json:
            tables = [row['name'] for row in result_json['results'][0]['rows']]
            print(f"Found tables: {tables}")
            
            # Delete data from each table
            for table in tables:
                print(f"Clearing table {table}...")
                delete_cmd = f"DELETE FROM {table};"
                
                del_result = subprocess.run(
                    ["wrangler", "d1", "execute", D1_DATABASE_NAME, 
                     "--command", delete_cmd, "--remote"],
                    capture_output=True,
                    text=True,
                    env=dict(os.environ, CLOUDFLARE_API_TOKEN=CLOUDFLARE_API_TOKEN)
                )
                
                if del_result.returncode != 0:
                    print(f"Warning: Could not clear table {table}: {del_result.stderr}")
                else:
                    print(f"Table {table} cleared successfully")
    except Exception as e:
        print(f"Warning: Error clearing tables: {str(e)}")
        print("Continuing with upload anyway...")

def generate_migrations():
    """Convert SQL files to Wrangler migrations"""
    print("Generating D1 migrations from SQLite databases...")
    
    # Create temp directory for downloads
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Create migrations directory
        migrations_dir = "migrations"
        os.makedirs(migrations_dir, exist_ok=True)
        
        # Generate timestamp for migration version
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        
        # First, create a schema-only migration to ensure tables exist
        schema_file = os.path.join(migrations_dir, f"{timestamp}_001_schema.sql")
        
        # Download databases
        local_dbs = {}
        for name, url in DATABASE_URLS.items():
            output_path = os.path.join(temp_dir, f"{name}.db")
            local_dbs[name] = download_database(url, output_path)
        
        # Extract and write table schemas
        with open(schema_file, 'w', encoding='utf-8') as f:
            for db_name, db_path in local_dbs.items():
                conn = sqlite3.connect(db_path)
                cursor = conn.cursor()
                
                # Get all tables
                cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
                tables = [row[0] for row in cursor.fetchall()]
                
                for table in tables:
                    # Get the CREATE TABLE statement
                    cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'")
                    create_sql = cursor.fetchone()[0]
                    
                    # Write table create statement
                    f.write(f"-- Table: {table}\n")
                    f.write(f"DROP TABLE IF EXISTS {table};\n")
                    f.write(f"{create_sql};\n\n")
                
                conn.close()
        
        print(f"Created schema migration: {schema_file}")
        
        # Generate data migrations with explicit column names for each table
        file_index = 2
        for db_name, db_path in local_dbs.items():
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row  # This enables column access by name
            cursor = conn.cursor()
            
            # Get all tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = [row[0] for row in cursor.fetchall()]
            
            for table in tables:
                # Get column info first
                cursor.execute(f"PRAGMA table_info({table})")
                columns = [col['name'] for col in cursor.fetchall()]
                
                # Skip if table has no columns (unlikely but possible)
                if not columns:
                    continue
                
                # Get row count
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                row_count = cursor.fetchone()[0]
                
                if row_count == 0:
                    continue  # Skip empty tables
                
                # Very small batch size for large tables
                batch_size = 10
                if table in ['movie_credits', 'tv_credits']:
                    batch_size = 5  # Smaller for very large tables
                
                # Generate INSERT statements with explicit column names
                for offset in range(0, row_count, batch_size):
                    batch_file = os.path.join(
                        migrations_dir, 
                        f"{timestamp}_{file_index:03d}_{table}_data.sql"
                    )
                    file_index += 1
                    
                    cursor.execute(f"SELECT * FROM {table} LIMIT {batch_size} OFFSET {offset}")
                    rows = cursor.fetchall()
                    
                    with open(batch_file, 'w', encoding='utf-8') as f:
                        f.write(f"-- Data for {table} (Batch {offset//batch_size + 1})\n\n")
                        
                        # Generate INSERT statements with explicit column lists
                        column_str = ", ".join([f"`{col}`" for col in columns])
                        
                        for row in rows:
                            values = []
                            for col in columns:
                                val = row[col]
                                if val is None:
                                    values.append("NULL")
                                elif isinstance(val, (int, float)):
                                    values.append(str(val))
                                elif isinstance(val, bytes):
                                    hex_data = val.hex()
                                    values.append(f"X'{hex_data}'")
                                else:
                                    # Escape single quotes in string values
                                    escaped_val = str(val).replace("'", "''")
                                    values.append(f"'{escaped_val}'")
                            
                            values_str = ", ".join(values)
                            # f.write(f"INSERT INTO `{table}` ({column_str}) VALUES ({values_str});\n")
                            f.write(f"INSERT OR REPLACE INTO `{table}` ({column_str}) VALUES ({values_str});\n")
                    
                    print(f"Created data migration for {table} (Batch {offset//batch_size + 1})")
            
            conn.close()
        
        print(f"Migration files created successfully in {os.path.abspath(migrations_dir)}")
        return True
        
    finally:
        # Clean up
        shutil.rmtree(temp_dir, ignore_errors=True)

def apply_migrations():
    """Apply migrations to D1 database"""
    print(f"Applying migrations to {D1_DATABASE_NAME}...")
    
    # Ensure wrangler.toml exists
    if not ensure_wrangler_toml():
        print("Cannot apply migrations without wrangler.toml")
        return False
    
    # First fix the migration files
    fix_migration_files()
    
    # Now try to apply migrations
    max_retries = 3
    for attempt in range(max_retries):
        result = subprocess.run(
            ["wrangler", "d1", "migrations", "apply", D1_DATABASE_NAME, "--remote"],
            capture_output=True,
            text=True,
            env=dict(os.environ, CLOUDFLARE_API_TOKEN=CLOUDFLARE_API_TOKEN)
        )
        
        if result.returncode == 0:
            print("Migrations applied successfully")
            return True
            
        # If we have errors, try to identify and fix them
        error_msg = result.stderr
        
        # Check for schema/column mismatch errors
        if "table" in error_msg and "has no column named" in error_msg:
            print(f"Schema mismatch detected (attempt {attempt+1}/{max_retries})")
            
            # Extract the problematic migration file
            import re
            match = re.search(r"Migration (.*?) failed", error_msg)
            if match:
                problem_file = match.group(1)
                print(f"Problem detected in migration file: {problem_file}")
                
                # Skip the problematic file
                file_path = os.path.join("migrations", problem_file)
                if os.path.exists(file_path):
                    os.rename(file_path, file_path + ".error")
                    print(f"Renamed problematic migration: {problem_file} to {problem_file}.error")
        else:
            print(f"Error applying migrations: {error_msg}")
            
        if attempt < max_retries - 1:
            print(f"Retrying in 5 seconds...")
            time.sleep(5)
    
    # If we get here, all attempts failed but we want to continue
    print("Migration completed with some errors - database may be partially updated")
    return True

def ensure_wrangler_toml():
    """Create wrangler.toml file if it doesn't exist"""
    wrangler_file = "wrangler.toml"
    
    if os.path.exists(wrangler_file):
        print(f"{wrangler_file} already exists")
        return True
    
    print(f"Creating {wrangler_file}...")
    
    # Get database ID
    result = subprocess.run(
        ["wrangler", "d1", "list", "--json"],
        capture_output=True,
        text=True,
        env=dict(os.environ, CLOUDFLARE_API_TOKEN=CLOUDFLARE_API_TOKEN)
    )
    
    if result.returncode != 0:
        print(f"Error listing databases: {result.stderr}")
        return False
    
    try:
        import json
        databases = json.loads(result.stdout)
        db_id = None
        
        for db in databases:
            if db.get('name') == D1_DATABASE_NAME:
                db_id = db.get('uuid')
                break
        
        if not db_id:
            print(f"Database {D1_DATABASE_NAME} not found")
            return False
            
        # Create wrangler.toml
        with open(wrangler_file, 'w') as f:
            f.write(f"""name = "actor-to-actor-api"

# D1 database configuration
[[d1_databases]]
binding = "DB"
database_name = "{D1_DATABASE_NAME}"
database_id = "{db_id}"
""")
        
        print(f"Created {wrangler_file} with database ID {db_id}")
        return True
    
    except Exception as e:
        print(f"Error creating {wrangler_file}: {str(e)}")
        return False

def show_migration_content(migration_file):
    """Print the content of a migration file for debugging"""
    print(f"Examining migration file: {migration_file}")
    with open(os.path.join("migrations", migration_file), 'r', encoding='utf-8') as f:
        print(f.read())

def download_github_file(repo_owner, repo_name, path, output_path):
    """Download a file from GitHub using the GitHub API with authentication"""
    github_token = os.environ.get("GITHUB_TOKEN")
    if not github_token:
        print("Warning: GITHUB_TOKEN not set. Using anonymous API which has lower rate limits.")
    
    api_url = f"https://api.github.com/repos/{repo_owner}/{repo_name}/contents/{path}"
    headers = {"Accept": "application/vnd.github.v3.raw"}
    
    if github_token:
        headers["Authorization"] = f"token {github_token}"
    
    response = requests.get(api_url, headers=headers, stream=True)
    response.raise_for_status()
    
    with open(output_path, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
    
    return output_path

def fix_schema_mismatches(migration_files_dir="migrations"):
    """Find and fix schema mismatches in migration files"""
    print("Checking for schema mismatches...")
    
    # First, get the actual schema from D1
    result = subprocess.run(
        ["wrangler", "d1", "execute", D1_DATABASE_NAME, 
         "--command", "SELECT name, sql FROM sqlite_master WHERE type='table'", "--remote", "--json"],
        capture_output=True,
        text=True,
        env=dict(os.environ, CLOUDFLARE_API_TOKEN=CLOUDFLARE_API_TOKEN)
    )
    
    if result.returncode != 0:
        print(f"Error getting schema: {result.stderr}")
        return False
    
    # Parse the D1 schema
    d1_tables = {}
    try:
        import json
        import re
        
        data = json.loads(result.stdout)
        for row in data.get('results', [{}])[0].get('rows', []):
            table_name = row.get('name')
            create_sql = row.get('sql')
            
            # Extract column names using regex
            if create_sql:
                # Find text between parentheses in CREATE TABLE statement
                match = re.search(r'\((.*)\)', create_sql, re.DOTALL)
                if match:
                    columns_text = match.group(1)
                    # Split by commas, but ignore commas within parentheses
                    columns = []
                    current = ""
                    paren_level = 0
                    for char in columns_text:
                        if char == '(' or char == '[':
                            paren_level += 1
                            current += char
                        elif char == ')' or char == ']':
                            paren_level -= 1
                            current += char
                        elif char == ',' and paren_level == 0:
                            columns.append(current.strip())
                            current = ""
                        else:
                            current += char
                    
                    if current:
                        columns.append(current.strip())
                    
                    # Extract just the column names
                    column_names = []
                    for col in columns:
                        # Match column name (first word in definition)
                        col_match = re.match(r'`?([^`\s]+)`?\s+', col)
                        if col_match:
                            column_names.append(col_match.group(1))
                    
                    d1_tables[table_name] = column_names
    except Exception as e:
        print(f"Error parsing schema: {str(e)}")
        return False
    
    print(f"D1 schema found for tables: {list(d1_tables.keys())}")
    
    # Now fix schema issues in migration files
    fixed_count = 0
    for filename in os.listdir(migration_files_dir):
        if not filename.endswith('.sql'):
            continue
            
        # Only check data migration files
        if '_data.sql' not in filename:
            continue
            
        # Extract table name from filename
        table_parts = filename.split('_')
        if len(table_parts) < 3:
            continue
            
        # Find the table name
        table_name = None
        for part in table_parts:
            if part in d1_tables:
                table_name = part
                break
        
        if not table_name:
            continue
            
        # Check if we need to fix this file
        filepath = os.path.join(migration_files_dir, filename)
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # Check for INSERT statements
        if 'INSERT' not in content:
            continue
            
        # Find all INSERT statements and check column lists
        modified = False
        output_lines = []
        
        for line in content.split('\n'):
            if line.strip().startswith('INSERT'):
                # Parse the column list from the INSERT statement
                col_match = re.search(r'INSERT\s+OR\s+REPLACE\s+INTO\s+`?\w+`?\s*\((.*?)\)', line)
                if col_match:
                    col_list = col_match.group(1)
                    cols = [c.strip('` ') for c in col_list.split(',')]
                    
                    # Check for columns that don't exist in D1
                    valid_cols = []
                    values_start_idx = line.find('VALUES')
                    if values_start_idx == -1:
                        output_lines.append(line)
                        continue
                        
                    values_text = line[values_start_idx:].strip()
                    values_match = re.search(r'VALUES\s*\((.*?)\)', values_text)
                    if values_match:
                        values_list = values_match.group(1).split(',')
                        
                        for i, col in enumerate(cols):
                            # Check if column exists in D1 schema
                            if col in d1_tables[table_name]:
                                valid_cols.append(col)
                            else:
                                print(f"Column {col} not found in D1 table {table_name}, skipping")
                    
                    if valid_cols:
                        # Rewrite the INSERT statement with valid columns
                        valid_col_str = ", ".join([f"`{col}`" for col in valid_cols])
                        output_lines.append(f"INSERT OR REPLACE INTO `{table_name}` ({valid_col_str}) VALUES ({values_match.group(1)});")
                        modified = True
                    else:
                        output_lines.append(line)
            else:
                output_lines.append(line)
        
        if modified:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write('\n'.join(output_lines))
            fixed_count += 1
            print(f"Fixed schema mismatch in {filename}")
    
    print(f"Fixed {fixed_count} migration files with schema mismatches")
    return True

def get_d1_schema():
    """Get the current schema from D1 database"""
    print("Fetching current D1 database schema...")
    
    result = subprocess.run(
        ["wrangler", "d1", "execute", D1_DATABASE_NAME, 
         "--command", "SELECT name, sql FROM sqlite_master WHERE type='table'", "--remote", "--json"],
        capture_output=True,
        text=True,
        env=dict(os.environ, CLOUDFLARE_API_TOKEN=CLOUDFLARE_API_TOKEN)
    )
    
    if result.returncode != 0:
        print(f"Error getting schema: {result.stderr}")
        return {}
    
    d1_tables = {}
    try:
        import json
        
        data = json.loads(result.stdout)
        rows = data.get('results', [{}])[0].get('rows', [])
        
        for row in rows:
            if not isinstance(row, dict):
                continue
                
            table_name = row.get('name')
            if not table_name:
                continue
            
            # Get columns directly with another query
            col_result = subprocess.run(
                ["wrangler", "d1", "execute", D1_DATABASE_NAME, 
                 f"--command", f"PRAGMA table_info({table_name})", "--remote", "--json"],
                capture_output=True,
                text=True,
                env=dict(os.environ, CLOUDFLARE_API_TOKEN=CLOUDFLARE_API_TOKEN)
            )
            
            if col_result.returncode == 0:
                col_data = json.loads(col_result.stdout)
                col_rows = col_data.get('results', [{}])[0].get('rows', [])
                
                columns = []
                for col_row in col_rows:
                    if isinstance(col_row, dict) and 'name' in col_row:
                        columns.append(col_row['name'])
                
                d1_tables[table_name] = columns
                print(f"Table {table_name} has columns: {columns}")
        
        return d1_tables
        
    except Exception as e:
        print(f"Error parsing schema: {str(e)}")
        return {}

def fix_migration_files():
    """Fix migration files to match the D1 schema"""
    print("Fixing migration files to match D1 schema...")
    
    # Get current D1 schema
    d1_schema = get_d1_schema()
    if not d1_schema:
        print("Could not get D1 schema, cannot fix migration files")
        return False
    
    # Process all migration files
    fixed_count = 0
    skipped_count = 0
    
    for filename in os.listdir("migrations"):
        if not filename.endswith('.sql'):
            continue
        
        # Skip schema files
        if "schema" in filename:
            continue
        
        # Find which table this migration is for
        table_name = None
        for table in d1_schema.keys():
            if table in filename:
                table_name = table
                break
        
        if not table_name:
            print(f"Couldn't determine table for {filename}, skipping")
            continue
        
        filepath = os.path.join("migrations", filename)
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Skip files without INSERT statements
        if "INSERT" not in content:
            continue
        
        # Fix the migration file
        fixed_content = []
        has_inserts = False
        
        for line in content.split('\n'):
            if line.strip().startswith('--') or not line.strip():
                fixed_content.append(line)
                continue
                
            if "INSERT" in line:
                has_inserts = True
                
                # Find column list
                import re
                col_match = re.search(r'\((.*?)\)\s+VALUES', line)
                if not col_match:
                    fixed_content.append(f"-- SKIPPED: {line}")
                    continue
                
                col_text = col_match.group(1)
                cols = [c.strip('` ') for c in col_text.split(',')]
                
                # Find value list
                val_match = re.search(r'VALUES\s*\((.*?)\)', line)
                if not val_match:
                    fixed_content.append(f"-- SKIPPED: {line}")
                    continue
                    
                val_text = val_match.group(1)
                
                # Split values, respecting quotes and parentheses
                values = []
                current = ""
                in_quotes = False
                for char in val_text:
                    if char == "'" and (not current or current[-1] != '\\'):
                        in_quotes = not in_quotes
                        current += char
                    elif char == ',' and not in_quotes:
                        values.append(current.strip())
                        current = ""
                    else:
                        current += char
                
                if current:
                    values.append(current.strip())
                
                # Filter to only include columns in the D1 schema
                valid_cols = []
                valid_values = []
                
                for i, col in enumerate(cols):
                    if col in d1_schema.get(table_name, []):
                        valid_cols.append(f"`{col}`")
                        if i < len(values):
                            valid_values.append(values[i])
                        else:
                            valid_values.append("NULL")
                
                if valid_cols:
                    new_line = f"INSERT OR REPLACE INTO `{table_name}` ({', '.join(valid_cols)}) VALUES ({', '.join(valid_values)});"
                    fixed_content.append(new_line)
                else:
                    fixed_content.append(f"-- SKIPPED: {line}")
            else:
                fixed_content.append(line)
        
        # Only write back if we modified the file and it has valid inserts
        if has_inserts:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write('\n'.join(fixed_content))
            fixed_count += 1
            print(f"Fixed migration file: {filename}")
        else:
            # Rename files with no valid inserts
            os.rename(filepath, filepath + ".skipped")
            skipped_count += 1
            print(f"Skipped migration file: {filename}")
    
    print(f"Fixed {fixed_count} migration files, skipped {skipped_count} files")
    return True

def direct_sql_execution_fallback():
    """Fallback method to update database if migrations fail"""
    print("Using direct SQL execution as fallback...")
    
    # Get schema first
    d1_schema = get_d1_schema()
    if not d1_schema:
        print("Could not get D1 schema for fallback")
        return False
    
    # Priority tables to update
    priority_tables = ["actors", "actor_connections"]
    
    for table in priority_tables:
        if table not in d1_schema:
            print(f"Table {table} not found in D1 schema, skipping")
            continue
            
        print(f"Executing direct SQL update for {table}...")
        
        # Find migration files for this table
        filenames = []
        for filename in os.listdir("migrations"):
            if filename.endswith('.sql') and table in filename and "schema" not in filename:
                filenames.append(filename)
        
        # Sort by filename to maintain order
        filenames.sort()
        
        for filename in filenames:
            filepath = os.path.join("migrations", filename)
            
            # Skip already error-marked files
            if filename.endswith('.error') or filename.endswith('.skipped'):
                continue
                
            print(f"Processing {filename}...")
            
            # Execute the file directly
            result = subprocess.run(
                ["wrangler", "d1", "execute", D1_DATABASE_NAME, 
                 "--file", filepath, "--remote"],
                capture_output=True,
                text=True,
                env=dict(os.environ, CLOUDFLARE_API_TOKEN=CLOUDFLARE_API_TOKEN)
            )
            
            if result.returncode != 0:
                print(f"Error executing {filename}: {result.stderr}")
            else:
                print(f"Successfully executed {filename}")
    
    return True

if __name__ == "__main__":
    # Force non-interactive mode for CI environments
    os.environ["CI"] = "true"
    
    # Parse command line arguments
    import sys
    if len(sys.argv) > 1:
        if sys.argv[1] == "generate-migrations":
            # Generate migration files
            if not verify_environment():
                exit(1)
            ensure_latest_wrangler()
            generate_migrations()
        elif sys.argv[1] == "apply-migrations":
            # Apply existing migrations
            if not verify_environment():
                exit(1)
            ensure_latest_wrangler()
            
            success = apply_migrations()
            
            # If migrations had errors, try direct SQL as fallback
            if not success:
                direct_sql_execution_fallback()
        else:
            print(f"Unknown command: {sys.argv[1]}")
            print("Available commands: generate-migrations, apply-migrations")
            exit(1)
    else:
        # Default behavior: sync database directly
        create_d1_database_if_not_exists()
        clear_tables_in_database()
        sync_database()