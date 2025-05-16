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
        os.makedirs("migrations", exist_ok=True)
        
        # Generate timestamp for migration version
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        
        # Download databases
        local_dbs = {}
        for name, url in DATABASE_URLS.items():
            output_path = os.path.join(temp_dir, f"{name}.db")
            local_dbs[name] = download_database(url, output_path)
        
        # Create schema migrations
        for db_name, db_path in local_dbs.items():
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            
            # Get list of tables
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'")
            tables = [row[0] for row in cursor.fetchall()]
            
            # Create migration file for schema
            schema_migration = f"migrations/{timestamp}_{db_name}_schema.sql"
            with open(schema_migration, 'w', encoding='utf-8') as f:
                for table in tables:
                    cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table}'")
                    create_sql = cursor.fetchone()[0]
                    f.write(f"DROP TABLE IF EXISTS {table};\n")
                    f.write(f"{create_sql};\n\n")
            
            print(f"Created schema migration for {db_name} database")
            
            # Create data migrations (smaller files for each table)
            for table in tables:
                # Get row count to decide if we need to split
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                row_count = cursor.fetchone()[0]
                
                if row_count == 0:
                    continue  # Skip empty tables
                
                # For small tables (<100 rows), create a single migration
                if row_count < 100:
                    data_migration = f"migrations/{timestamp}_{db_name}_{table}_data.sql"
                    with open(data_migration, 'w', encoding='utf-8') as f:
                        # Get all data
                        cursor.execute(f"SELECT * FROM {table}")
                        rows = cursor.fetchall()
                        column_names = [description[0] for description in cursor.description]
                        
                        # Generate INSERT statements
                        f.write(f"-- Data migration for {table} table ({row_count} rows)\n\n")
                        
                        column_str = ', '.join(column_names)
                        for row in rows:
                            # Format values
                            values = []
                            for val in row:
                                if val is None:
                                    values.append("NULL")
                                elif isinstance(val, (int, float)):
                                    values.append(str(val))
                                elif isinstance(val, bytes):
                                    hex_data = val.hex()
                                    values.append(f"X'{hex_data}'")
                                else:
                                    escaped_val = str(val).replace("'", "''")
                                    values.append(f"'{escaped_val}'")
                            
                            values_str = ', '.join(values)
                            f.write(f"INSERT INTO {table} ({column_str}) VALUES ({values_str});\n")
                    
                    print(f"Created data migration for {table} table ({row_count} rows)")
                else:
                    # For large tables, split into multiple migration files (100 rows per file)
                    batch_size = 100
                    cursor.execute(f"SELECT * FROM {table}")
                    column_names = [description[0] for description in cursor.description]
                    column_str = ', '.join(column_names)
                    
                    batch_num = 1
                    while True:
                        rows = cursor.fetchmany(batch_size)
                        if not rows:
                            break
                            
                        data_migration = f"migrations/{timestamp}_{db_name}_{table}_data_{batch_num:03d}.sql"
                        with open(data_migration, 'w', encoding='utf-8') as f:
                            f.write(f"-- Data migration for {table} table (batch {batch_num}, {len(rows)} rows)\n\n")
                            
                            for row in rows:
                                # Format values
                                values = []
                                for val in row:
                                    if val is None:
                                        values.append("NULL")
                                    elif isinstance(val, (int, float)):
                                        values.append(str(val))
                                    elif isinstance(val, bytes):
                                        hex_data = val.hex()
                                        values.append(f"X'{hex_data}'")
                                    else:
                                        escaped_val = str(val).replace("'", "''")
                                        values.append(f"'{escaped_val}'")
                                
                                values_str = ', '.join(values)
                                f.write(f"INSERT INTO {table} ({column_str}) VALUES ({values_str});\n")
                                
                        print(f"Created data migration for {table} table (batch {batch_num}, {len(rows)} rows)")
                        batch_num += 1
            
            conn.close()
        
        print("\nMigration files created successfully!")
        print(f"Migration files location: {os.path.abspath('migrations')}")
        print("\nTo apply migrations:")
        print(f"1. cd to your worker directory")
        print(f"2. wrangler d1 migrations apply {D1_DATABASE_NAME}")
        
    finally:
        # Clean up temp directory
        shutil.rmtree(temp_dir, ignore_errors=True)

def apply_migrations():
    """Apply migrations to D1 database"""
    print(f"Applying migrations to {D1_DATABASE_NAME}...")
    
    # Get the directory containing wrangler.toml
    wrangler_dir = "."  # Change this if wrangler.toml is in a subdirectory
    
    result = subprocess.run(
        ["wrangler", "d1", "migrations", "apply", D1_DATABASE_NAME, "--remote"],
        capture_output=True,
        text=True,
        env=dict(os.environ, CLOUDFLARE_API_TOKEN=CLOUDFLARE_API_TOKEN),
        cwd=wrangler_dir  # This ensures Wrangler runs in the directory with wrangler.toml
    )
    
    if result.returncode != 0:
        print(f"Error applying migrations: {result.stderr}")
        return False
    else:
        print("Migrations applied successfully")
        return True

if __name__ == "__main__":
    # Force non-interactive mode for CI environments
    os.environ["CI"] = "true"
    
    # Verify environment first
    if not verify_environment():
        print("Environment verification failed. Exiting.")
        exit(1)
    
    # Ensure latest wrangler is installed
    ensure_latest_wrangler()
    
    # Parse command line arguments
    import sys
    if len(sys.argv) > 1:
        if sys.argv[1] == "generate-migrations":
            # Generate migration files
            generate_migrations()
        elif sys.argv[1] == "apply-migrations":
            # Apply existing migrations
            apply_migrations()
        else:
            print(f"Unknown command: {sys.argv[1]}")
            print("Available commands: generate-migrations, apply-migrations")
            exit(1)
    else:
        # Default behavior: sync database directly
        create_d1_database_if_not_exists()
        clear_tables_in_database()
        sync_database()