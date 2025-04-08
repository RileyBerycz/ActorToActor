import os
import glob
import sqlite3
import sys
import shutil
import subprocess

def get_db_files(search_dirs):
    """Get database files from multiple locations."""
    all_files = []
    regions = ["EU", "US", "UK", "ASIA", "GLOBAL"]
    
    for directory in search_dirs:
        if os.path.exists(directory):
            print(f"Searching in: {directory}")
            
            # Get all .db files in the directory
            db_files = glob.glob(os.path.join(directory, "*.db"))
            
            if db_files:
                print(f"  Found {len(db_files)} database files:")
                for file in db_files:
                    print(f"    - {os.path.basename(file)}")
                
                # Look for region-specific databases
                region_files = []
                for file in db_files:
                    filename = os.path.basename(file).upper()
                    matched_region = None
                    
                    # Check if file matches any region pattern
                    for region in regions:
                        if f"ACTORS_{region}" in filename:
                            matched_region = region
                            break
                    
                    if matched_region:
                        region_files.append(file)
                        print(f"    → Matched region: {matched_region}")
                    else:
                        print(f"    → No region match")
                
                if not region_files:
                    print("  No region-specific databases found. Using all .db files instead.")
                    all_files.extend(db_files)
                else:
                    all_files.extend(region_files)
            else:
                print("  No .db files found in this directory")
    
    return all_files

def ensure_output_directory(output_path):
    """Create output directory if it doesn't exist."""
    output_dir = os.path.dirname(output_path)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)
        print(f"Created output directory: {output_dir}")
    return output_dir

def combine_databases(output_db_path, search_dirs):
    """Combine multiple databases into a single database file."""
    # Ensure output directory exists
    ensure_output_directory(output_db_path)
    
    # Get database files from all search directories
    db_files = get_db_files(search_dirs)
    
    if not db_files:
        print("No matching database files found in any location.")
        return
    
    print(f"Found {len(db_files)} database files to combine:")
    for db in db_files:
        print(f"  - {db}")
    
    # Remove the output file if it already exists
    if os.path.exists(output_db_path):
        os.remove(output_db_path)
    
    # Create the output database
    output_conn = sqlite3.connect(output_db_path)
    
    tables_created = set()
    
    for db_file in db_files:
        print(f"Processing: {os.path.basename(db_file)}")
        
        # Connect to the source database
        source_conn = sqlite3.connect(db_file)
        cursor = source_conn.cursor()
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall() if not row[0].startswith('sqlite_')]
        
        for table_name in tables:
            # Get table schema
            cursor.execute(f"SELECT sql FROM sqlite_master WHERE type='table' AND name='{table_name}';")
            create_table_sql = cursor.fetchone()[0]
            
            # Create the table in the output database if it doesn't exist
            if table_name not in tables_created:
                try:
                    output_conn.execute(create_table_sql)
                    output_conn.commit()
                    tables_created.add(table_name)
                except sqlite3.OperationalError as e:
                    print(f"  Warning: Could not create table {table_name}: {e}")
                    continue
            
            # Get column names
            cursor.execute(f"PRAGMA table_info({table_name});")
            columns = [col[1] for col in cursor.fetchall()]
            
            if not columns:
                continue
            
            # Get data from source table
            cursor.execute(f"SELECT * FROM {table_name};")
            rows = cursor.fetchall()
            
            if rows:
                # Create INSERT statement with column names
                columns_str = ', '.join(columns)
                placeholders = ', '.join(['?'] * len(columns))
                insert_sql = f"INSERT OR IGNORE INTO {table_name} ({columns_str}) VALUES ({placeholders})"
                
                # Insert data in batches
                batch_size = 1000
                for i in range(0, len(rows), batch_size):
                    batch = rows[i:i+batch_size]
                    output_conn.executemany(insert_sql, batch)
                
                output_conn.commit()
                print(f"  Added {len(rows)} records to table {table_name}")
        
        source_conn.close()
    
    output_conn.close()
    print(f"Successfully combined databases into {output_db_path}")

def cleanup_old_databases(output_db_path):
    """Remove and untrack old database files after successful combination."""
    print("\nCleaning up old database files...")
    
    # Skip cleanup if combination wasn't successful
    if not os.path.exists(output_db_path):
        print("Skipping cleanup because output database wasn't created")
        return
    
    # Directories to search for old databases
    search_dirs = [
        os.path.join(os.getcwd(), "public"),
        os.path.join(os.getcwd(), "actor-game", "public"),
        os.getcwd()
    ]
    
    # Patterns to match old databases (but exclude the new combined one)
    old_db_patterns = ["actors_*.db"]
    
    # Track statistics
    removed_count = 0
    untracked_count = 0
    
    # Find and remove old database files
    for directory in search_dirs:
        if not os.path.exists(directory):
            continue
            
        for pattern in old_db_patterns:
            old_files = glob.glob(os.path.join(directory, pattern))
            
            for file_path in old_files:
                # Skip the output file
                if os.path.abspath(file_path) == os.path.abspath(output_db_path):
                    continue
                
                print(f"Found old database: {file_path}")
                
                # Check if file is tracked by Git
                result = subprocess.run(
                    ['git', 'ls-files', '--error-unmatch', file_path], 
                    stdout=subprocess.PIPE, 
                    stderr=subprocess.PIPE,
                    text=True,
                    shell=True
                )
                
                if result.returncode == 0:  # File is tracked
                    print(f"  Removing from Git tracking: {os.path.basename(file_path)}")
                    try:
                        subprocess.run(['git', 'rm', file_path], check=True, shell=True)
                        untracked_count += 1
                    except subprocess.CalledProcessError:
                        print(f"  Failed to remove from Git, deleting manually")
                        os.remove(file_path)
                else:  # File is not tracked
                    print(f"  Deleting: {os.path.basename(file_path)}")
                    os.remove(file_path)
                
                removed_count += 1
    
    print(f"Cleanup complete: Removed {removed_count} files, untracked {untracked_count} from Git")
    
    # Update .gitignore to exclude actor_*.db files in the future
    gitignore_path = os.path.join(os.getcwd(), '.gitignore')
    gitignore_patterns = [
        "# Exclude old actor databases",
        "actors_*.db",
        "public/actors_*.db",
        "actor-game/public/actors_*.db"
    ]
    
    if os.path.exists(gitignore_path):
        with open(gitignore_path, 'r') as f:
            content = f.read()
        
        # Check if patterns already exist
        missing_patterns = [p for p in gitignore_patterns if p not in content]
        
        if missing_patterns:
            with open(gitignore_path, 'a') as f:
                f.write("\n\n" + "\n".join(missing_patterns) + "\n")
            print("Updated .gitignore to exclude old database patterns")
    else:
        # Create new .gitignore
        with open(gitignore_path, 'w') as f:
            f.write("\n".join(gitignore_patterns) + "\n")
        print("Created .gitignore to exclude old database patterns")

if __name__ == "__main__":
    # Multiple search directories to look for source databases
    search_dirs = [
        os.path.join(os.getcwd(), "public"),
        os.path.join(os.getcwd(), "actor-game", "public"),
        os.getcwd()
    ]
    
    # Set default output path to actor-game/public directory
    output_db_path = os.path.join(os.getcwd(), "actor-game", "public", "actors.db")
    
    # Allow command-line arguments to override defaults
    if len(sys.argv) > 1:
        output_db_path = sys.argv[1]
    
    print(f"Search directories:")
    for dir in search_dirs:
        print(f"  - {dir}")
    print(f"Output database: {output_db_path}")
    
    # Add needed import for the cleanup function
    import subprocess
    
    # Combine the databases
    combine_databases(output_db_path, search_dirs)
    
    # Clean up old database files
    cleanup_old_databases(output_db_path)
    
    print("\nProcess completed.")