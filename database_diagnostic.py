import os
import sqlite3
import pandas as pd

def debug_database(db_path):
    """Analyze a SQLite database and print detailed information"""
    if not os.path.exists(db_path):
        print(f"❌ Database not found at {db_path}")
        return
    
    file_size_mb = os.path.getsize(db_path) / (1024 * 1024)
    print(f"📊 Database: {db_path} (Size: {file_size_mb:.2f} MB)")
    
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print(f"📋 Tables found: {[t[0] for t in tables]}")
        
        # Get row counts for each table
        for table in tables:
            table_name = table[0]
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            count = cursor.fetchone()[0]
            print(f"  - {table_name}: {count} rows")
        
        # For actors, get a sample
        if 'actors' in [t[0] for t in tables]:
            cursor.execute("SELECT * FROM actors LIMIT 5")
            columns = [description[0] for description in cursor.description]
            sample = cursor.fetchall()
            
            print(f"\n📝 Sample actors columns: {columns}")
            for row in sample:
                print(f"  - {row}")
            
            # Get actor popularity range
            cursor.execute("SELECT MIN(popularity), MAX(popularity), AVG(popularity) FROM actors")
            pop_stats = cursor.fetchone()
            print(f"\n📈 Actor popularity: Min={pop_stats[0]:.2f}, Max={pop_stats[1]:.2f}, Avg={pop_stats[2]:.2f}")
        
        # Check for movie credits
        if 'movie_credits' in [t[0] for t in tables]:
            cursor.execute("SELECT COUNT(DISTINCT actor_id) FROM movie_credits")
            actor_count = cursor.fetchone()[0]
            print(f"\n🎬 Actors with movie credits: {actor_count}")
            
            # Get credits per actor
            cursor.execute("""
                SELECT actor_id, COUNT(*) as credit_count 
                FROM movie_credits 
                GROUP BY actor_id 
                ORDER BY credit_count DESC 
                LIMIT 5
            """)
            top_actors = cursor.fetchall()
            print(f"📊 Top actors by movie credit count:")
            for actor_id, count in top_actors:
                cursor.execute(f"SELECT name FROM actors WHERE id = {actor_id}")
                name_row = cursor.fetchone()
                name = name_row[0] if name_row else "Unknown"
                print(f"  - {name} (ID: {actor_id}): {count} movies")
        
        conn.close()
        
    except Exception as e:
        print(f"❌ Error analyzing database: {e}")

# Run diagnostics on all databases
for region in ['GLOBAL', 'US', 'UK']:
    for path in [
        f"public/actors_{region}.db",
        f"actor-game/public/actors_{region}.db",
        f"./actors_{region}.db"
    ]:
        if os.path.exists(path):
            debug_database(path)
            break