import os
import sqlite3

# Paths to check:
DB_PATHS = [
    os.path.join("actor-game", "public", "actors.db"),
    os.path.join("actor-game", "public", "actor_connections.db")
]

def check_database(db_path):
    if not os.path.exists(db_path):
        print(f"File not found: {db_path}")
        return

    print(f"Checking {db_path}...")
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        # List all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]

        if not tables:
            print("  No tables found.")
        else:
            print(f"  Tables found: {tables}")

            # Example: Check if there's data for region='United Kingdom'
            # in the actor_connections table (if it exists)
            if "actor_connections" in tables:
                try:
                    cursor.execute("PRAGMA table_info(actor_connections)")
                    columns = [col[1] for col in cursor.fetchall()]
                    if "region" in columns:
                        cursor.execute("SELECT COUNT(*) FROM actor_connections WHERE region='United Kingdom'")
                        count = cursor.fetchone()[0]
                        print(f"  Rows with region='United Kingdom': {count}")
                    else:
                        print("  'actor_connections' table has no 'region' column.")
                except Exception as e:
                    print(f"  Error querying actor_connections: {e}")

        conn.close()
    except Exception as e:
        print(f"  Error opening {db_path}: {e}")

if __name__ == "__main__":
    for path in DB_PATHS:
        check_database(path)