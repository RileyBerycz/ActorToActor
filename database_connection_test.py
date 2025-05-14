#!/usr/bin/env python
# filepath: c:\Projects\ActorToActor\test_connection.py
import sqlite3
import os
import json
import gzip

def test_basquiat_connection():
    print("==== Testing 'Basquiat' Connection Issue ====")
    
    # Find the actors database
    possible_paths = [
        "public/actors.db",
        "actor-game/public/actors.db",
        "./actors.db"
    ]
    
    actors_db = None
    for path in possible_paths:
        if os.path.exists(path):
            actors_db = path
            print(f"Found actors database at {path}")
            break
    
    if not actors_db:
        print("Error: Actors database not found!")
        return
        
    # Connect to the actors database
    conn = sqlite3.connect(actors_db)
    conn.row_factory = sqlite3.Row  # This enables column name access
    cursor = conn.cursor()
    
    # Check movie credits
    print("\n=== Checking Movie Credits ===")
    movie_query = """
    SELECT mc1.actor_id, a1.name, mc1.character, 
           mc2.actor_id, a2.name, mc2.character, 
           mc1.title, mc1.id
    FROM movie_credits mc1
    JOIN movie_credits mc2 ON mc1.id = mc2.id
    JOIN actors a1 ON mc1.actor_id = a1.id
    JOIN actors a2 ON mc2.actor_id = a2.id
    WHERE (a1.name LIKE '%Samuel%Jackson%' AND a2.name LIKE '%Pedro%Pascal%')
    OR (a1.name LIKE '%Pedro%Pascal%' AND a2.name LIKE '%Samuel%Jackson%');
    """
    
    cursor.execute(movie_query)
    movie_results = cursor.fetchall()
    
    if movie_results:
        print(f"Found {len(movie_results)} movie connections:")
        for row in movie_results:
            print(f"Movie: {row['title']} (ID: {row['id']})")
            print(f"  {row['name']} as '{row['character']}'")
            print(f"  {row['name']} as '{row['character']}'")
    else:
        print("No movie connections found")
    
    # Check TV credits
    print("\n=== Checking TV Credits ===")
    tv_query = """
    SELECT tc1.actor_id, a1.name, tc1.character, 
           tc2.actor_id, a2.name, tc2.character, 
           tc1.name as show_name, tc1.id
    FROM tv_credits tc1
    JOIN tv_credits tc2 ON tc1.id = tc2.id
    JOIN actors a1 ON tc1.actor_id = a1.id
    JOIN actors a2 ON tc2.actor_id = a2.id
    WHERE (a1.name LIKE '%Samuel%Jackson%' AND a2.name LIKE '%Pedro%Pascal%')
    OR (a1.name LIKE '%Pedro%Pascal%' AND a2.name LIKE '%Samuel%Jackson%');
    """
    
    cursor.execute(tv_query)
    tv_results = cursor.fetchall()
    
    if tv_results:
        print(f"Found {len(tv_results)} TV connections:")
        for row in tv_results:
            print(f"Show: {row['show_name']} (ID: {row['id']})")
            print(f"  {row['name']} as '{row['character']}'")
            print(f"  {row['name']} as '{row['character']}'")
    else:
        print("No TV connections found")
    
    # Check for the precomputed path in the connections database
    print("\n=== Checking Precomputed Connections ===")
    connection_db = "actor-game/public/actor_connections.db"
    
    if os.path.exists(connection_db):
        conn2 = sqlite3.connect(connection_db)
        conn2.row_factory = sqlite3.Row
        cursor2 = conn2.cursor()
        
        # Get actor IDs
        cursor.execute("SELECT id FROM actors WHERE name LIKE '%Samuel%Jackson%'")
        samuel_id = cursor.fetchone()
        cursor.execute("SELECT id FROM actors WHERE name LIKE '%Pedro%Pascal%'")
        pedro_id = cursor.fetchone()
        
        if samuel_id and pedro_id:
            samuel_id = samuel_id['id']
            pedro_id = pedro_id['id']
            
            # Check both directions
            cursor2.execute("""
                SELECT * FROM actor_connections 
                WHERE (start_id = ? AND target_id = ?) OR (start_id = ? AND target_id = ?)
            """, (samuel_id, pedro_id, pedro_id, samuel_id))
            
            connection = cursor2.fetchone()
            
            if connection:
                print(f"Found precomputed connection with length {connection['connection_length']} and difficulty {connection['difficulty']}")
                
                # Decompress the path data
                path_data = gzip.decompress(connection['optimal_path']).decode('utf-8')
                path_items = json.loads(path_data)
                
                print("\nFull path:")
                for item in path_items:
                    if item.get('t') == 'a':  # Actor
                        print(f"Actor: {item.get('n')} (ID: {item.get('i')})")
                    elif item.get('t') == 'm':  # Movie or TV
                        print(f"Media: {item.get('n')} (ID: {item.get('i')})")
            else:
                print("No precomputed connection found")
        else:
            print("Could not find actor IDs")
        
        conn2.close()
    else:
        print(f"Connections database not found at {connection_db}")
    
    # Check if there's any title with 'basquiat' in it
    print("\n=== Searching for 'Basquiat' Titles ===")
    cursor.execute("""
    SELECT 'MOVIE' as type, id, title FROM movie_credits 
    WHERE LOWER(title) LIKE '%basquiat%'
    UNION
    SELECT 'TV' as type, id, name as title FROM tv_credits
    WHERE LOWER(name) LIKE '%basquiat%'
    """)
    
    basquiat_results = cursor.fetchall()
    
    if basquiat_results:
        print(f"Found {len(basquiat_results)} titles containing 'Basquiat':")
        for row in basquiat_results:
            print(f"{row['type']}: {row['title']} (ID: {row['id']})")
            
            # See if these two actors are in this particular title
            params = (row['id'],)
            if row['type'] == 'MOVIE':
                cursor.execute("""
                    SELECT a.name, mc.character
                    FROM movie_credits mc
                    JOIN actors a ON mc.actor_id = a.id
                    WHERE mc.id = ? AND (a.name LIKE '%Samuel%Jackson%' OR a.name LIKE '%Pedro%Pascal%')
                """, params)
            else:
                cursor.execute("""
                    SELECT a.name, tc.character
                    FROM tv_credits tc
                    JOIN actors a ON tc.actor_id = a.id
                    WHERE tc.id = ? AND (a.name LIKE '%Samuel%Jackson%' OR a.name LIKE '%Pedro%Pascal%')
                """, params)
                
            actors_in_title = cursor.fetchall()
            if actors_in_title:
                print("  Actors in this title:")
                for actor in actors_in_title:
                    print(f"    {actor['name']} as '{actor['character']}'")
            else:
                print("  No matching actors in this title")
    else:
        print("No titles containing 'Basquiat' found")
    
    conn.close()
    
    print("\n==== Test Complete ====")

def test_specific_actor_connection(actor1_name="Jackie Chan", actor2_name="Keanu Reeves"):
    print(f"==== Testing Connection Between '{actor1_name}' and '{actor2_name}' ====")
    
    # Find the actors database
    possible_paths = [
        "public/actors.db",
        "actor-game/public/actors.db",
        "./actors.db"
    ]
    
    actors_db = None
    for path in possible_paths:
        if os.path.exists(path):
            actors_db = path
            print(f"Found actors database at {path}")
            break
    
    if not actors_db:
        print("Error: Actors database not found!")
        return
        
    # Connect to the actors database
    conn = sqlite3.connect(actors_db)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()
    
    # Get actor IDs
    cursor.execute(f"SELECT id FROM actors WHERE name LIKE '%{actor1_name.split()[0]}%{actor1_name.split()[-1]}%'")
    actor1_id = cursor.fetchone()
    cursor.execute(f"SELECT id FROM actors WHERE name LIKE '%{actor2_name.split()[0]}%{actor2_name.split()[-1]}%'")
    actor2_id = cursor.fetchone()
    
    if not actor1_id or not actor2_id:
        print(f"Could not find IDs for {actor1_name} and/or {actor2_name}")
        return
        
    actor1_id = actor1_id['id']
    actor2_id = actor2_id['id']
    print(f"Actor IDs: {actor1_name}={actor1_id}, {actor2_name}={actor2_id}")
    
    # Check movie credits where both actors appear
    movie_query = """
    SELECT DISTINCT m1.id, m1.title, m1.release_date, m1.popularity, m1.character as char1, m2.character as char2
    FROM movie_credits m1
    JOIN movie_credits m2 ON m1.id = m2.id
    WHERE m1.actor_id = ? AND m2.actor_id = ?
    """
    cursor.execute(movie_query, (actor1_id, actor2_id))
    movie_results = cursor.fetchall()
    
    if movie_results:
        print(f"\nFound {len(movie_results)} shared movies:")
        for row in movie_results:
            print(f"Movie: {row['title']} (ID: {row['id']}, Released: {row['release_date']})")
            print(f"  {actor1_name} as '{row['char1']}'")
            print(f"  {actor2_name} as '{row['char2']}'")
            print(f"  Popularity: {row['popularity']}")
    else:
        print("\nNo shared movies found")
    
    # Check for problematic movie types
    print("\n=== Checking for problematic movie types ===")
    problematic_movies_query = """
    SELECT id, title, lower(title) as lower_title 
    FROM movie_credits 
    WHERE actor_id IN (?, ?) 
    AND (
        lower(title) LIKE '%documentary%' OR
        lower(title) LIKE '%compilation%' OR
        lower(title) LIKE '%anthology%' OR
        lower(title) LIKE '%collection%' OR
        lower(title) LIKE '%final cut%' OR
        lower(title) LIKE '%behind the scenes%' OR
        lower(title) LIKE '%making of%'
    )
    GROUP BY id
    """
    cursor.execute(problematic_movies_query, (actor1_id, actor2_id))
    problematic_results = cursor.fetchall()
    
    if problematic_results:
        print(f"Found {len(problematic_results)} potentially problematic movies:")
        for row in problematic_results:
            print(f"Movie: {row['title']} (ID: {row['id']})")
            
            # Check if both actors are in this movie
            check_query = """
            SELECT COUNT(DISTINCT actor_id) as actor_count
            FROM movie_credits
            WHERE id = ? AND actor_id IN (?, ?)
            """
            cursor.execute(check_query, (row['id'], actor1_id, actor2_id))
            count_result = cursor.fetchone()
            
            if count_result and count_result['actor_count'] == 2:
                print("  ⚠️ BOTH ACTORS APPEAR IN THIS FILM - FALSE CONNECTION DETECTED!")
            else:
                print("  Only one actor appears in this film")
    else:
        print("No potentially problematic movies found")
    
    conn.close()

if __name__ == "__main__":
    test_basquiat_connection()
    # test_specific_actor_connection("Jackie Chan", "Keanu Reeves")
    # You can test other pairs too:
    # test_specific_actor_connection("Samuel L. Jackson", "Pedro Pascal")