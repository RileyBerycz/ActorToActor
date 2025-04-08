import networkx as nx
import matplotlib.pyplot as plt
import sqlite3
import os
import argparse

def load_actor_data(db_path, limit=20):
    """Load a random set of well-connected actors from the database and the shared movie connections among them."""
    print(f"Loading actor data from {db_path}")
    
    if not os.path.exists(db_path):
        print(f"Database not found at {db_path}")
        return None, None
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Select random well-connected actors (more than 10 movies)
    actor_query = """
        SELECT a.id, a.name, COUNT(DISTINCT mc.id) as movie_count
        FROM actors a
        JOIN movie_credits mc ON a.id = mc.actor_id
        GROUP BY a.id
        HAVING movie_count > 10
        ORDER BY RANDOM()
        LIMIT ?
    """
    
    actors = {}
    cursor.execute(actor_query, (limit,))
    
    for row in cursor.fetchall():
        actor_id, name, _ = row
        actors[str(actor_id)] = {
            "name": name
        }
    
    print(f"Loaded {len(actors)} actors")
    
    # Retrieve connections (shared movies) between these actors.
    actor_ids = list(map(int, actors.keys()))
    placeholders = ','.join(['?'] * len(actor_ids))
    
    cursor.execute(f"""
        SELECT mc1.actor_id, mc2.actor_id, mc1.id, mc1.title, mc1.popularity
        FROM movie_credits mc1
        JOIN movie_credits mc2 ON mc1.id = mc2.id
        WHERE mc1.actor_id < mc2.actor_id
        AND mc1.actor_id IN ({placeholders})
        AND mc2.actor_id IN ({placeholders})
    """, actor_ids + actor_ids)
    
    connection_dict = {}
    for a1, a2, movie_id, movie_title, movie_popularity in cursor.fetchall():
        key = (str(a1), str(a2))
        if key not in connection_dict:
            connection_dict[key] = []
        connection_dict[key].append({
            "id": movie_id, 
            "title": movie_title,
            "popularity": movie_popularity
        })
    
    connections = []
    for (a1, a2), projects in connection_dict.items():
        projects.sort(key=lambda x: x.get("popularity", 0), reverse=True)
        top_project = projects[0]["title"] if projects else "Unknown"
        connections.append((a1, a2, len(projects), top_project))
    
    conn.close()
    print(f"Found {len(connections)} connections between actors")
    return actors, connections

def visualize_network(actors, connections):
    """Create a basic network visualization using actor names only."""
    # Create graph and add nodes with actor names
    G = nx.Graph()
    for actor_id, actor_data in actors.items():
        G.add_node(actor_id, name=actor_data["name"])
    
    # Add edges with connection details (number of shared projects)
    for actor1, actor2, weight, movie in connections:
        G.add_edge(actor1, actor2, weight=weight, movie=movie)
    
    # Use a spring layout for a natural arrangement of nodes
    pos = nx.spring_layout(G, k=2, iterations=50, seed=42)
    
    # Draw edges first so they appear beneath the nodes
    plt.figure(figsize=(20, 16))
    nx.draw_networkx_edges(G, pos, edge_color='#555555', width=[0.5 + (G[u][v]['weight'] / 5) * 2.0 for u,v in G.edges()])
    
    # Draw nodes with a modest size and color
    nx.draw_networkx_nodes(G, pos, node_color='skyblue', node_size=800)
    
    # Draw node labels (actor names) with a clear font
    labels = {actor_id: data["name"] for actor_id, data in G.nodes(data=True)}
    nx.draw_networkx_labels(G, pos, labels, font_size=10, font_weight='bold')
    
    # Optionally, add edge labels (showing the top shared movie) at the midpoints of the edges
    edge_labels = {(u, v): G[u][v]['movie'] for u,v in G.edges()}
    nx.draw_networkx_edge_labels(G, pos, edge_labels=edge_labels, font_size=8, label_pos=0.5)
    
    plt.title("Actor Connections", fontsize=16)
    plt.axis('off')
    plt.tight_layout()
    
    # Save and show the network visualization
    plt.savefig('actor_network_basic.png', dpi=300, bbox_inches='tight')
    print("Saved basic visualization to actor_network_basic.png")
    plt.show()
    return G

if __name__ == "__main__":
    # Use argparse to allow customization of the number of actors
    parser = argparse.ArgumentParser(description="Visualize actor connections from a movie database using names only.")
    parser.add_argument("--limit", type=int, default=20, help="Number of random well-connected actors to visualize.")
    args = parser.parse_args()
    
    # Locate the database file from common paths
    db_paths = [
        "actor-game/public/actors.db",
        "public/actors.db",
        "./actors.db"
    ]
    
    db_path = None
    for path in db_paths:
        if os.path.exists(path):
            db_path = path
            print(f"Found database at {path}")
            break
    
    if not db_path:
        print("Database not found!")
        exit(1)
    
    # Load data and visualize using the specified limit
    actors, connections = load_actor_data(db_path, limit=args.limit)
    
    if actors and connections:
        G = visualize_network(actors, connections)
        
        # Print connection statistics for the top 5 actors by number of connections
        print("\nMost connected actors:")
        degree_dict = dict(G.degree())
        top_actors = sorted(degree_dict.items(), key=lambda x: x[1], reverse=True)[:5]
        for actor_id, degree in top_actors:
            actor_name = G.nodes[actor_id]['name']
            print(f"  {actor_name}: {degree} connections")
    else:
        print("Failed to load actor data")
