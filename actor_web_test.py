import networkx as nx
import random

import matplotlib.pyplot as plt

def generate_sample_data(num_actors=10, connection_density=0.3):
    """Generate sample actor data for testing"""
    actors = [f"Actor_{i}" for i in range(1, num_actors+1)]
    connections = []
    
    for i in range(len(actors)):
        for j in range(i+1, len(actors)):
            if random.random() < connection_density:
                connections.append((actors[i], actors[j]))
    
    return actors, connections

def build_actor_graph(actors, connections):
    """Build a graph from actor connections"""
    G = nx.Graph()
    
    # Add actors as nodes
    for actor in actors:
        G.add_node(actor)
    
    # Add connections as edges
    for actor1, actor2 in connections:
        G.add_edge(actor1, actor2)
    
    return G

def visualize_actor_network(G, title="Actor Connection Network"):
    """Visualize the actor network"""
    plt.figure(figsize=(12, 8))
    
    # Create layout for the graph
    pos = nx.spring_layout(G, seed=42)
    
    # Draw the graph
    nx.draw(G, pos, with_labels=True, node_color='skyblue', 
            node_size=1500, edge_color='gray', width=1, alpha=0.7)
    
    plt.title(title)
    plt.tight_layout()
    plt.show()
    
    return pos

def print_network_stats(G):
    """Print basic statistics about the network"""
    print(f"Number of actors: {G.number_of_nodes()}")
    print(f"Number of connections: {G.number_of_edges()}")
    
    if nx.is_connected(G):
        print("Network is fully connected")
        print(f"Average shortest path length: {nx.average_shortest_path_length(G):.2f}")
    else:
        print("Network is not fully connected")
        
    print(f"Average connections per actor: {2 * G.number_of_edges() / G.number_of_nodes():.2f}")

if __name__ == "__main__":
    # Generate sample data or replace with actual data
    actors, connections = generate_sample_data(15, 0.2)
    
    # Build and analyze the graph
    G = build_actor_graph(actors, connections)
    
    # Print statistics
    print_network_stats(G)
    
    # Visualize the network
    visualize_actor_network(G)
    
    # Find actors with most connections
    degree_dict = dict(G.degree())
    top_actors = sorted(degree_dict.items(), key=lambda x: x[1], reverse=True)[:5]
    print("\nActors with most connections:")
    for actor, degree in top_actors:
        print(f"{actor}: {degree} connections")