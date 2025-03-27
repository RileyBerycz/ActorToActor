import { useState, useEffect } from 'react';

function ActorGame() {
  const [actorData, setActorData] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    // During development, fetch locally
    const isDev = process.env.NODE_ENV === 'development';
    const dataUrl = isDev 
      ? '/actors_data.json' // Local path for development
      : 'https://raw.githubusercontent.com/YOUR_USERNAME/ActorToActor/main/actors_data.json'; // Production path
    
    fetch(dataUrl)
      .then(response => response.json())
      .then(data => {
        setActorData(data);
        setLoading(false);
      })
      .catch(error => {
        console.error('Error fetching actor data:', error);
        setLoading(false);
      });
  }, []);

  if (loading) return <div>Loading game data...</div>;
  if (!actorData) return <div>Failed to load game data</div>;
  
  return (
    <div className="actor-game">
      <h1>Actor To Actor Game</h1>
      <p>Game loaded with {Object.keys(actorData).length} actors</p>
      {/* Your game UI components here */}
    </div>
  );
}

export default ActorGame;