import React, { useState, useEffect } from 'react';
import './GameControls.css';

function GameControls({ 
  actorData, 
  settings, 
  baseImgUrl, 
  profileSize, 
  posterSize, 
  path, 
  setPath, 
  gamePhase,
  startActor,
  targetActor,
  onSelection
}) {
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState([]);
  const [searchType, setSearchType] = useState('actor'); // 'actor' or 'movie'
  const [lastSelection, setLastSelection] = useState(null);

  // When game starts, set the starting actor as the first item in the path
  useEffect(() => {
    if (gamePhase === 'playing' && startActor && path.length === 0) {
      // Initialize path with starting actor
      onSelection({
        id: startActor.id,
        type: 'actor',
        name: startActor.name,
        profile_path: startActor.profile_path
      });
      setLastSelection('actor');
    }
  }, [gamePhase, startActor, path.length, onSelection]);

  // Determine what to search for based on game settings and last selection
  useEffect(() => {
    if (path.length > 0) {
      const lastItem = path[path.length - 1];
      
      if (lastItem.type === 'actor') {
        setSearchType('movie');
      } else {
        setSearchType('actor');
      }
      
      setLastSelection(lastItem.type);
    } else if (startActor) {
      // We start with the actor, so next search should be for movies
      setSearchType('movie');
    }
  }, [path, startActor]);

  // Search handler
  const handleSearch = (query) => {
    setSearchQuery(query);
    
    if (!query.trim() || !actorData) {
      setSearchResults([]);
      return;
    }
    
    const lowerQuery = query.toLowerCase();
    
    if (searchType === 'actor') {
      // Search for actors
      const results = Object.entries(actorData)
        .filter(([id, actor]) => {
          // Don't show actors already in the path
          const alreadyInPath = path.some(item => item.type === 'actor' && item.id === id);
          return !alreadyInPath && actor.name.toLowerCase().includes(lowerQuery);
        })
        .map(([id, actor]) => ({
          id,
          type: 'actor',
          name: actor.name,
          profile_path: actor.profile_path,
          popularity: actor.popularity
        }))
        .sort((a, b) => b.popularity - a.popularity)
        .slice(0, 10);
      
      setSearchResults(results);
    } else {
      // Search for movies/shows
      // First, get the last actor in the path to find their movies
      const lastActor = path[path.length - 1];
      if (lastActor && lastActor.type === 'actor' && actorData[lastActor.id]) {
        const actor = actorData[lastActor.id];
        
        // Get movies and TV shows based on difficulty
        let credits = actor.movie_credits;
        if (settings.difficulty === 'hard') {
          credits = [...credits, ...actor.tv_credits];
        }
        
        // Apply MCU filter if needed
        if (settings.excludeMcu) {
          credits = credits.filter(credit => !credit.is_mcu);
        }
        
        // Filter by query and not already in path
        const results = credits
          .filter(credit => {
            const title = credit.title || credit.name;
            const alreadyInPath = path.some(item => item.type === 'movie' && item.id === credit.id);
            return !alreadyInPath && title.toLowerCase().includes(lowerQuery);
          })
          .map(credit => ({
            id: credit.id,
            type: 'movie',
            title: credit.title || credit.name,
            poster_path: credit.poster_path,
            popularity: credit.popularity
          }))
          .sort((a, b) => b.popularity - a.popularity)
          .slice(0, 10);
        
        setSearchResults(results);
      }
    }
  };

  // Handle selection of an item from search results
  const handleSelectResult = (result) => {
    if (result.type === 'actor') {
      // Handle selection of an actor
      onSelection({
        id: result.id,
        type: 'actor',
        name: result.name,
        profile_path: result.profile_path
      });
    } else {
      // Handle selection of a movie/show
      onSelection({
        id: result.id,
        type: 'movie',
        title: result.title,
        poster_path: result.poster_path
      });
    }
    
    // Clear search after selection
    setSearchQuery('');
    setSearchResults([]);
  };

  return (
    <div className="game-controls">
      {gamePhase === 'playing' && (
        <div className="search-container">
          <div className="search-instructions">
            {searchType === 'movie' 
              ? `Find a movie that ${path[path.length - 1]?.name || startActor?.name} appeared in` 
              : `Find an actor who appeared in ${path[path.length - 1]?.title}`}
          </div>
          
          <input
            type="text"
            className="search-input"
            placeholder={searchType === 'actor' ? "Search for an actor..." : "Search for a movie..."}
            value={searchQuery}
            onChange={(e) => handleSearch(e.target.value)}
          />
          
          {searchResults.length > 0 && (
            <div className="search-results">
              {searchResults.map(result => (
                <div 
                  key={`${result.type}-${result.id}`} 
                  className="search-result-item"
                  onClick={() => handleSelectResult(result)}
                >
                  <img 
                    src={result.profile_path || result.poster_path 
                      ? `${baseImgUrl}${result.profile_path ? profileSize : posterSize}${result.profile_path || result.poster_path}` 
                      : '/placeholder.png'} 
                    alt={result.name || result.title}
                    className="search-result-img"
                  />
                  <div className="search-result-name">
                    {result.name || result.title}
                  </div>
                </div>
              ))}
            </div>
          )}
        </div>
      )}
    </div>
  );
}

export default GameControls;