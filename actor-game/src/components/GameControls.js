import React, { useState, useEffect } from 'react';
import '../css/GameControls.css';

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
  const [searchType, setSearchType] = useState('movie'); // Start with movie search since we have start actor

  // Set search type based on last selection
  useEffect(() => {
    if (path.length > 0) {
      const lastItem = path[path.length - 1];
      setSearchType(lastItem.type === 'actor' ? 'movie' : 'actor');
    } else if (startActor) {
      // If we're at the beginning, search for movies (since we have start actor)
      setSearchType('movie');
    }
  }, [path, startActor]);

  // When game starts, add the starting actor to path if not already there
  useEffect(() => {
    if (gamePhase === 'playing' && startActor && path.length === 0) {
      onSelection({
        id: startActor.id,
        type: 'actor',
        name: startActor.name,
        profile_path: startActor.profile_path
      });
    }
  }, [gamePhase, startActor, path.length, onSelection]);

  // Handle search input change
  const handleSearch = (query) => {
    setSearchQuery(query);
    
    if (!query.trim() || !actorData) {
      setSearchResults([]);
      return;
    }

    const lowerQuery = query.toLowerCase();
    
    if (searchType === 'actor') {
      // Search for actors who were in the last selected movie
      const lastMovie = path[path.length - 1];
      
      if (!lastMovie || lastMovie.type !== 'movie') {
        return;
      }
      
      // Find all actors who appeared in this movie
      const matchingActors = Object.entries(actorData)
        .filter(([actorId, actor]) => {
          // Check if actor was in this movie
          const wasInMovie = actor.movie_credits.some(credit => credit.id === lastMovie.id) ||
            (settings.difficulty === 'hard' ? actor.tv_credits.some(credit => credit.id === lastMovie.id) : false);
          
          // Don't show actors already in the path
          const alreadyInPath = path.some(item => item.type === 'actor' && item.id === actorId);
          
          // Match name to query
          const nameMatches = actor.name.toLowerCase().includes(lowerQuery);
          
          return wasInMovie && !alreadyInPath && nameMatches;
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
      
      setSearchResults(matchingActors);
    } else {
      // Search for movies that the last actor was in
      const lastActor = path.length > 0 ? path[path.length - 1] : startActor;
      
      if (!lastActor || (lastActor.type !== 'actor' && !startActor)) {
        return;
      }
      
      const actor = actorData[lastActor.id || startActor.id];
      if (!actor) return;
      
      // Get movies and TV shows based on difficulty
      let credits = [...actor.movie_credits];
      if (settings.difficulty === 'hard') {
        credits = [...credits, ...actor.tv_credits];
      }
      
      // Apply MCU filter if needed
      if (settings.excludeMcu) {
        credits = credits.filter(credit => !credit.is_mcu);
      }
      
      // Filter by query and not already in path
      const matchingCredits = credits
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
      
      setSearchResults(matchingCredits);
    }
  };

  // Update how selections are handled for easy mode

  // This function handles when a user selects an actor in easy mode
  const handleEasyModeActorSelection = (selectedActor) => {
    // First, add the selected actor to the path
    onSelection({
      id: selectedActor.id,
      type: 'actor',
      name: selectedActor.name,
      profile_path: selectedActor.profile_path
    });
    
    // Now find a shared movie between the last actor and this one
    const lastActorId = path.length > 0 ? 
      path[path.length - 2].id : // Get previous actor
      startActor.id;             // Or use start actor if this is first selection
    
    const lastActor = actorData[lastActorId];
    const selectedActorData = actorData[selectedActor.id];
    
    if (!lastActor || !selectedActorData) return;
    
    // Find shared movies
    const lastActorMovieIds = new Set(lastActor.movie_credits.map(m => m.id));
    
    // Find first shared movie
    const sharedMovie = selectedActorData.movie_credits.find(m => lastActorMovieIds.has(m.id));
    
    if (sharedMovie) {
      // Insert the movie before the actor in the path
      setTimeout(() => {
        onSelection({
          id: sharedMovie.id,
          type: 'movie',
          title: sharedMovie.title,
          poster_path: sharedMovie.poster_path
        });
      }, 300); // Small delay for visual effect
    }
  };

  // Handle selection from search results
  const handleSelectResult = (result) => {
    if (settings.difficulty === 'easy' && result.type === 'actor') {
      // In easy mode, handle actor selections specially
      handleEasyModeActorSelection(result);
    } else {
      // Normal handling for other modes
      onSelection(result);
    }
    
    setSearchQuery('');
    setSearchResults([]);
  };

  // Only render game controls when in playing mode
  if (gamePhase !== 'playing') {
    return null;
  }

  const lastItem = path.length > 0 ? path[path.length - 1] : startActor;

  return (
    <div className="game-controls">
      <div className="search-container">
        {/* Display instructions based on what we're searching for */}
        <div className="search-instructions">
          {searchType === 'movie' 
            ? `Find a movie that ${lastItem?.name || ''} appeared in`
            : `Find an actor who appeared in ${lastItem?.title || ''}`}
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
    </div>
  );
}

export default GameControls;