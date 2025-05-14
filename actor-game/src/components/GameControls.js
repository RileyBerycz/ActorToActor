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
  onSelection,
  onComplete
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

  // Add this debugging function to help identify shared movies
  useEffect(() => {
    // Only run this in development to help with debugging
    if (process.env.NODE_ENV === 'development' && startActor && targetActor && actorData) {
      console.log("Checking for direct connections between actors...");
      
      const startActorData = actorData[startActor.id];
      const targetActorData = actorData[targetActor.id];
      
      if (startActorData && targetActorData) {
        // Get movie IDs for start actor
        const startMovieIds = new Set(
          startActorData.movie_credits.map(m => m.id)
        );
        
        // Find shared movies
        const sharedMovies = targetActorData.movie_credits.filter(
          m => startMovieIds.has(m.id)
        );
        
        if (sharedMovies.length > 0) {
          console.log("Found shared movies:", sharedMovies.map(m => m.title || m.name));
        } else {
          console.log("No shared movies found between the actors");
        }
      }
    }
  }, [startActor, targetActor, actorData]);

  // Handle search input change
  const handleSearch = (query) => {
    setSearchQuery(query);
    
    if (!query.trim() || !actorData) {
      setSearchResults([]);
      return;
    }

    // Add logging for debugging
    console.log("Search query with settings:", query, settings.difficulty);
    
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
          const wasInMovie = actor.movie_credits.some(credit => 
            credit.id === lastMovie.id
          ) || (settings.difficulty === 'hard' && actor.tv_credits ? 
            actor.tv_credits.some(credit => credit.id === lastMovie.id) : false);
          
          // Include the target actor if they were in this movie!
          const isTargetActor = targetActor && actorId === targetActor.id && wasInMovie;
          
          // Don't show actors already in the path EXCEPT the target actor if valid
          const alreadyInPath = path.some(item => 
            item.type === 'actor' && item.id === actorId && !isTargetActor
          );
          
          // Match name to query
          const nameMatches = actor.name.toLowerCase().includes(lowerQuery);
          
          return wasInMovie && !alreadyInPath && nameMatches;
        })
        .map(([id, actor]) => ({
          id,
          type: 'actor',
          name: actor.name || 'Unknown Actor',
          profile_path: actor.profile_path,
          popularity: actor.popularity || 0
        }))
        .sort((a, b) => b.popularity - a.popularity)
        .slice(0, 10);
      
      setSearchResults(matchingActors);
    } else {
      // Logic for searching movies/shows based on difficulty
      const lastActor = path.length > 0 ? path[path.length - 1] : startActor;
      if (!lastActor || (lastActor.type !== 'actor' && !startActor)) {
        return;
      }
      
      const actor = actorData[lastActor.id || startActor.id];
      if (!actor) return;
      
      // Get movies only for easy/normal mode, include TV for hard mode
      let credits = [...actor.movie_credits];
      
      // Only add TV credits in hard mode
      if (settings.difficulty === 'hard') {
        credits = [...credits, ...actor.tv_credits];
      }
      
      // Always apply MCU filter if needed
      if (settings.excludeMcu) {
        credits = credits.filter(credit => !credit.is_mcu);
      }
      
      // Add additional filtering for talk shows regardless of difficulty
      const talkShowKeywords = ['tonight show', 'late night', 'late show', 'jimmy'];
      credits = credits.filter(credit => {
        const title = (credit.title || '').toLowerCase();
        return !talkShowKeywords.some(keyword => title.includes(keyword));
      });
      
      // Filter by query and not already in path
      const filteredCredits = credits.filter(credit => {
        const title = (credit.title || '').toLowerCase();
        const character = (credit.character || '').toLowerCase();
        
        // Filter out compilations, documentaries, etc.
        if (title.includes('documentary') ||
            title.includes('compilation') ||
            title.includes('anthology') || 
            title.includes('collection') ||
            title.includes('final cut') ||
            title.includes('behind the scenes') ||
            title.includes('making of')) {
          return false;
        }
        
        // Filter out archive footage appearances
        if (character.includes('himself') || 
            character.includes('herself') ||
            character.includes('self') ||
            character === '' ||
            character.includes('archive footage') ||
            character.includes('archival') ||
            character.includes('stock footage') ||
            character.includes('clips')) {
          return false;
        }
        
        // Also filter by credit type if available
        const creditType = (credit.credit_type || '').toLowerCase();
        if (creditType.includes('archive') || 
            creditType === 'cameo' || 
            creditType.includes('footage')) {
          return false;
        }
        
        return true;
      });
      
      // Then apply search query filtering to the already filtered credits
      const matchingCredits = filteredCredits
        .filter(credit => {
          const title = credit.title || credit.name || '';
          const alreadyInPath = path.some(item => item.type === 'movie' && item.id === credit.id);
          return !alreadyInPath && title.toLowerCase().includes(lowerQuery);
        })
        .map(credit => ({
          id: credit.id,
          type: 'movie',  // Always set type to 'movie' for consistency
          title: credit.title || credit.name || 'Unknown Title',
          poster_path: credit.poster_path,
          popularity: credit.popularity || 0
        }))
        .sort((a, b) => b.popularity - a.popularity)
        .slice(0, 10);
      
      // Use the properly filtered results
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

  // Fix handleSelectResult to properly check for win condition
  const handleSelectResult = (result) => {
    // Special handling for easy mode - use the helper function
    if (settings.difficulty === 'easy' && result.type === 'actor') {
      handleEasyModeActorSelection(result);
      return;
    }
    
    // Add logging to debug
    console.log("Selected result:", result);
    console.log("Target actor:", targetActor);
    
    // First add the selection to the path
    onSelection(result);
    
    // Check if this selection completes the path to target actor
    if (result.type === 'actor' && targetActor && 
      String(result.id) === String(targetActor.id)) {
      // We found the target! Trigger win condition after a short delay
      console.log("TARGET ACTOR FOUND! Path complete.");
      console.log("Selection ID:", result.id, "Target ID:", targetActor.id);
      
      setTimeout(() => {
        if (typeof onComplete === 'function') {
          // Create a proper array including the final selection
          const completePath = [...path, result];
          console.log("Calling onComplete with path:", completePath);
          onComplete(completePath);
        } else {
          console.error("onComplete is not a function:", onComplete);
        }
      }, 500);
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
          {searchType === 'actor' 
            ? `Find an actor who appeared in ${lastItem?.title || ''}`
            : settings.difficulty === 'hard'
              ? `Find a movie or TV show that ${lastItem?.name || ''} appeared in`
              : `Find a movie that ${lastItem?.name || ''} appeared in`
          }
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