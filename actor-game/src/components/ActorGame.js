import { useState, useEffect, useCallback } from 'react';
import '../css/ActorGame.css'; 

// API base URL for Docker deployment
const API_BASE = process.env.NODE_ENV === 'development' 
  ? 'http://localhost:5000/api'
  : '/api';

function ActorGame({ settings, onReset }) {
  // Game state
  const [gameState, setGameState] = useState('loading');
  const [startActor, setStartActor] = useState(null);
  const [targetActor, setTargetActor] = useState(null);
  const [currentPath, setCurrentPath] = useState([]);
  const [currentActor, setCurrentActor] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);
  const [hint, setHint] = useState(null);
  const [optimalPath, setOptimalPath] = useState(null);
  const [showOptimalPath, setShowOptimalPath] = useState(false);
  
  // UI state
  const [searchQuery, setSearchQuery] = useState('');
  const [searchResults, setSearchResults] = useState([]);
  const [availableMovies, setAvailableMovies] = useState([]);
  const [loadingMessage, setLoadingMessage] = useState('Loading game...');
  const [gameMode, setGameMode] = useState('selectMovie');
  const [selectedMovie, setSelectedMovie] = useState(null);
  const [pathMovies, setPathMovies] = useState({});
  const [pathActors, setPathActors] = useState({});
  
  const BASE_IMG_URL = "https://image.tmdb.org/t/p/";
  const defaultImageUrl = "/placeholder-actor.png";

  const getImageUrl = (path, size = 'w185') => {
    if (!path) return defaultImageUrl;
    const normalizedPath = path.startsWith('/') ? path : `/${path}`;
    return `${BASE_IMG_URL}${size}${normalizedPath}`;
  };

  const apiCall = useCallback(async (endpoint, options = {}) => {
    try {
      const url = `${API_BASE}/${endpoint}`;
      const response = await fetch(url, {
        headers: { 'Content-Type': 'application/json', ...options.headers },
        ...options
      });
      if (!response.ok) throw new Error(`API Error: ${response.status}`);
      return await response.json();
    } catch (error) {
      console.error(`API Error:`, error);
      throw error;
    }
  }, []);

  const startNewGame = useCallback(async () => {
    try {
      setLoading(true);
      setLoadingMessage('Setting up your game...');
      setGameState('loading');
      
      const gameData = await apiCall(`game/start?difficulty=${settings.difficulty}&exclude_mcu=${settings.excludeMCU || false}`);
      
      setStartActor(gameData.start_actor);
      setTargetActor(gameData.target_actor);
      setCurrentActor(gameData.start_actor);
      setCurrentPath([gameData.start_actor.id]);
      setOptimalPath(gameData.optimal_path || null);
      setGameState('playing');
      setGameMode('selectMovie');
      setLoading(false);
      
    } catch (error) {
      setError('Failed to start game: ' + error.message);
      setLoading(false);
    }
  }, [settings, apiCall]);

  const searchActors = useCallback(async (query) => {
    if (!query || query.length < 2) {
      setSearchResults([]);
      return;
    }
    try {
      const results = await apiCall(`search?q=${encodeURIComponent(query)}`);
      setSearchResults(results.actors || []);
    } catch (error) {
      setSearchResults([]);
    }
  }, [apiCall]);

  const getActorMovies = useCallback(async (actorId) => {
    try {
      const movies = await apiCall(`actors/${actorId}/movies?exclude_mcu=${settings.excludeMCU || false}`);
      setAvailableMovies(movies || []);
    } catch (error) {
      setAvailableMovies([]);
    }
  }, [apiCall, settings.excludeMCU]);

  const selectMovie = useCallback((movie) => {
    setSelectedMovie(movie);
    setPathMovies(prev => ({ ...prev, [movie.id]: movie.title }));
    setGameMode('selectActor');
    setSearchQuery('');
    setSearchResults([]);
    setCurrentPath(prev => [...prev, movie.id]);
    setAvailableMovies([]);
  }, []);

  const selectActor = useCallback(async (actor) => {
    const newPath = [...currentPath, actor.id];
    setPathActors(prev => ({ ...prev, [actor.id]: actor.name }));
    setCurrentPath(newPath);
    setCurrentActor(actor);
    setSelectedMovie(null);
    setGameMode('selectMovie');
    setSearchQuery('');
    setSearchResults([]);
    setAvailableMovies([]);
    
    if (actor.id === targetActor.id) {
      try {
        const validation = await apiCall('game/validate-path', {
          method: 'POST',
          body: JSON.stringify({
            path: newPath,
            start_actor_id: startActor.id,
            target_actor_id: targetActor.id,
            difficulty: settings.difficulty,
            exclude_mcu: settings.excludeMCU
          })
        });
        
        if (validation.valid) {
          setGameState('won');
        } else {
          setError('Invalid path: ' + (validation.error || 'Unknown error'));
        }
      } catch (error) {
        setError('Error validating your solution');
      }
    }
  }, [currentPath, targetActor, startActor, settings, apiCall]);

  const getHint = useCallback(async () => {
    if (optimalPath) {
      setHint('Check the solution to see the optimal path!');
      return;
    }
    
    try {
      const result = await apiCall(`game/find-path?start_id=${startActor.id}&target_id=${targetActor.id}&difficulty=${settings.difficulty}&exclude_mcu=${settings.excludeMCU}`);
      setOptimalPath(result.path);
      setHint('Optimal path loaded! Click "Show Solution" to see it.');
    } catch (error) {
      setHint('Try looking for popular movies or actors with many connections!');
    }
  }, [startActor, targetActor, settings, optimalPath, apiCall]);

  const resetPath = useCallback(() => {
    setCurrentPath([startActor.id]);
    setCurrentActor(startActor);
    setSelectedMovie(null);
    setGameMode('selectMovie');
    setSearchQuery('');
    setSearchResults([]);
    setError(null);
    setGameState('playing');
  }, [startActor]);

  // Initialize game on mount or settings change
  useEffect(() => {
    startNewGame();
  }, [startNewGame]);

  // Load available movies when current actor changes and user starts searching
  useEffect(() => {
    if (currentActor && gameMode === 'selectMovie' && availableMovies.length === 0) {
      getActorMovies(currentActor.id);
    }
  }, [currentActor, gameMode, getActorMovies, availableMovies.length]);

  // Handle search input changes
  useEffect(() => {
    if (gameMode === 'selectActor') {
      const timeoutId = setTimeout(() => {
        searchActors(searchQuery);
      }, 300);
      return () => clearTimeout(timeoutId);
    }
  }, [searchQuery, gameMode, searchActors]);

  if (loading) {
    return (
      <div className="actor-game loading">
        <div className="loading-container">
          <div className="loading-spinner"></div>
          <h2>{loadingMessage}</h2>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="actor-game error">
        <div className="error-container">
          <h2>Game Error</h2>
          <p>{error}</p>
          <button onClick={startNewGame} className="retry-button">
            Try Again
          </button>
        </div>
      </div>
    );
  }

  return (
    <div className="actor-game">
      {/* Game Header */}
      <div className="game-header">
        <div className="game-challenge">
          <div className="actor-card start">
            <img 
              src={getImageUrl(startActor?.profile_path)} 
              alt={startActor?.name}
              onError={(e) => { e.target.src = defaultImageUrl; }}
            />
            <h3>{startActor?.name}</h3>
          </div>
          
          <div className="path-arrow">
            <span>Connect to</span>
            <div className="arrow">→</div>
          </div>
          
          <div className="actor-card target">
            <img 
              src={getImageUrl(targetActor?.profile_path)} 
              alt={targetActor?.name}
              onError={(e) => { e.target.src = defaultImageUrl; }}
            />
            <h3>{targetActor?.name}</h3>
          </div>
        </div>
        
        <div className="game-info">
          <div className="difficulty">
            {settings.difficulty} 
            {settings.difficulty === 'easy' && ' (max 2 connections)'}
            {settings.difficulty === 'normal' && ' (max 4 connections)'}
            {settings.difficulty === 'hard' && ' (max 6 connections)'}
          </div>
          <div className="path-length">Connections: {Math.floor(currentPath.length / 2)}</div>
          {settings.excludeMCU && <div className="mcu-filter">MCU Excluded</div>}
        </div>
      </div>

      {/* Current Path Display */}
      <div className="current-path">
        <div className="path-header">
          <h3>Your Path</h3>
          <div className="path-stats">
            <span className="connections-count">{Math.floor(currentPath.length / 2)} connections</span>
            <span className="difficulty-indicator">{settings.difficulty}</span>
          </div>
        </div>
        
          <div className="path-visualization">
            {currentPath.length > 0 && (
              <div className="path-chain">
                {/* Start Actor */}
                <div className="path-node actor start">
                  <div className="node-avatar">
                    <img 
                      src={getImageUrl(startActor?.profile_path)} 
                      alt={startActor?.name}
                      onError={(e) => { e.target.src = defaultImageUrl; }}
                    />
                  </div>
                  <div className="node-label">{startActor?.name}</div>
                  <div className="path-role start">START</div>
                </div>

                {/* Arrow between start and first movie */}
                {currentPath.length > 1 && (
                  <div className="path-connector">
                    <div className="connector-line"></div>
                  </div>
                )}
                
                {/* Path items: movie → actor → movie → actor ... */}
                {currentPath.slice(1).map((item, index) => (
                  index % 2 === 0 ? (
                    <div key={index} className="path-segment movie">
                      <div className="path-node movie">
                        <div className="node-icon">🎬</div>
                        <div className="node-label">{pathMovies[item] || `Movie ${item}`}</div>
                      </div>
                      {index < currentPath.length - 2 && (
                        <div className="path-connector">
                          <div className="connector-line"></div>
                        </div>
                      )}
                    </div>
                  ) : (
                    <div key={index} className="path-segment actor">
                      <div className="path-node actor">
                        <div className="node-label path-actor-name">{pathActors[item] || `Actor ${item}`}</div>
                      </div>
                      {/* Arrow between this actor and next movie, if any */}
                      {index < currentPath.length - 2 && (
                        <div className="path-connector">
                          <div className="connector-line"></div>
                        </div>
                      )}
                    </div>
                  )
                ))}
                
                {/* Target indicator */}
                {gameState === 'playing' && currentPath.length > 1 && (
                  <div className="path-connector">
                    <div className="connector-line"></div>
                  </div>
                )}
                {gameState === 'playing' && (
                  <div className="path-node target">
                    <div className="node-avatar">
                      <img 
                        src={getImageUrl(targetActor?.profile_path)} 
                        alt={targetActor?.name}
                        onError={(e) => { e.target.src = defaultImageUrl; }}
                      />
                    </div>
                    <div className="node-label">{targetActor?.name}</div>
                    <div className="target-badge">TARGET</div>
                  </div>
                )}
              </div>
            )}
          </div>
      </div>

      {/* Game Controls */}
      <div className="game-controls">
        {gameState === 'playing' && (
          <>
            {gameMode === 'selectMovie' && (
              <div className="movie-selection">
                <div className="step-header">
                  <div className="step-number">Step {Math.floor(currentPath.length / 2) + 1}</div>
                  <h3>Choose a movie starring <span className="actor-highlight">{currentActor?.name}</span></h3>
                </div>
                
                <div className="search-box">
                  <input
                    type="text"
                    placeholder={`Search for movies with ${currentActor?.name}...`}
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    className="movie-search"
                    autoFocus
                  />
                  <div className="search-icon">🎬</div>
                </div>
                
                {searchQuery.length > 0 && (
                  <div className="search-results movies">
                    {availableMovies
                      .filter(movie => 
                        movie.title.toLowerCase().includes(searchQuery.toLowerCase()) ||
                        (movie.character && movie.character.toLowerCase().includes(searchQuery.toLowerCase()))
                      )
                      .slice(0, 8)
                      .map(movie => (
                        <div 
                          key={movie.id} 
                          className="movie-card clickable"
                          onClick={() => selectMovie(movie)}
                        >
                          <div className="movie-info">
                            <div className="movie-title">{movie.title}</div>
                            <div className="movie-details">
                              <span className="movie-character">as {movie.character}</span>
                              <span className="movie-year">
                                ({movie.release_date ? new Date(movie.release_date).getFullYear() : 'N/A'})
                              </span>
                            </div>
                          </div>
                        </div>
                      ))
                    }
                    {availableMovies.filter(movie => 
                      movie.title.toLowerCase().includes(searchQuery.toLowerCase())
                    ).length === 0 && searchQuery.length > 2 && (
                      <div className="no-results">
                        No movies found matching "{searchQuery}"
                      </div>
                    )}
                  </div>
                )}
                
                {searchQuery.length === 0 && (
                  <div className="search-hint">
                    💡 Start typing to search for movies starring {currentActor?.name}
                  </div>
                )}
              </div>
            )}

            {gameMode === 'selectActor' && selectedMovie && (
              <div className="actor-selection">
                <div className="step-header">
                  <div className="step-number">Step {Math.floor(currentPath.length / 2) + 1}</div>
                  <h3>Who else appeared in <span className="movie-highlight">"{selectedMovie.title}"</span>?</h3>
                </div>
                
                <div className="search-box">
                  <input
                    type="text"
                    placeholder="Type actor's name..."
                    value={searchQuery}
                    onChange={(e) => setSearchQuery(e.target.value)}
                    className="actor-search"
                    autoFocus
                  />
                  <div className="search-icon">🎭</div>
                </div>
                
                {searchQuery.length > 1 && (
                  <div className="search-results actors">
                    {searchResults.length > 0 ? (
                      searchResults.slice(0, 6).map(actor => (
                        <div 
                          key={actor.id}
                          className="actor-result clickable"
                          onClick={() => selectActor(actor)}
                        >
                          <div className="actor-avatar">
                            <img 
                              src={getImageUrl(actor.profile_path)} 
                              alt={actor.name}
                              onError={(e) => { e.target.src = defaultImageUrl; }}
                            />
                          </div>
                          <div className="actor-info">
                            <span className="actor-name">{actor.name}</span>
                            {actor.id === targetActor?.id && (
                              <span className="target-indicator">🎯 TARGET!</span>
                            )}
                          </div>
                        </div>
                      ))
                    ) : (
                      <div className="no-results">
                        No actors found matching "{searchQuery}"
                      </div>
                    )}
                  </div>
                )}
                
                {searchQuery.length <= 1 && (
                  <div className="search-hint">
                    💡 Start typing to search for actors who appeared in "{selectedMovie.title}"
                  </div>
                )}
              </div>
            )}
          </>
        )}

        {gameState === 'won' && (
          <div className="victory">
            <h2>🎉 Congratulations! 🎉</h2>
            <p>You successfully connected {startActor?.name} to {targetActor?.name}!</p>
            <p>Your path used {Math.floor(currentPath.length / 2)} connections.</p>
          </div>
        )}
      </div>

      {/* Action Buttons */}
      <div className="action-buttons">
        <button onClick={getHint} className="hint-button">
          Get Hint
        </button>
        
        {optimalPath && (
          <button 
            onClick={() => setShowOptimalPath(!showOptimalPath)} 
            className="solution-button"
          >
            {showOptimalPath ? 'Hide Solution' : 'Show Solution'}
          </button>
        )}
        
        <button onClick={resetPath} className="reset-path-button">
          Reset Path
        </button>
        
        <button onClick={startNewGame} className="new-game-button">
          New Game
        </button>
        
        <button onClick={onReset} className="back-button">
          Back to Menu
        </button>
      </div>

      {/* Hint Display */}
      {hint && (
        <div className="hint-display">
          <div className="hint-content">
            <span className="hint-icon">💡</span>
            <span>{hint}</span>
            <button onClick={() => setHint(null)} className="close-hint">×</button>
          </div>
        </div>
      )}

      {/* Optimal Path Display */}
      {showOptimalPath && optimalPath && (
        <div className="optimal-path-display">
          <h3>Optimal Solution:</h3>
          <div className="optimal-path">
            {optimalPath.map((item, index) => (
              <div key={index} className={`path-item ${index % 2 === 0 ? 'actor' : 'movie'}`}>
                {index % 2 === 0 ? 
                  `Actor: ${item.n || item.name || 'Unknown'}` : 
                  `Movie: ${item.n || item.title || 'Unknown'}`
                }
                {index < optimalPath.length - 1 && <span className="path-arrow">→</span>}
              </div>
            ))}
          </div>
        </div>
      )}
    </div>
  );
}

export default ActorGame;
