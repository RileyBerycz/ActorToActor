import { useState, useEffect, useCallback } from 'react';
import '../css/ActorGame.css'; 

// API base URL for Docker deployment
const API_BASE = process.env.NODE_ENV === 'development' 
  ? 'http://localhost:5000/api'
  : '/api';

function ActorGame({ settings, onReset, gameMode, dailyConnection }) {
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
  const [uiMode, setUiMode] = useState('selectMovie');
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
      
      if (gameMode === 'daily' && dailyConnection?.available) {
        // Daily connection mode
        setStartActor(dailyConnection.start_actor);
        setTargetActor(dailyConnection.target_actor);
        setCurrentActor(dailyConnection.start_actor);
        setCurrentPath([dailyConnection.start_actor.id]);
        setOptimalPath(dailyConnection.optimal_path || null);
        setGameState('playing');
        setUiMode('selectMovie');
        setLoading(false);
      } else {
        const gameData = await apiCall(`game/start?difficulty=${settings.difficulty}&exclude_mcu=${settings.excludeMCU || false}`);
        
        setStartActor(gameData.start_actor);
        setTargetActor(gameData.target_actor);
        setCurrentActor(gameData.start_actor);
        setCurrentPath([gameData.start_actor.id]);
        setOptimalPath(gameData.optimal_path || null);
        setGameState('playing');
        setUiMode('selectMovie');
        setLoading(false);
      }
    } catch (error) {
      setError('Failed to start game: ' + error.message);
      setLoading(false);
    }
  }, [settings, gameMode, dailyConnection, apiCall]);

  const searchActors = useCallback(async (query, movieId) => {
    if (!query || query.length < 2) {
      setSearchResults([]);
      return;
    }
    try {
      if (movieId) {
        const results = await apiCall(`movies/${movieId}/cast?search=${encodeURIComponent(query)}`);
        setSearchResults(results.cast || []);
      } else {
        const results = await apiCall(`search?q=${encodeURIComponent(query)}`);
        setSearchResults(results.actors || []);
      }
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

  const selectMovie = useCallback(async (movie) => {
    setSelectedMovie(movie);
    setPathMovies(prev => ({ ...prev, [movie.id]: movie.title }));
    const newPath = [...currentPath, movie.id];
    setCurrentPath(newPath);
    setAvailableMovies([]);
    
    // Check if target actor is in this movie -> auto-complete
    try {
      const result = await apiCall(`movies/${movie.id}/has-actor/${targetActor.id}`);
      if (result.has_actor) {
        setPathActors(prev => ({ ...prev, [targetActor.id]: targetActor.name }));
        const fullPath = [...newPath, targetActor.id];
        setCurrentPath(fullPath);
        setCurrentActor(targetActor);
        
        const validation = await apiCall('game/validate-path', {
          method: 'POST',
          body: JSON.stringify({
            path: fullPath,
            start_actor_id: startActor.id,
            target_actor_id: targetActor.id,
            difficulty: settings.difficulty,
            exclude_mcu: settings.excludeMCU
          })
        });
        
        if (validation.valid) {
          setGameState('won');
          return;
        }
      }
    } catch (e) {
      // Auto-complete check failed, fall through to normal actor search
    }
    
    setUiMode('selectActor');
    setSearchQuery('');
    setSearchResults([]);
  }, [currentPath, targetActor, startActor, settings, apiCall]);

  const selectActor = useCallback(async (actor) => {
    const newPath = [...currentPath, actor.id];
    setPathActors(prev => ({ ...prev, [actor.id]: actor.name }));
    setCurrentPath(newPath);
    setCurrentActor(actor);
    setSelectedMovie(null);
    setUiMode('selectMovie');
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
      const result = await apiCall(`game/find-path?start_id=${startActor.id}&target_id=${targetActor.id}`);
      if (result.path) {
        setOptimalPath(result.path);
        const conns = result.length || Math.floor((result.path.length - 1) / 2);
        setHint(`Optimal path found: ${conns} connection${conns !== 1 ? 's' : ''}! Click "Show Solution" to see it.`);
      } else {
        setHint('Try looking for popular movies or actors with many connections!');
      }
    } catch (error) {
      setHint('Try looking for popular movies or actors with many connections!');
    }
  }, [startActor, targetActor, optimalPath, apiCall]);

  const resetPath = useCallback(() => {
    setCurrentPath([startActor.id]);
    setCurrentActor(startActor);
    setSelectedMovie(null);
    setUiMode('selectMovie');
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
    if (currentActor && uiMode === 'selectMovie' && availableMovies.length === 0) {
      getActorMovies(currentActor.id);
    }
  }, [currentActor, uiMode, getActorMovies, availableMovies.length]);

  // Handle search input changes
  useEffect(() => {
    if (uiMode === 'selectActor') {
      const timeoutId = setTimeout(() => {
        searchActors(searchQuery, selectedMovie?.id);
      }, 300);
      return () => clearTimeout(timeoutId);
    }
  }, [searchQuery, uiMode, searchActors, selectedMovie]);

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

  const connectionCount = Math.floor(currentPath.length / 2);
  const maxConnections = settings.difficulty === 'easy' ? 2 : settings.difficulty === 'normal' ? 4 : 6;

  return (
    <div className="actor-game">
      {/* Challenge bar */}
      <div className="challenge-bar">
        <div className="challenge-actors">
          <div className="challenge-actor">
            <img src={getImageUrl(startActor?.profile_path)} alt={startActor?.name} />
            <div className="challenge-name">{startActor?.name}</div>
          </div>
          <div className="challenge-vs">
            <div className="vs-line"></div>
            <div className="vs-text">vs</div>
            <div className="vs-line"></div>
          </div>
          <div className="challenge-actor target">
            <img src={getImageUrl(targetActor?.profile_path)} alt={targetActor?.name} />
            <div className="challenge-name">{targetActor?.name}</div>
          </div>
        </div>
        <div className="challenge-meta">
          <span className="meta-difficulty">{settings.difficulty}</span>
          {gameMode === 'daily' && <span className="meta-daily">Daily</span>}
          <span className="meta-connections">{connectionCount}/{maxConnections}</span>
          {settings.excludeMCU && <span className="meta-mcu">No MCU</span>}
        </div>
      </div>

      {/* Path chain */}
      <div className="path-chain-area">
        {currentPath.length > 0 && (
          <div className="path-chain">
            <div className="chain-node start">
              <div className="chain-avatar">
                <img src={getImageUrl(startActor?.profile_path)} alt={startActor?.name} />
              </div>
            </div>

            {currentPath.slice(1).map((item, index) => (
              <div key={index} className="chain-link">
                <div className="chain-arrow">→</div>
                <div className={`chain-node ${index % 2 === 0 ? 'movie' : 'actor'}`}>
                  {index % 2 === 0 ? (
                    <div className="chain-movie-label">{pathMovies[item] || `Movie ${item}`}</div>
                  ) : (
                    <div className="chain-actor-label">{pathActors[item] || `Actor ${item}`}</div>
                  )}
                </div>
              </div>
            ))}

            {gameState === 'playing' && (
              <div className="chain-link">
                <div className="chain-arrow">→</div>
                <div className="chain-node target">
                  <div className="chain-avatar">
                    <img src={getImageUrl(targetActor?.profile_path)} alt={targetActor?.name} />
                  </div>
                  <div className="chain-target-badge">TARGET</div>
                </div>
              </div>
            )}
          </div>
        )}
      </div>

      {/* Interaction area */}
      <div className="interaction-area">
        {gameState === 'playing' && uiMode === 'selectMovie' && (
          <div className="interaction-card">
            <div className="interaction-prompt">
              Pick a movie starring <span className="accent">{currentActor?.name}</span>
            </div>
            <div className="search-field">
              <input
                type="text"
                placeholder="Search movies..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                autoFocus
              />
            </div>
            <div className="search-results-list">
              {searchQuery.length > 0 ? (
                availableMovies
                  .filter(m => m.title.toLowerCase().includes(searchQuery.toLowerCase()) ||
                    (m.character && m.character.toLowerCase().includes(searchQuery.toLowerCase())))
                  .slice(0, 6)
                  .map(movie => (
                    <div key={movie.id} className="result-item" onClick={() => selectMovie(movie)}>
                      <div className="result-title">{movie.title}</div>
                      <div className="result-sub">
                        <span>as {movie.character}</span>
                        <span>{movie.release_date ? new Date(movie.release_date).getFullYear() : ''}</span>
                      </div>
                    </div>
                  ))
              ) : (
                <div className="result-hint">Type to search movies starring {currentActor?.name}</div>
              )}
              {searchQuery.length > 2 && availableMovies.filter(m =>
                m.title.toLowerCase().includes(searchQuery.toLowerCase())
              ).length === 0 && (
                <div className="result-empty">No movies match "{searchQuery}"</div>
              )}
            </div>
          </div>
        )}

        {gameState === 'playing' && uiMode === 'selectActor' && selectedMovie && (
          <div className="interaction-card">
            <div className="interaction-prompt">
              Who else was in <span className="accent">{selectedMovie.title}</span>?
            </div>
            <div className="search-field">
              <input
                type="text"
                placeholder="Search actors..."
                value={searchQuery}
                onChange={(e) => setSearchQuery(e.target.value)}
                autoFocus
              />
            </div>
            <div className="search-results-list">
              {searchQuery.length > 1 ? (
                searchResults.length > 0 ? (
                  searchResults.slice(0, 6).map(actor => (
                    <div key={actor.id} className="result-item actor" onClick={() => selectActor(actor)}>
                      <div className="result-avatar">
                        <img src={getImageUrl(actor.profile_path)} alt={actor.name} />
                      </div>
                      <div className="result-info">
                        <div className="result-title">{actor.name}</div>
                        {actor.id === targetActor?.id && (
                          <div className="result-target">TARGET</div>
                        )}
                      </div>
                    </div>
                  ))
                ) : (
                  <div className="result-empty">No actors match "{searchQuery}"</div>
                )
              ) : (
                <div className="result-hint">Type to search actors in {selectedMovie.title}</div>
              )}
            </div>
          </div>
        )}

        {gameState === 'won' && (
          <div className="interaction-card victory-card">
            <div className="victory-emoji">🎉</div>
            <div className="victory-title">Connected!</div>
            <div className="victory-detail">{startActor?.name} → {targetActor?.name}</div>
            <div className="victory-path">{connectionCount} connection{connectionCount !== 1 ? 's' : ''}</div>
          </div>
        )}

        {/* Show Solution button */}
        {optimalPath && optimalPath.length > 0 && (
          <button
            onClick={() => setShowOptimalPath(!showOptimalPath)}
            className="action-btn solution"
          >
            {showOptimalPath ? 'Hide Solution' : 'Show Solution'}
          </button>
        )}

        {/* Solution display */}
        {showOptimalPath && optimalPath && (
          <div className="optimal-path-display">
            <div className="optimal-path-header">Optimal Path ({Math.floor(optimalPath.length / 2)} connections)</div>
            <div className="optimal-path-items">
              {optimalPath.map((item, index) => (
                <span key={index} className={`optimal-item ${item.type}`}>
                  {item.name || item.title}
                  {index < optimalPath.length - 1 && <span className="optimal-arrow"> → </span>}
                </span>
              ))}
            </div>
          </div>
        )}

        {/* Action buttons */}
        <div className="action-row">
          <button onClick={getHint} className="action-btn hint">Hint</button>
          <button onClick={resetPath} className="action-btn reset">Reset</button>
          <button onClick={gameMode === 'daily' ? onReset : startNewGame} className="action-btn new">
            {gameMode === 'daily' ? 'Back Home' : 'New Game'}
          </button>
          <button onClick={onReset} className="action-btn back">Back</button>
        </div>
      </div>

      {hint && (
        <div className="hint-toast">
          <span>{hint}</span>
          <button onClick={() => setHint(null)}>×</button>
        </div>
      )}
    </div>
  );
}

export default ActorGame;
