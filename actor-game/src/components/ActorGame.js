import { useState, useEffect, useCallback } from 'react';
import GameControls from './GameControls';
import PathDisplay from './PathDisplay';
import '../css/ActorGame.css'; 

// Database location constants
const DB_URLS = {
  LOCAL: 'actors.db',
  GITHUB: 'https://raw.githubusercontent.com/RileyBerycz/ActorToActor/main/actor-game/public/actors.db',
  CONNECTION_DB: 'actor_connections.db',
  GITHUB_CONNECTION_DB: 'https://raw.githubusercontent.com/RileyBerycz/ActorToActor/main/actor-game/public/actor_connections.db',
  API_BASE: 'https://actor-to-actor-api.rileyberycz.workers.dev/api'
};

function ActorGame({ settings, onReset, initialLoading }) {
  // 1. STATE VARIABLES AND CONSTANTS
  // State variables
  const [actorData, setActorData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [isLoading, setIsLoading] = useState(initialLoading || true);
  const [loadingProgress, setLoadingProgress] = useState(0);
  const [loadingMessage, setLoadingMessage] = useState("Loading actors...");
  const [error, setError] = useState(null);
  const [path, setPath] = useState([]);
  const [gamePhase, setGamePhase] = useState('initializing');
  const [targetActor, setTargetActor] = useState(null);
  const [startActor, setStartActor] = useState(null);
  const [hintAvailable, setHintAvailable] = useState(false);
  const [hint, setHint] = useState(null);
  const [optimalPath, setOptimalPath] = useState([]);
  const [showOptimalPath, setShowOptimalPath] = useState(false);
  const [hintTimer, setHintTimer] = useState(null);
  // eslint-disable-next-line no-unused-vars
  const [dataSource, setDataSource] = useState(null);
  const [selectingActors, setSelectingActors] = useState(false);
  const [pathConnectionCount, setPathConnectionCount] = useState(0);

  // Constants
  const BASE_IMG_URL = "https://image.tmdb.org/t/p/";
  const PROFILE_SIZE = "w185";
  const POSTER_SIZE = "w342";
  const defaultImageUrl = "/placeholder-actor.png";

  
  // Helper function to get image URL 
  const getImageUrl = (path, size = 'w185') => {
    if (!path) return defaultImageUrl;
    const normalizedPath = path.startsWith('/') ? path : `/${path}`;
    return `${BASE_IMG_URL}${size}${normalizedPath}`;
  };

  // 2. API HELPER FUNCTIONS
  const fetchFromApi = useCallback(async (endpoint, params = {}) => {
    const queryString = Object.keys(params)
      .map(key => `${encodeURIComponent(key)}=${encodeURIComponent(params[key])}`)
      .join('&');
      
    const url = `${DB_URLS.API_BASE}/${endpoint}${queryString ? '?' + queryString : ''}`;
    console.log(`🔍 [DEBUG] Fetching from API: ${url}`);
    
    try {
      const response = await fetch(url);
      if (!response.ok) {
        throw new Error(`API error: ${response.status} ${response.statusText}`);
      }
      return await response.json();
    } catch (error) {
      console.error(`❌ [DEBUG] API fetch error:`, error);
      throw error;
    }
  }, []);

  const getRandomConnection = useCallback(async (region, difficulty) => {
    return fetchFromApi('connections', { region, difficulty });
  }, [fetchFromApi]);

  const getActorData = useCallback(async (actorIds) => {
    if (!actorIds || actorIds.length === 0) return {};
    return fetchFromApi('actors', { ids: actorIds.join(',') });
  }, [fetchFromApi]);

  const fetchActorCredits = useCallback(async (actorId) => {
    if (!actorId) return [];
    
    try {
      const creditsData = await fetchFromApi('credits', { actor_id: actorId });
      return creditsData || [];
    } catch (error) {
      console.error("❌ [DEBUG] Error fetching actor credits:", error);
      return [];
    }
  }, [fetchFromApi]);

  // 3. GAME LOGIC FUNCTIONS
  // Select actors with valid path - robust implementation
  const selectActors = useCallback(async () => {
    if (!selectingActors) {
      console.log("🔍 [DEBUG] Starting actor selection process");
      console.time("ActorSelection");
      setSelectingActors(true);
      setIsLoading(true);
      
      try {
        setLoadingMessage("Finding actors for your game...");
        setLoadingProgress(85);
        
        // First try to get connection from API
        try {
          console.log("🔍 [DEBUG] Requesting optimal connection from API");
          const connectionData = await getRandomConnection(settings.region, settings.difficulty);
          console.log("✅ [DEBUG] API returned connection data:", connectionData);
          
          if (connectionData && connectionData.start_id && connectionData.target_id) {
            // Get complete actor data for these two actors
            console.log("🔍 [DEBUG] Requesting actor details");
            const actorDetails = await getActorData([connectionData.start_id, connectionData.target_id]);
            console.log("✅ [DEBUG] API returned actor details");
            
            // Create actor objects
            const start = { 
              id: connectionData.start_id, 
              ...actorDetails[connectionData.start_id], 
              type: 'actor' 
            };
            
            const target = { 
              id: connectionData.target_id, 
              ...actorDetails[connectionData.target_id], 
              type: 'actor' 
            };
            
            // Update actor data in state
            setActorData(prevData => ({
              ...prevData,
              ...actorDetails
            }));
            
            // Set actors and game state
            setStartActor(start);
            setTargetActor(target);
            setGamePhase('playing');
            
            // Set optimal path if available in the API response
            if (connectionData.optimal_path) {
              try {
                // Process optimal path from API
                const optimalPath = JSON.parse(connectionData.optimal_path);
                setOptimalPath(optimalPath);
              } catch (err) {
                console.warn("⚠️ [DEBUG] Error parsing optimal path:", err);
                // Will calculate path later if needed
              }
            }
            
            // Finish loading
            setLoadingProgress(100);
            setLoadingMessage("Ready to play!");
            setTimeout(() => {
              setIsLoading(false);
              setLoading(false);
              setSelectingActors(false);
            }, 800);
            
            console.timeEnd("ActorSelection");
            return; // We're done!
          }
        } catch (apiError) {
          console.error("❌ [DEBUG] API error:", apiError);
          console.log("🔄 [DEBUG] Falling back to local actor selection");
        }
        
        // Fallback to existing method if API fails
        // (Keep your existing fallback code here)
        
      } catch (error) {
        console.error("❌ [DEBUG] Error in actor selection:", error);
        setError(`Failed to select actors: ${error.message}`);
        setIsLoading(false);
        setLoading(false);
        setSelectingActors(false);
      } finally {
        console.timeEnd("ActorSelection");
      }
    }
  }, [settings.difficulty, settings.region, selectingActors, getActorData, getRandomConnection]);
  
  // Generate hint for user
  const generateHint = useCallback(() => {
    if (!optimalPath || optimalPath.length === 0) {
      return {
        message: "Try finding a popular movie that one of these actors starred in!",
        type: "general"
      };
    }
    
    // Find appropriate hint based on path position
    if (path.length === 0) {
      const firstStep = optimalPath[1]; // First movie in optimal path
      if (firstStep && firstStep.type === 'movie') {
        return {
          message: `Try looking for a movie that ${startActor?.name} was in that starts with "${firstStep.title.charAt(0)}"`,
          type: "movie"
        };
      }
    }
    
    const lastItem = path[path.length - 1];
    if (!lastItem) return { message: "Consider recent popular movies", type: "general" };
    
    // Find position in optimal path
    const currentOptimalIndex = optimalPath.findIndex(
      item => item.type === lastItem.type && String(item.id) === String(lastItem.id)
    );
    
    if (currentOptimalIndex >= 0 && currentOptimalIndex < optimalPath.length - 1) {
      const nextStep = optimalPath[currentOptimalIndex + 1];
      if (nextStep) {
        const firstLetter = nextStep.type === 'actor' ? 
          nextStep.name.charAt(0) : nextStep.title.charAt(0);
          
        return {
          message: `Try looking for a ${nextStep.type === 'actor' ? 'person' : 'movie/show'} that starts with "${firstLetter}"`,
          type: nextStep.type
        };
      }
    }
    
    return {
      message: "Try finding actors who worked in big-budget movies or franchises.",
      type: "general"
    };
  }, [optimalPath, path, startActor]);

  // Show hint to user
  const showHint = useCallback(() => {
    const newHint = generateHint();
    console.log("Generated hint:", newHint);
    
    if (hintTimer) {
      clearTimeout(hintTimer);
    }
    
    setHint(newHint);
    setHintAvailable(false);
    
    const timer = setTimeout(() => {
      setHintAvailable(true);
    }, 60000);
    
    setHintTimer(timer);
  }, [generateHint, hintTimer]);

  // Handle selection of actors/movies in the game
  const handleSelection = useCallback((selection) => {
    console.log("Adding to path:", selection);
    
    const processedSelection = {
      ...selection,
      name: selection.name || selection.title || 'Unknown',
      type: selection.type || (selection.profile_path ? 'actor' : 'movie')
    };
    
    setPath(prevPath => [...prevPath, processedSelection]);
  }, []);

  // Handle path completion
  const handlePathComplete = useCallback((completedPath) => {
    console.log("Path completion triggered!");
    console.log("Completed path:", completedPath);
    
    // Safety check for array
    if (!completedPath || !Array.isArray(completedPath)) {
      console.error("Error: completedPath is not an array:", completedPath);
      completedPath = Array.isArray(path) ? [...path] : [];
      if (targetActor && !completedPath.some(item => item.id === targetActor.id)) {
        completedPath.push(targetActor);
      }
    }
    
    // Update path
    setPath(completedPath);
    
    // Calculate connection count
    const connectionCount = Math.floor(completedPath.length / 2);
    setPathConnectionCount(connectionCount);
    
    // Set game phase to completed
    setGamePhase('completed');
  }, [path, targetActor]);

  // Load actor data from SQLite DB
  const loadDatabase = useCallback(async () => {
    try {
      setLoading(true);
      setIsLoading(true);
      setLoadingMessage("Connecting to game server...");
      setLoadingProgress(10);
      
      // Simple health check to ensure API is available
      const healthCheck = await fetchFromApi('health');
      if (!healthCheck || healthCheck.status !== 'ok') {
        throw new Error("API connection failed");
      }
      
      setLoadingProgress(50);
      setLoadingMessage("Ready to play!");
      setDataSource("api");
      
      // Initialize empty actor data object - will be populated as needed
      setActorData({});
      
      return true;
    } catch (error) {
      console.error("API connection error:", error);
      setError(`Connection error: ${error.message}`);
      setLoading(false);
      setIsLoading(false);
      return false;
    }
  }, [fetchFromApi]); // Add fetchFromApi as a dependency

  // 4. EFFECT HOOKS
  // Main data loading effect
  useEffect(() => {
    const loadData = async () => {
      try {
        const success = await loadDatabase();
        
        if (success) {
          // Wait briefly then select actors
          setTimeout(() => {
            selectActors();
          }, 500);
        }
      } catch (err) {
        console.error("Error in data loading flow:", err);
        setError(`Failed to load: ${err.message}`);
        setLoading(false);
        setIsLoading(false);
        setSelectingActors(false);
      }
    };
    
    if (loading && !actorData && !selectingActors) {
      loadData();
    }
  }, [loadDatabase, selectActors, loading, actorData, selectingActors]);

  // Manage loading screen visibility
  useEffect(() => {
    console.log("Loading state:", { 
      isLoading, 
      actorData: !!actorData, 
      startActor: !!startActor, 
      targetActor: !!targetActor, 
      gamePhase 
    });
    
    // Update loading message based on state
    if (gamePhase !== 'completed') {
      if (!actorData) {
        setLoadingMessage("Loading actor database...");
        setLoadingProgress(prev => Math.max(prev, 20));
      } else if (!startActor || !targetActor) {
        setLoadingMessage("Finding actors for your game...");
        setLoadingProgress(prev => Math.max(prev, 60));
      } else if (gamePhase !== 'playing') {
        setLoadingMessage("Setting up game...");
        setLoadingProgress(prev => Math.max(prev, 90));
      } else {
        setLoadingProgress(100);
        
        // Hide loading screen with delay for smooth transition
        const timer = setTimeout(() => {
          setIsLoading(false);
        }, 1000);
        
        return () => clearTimeout(timer);
      }
    } else {
      // Don't show loading during completed game
      setIsLoading(false);
    }
  }, [actorData, startActor, targetActor, gamePhase, isLoading]);

  // Hint timer management
  useEffect(() => {
    if (startActor && targetActor && gamePhase === 'playing') {
      // Reset hint state
      setHint(null);
      setHintAvailable(false);
      
      // Clear any existing timers
      if (hintTimer) {
        clearTimeout(hintTimer);
      }
      
      // Set new hint timer
      const timer = setTimeout(() => {
        setHintAvailable(true);
      }, 90000); // 1.5 minutes
      
      setHintTimer(timer);
      
      // Cleanup
      return () => {
        if (hintTimer) {
          clearTimeout(hintTimer);
        }
      };
    }
  }, [startActor, targetActor, gamePhase, hintTimer]);

  // 5. RETURN/JSX
  return (
    <div className="actor-game">
      {/* Loading overlay with solid background */}
      {isLoading && (
        <div style={{
          position: 'fixed',
          top: 0,
          left: 0,
          width: '100vw',
          height: '100vh',
          backgroundColor: '#1a1a2e', // Solid color background
          display: 'flex',
          justifyContent: 'center',
          alignItems: 'center',
          zIndex: 9999,
          opacity: 1
        }}>
          <div style={{
            background: 'white',
            padding: '30px',
            borderRadius: '8px',
            boxShadow: '0 5px 20px rgba(0, 0, 0, 0.3)',
            textAlign: 'center',
            maxWidth: '400px',
            width: '90%'
          }}>
            <h2>Loading Game...</h2>
            <div style={{
              width: '100%',
              height: '12px',
              background: '#eee',
              borderRadius: '6px',
              overflow: 'hidden',
              margin: '20px 0'
            }}>
              <div style={{
                width: `${loadingProgress}%`,
                height: '100%',
                background: 'linear-gradient(to right, #4CAF50, #8BC34A)',
                transition: 'width 0.5s'
              }}></div>
            </div>
            <p>{loadingMessage || "Preparing your challenge..."}</p>
          </div>
        </div>
      )}
      
      {/* Error display */}
      {error && (
        <div className="error-message">
          <h3>Error</h3>
          <p>{error}</p>
          <button onClick={onReset}>Try Again</button>
        </div>
      )}
      
      {/* Game header with actors */}
      <div className="actor-game-header">
        <div className="start-actor">
          <img 
            src={getImageUrl(startActor?.profile_path, PROFILE_SIZE)} 
            alt={startActor?.name || 'Starting Actor'} 
            className="actor-image"
          />
          <div className="actor-name">{startActor?.name || 'Loading...'}</div>
          <div className="actor-label">START</div>
        </div>
        
        <div className="connection-arrow">→</div>
        
        <div className="target-actor">
          <img 
            src={getImageUrl(targetActor?.profile_path, PROFILE_SIZE)} 
            alt={targetActor?.name || 'Target Actor'} 
            className="actor-image"
          />
          <div className="actor-name">{targetActor?.name || 'Loading...'}</div>
          <div className="actor-label">TARGET</div>
        </div>
      </div>
      
      {/* Path display */}
      <div className="path-display-container">
        <PathDisplay 
          baseImgUrl={BASE_IMG_URL}
          profileSize={PROFILE_SIZE}
          posterSize={POSTER_SIZE}
          actorData={actorData}
          path={path}
          startActor={startActor}
          targetActor={targetActor}
        />
      </div>
      
      {/* Game controls */}
      <div className="game-controls-container">
        <GameControls 
          actorData={actorData}
          settings={settings}
          baseImgUrl={BASE_IMG_URL}
          profileSize={PROFILE_SIZE}
          posterSize={POSTER_SIZE}
          path={path}
          setPath={setPath}
          gamePhase={gamePhase}
          startActor={startActor}
          targetActor={targetActor}
          onSelection={handleSelection}
          onComplete={handlePathComplete}
          fetchActorCredits={fetchActorCredits}  
        />
      </div>
      
      {/* Hint button */}
      {gamePhase === 'playing' && hintAvailable && (
        <div className="hint-container">
          <button className="hint-button" onClick={showHint}>
            Need a Hint?
          </button>
        </div>
      )}
      
      {/* Hint display */}
      {hint && (
        <div className="hint-display">
          <div className="hint-icon">💡</div>
          <div className="hint-message">{hint.message}</div>
        </div>
      )}
      
      {/* Game completion screen */}
      {gamePhase === 'completed' && (
        <div className="game-completion">
          <h2>Congratulations!</h2>
          <p>You successfully connected {startActor?.name} to {targetActor?.name} in {pathConnectionCount} steps!</p>
          
          {/* Optimal path section */}
          {optimalPath && optimalPath.length > 0 && (
            <div className="optimal-path-section">
              <button 
                className="show-optimal_path-button"  // Changed from "show-optimal_path-button"
                onClick={() => setShowOptimalPath(!showOptimalPath)}
              >
                {showOptimalPath ? "Hide" : "Show"} Optimal Path ({Math.floor(optimalPath?.length / 2) || 0} steps)
              </button>
              
              {showOptimalPath && (
                <div className="optimal-path-display">
                  <p>The shortest possible path:</p>
                  <div className="optimal-path-items">
                    {optimalPath.map((item, index) => (
                      <div key={index} className={`optimal-item ${item.type}`}>
                        {item.type === 'actor' ? (
                          <>
                            <img 
                              src={getImageUrl(item.profile_path, PROFILE_SIZE)} 
                              alt={item.name} 
                            />
                            <div className="item-name">{item.name}</div>
                          </>
                        ) : (
                          <>
                            <img 
                              src={getImageUrl(item.poster_path, POSTER_SIZE)} 
                              alt={item.title} 
                            />
                            <div className="item-name">{item.title}</div>
                          </>
                        )}
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </div>
          )}
          
          <button className="reset-game-button" onClick={onReset}>
            Play Again
          </button>
        </div>
      )}
      
      {/* Debug info - data source display (development only) */}
      {dataSource && process.env.NODE_ENV === 'development' && (
        <div className="debug-info" style={{fontSize: '0.7rem', opacity: 0.7, margin: '10px 0 0'}}>
          Data source: {dataSource}
        </div>
      )}
    </div>
  );
}

export default ActorGame;