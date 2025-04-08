import { useState, useEffect, useCallback, useRef } from 'react';
import initSqlJs from 'sql.js';
import { collection, query, where, limit, getDocs } from 'firebase/firestore';
import GameControls from './GameControls';
import PathDisplay from './PathDisplay';
import '../css/ActorGame.css'; 
import { db as firebaseDb } from '../firebase';

function ActorGame({ settings, onReset }) {
  const [actorData, setActorData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [loadingProgress, setLoadingProgress] = useState(0);
  const [loadingMessage, setLoadingMessage] = useState("Loading actors...");
  const [error, setError] = useState(null);
  const [path, setPath] = useState([]);
  const [gamePhase, setGamePhase] = useState('initializing');
  const [targetActor, setTargetActor] = useState(null);
  const [startActor, setStartActor] = useState(null);
  const [hintAvailable, setHintAvailable] = useState(false);
  const [hint, setHint] = useState(null);
  const [optimalPath, setOptimalPath] = useState(null);
  const [showOptimalPath, setShowOptimalPath] = useState(false);
  const [hintTimer, setHintTimer] = useState(null);

  const BASE_IMG_URL = "https://image.tmdb.org/t/p/";
  const PROFILE_SIZE = "w185";
  const POSTER_SIZE = "w342";
  
  const selectActorsRef = useRef(null);

  const getImageUrl = (path, size = 'w185') => {
    if (!path) return defaultImageUrl;
    const normalizedPath = path.startsWith('/') ? path : `/${path}`;
    return `${BASE_IMG_URL}${size}${normalizedPath}`;
  };

  const calculateOptimalPath = useCallback((startId, targetId) => {
    if (!actorData) return [];
    
    const queue = [];
    const visited = new Set();
    const previous = new Map();
    
    // Add max depth limit based on difficulty
    let maxDepth;
    switch(settings.difficulty) {
      case 'easy': maxDepth = 8; break;  // Up to 4 actor connections
      case 'normal': maxDepth = 12; break; // Up to 6 actor connections
      case 'hard': maxDepth = 20; break;   // Up to 10 actor connections
      default: maxDepth = 20;
    }
    
    queue.push({
      id: startId,
      type: 'actor',
      steps: 0,
      depth: 0
    });
    visited.add(`actor-${startId}`);
    
    while (queue.length > 0) {
      const current = queue.shift();
      
      // Check for target
      if (current.type === 'actor' && current.id === targetId) {
        const path = [];
        let node = current;
        
        while (node) {
          path.unshift(node);
          node = previous.get(`${node.type}-${node.id}`);
        }
        
        return path;
      }
      
      // Skip if we've reached max depth
      if (current.depth >= maxDepth) continue;
      
      if (current.type === 'actor') {
        const actor = actorData[current.id];
        if (!actor) continue;
        
        const credits = [
          ...actor.movie_credits,
          ...(settings.difficulty === 'hard' ? actor.tv_credits : [])
        ];
        
        const filteredCredits = settings.excludeMcu ? 
          credits.filter(c => !c.is_mcu) : credits;
        
        // Prioritize popular movies to find better paths
        const sortedCredits = [...filteredCredits].sort((a, b) => b.popularity - a.popularity);
        
        for (const credit of sortedCredits.slice(0, 15)) { // Limit to 15 most popular movies
          const creditId = `movie-${credit.id}`;
          if (!visited.has(creditId)) {
            const nextNode = {
              id: credit.id,
              type: 'movie',
              title: credit.title || credit.name,
              poster_path: credit.poster_path,
              steps: current.steps + 1,
              depth: current.depth + 1
            };
            
            queue.push(nextNode);
            visited.add(creditId);
            previous.set(creditId, current);
          }
        }
      } else {
        // Sample actors to avoid checking too many
        const relevantActors = Object.entries(actorData)
          .filter(([actorId, actor]) => {
            return actor.movie_credits.some(c => c.id === current.id) ||
              (settings.difficulty === 'hard' ? 
                actor.tv_credits.some(c => c.id === current.id) : false);
          })
          .sort(() => 0.5 - Math.random()) // Randomize
          .slice(0, 20); // Limit to 20 actors per movie
        
        for (const [actorId, actor] of relevantActors) {
          const actorNodeId = `actor-${actorId}`;
          if (!visited.has(actorNodeId)) {
            const nextNode = {
              id: actorId,
              type: 'actor',
              name: actor.name,
              profile_path: actor.profile_path,
              steps: current.steps + 1,
              depth: current.depth + 1
            };
            
            queue.push(nextNode);
            visited.add(actorNodeId);
            previous.set(actorNodeId, current);
          }
        }
      }
    }
    
    return [];
  }, [actorData, settings]);

  const selectActorsWithValidPath = useCallback(() => {
    if (!actorData) return;
    
    setLoadingMessage("Finding actors for your game...");
    
    // Filter actors with profile images for better UI
    const filteredActors = Object.entries(actorData)
      .filter(([_, actor]) => actor.profile_path)
      .map(([id, actor]) => ({
        id,
        ...actor
      }));
    
    // Sort by popularity for better start/target selection
    const sortedActors = [...filteredActors].sort((a, b) => b.popularity - a.popularity);
    
    if (sortedActors.length < 50) {
      setError("Not enough actors available. Try a different region.");
      return;
    }
    
    // Define path length targets based on difficulty
    // These are actor-to-actor connections (each connection is actor-movie-actor)
    const pathLengthRanges = {
      'easy': [1, 3],     // 1-3 actor connections
      'normal': [3, 5],   // 3-5 actor connections
      'hard': [5, 8]      // 5-8 actor connections
    };
    
    const [minConnections, maxConnections] = pathLengthRanges[settings.difficulty] || [1, 8];
    
    // Top 10% for start actor (always well-known)
    const topActors = sortedActors.slice(0, Math.max(5, Math.floor(sortedActors.length * 0.1)));
    
    // Sample multiple start actors to try
    const startActorCandidates = sampleActors(topActors, 5);
    
    // Initialize variables to track our best match
    let bestStartActor = null;
    let bestTargetActor = null;
    let bestPath = null;
    let attempts = 0;
    const maxAttempts = 100;
    
    // Function to sample actors from a pool
    function sampleActors(actorPool, count) {
      const shuffled = [...actorPool].sort(() => 0.5 - Math.random());
      return shuffled.slice(0, Math.min(count, actorPool.length));
    }
    
    // For each potential start actor
    for (const startActor of startActorCandidates) {
      setLoadingMessage(`Testing ${startActor.name} as start actor...`);
      
      // Select target actor pool based on difficulty
      let targetPool;
      const totalCount = sortedActors.length;
      
      switch(settings.difficulty) {
        case 'easy':
          // Well-known actors (top 30%) for easy mode
          targetPool = sortedActors.slice(0, Math.floor(totalCount * 0.3));
          break;
        case 'normal':
          // Medium popularity (15%-60%) for normal mode
          targetPool = sortedActors.slice(
            Math.floor(totalCount * 0.15),
            Math.floor(totalCount * 0.6)
          );
          break;
        case 'hard':
          // Less known actors (40%-100%) for hard mode
          targetPool = sortedActors.slice(Math.floor(totalCount * 0.4));
          break;
        default:
          targetPool = sortedActors;
      }
      
      // Remove the start actor from potential targets
      targetPool = targetPool.filter(actor => actor.id !== startActor.id);
      
      // Sample a subset of target actors to try (for efficiency)
      const targetCandidates = sampleActors(targetPool, 10);
      
      // Try each target actor
      for (const targetActor of targetCandidates) {
        attempts++;
        if (attempts > maxAttempts) break;
        
        setLoadingMessage(`Testing connection: ${startActor.name} → ${targetActor.name}`);
        setLoadingProgress(Math.min(90, 50 + Math.floor((attempts / maxAttempts) * 40)));
        
        // Calculate path between these actors
        const path = calculateOptimalPath(startActor.id, targetActor.id);
        
        // If path exists
        if (path.length > 0) {
          // Count actual connections (every 2 steps is one actor-to-actor connection)
          const connectionCount = Math.floor(path.length / 2);
          
          console.log(`Found path from ${startActor.name} to ${targetActor.name} with ${connectionCount} connections`);
          
          // Check if this path matches our difficulty criteria
          if (connectionCount >= minConnections && connectionCount <= maxConnections) {
            // Perfect match for our difficulty!
            bestStartActor = startActor;
            bestTargetActor = targetActor;
            bestPath = path;
            
            // Early exit - we found what we wanted
            setLoadingMessage(`Found perfect match: ${startActor.name} → ${targetActor.name} (${connectionCount} connections)`);
            break;
          }
          
          // If we haven't found a path yet, or this path is better for our difficulty,
          // update our best candidates
          if (!bestPath || (connectionCount >= minConnections && connectionCount <= maxConnections)) {
            bestStartActor = startActor;
            bestTargetActor = targetActor;
            bestPath = path;
          }
        }
      }
      
      // If we already found a perfect match, stop trying
      if (bestPath && Math.floor(bestPath.length / 2) >= minConnections && Math.floor(bestPath.length / 2) <= maxConnections) {
        break;
      }
    }
    
    // If we found any valid path, use it
    if (bestPath && bestPath.length > 0) {
      const connectionCount = Math.floor(bestPath.length / 2);
      setLoadingMessage(`Ready to play! Found a ${connectionCount}-connection path.`);
      setStartActor(bestStartActor);
      setTargetActor(bestTargetActor);
      setOptimalPath(bestPath);
      setLoadingProgress(100);
      
      setTimeout(() => {
        setGamePhase('playing');
        setLoading(false);
      }, 500);
      return;
    }
    
    // If we got here, we couldn't find a suitable path
    setError("Couldn't find a valid actor pairing. Try changing settings or regions.");
  }, [actorData, calculateOptimalPath, settings]);

  useEffect(() => {
    selectActorsRef.current = selectActorsWithValidPath;
  }, [selectActorsWithValidPath]);

  const generateHint = useCallback(() => {
    if (!optimalPath || path.length === 0) return null;
    
    const lastItem = path[path.length - 1];
    
    const currentOptimalIndex = optimalPath.findIndex(
      item => item.type === lastItem.type && item.id === lastItem.id
    );
    
    if (currentOptimalIndex >= 0 && currentOptimalIndex < optimalPath.length - 1) {
      const nextStep = optimalPath[currentOptimalIndex + 1];
      return {
        message: `Consider looking for a ${nextStep.type === 'actor' ? 'person' : 'movie/show'} that starts with "${nextStep.type === 'actor' ? nextStep.name.charAt(0) : nextStep.title.charAt(0)}"`,
        type: nextStep.type
      };
    } else {
      return {
        message: "You might be on a longer path. Try exploring other connections.",
        type: "general"
      };
    }
  }, [optimalPath, path]);

  const showHint = useCallback(() => {
    const newHint = generateHint();
    setHint(newHint);
    
    if (hintTimer) clearTimeout(hintTimer);
    setHintAvailable(false);
    
    const timer = setTimeout(() => {
      setHintAvailable(true);
    }, 120000);
    
    setHintTimer(timer);
  }, [generateHint, hintTimer]);
  
  const handleSelection = useCallback((selection) => {
    setPath(prev => [...prev, selection]);
    
    if (selection.type === 'actor' && selection.id === targetActor?.id) {
      setGamePhase('completed');
    }
  }, [targetActor]);

  useEffect(() => {
    if (actorData && !loading && !startActor && gamePhase === 'initializing' && selectActorsRef.current) {
      selectActorsRef.current();
    }
  }, [actorData, loading, startActor, gamePhase]);

  useEffect(() => {
    if (startActor && targetActor && actorData) {
      setHint(null);
      setHintAvailable(false);
      
      if (hintTimer) {
        clearTimeout(hintTimer);
      }
      
      const timer = setTimeout(() => {
        setHintAvailable(true);
      }, 120000);
      
      setHintTimer(timer);
    }
    
    return () => {
      if (hintTimer) {
        clearTimeout(hintTimer);
      }
    };
  }, [startActor, targetActor, actorData, hintTimer]);

  useEffect(() => {
    if (path.length > 0) {
      if (hintTimer) {
        clearTimeout(hintTimer);
        setHint(null);
        
        const timer = setTimeout(() => {
          setHintAvailable(true);
        }, 120000);
        
        setHintTimer(timer);
      }
    }
  }, [path, hintTimer]);

  useEffect(() => {
    async function loadFromFirebase() {
      try {
        setLoadingMessage("Loading data from Firebase...");
        const actorsRef = collection(firebaseDb, "actors");
        const q = query(
          actorsRef, 
          where("regions", "array-contains", settings.region),
          limit(500)
        );
        
        setLoadingProgress(30);
        const actorSnapshot = await getDocs(q);
        
        if (actorSnapshot.empty) {
          console.log("No data found in Firebase, falling back to SQLite");
          return null;
        }
        
        const actors = {};
        let processedCount = 0;
        const totalActors = actorSnapshot.size;
        
        setLoadingMessage(`Loading ${totalActors} actors from Firebase...`);
        
        for (const doc of actorSnapshot.docs) {
          const data = doc.data();
          const actorId = doc.id;
          
          const movieCreditsRef = collection(firebaseDb, `actors/${actorId}/movie_credits`);
          const movieCreditsSnapshot = await getDocs(movieCreditsRef);
          
          const movieCredits = movieCreditsSnapshot.docs.map(doc => ({
            id: doc.id,
            ...doc.data()
          }));
          
          const tvCreditsRef = collection(firebaseDb, `actors/${actorId}/tv_credits`);
          const tvCreditsSnapshot = await getDocs(tvCreditsRef);
          
          const tvCredits = tvCreditsSnapshot.docs.map(doc => ({
            id: doc.id,
            ...doc.data()
          }));
          
          actors[actorId] = {
            name: data.name,
            popularity: data.popularity,
            profile_path: data.profile_path,
            place_of_birth: data.place_of_birth,
            regions: data.regions,
            movie_credits: movieCredits,
            tv_credits: tvCredits
          };
          
          processedCount++;
          const progressPercentage = 30 + (processedCount / totalActors) * 60;
          setLoadingProgress(Math.round(progressPercentage));
        }
        
        console.log(`Successfully loaded data from Firebase with ${Object.keys(actors).length} actors`);
        return actors;
      } catch (error) {
        console.warn("Firebase error, falling back to SQLite:", error);
        return null;
      }
    }
    
    async function loadFromSQLite() {
      const SQL = await initSqlJs({
        locateFile: file => `https://cdnjs.cloudflare.com/ajax/libs/sql.js/1.8.0/${file}`
      });
      
      setLoadingProgress(50);
      const dbFile = await fetch(`/actors_${settings.region}.db`);
      setLoadingProgress(70);
      
      if (!dbFile.ok) {
        throw new Error(`Failed to load actor database: ${dbFile.status} ${dbFile.statusText}`);
      }
      
      const buffer = await dbFile.arrayBuffer();
      setLoadingProgress(80);
      
      const sqlDb = new SQL.Database(new Uint8Array(buffer));
      setLoadingProgress(90);
      
      const actors = {};
      const actorsResults = sqlDb.exec(`SELECT id, name, popularity, profile_path, place_of_birth FROM actors`);
      
      if (actorsResults.length === 0 || actorsResults[0].values.length === 0) {
        throw new Error("No actor data found in database");
      }
      
      for (const [id, name, popularity, profile_path, place_of_birth] of actorsResults[0].values) {
        const actorId = id.toString();
        
        const regionsResult = sqlDb.exec(`SELECT region FROM actor_regions WHERE actor_id = ${id}`);
        const regions = regionsResult[0]?.values.map(row => row[0]) || [];
        
        const movieCreditsResult = sqlDb.exec(`
          SELECT id, title, character, popularity, release_date, poster_path, is_mcu 
          FROM movie_credits 
          WHERE actor_id = ${id}
        `);
        
        const movieCredits = movieCreditsResult[0]?.values.map(
          ([id, title, character, popularity, release_date, poster_path, is_mcu]) => ({
            id, 
            title, 
            character, 
            popularity, 
            release_date, 
            poster_path,
            is_mcu: !!is_mcu
          })
        ) || [];
        
        const tvCreditsResult = sqlDb.exec(`
          SELECT id, name, character, popularity, first_air_date, poster_path, is_mcu 
          FROM tv_credits 
          WHERE actor_id = ${id}
        `);
        
        const tvCredits = tvCreditsResult[0]?.values.map(
          ([id, name, character, popularity, first_air_date, poster_path, is_mcu]) => ({
            id, 
            name, 
            character, 
            popularity, 
            first_air_date, 
            poster_path,
            is_mcu: !!is_mcu
          })
        ) || [];
        
        actors[actorId] = {
          name,
          popularity,
          profile_path,
          place_of_birth,
          regions,
          movie_credits: movieCredits,
          tv_credits: tvCredits
        };
      }
      
      console.log(`Successfully loaded data from SQLite with ${Object.keys(actors).length} actors`);
      return actors;
    }
    
    async function loadActorData() {
      try {
        setLoading(true);
        setLoadingProgress(10);
        
        const firebaseData = await loadFromFirebase();
        
        if (firebaseData) {
          setActorData(firebaseData);
          setLoadingProgress(100);
          setTimeout(() => setLoading(false), 500);
          return;
        }
        
        const sqliteData = await loadFromSQLite();
        setActorData(sqliteData);
        
        setLoadingProgress(100);
        setTimeout(() => setLoading(false), 500);
      } catch (error) {
        console.error("Error loading actor data:", error);
        setError(error.message);
        setLoading(false);
      }
    }
    
    loadActorData();
  }, [settings.region]);

  if (loading) {
    return (
      <div className="loading-container">
        <div className="loading-progress-bar">
          <div 
            className="loading-progress-fill" 
            style={{ width: `${loadingProgress}%` }}
          ></div>
        </div>
        <div className="loading-text">{loadingMessage}</div>
      </div>
    );
  }
  
  if (error) {
    return <div className="error-message">Error: {error}</div>;
  }
  
  return (
    <div className="actor-game">
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
        />
      </div>
      
      {gamePhase === 'playing' && hintAvailable && (
        <div className="hint-container">
          <button className="hint-button" onClick={showHint}>
            Need a Hint?
          </button>
        </div>
      )}
      
      {hint && (
        <div className="hint-display">
          <div className="hint-icon">💡</div>
          <div className="hint-message">{hint.message}</div>
        </div>
      )}
      
      {gamePhase === 'completed' && (
        <div className="game-completion">
          <h2>Congratulations!</h2>
          <p>You successfully connected {startActor?.name} to {targetActor?.name} in {path.length} steps!</p>
          
          {optimalPath && optimalPath.length > 0 && (
            <div className="optimal-path-section">
              <button 
                className="show-optimal-path-button"
                onClick={() => setShowOptimalPath(!showOptimalPath)}
              >
                {showOptimalPath ? "Hide" : "Show"} Optimal Path ({Math.floor(optimalPath.length / 2)} steps)
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
    </div>
  );
}

export default ActorGame;