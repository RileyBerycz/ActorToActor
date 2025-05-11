import { useState, useEffect, useCallback, useRef } from 'react';
import { collection, query, where, limit, getDocs } from 'firebase/firestore';
import GameControls from './GameControls';
import PathDisplay from './PathDisplay';
import '../css/ActorGame.css'; 
import { db as firebaseDb } from '../firebase';

// Constants for database locations
const DB_URLS = {
  LOCAL: 'actors.db',
  GITHUB: 'https://raw.githubusercontent.com/RileyBerycz/ActorToActor/main/actor-game/public/actors.db',
  CONNECTION_DB: 'actor_connections.db',
  GITHUB_CONNECTION_DB: 'https://raw.githubusercontent.com/RileyBerycz/ActorToActor/main/actor-game/public/actor_connections.db'
};

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
  const [dataSource, setDataSource] = useState(null);
  const [selectingActors, setSelectingActors] = useState(false);
  const [pathConnectionCount, setPathConnectionCount] = useState(0);

  const BASE_IMG_URL = "https://image.tmdb.org/t/p/";
  const PROFILE_SIZE = "w185";
  const POSTER_SIZE = "w342";
  const defaultImageUrl = "/placeholder-actor.png";
  
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
    
    let maxDepth;
    switch(settings.difficulty) {
      case 'easy': maxDepth = 8; break;
      case 'normal': maxDepth = 12; break;
      case 'hard': maxDepth = 20; break;
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
      
      if (current.type === 'actor' && current.id === targetId) {
        const path = [];
        let node = current;
        
        while (node) {
          path.unshift(node);
          node = previous.get(`${node.type}-${node.id}`);
        }
        
        return path;
      }
      
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
        
        const sortedCredits = [...filteredCredits].sort((a, b) => b.popularity - a.popularity);
        
        for (const credit of sortedCredits.slice(0, 15)) {
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
        const relevantActors = Object.entries(actorData)
          .filter(([actorId, actor]) => {
            return actor.movie_credits.some(c => c.id === current.id) ||
              (settings.difficulty === 'hard' ? 
                actor.tv_credits.some(c => c.id === current.id) : false);
          })
          .sort(() => 0.5 - Math.random())
          .slice(0, 20);
        
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

  // Simplify the selectActorsWithValidPath function
  const selectActorsWithValidPath = useCallback(() => {
    if (!actorData || selectingActors) return;
    
    console.log("Starting actor selection");
    setSelectingActors(true);
    
    try {
      setLoadingMessage("Finding actors for your game...");
      setLoadingProgress(85);
      
      // Get actors with profile images
      const filteredActors = Object.entries(actorData)
        .filter(([_, actor]) => actor.profile_path)
        .map(([id, actor]) => ({
          id,
          ...actor
        }))
        .sort((a, b) => b.popularity - a.popularity);
      
      if (filteredActors.length < 2) {
        setError("Not enough actors available. Try a different region.");
        setLoading(false);
        setSelectingActors(false);
        return;
      }
      
      // Simple selection - just take two popular actors
      const startIndex = Math.floor(Math.random() * Math.min(10, filteredActors.length));
      let targetIndex;
      do {
        targetIndex = Math.floor(Math.random() * Math.min(20, filteredActors.length));
      } while (targetIndex === startIndex);
      
      const startActor = filteredActors[startIndex];
      const targetActor = filteredActors[targetIndex];
      
      console.log(`Selected ${startActor.name} and ${targetActor.name}`);
      
      // Find path between them for hints
      const path = calculateOptimalPath(startActor.id, targetActor.id);
      setOptimalPath(path);
      
      // Set state and start game
      setStartActor({...startActor, type: 'actor'});
      setTargetActor({...targetActor, type: 'actor'});
      
      setLoadingProgress(100);
      setLoadingMessage("Ready to play!");
      
      setTimeout(() => {
        setGamePhase('playing');
        setLoading(false);
        setSelectingActors(false);
      }, 1000);
    } catch (error) {
      console.error("Error selecting actors:", error);
      setError(`Error: ${error.message}`);
      setLoading(false);
      setSelectingActors(false);
    }
  }, [actorData, calculateOptimalPath]);

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
    console.log("Adding to path:", selection);
    // Ensure selection has all required fields
    const processedSelection = {
      ...selection,
      name: selection.name || selection.title || 'Unknown',
      type: selection.type || (selection.profile_path ? 'actor' : 'movie')
    };
    
    // Add the selection to the path
    setPath(prevPath => [...prevPath, processedSelection]);
    
    // Check if this completes the path to the target
    if (selection.type === 'actor' && targetActor && selection.id === targetActor.id) {
      console.log("Target reached!");
      // Path is complete - allow time for state update before changing phase
      setTimeout(() => {
        setGamePhase('completed');
      }, 800);
    }
  }, [targetActor]);

// Fix the handlePathComplete function to count connections properly
const handlePathComplete = (completedPath) => {
  console.log("Path completion triggered!");
  console.log("Completed path:", completedPath);
  
  // Set the full path
  setPath(completedPath);
  
  // Filter out duplicate entries to get the correct path
  const uniquePath = [];
  let lastType = null;
  
  for (const item of completedPath) {
    // Skip consecutive items of the same type (prevents duplicates)
    if (item.type !== lastType) {
      uniquePath.push(item);
      lastType = item.type;
    }
  }
  
  // Calculate actual connections (shared movies)
  // In a valid path: actor -> movie -> actor -> movie -> actor
  // Number of connections = number of movies = Math.floor(uniquePath.length / 2)
  const connectionCount = Math.floor(uniquePath.length / 2);
  
  console.log("Unique path:", uniquePath);
  console.log("Connection count:", connectionCount);
  
  // Store the cleaned path and connection count for display
  setOptimalPath(uniquePath);
  setPathConnectionCount(connectionCount);
  setGamePhase('completed');
};

  useEffect(() => {
    if (actorData && !loading && !startActor && gamePhase === 'initializing' && 
        selectActorsRef.current && !selectingActors) { // Add !selectingActors check
      selectActorsRef.current();
    }
  }, [actorData, loading, startActor, gamePhase, selectingActors]);

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

  const loadFromSqlite = useCallback(async () => {
    try {
      setLoading(true);
      setLoadingMessage("Loading actors from database...");
      setLoadingProgress(10);
      
      const cacheKey = `actor-data-${settings.region}-${settings.difficulty}`;
      const cachedData = localStorage.getItem(cacheKey);
      
      if (cachedData) {
        try {
          setLoadingMessage("Loading data from browser cache...");
          const parsedData = JSON.parse(cachedData);
          const cacheTimestamp = parsedData._timestamp || 0;
          const currentTime = new Date().getTime();
          
          if (currentTime - cacheTimestamp < 24 * 60 * 60 * 1000) {
            setLoadingMessage("Using cached actor data...");
            setLoadingProgress(90);
            
            const { _timestamp, _dataSource, ...actorDataOnly } = parsedData;
            
            // Updated: Set actor data but don't return yet
            setDataSource(`cache:${_dataSource}`);
            setActorData(actorDataOnly);
            
            // Progress to actor selection but STAY in loading state
            setLoadingProgress(80);
            setLoadingMessage("Data loaded! Selecting actors for your game...");
            
            // Critical fix: Directly call actor selection with a slight delay
            // to ensure state updates have propagated
            setTimeout(() => {
              if (!selectingActors) {
                console.log("Directly triggering actor selection after cache load");
                selectActorsWithValidPath();
              }
            }, 300);
            
            return true;
          } else {
            setLoadingMessage("Cache expired, fetching fresh data...");
          }
        } catch (err) {
          console.error("Error loading from cache:", err);
          setLoadingMessage("Cache corrupted, fetching fresh data...");
        }
      }
      
      let SQL;
      try {
        setLoadingMessage("Initializing SQL.js...");
        
        const SQL_CDN_URL = 'https://sql.js.org/dist/sql-wasm.wasm';
        
        const initSqlJs = (await import('sql.js')).default;
        SQL = await initSqlJs({
          locateFile: file => SQL_CDN_URL
        });
        
        setLoadingMessage("SQL.js initialized successfully!");
      } catch (err) {
        console.error("Error initializing SQL.js:", err);
        setLoadingMessage("Error loading SQL engine. Trying fallback...");
        
        try {
          const FALLBACK_CDN_URL = 'https://cdn.jsdelivr.net/npm/sql.js@1.8.0/dist/sql-wasm.wasm';
          
          const initSqlJs = (await import('sql.js')).default;
          SQL = await initSqlJs({
            locateFile: file => FALLBACK_CDN_URL
          });
          
          setLoadingMessage("SQL.js initialized with fallback!");
        } catch (secondErr) {
          console.error("Fatal error initializing SQL.js:", secondErr);
          throw new Error("Could not initialize SQL.js. Please reload or try a different browser.");
        }
      }
      
      let response = null;
      let dataSourceName = '';
      
      try {
        setLoadingMessage("Checking for local database...");
        response = await fetch(DB_URLS.LOCAL);
        if (response.ok) {
          dataSourceName = 'local';
          setLoadingMessage("Loading from local database...");
        }
      } catch (err) {
        console.log("Local database not found, trying GitHub...");
      }
      
      if (!response || !response.ok) {
        try {
          setLoadingMessage("Loading from GitHub...");
          response = await fetch(DB_URLS.GITHUB);
          if (response.ok) {
            dataSourceName = 'github';
          }
        } catch (err) {
          console.log("GitHub database not found either");
        }
      }
      
      if (!response || !response.ok) {
        try {
          setLoadingMessage("Checking for connections database...");
          response = await fetch(DB_URLS.CONNECTION_DB);
          if (response.ok) {
            dataSourceName = 'connection_local';
            setLoadingMessage("Loading from connections database...");
          }
        } catch (err) {
          console.log("Local connections database not found, trying GitHub...");
        }
      }
      
      if (!response || !response.ok) {
        try {
          setLoadingMessage("Loading connections database from GitHub...");
          response = await fetch(DB_URLS.GITHUB_CONNECTION_DB);
          if (response.ok) {
            dataSourceName = 'connection_github';
          }
        } catch (err) {
          console.log("No database found at any location");
          throw new Error("Could not find actor database at any location");
        }
      }
      
      if (!response || !response.ok) {
        throw new Error("Could not load actor database");
      }
      
      setLoadingMessage(`Loading actors from ${dataSourceName}...`);
      setLoadingProgress(20);
      
      const arrayBuffer = await response.arrayBuffer();
      const uInt8Array = new Uint8Array(arrayBuffer);
      
      const db = new SQL.Database(uInt8Array);
      setLoadingMessage("Processing actor data...");
      setLoadingProgress(40);
      
      const actorQuery = dataSourceName.includes('connection') 
        ? `SELECT * FROM actors WHERE id IN (
             SELECT actor_id FROM actor_regions WHERE region = '${settings.region}'
           )`
        : `SELECT a.* FROM actors a
           JOIN actor_regions ar ON a.id = ar.actor_id
           WHERE ar.region = '${settings.region}'`;
      
      const actorResult = db.exec(actorQuery);
      
      if (!actorResult[0] || !actorResult[0].values) {
        throw new Error(`No actors found for region ${settings.region}`);
      }
      
      setLoadingMessage(`Found ${actorResult[0].values.length} actors for ${settings.region}. Loading credits...`);
      setLoadingProgress(60);
      
      const actors = {};
      const columns = actorResult[0].columns;
      
      for (const row of actorResult[0].values) {
        const actor = {};
        
        columns.forEach((col, idx) => {
          actor[col] = row[idx];
        });
        
        const actorId = actor.id.toString();
        
        actors[actorId] = {
          ...actor,
          movie_credits: [],
          tv_credits: []
        };
      }
      
      setLoadingMessage("Loading movie credits...");
      const movieQuery = `
        SELECT * FROM movie_credits 
        WHERE actor_id IN (
          SELECT id FROM actors WHERE id IN (
            SELECT actor_id FROM actor_regions WHERE region = '${settings.region}'
          )
        )
      `;
      
      const movieResult = db.exec(movieQuery);
      
      if (movieResult[0] && movieResult[0].values) {
        const movieColumns = movieResult[0].columns;
        
        for (const row of movieResult[0].values) {
          const credit = {};
          
          movieColumns.forEach((col, idx) => {
            credit[col] = row[idx];
          });
          
          const actorId = credit.actor_id.toString();
          
          if (actors[actorId]) {
            actors[actorId].movie_credits.push(credit);
          }
        }
      }
      
      if (settings.difficulty === 'hard') {
        setLoadingMessage("Loading TV credits for hard mode...");
        
        const tvQuery = `
          SELECT * FROM tv_credits 
          WHERE actor_id IN (
            SELECT id FROM actors WHERE id IN (
              SELECT actor_id FROM actor_regions WHERE region = '${settings.region}'
            )
          )
        `;
        
        const tvResult = db.exec(tvQuery);
        
        if (tvResult[0] && tvResult[0].values) {
          const tvColumns = tvResult[0].columns;
          
          for (const row of tvResult[0].values) {
            const credit = {};
            
            tvColumns.forEach((col, idx) => {
              credit[col] = row[idx];
            });
            
            const actorId = credit.actor_id.toString();
            
            if (actors[actorId]) {
              actors[actorId].tv_credits.push(credit);
            }
          }
        }
      }
      
      setLoadingMessage("Processing data...");
      setLoadingProgress(80);
      
      const filteredActors = {};
      for (const [id, actor] of Object.entries(actors)) {
        if (actor.movie_credits.length >= 3) {
          filteredActors[id] = actor;
        }
      }
      
      const dataWithMetadata = {
        ...filteredActors,
        _timestamp: new Date().getTime(),
        _dataSource: dataSourceName
      };
      
      try {
        setLoadingMessage("Caching data for future use...");
        localStorage.setItem(cacheKey, JSON.stringify(dataWithMetadata));
      } catch (err) {
        console.warn("Failed to cache data:", err);
        try {
          for (let i = 0; i < localStorage.length; i++) {
            const key = localStorage.key(i);
            if (key?.startsWith('actor-data-') && key !== cacheKey) {
              localStorage.removeItem(key);
            }
          }
          localStorage.setItem(cacheKey, JSON.stringify(dataWithMetadata));
        } catch (clearErr) {
          console.error("Cannot store data in localStorage even after clearing:", clearErr);
        }
      }
      
      setDataSource(dataSourceName);
      setActorData(filteredActors);
      setLoadingProgress(100);
      setLoadingMessage("Actor data loaded and cached successfully!");
      setLoadingProgress(80);
      setLoadingMessage("Data loaded! Selecting actors for your game...");
      
      return true;
    } catch (error) {
      console.error("Error loading actor data:", error);
      console.error("Stack trace:", error.stack);
      setError(`Failed to load actor data: ${error.message}`);
      setLoading(false);
      setSelectingActors(false);
      return false;
    }
  }, [settings.region, settings.difficulty, selectActorsWithValidPath, selectingActors]);

  useEffect(() => {
    const loadData = async () => {
      try {
        const success = await loadFromSqlite();
        
        if (success && !selectingActors) {
          // Wait a moment before selecting actors to allow state updates to complete
          setTimeout(() => {
            selectActorsWithValidPath();
          }, 500);
        }
      } catch (err) {
        console.error("Error in data loading flow:", err);
        setError(`Failed to load: ${err.message}`);
        setLoading(false);
        setSelectingActors(false);
      }
    };
    
    if (loading && !actorData && !selectingActors) {
      loadData();
    }
  }, [loadFromSqlite, selectActorsWithValidPath, loading, actorData, selectingActors]);

  useEffect(() => {
    if (actorData && loading && loadingProgress >= 95) {
      const safetyTimer = setTimeout(() => {
        console.log("Safety timer forcing game to start");
        
        // Force game to playing state if it gets stuck
        if (loading) {
          setLoading(false);
          
          // If we have actors selected but game won't start
          if (startActor && targetActor) {
            setGamePhase('playing');
          } else {
            // Emergency actor selection
            const actors = Object.entries(actorData);
            if (actors.length >= 2) {
              const randomStart = actors[Math.floor(Math.random() * actors.length)][1];
              const randomTarget = actors[Math.floor(Math.random() * actors.length)][1];
              
              setStartActor({
                id: randomStart.id,
                ...randomStart
              });
              
              setTargetActor({
                id: randomTarget.id,
                ...randomTarget
              });
              
              setGamePhase('playing');
            }
          }
          
          setSelectingActors(false);
        }
      }, 8000);
      
      return () => clearTimeout(safetyTimer);
    }
  }, [actorData, loading, loadingProgress, startActor, targetActor]);

  // Add this effect to force the game to start if it gets stuck
  useEffect(() => {
    if (actorData && loading && loadingProgress >= 80) {
      // Add a safety timeout to guarantee the game starts
      const safetyTimer = setTimeout(() => {
        console.log("Safety timeout triggered - forcing game to start");
        
        if (loading && actorData) {
          // Get any two popular actors
          const actors = Object.entries(actorData)
            .filter(([_, actor]) => actor.profile_path)
            .sort((a, b) => b[1].popularity - a[1].popularity)
            .slice(0, 20);
          
          if (actors.length >= 2) {
            const startIndex = Math.floor(Math.random() * Math.min(10, actors.length));
            const targetIndex = Math.floor(Math.random() * Math.min(10, actors.length));
            
            const startActorData = {
              id: actors[startIndex][0],
              ...actors[startIndex][1]
            };
            
            const targetActorData = {
              id: actors[targetIndex][0],
              ...actors[targetIndex][1]
            };
            
            console.log(`Emergency selection: ${startActorData.name} and ${targetActorData.name}`);
            
            // Force all state updates
            setStartActor(startActorData);
            setTargetActor(targetActorData);
            setGamePhase('playing');
            setLoading(false);
            setSelectingActors(false);
          } else {
            setError("Not enough actors available. Try a different region.");
            setLoading(false);
            setSelectingActors(false);
          }
        }
      }, 5000); // 5 second safety timeout
      
      return () => clearTimeout(safetyTimer);
    }
  }, [actorData, loading, loadingProgress]);

  // Add this effect to ensure game progress after loading
  useEffect(() => {
    // Safety timeout - force game to start if stuck at 100% loading
    if (actorData && loading && loadingProgress >= 95) {
      const safetyTimer = setTimeout(() => {
        console.log("Safety timer forcing game to start");
        
        if (loading) {
          if (startActor && targetActor) {
            setGamePhase('playing');
            setLoading(false);
            setSelectingActors(false);
          } else {
            // Emergency actor selection from actorData
            const actors = Object.entries(actorData);
            if (actors.length >= 2) {
              const start = actors[Math.floor(Math.random() * actors.length)][1];
              const target = actors[Math.floor(Math.random() * actors.length)][1];
              
              setStartActor({
                id: start.id,
                name: start.name || "Unknown Actor",
                profile_path: start.profile_path,
                type: 'actor'
              });
              
              setTargetActor({
                id: target.id, 
                name: target.name || "Unknown Actor",
                profile_path: target.profile_path,
                type: 'actor'
              });
              
              setGamePhase('playing');
              setLoading(false);
              setSelectingActors(false);
            }
          }
        }
      }, 8000); // 8 seconds safety timeout
      
      return () => clearTimeout(safetyTimer);
    }
  }, [actorData, loading, loadingProgress, startActor, targetActor]);

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
          onComplete={handlePathComplete} // Add this prop
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