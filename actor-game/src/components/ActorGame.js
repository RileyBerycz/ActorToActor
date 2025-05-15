import { useState, useEffect, useCallback } from 'react';
import GameControls from './GameControls';
import PathDisplay from './PathDisplay';
import '../css/ActorGame.css'; 

// Database location constants
const DB_URLS = {
  LOCAL: 'actors.db',
  GITHUB: 'https://raw.githubusercontent.com/RileyBerycz/ActorToActor/main/actor-game/public/actors.db',
  CONNECTION_DB: 'actor_connections.db',
  GITHUB_CONNECTION_DB: 'https://raw.githubusercontent.com/RileyBerycz/ActorToActor/main/actor-game/public/actor_connections.db'
};

function ActorGame({ settings, onReset, initialLoading }) {
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
  const SQL_CDN_URL = 'https://sql.js.org/dist/sql-wasm.wasm';
  const FALLBACK_CDN_URL = 'https://cdn.jsdelivr.net/npm/sql.js@1.8.0/dist/sql-wasm.wasm';
  
  // Helper function to get image URL 
  const getImageUrl = (path, size = 'w185') => {
    if (!path) return defaultImageUrl;
    const normalizedPath = path.startsWith('/') ? path : `/${path}`;
    return `${BASE_IMG_URL}${size}${normalizedPath}`;
  };

  // Helper function to filter valid credits
  const filterValidCredits = (credits) => {
    return credits.filter(credit => {
      const title = (credit.title || '').toLowerCase();
      const character = (credit.character || '').toLowerCase();
      
      // Skip talk shows/entertainment programs
      if (title.includes('tonight show') || 
          title.includes('late night') || 
          title.includes('jimmy') || 
          title.includes('ellen')) {
        return false;
      }
      
      // Filter out compilations and documentaries
      if (title.includes('documentary') ||
          title.includes('compilation') ||
          title.includes('anthology') || 
          title.includes('collection') ||
          title.includes('final cut') ||
          title.includes('behind the scenes') ||
          title.includes('making of')) {
        return false;
      }
      
      // Filter character types that indicate non-narrative appearances
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
      
      return true;
    });
  };

  // Calculate the optimal path between actors using BFS
  const calculateOptimalPath = useCallback((startId, targetId) => {
    if (!actorData || !startId || !targetId) return [];
    
    try {
      console.log(`Calculating optimal path from ${startId} to ${targetId}`);
      
      // Set depth limit based on difficulty
      let maxDepth;
      switch(settings.difficulty) {
        case 'easy': maxDepth = 6; break; 
        case 'normal': maxDepth = 10; break;
        case 'hard': maxDepth = 14; break;
        default: maxDepth = 10;
      }
      
      const queue = [{
        id: startId,
        type: 'actor',
        depth: 0
      }];
      
      const visited = new Set([`actor-${startId}`]);
      const previous = new Map();
      
      while (queue.length > 0) {
        const current = queue.shift();
        
        // Check if we found the target
        if (current.type === 'actor' && current.id === targetId) {
          // Reconstruct and return the path
          const path = [];
          let node = current;
          
          while (node) {
            path.unshift({
              id: node.id,
              type: node.type,
              name: node.name || (node.type === 'actor' ? actorData[node.id]?.name : null),
              profile_path: node.type === 'actor' ? actorData[node.id]?.profile_path : null,
              poster_path: node.type === 'movie' ? node.poster_path : null,
              title: node.type === 'movie' ? node.title : null
            });
            
            const prevKey = `${node.type}-${node.id}`;
            node = previous.get(prevKey);
          }
          
          console.log("Found optimal path:", path);
          return path;
        }
        
        // Don't go beyond max depth
        if (current.depth >= maxDepth) continue;
        
        // Handle actor node - find connected movies
        if (current.type === 'actor') {
          const actor = actorData[current.id];
          if (!actor) continue;
          
          // Get actor's credits
          const credits = [
            ...actor.movie_credits,
            ...(settings.difficulty === 'hard' ? actor.tv_credits : [])
          ];
          
          // Apply filters
          const filteredCredits = settings.excludeMcu ? 
            credits.filter(c => !c.is_mcu) : credits;
          const validCredits = filterValidCredits(filteredCredits);
          
          // Sort by popularity for better paths
          const sortedCredits = [...validCredits]
            .sort((a, b) => b.popularity - a.popularity)
            .slice(0, 15); // limit for performance
          
          // Add each movie to the queue
          for (const credit of sortedCredits) {
            const creditId = `movie-${credit.id}`;
            if (!visited.has(creditId)) {
              const nextNode = {
                id: credit.id,
                type: 'movie',
                title: credit.title,
                poster_path: credit.poster_path,
                depth: current.depth + 1
              };
              
              queue.push(nextNode);
              visited.add(creditId);
              previous.set(creditId, current);
            }
          }
        } else {
          // Handle movie node - find connected actors
          const movieId = current.id;
          
          // Find actors in this movie
          const actorsInMovie = Object.entries(actorData)
            .filter(([_, actor]) => {
              return actor.movie_credits.some(c => c.id === movieId) ||
                (settings.difficulty === 'hard' && actor.tv_credits.some(c => c.id === movieId));
            })
            .map(([id]) => id);
          
          // Add each actor to the queue
          for (const actorId of actorsInMovie.slice(0, 20)) { // limit for performance
            const actorNodeId = `actor-${actorId}`;
            if (!visited.has(actorNodeId)) {
              visited.add(actorNodeId);
              const nextNode = {
                id: actorId,
                type: 'actor',
                depth: current.depth + 1
              };
              
              queue.push(nextNode);
              previous.set(actorNodeId, current);
            }
          }
        }
      }
      
      console.log("No optimal path found");
      return [];
    } catch (error) {
      console.error("Error calculating optimal path:", error);
      return [];
    }
  }, [actorData, settings.difficulty, settings.excludeMcu]);
  
  // Check if a path exists between actors
  const verifyPathExists = useCallback((startId, targetId, maxLength = 10) => {
    if (!actorData || !startId || !targetId) return false;
    
    try {
      // Simplified BFS to just check existence
      const visited = new Set([`actor-${startId}`]);
      const queue = [{ id: startId, type: 'actor', depth: 0 }];
      
      while (queue.length > 0) {
        const current = queue.shift();
        
        if (current.type === 'actor' && current.id === targetId) {
          return true; // Path found
        }
        
        if (current.depth >= maxLength) continue;
        
        if (current.type === 'actor') {
          const actor = actorData[current.id];
          if (!actor) continue;
          
          // Get credits
          const credits = [
            ...actor.movie_credits,
            ...(settings.difficulty === 'hard' ? actor.tv_credits : [])
          ];
          
          // Apply filters
          const validCredits = filterValidCredits(credits);
          
          // Process limited number for performance
          const topCredits = validCredits
            .sort((a, b) => b.popularity - a.popularity)
            .slice(0, 10);
          
          for (const credit of topCredits) {
            const creditId = `movie-${credit.id}`;
            if (!visited.has(creditId)) {
              visited.add(creditId);
              queue.push({
                id: credit.id,
                type: 'movie',
                depth: current.depth + 1
              });
            }
          }
        } else { // Movie node
          const movieId = current.id;
          
          // Find actors in this movie
          const actorsInMovie = Object.entries(actorData)
            .filter(([_, actor]) => 
              actor.movie_credits.some(c => c.id === movieId) ||
              (settings.difficulty === 'hard' && actor.tv_credits.some(c => c.id === movieId))
            )
            .map(([id]) => id);
          
          for (const actorId of actorsInMovie.slice(0, 8)) {
            const actorNodeId = `actor-${actorId}`;
            if (!visited.has(actorNodeId)) {
              visited.add(actorNodeId);
              queue.push({
                id: actorId,
                type: 'actor',
                depth: current.depth + 1
              });
            }
          }
        }
      }
      
      return false; // No path found
    } catch (error) {
      console.error("Error verifying path:", error);
      return false;
    }
  }, [actorData, settings.difficulty]);

  // Select actors with valid path - robust implementation
  const selectActors = useCallback(async () => {
    if (!actorData || selectingActors) return;
    
    console.log("🔍 [DEBUG] Starting actor selection process");
    console.time("ActorSelection");
    setSelectingActors(true);
    setIsLoading(true);
    
    try {
      setLoadingMessage("Finding actors for your game...");
      setLoadingProgress(85);
      
      // Try to load connections database
      setLoadingMessage("Loading optimal connections...");
      console.time("ConnectionDB");
      
      // Try each connection database source
      let connectionDb = null;
      let response = null;
      
      // Try local connection db first, then GitHub
      const connectionSources = [
        { url: DB_URLS.CONNECTION_DB, name: 'local connections' },
        { url: DB_URLS.GITHUB_CONNECTION_DB, name: 'remote connections' }
      ];
      
      for (const source of connectionSources) {
        try {
          console.log(`🔍 [DEBUG] Trying to load from ${source.name}...`);
          console.time(`Fetch:${source.name}`);
          response = await fetch(source.url);
          console.timeEnd(`Fetch:${source.name}`);
          
          if (response.ok) {
            console.log(`✅ [DEBUG] Successfully loaded from ${source.name}`);
            break;
          }
        } catch (err) {
          console.log(`❌ [DEBUG] ${source.name} not available:`, err.message);
        }
      }
      
      if (!response || !response.ok) {
        console.warn("❌ [DEBUG] Connections database not found, will use random selection");
        // Continue with manual actor selection below
      } else {
        // Load connection database and select a pre-made path
        console.time("ArrayBuffer");
        const arrayBuffer = await response.arrayBuffer();
        console.timeEnd("ArrayBuffer");
        
        const uInt8Array = new Uint8Array(arrayBuffer);
        
        // Initialize SQL.js if not already done
        if (!window.SQL) {
          console.log("🔍 [DEBUG] Initializing SQL.js");
          console.time("SQLInit");
          try {
            const initSqlJs = (await import('sql.js')).default;
            window.SQL = await initSqlJs({ locateFile: file => SQL_CDN_URL });
            console.log("✅ [DEBUG] SQL.js initialized from primary source");
          } catch (err) {
            console.warn("⚠️ [DEBUG] Primary SQL init failed, trying fallback:", err);
            try {
              const initSqlJs = (await import('sql.js')).default;
              window.SQL = await initSqlJs({ locateFile: file => FALLBACK_CDN_URL });
              console.log("✅ [DEBUG] SQL.js initialized from fallback source");
            } catch (err2) {
              console.error("❌ [DEBUG] Both SQL init attempts failed:", err2);
            }
          }
          console.timeEnd("SQLInit");
        }
        
        // Open the connection db
        console.time("OpenDB");
        connectionDb = new window.SQL.Database(uInt8Array);
        console.timeEnd("OpenDB");
        console.log("✅ [DEBUG] Connection database opened successfully");
        
        // Query for connections matching the current difficulty
        let queryText = '';

        // Query for connections matching the current difficulty
        try {
          console.time("ExecuteQuery");
          
          // Simple query without popularity - just difficulty and region
          queryText = `
            SELECT * FROM actor_connections 
            WHERE difficulty = '${settings.difficulty}'
            AND region = '${settings.region}'
            ORDER BY RANDOM()
            LIMIT 1
          `;
          
          console.log("🔍 [DEBUG] Executing query:", queryText);
          let result = connectionDb.exec(queryText);
          console.timeEnd("ExecuteQuery");
          
          console.log("🔍 [DEBUG] Query result structure:", 
            result ? 
              `Found ${result.length} result sets, first set has ${result[0]?.values?.length || 0} rows` : 
              "No results");
        
          // If no results, fall back to GLOBAL region
          if (!result[0] || !result[0].values || result[0].values.length === 0) {
            console.log(`🔄 [DEBUG] No connections for ${settings.region}, falling back to GLOBAL`);
            console.time("FallbackQuery");
            const fallbackQuery = `
              SELECT * FROM actor_connections 
              WHERE difficulty = '${settings.difficulty}'
              AND region = 'GLOBAL'
              ORDER BY RANDOM()
              LIMIT 1
            `;
            
            console.log("🔍 [DEBUG] Executing fallback query:", fallbackQuery);
            result = connectionDb.exec(fallbackQuery);
            console.timeEnd("FallbackQuery");
            
            console.log("🔍 [DEBUG] Fallback query result:", 
              result ? 
                `Found ${result.length} result sets, first set has ${result[0]?.values?.length || 0} rows` : 
                "No results");
          }
        
          if (result && result[0] && result[0].values && result[0].values.length > 0) {
            console.log(`✅ [DEBUG] Found ${result[0].values.length} pre-calculated paths`);
            
            // Select a random path from the results
            const pathIndex = Math.floor(Math.random() * result[0].values.length);
            const columns = result[0].columns;
            const pathData = {};
            
            columns.forEach((col, idx) => {
              pathData[col] = result[0].values[pathIndex][idx];
            });
            
            console.log("🔍 [DEBUG] Selected path data:", {
              start_id: pathData.start_id,
              target_id: pathData.target_id,
              difficulty: pathData.difficulty,
              connection_length: pathData.connection_length,
              region: pathData.region,
              has_optimal_path: !!pathData.optimal_path
            });
            
            // Get the start and target actor IDs
            const startId = pathData.start_id;
            const targetId = pathData.target_id;
            
            // Check if actors exist in data
            console.log("🔍 [DEBUG] Checking if actors exist in loaded data:", {
              startExists: !!actorData[startId],
              targetExists: !!actorData[targetId]
            });
            
            // Load just those two specific actors
            if (actorData[startId] && actorData[targetId]) {
              const start = { id: startId, ...actorData[startId], type: 'actor' };
              const target = { id: targetId, ...actorData[targetId], type: 'actor' };
              
              // Decompress optimal path data
              console.time("DecompressPath");
              const compressedPath = pathData.optimal_path;
              let optimalPath = [];
              
              try {
                // The path is compressed as a gzipped JSON string
                console.log("🔍 [DEBUG] Attempting to decompress optimal path");
                const textDecoder = new TextDecoder('utf-8');
                const gunzipBlob = (new Blob([compressedPath], { type: 'application/gzip' }));
                
                // We'll need to use streaming decompression APIs if available
                if ('DecompressionStream' in window) {
                  console.log("🔍 [DEBUG] Using DecompressionStream API");
                  const ds = new DecompressionStream('gzip');
                  const decompressedStream = gunzipBlob.stream().pipeThrough(ds);
                  const decompressedBlob = await new Response(decompressedStream).blob();
                  const jsonText = textDecoder.decode(await decompressedBlob.arrayBuffer());
                  
                  // Decompress the path
                  const compressedPathData = JSON.parse(jsonText);
                  
                  console.log("🔍 [DEBUG] Path JSON parsed successfully, contains", 
                    Array.isArray(compressedPathData) ? compressedPathData.length : "non-array data");
                
                  // Convert the compressed format back to the full format
                  optimalPath = compressedPathData.map(item => {
                    const fullItem = {
                      type: item.t === 'a' ? 'actor' : 'movie',
                      id: item.i
                    };
                    
                    if (item.t === 'a') {
                      fullItem.name = item.n;
                      fullItem.profile_path = item.p;
                    } else {
                      fullItem.title = item.n;
                      fullItem.poster_path = item.p;
                    }
                    
                    return fullItem;
                  });
                  
                  console.log("✅ [DEBUG] Successfully decompressed optimal path with", optimalPath.length, "items");
                } else {
                  console.warn("⚠️ [DEBUG] DecompressionStream not available, can't decompress path");
                }
              } catch (decompressionError) {
                console.warn("⚠️ [DEBUG] Error decompressing path:", decompressionError);
                console.log("🔄 [DEBUG] Will calculate path manually");
                // We'll calculate the path manually if decompression fails
                optimalPath = calculateOptimalPath(startId, targetId);
              }
              console.timeEnd("DecompressPath");
              
              // Set actors and game state
              console.log(`✅ [DEBUG] Using connection: ${start.name} → ${target.name}`);
              setStartActor(start);
              setTargetActor(target);
              setOptimalPath(optimalPath);
              setGamePhase('playing');
              
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
            } else {
              console.warn(`❌ [DEBUG] Actor not found in data: ${!actorData[startId] ? startId : ''} ${!actorData[targetId] ? targetId : ''}`);
              console.log("🔄 [DEBUG] Falling back to random selection");
            }
          } else {
            console.log("❌ [DEBUG] No connections found for query:", queryText);
          }
        } catch (dbError) {
          console.error("❌ [DEBUG] Error executing query:", dbError);
          console.log("Query was:", queryText);
        } finally {
          if (connectionDb) {
            try {
              connectionDb.close();
            } catch (err) {
              console.warn("⚠️ [DEBUG] Error closing connection DB:", err);
            }
          }
        }
        console.timeEnd("ConnectionDB");
      }
      
      // FALLBACK: Manual actor selection if we reach here
      console.log("🔄 [DEBUG] Using fallback actor selection method");
      console.time("ManualActorSelection");
      
      console.log("🔍 [DEBUG] Total actors available:", Object.keys(actorData).length);
      
      // Step 1: Get all actors with profile images
      console.time("FilterActors");
      const actorsWithProfiles = Object.entries(actorData)
        .filter(([_, actor]) => {
          return actor.profile_path && actor.movie_credits.length >= 3;
        })
        .map(([id, actor]) => ({
          id,
          ...actor,
          type: 'actor'
        }));
      console.timeEnd("FilterActors");
      
      console.log(`✅ [DEBUG] Found ${actorsWithProfiles.length} actors with profile images`);
      
      if (actorsWithProfiles.length < 2) {
        throw new Error(`❌ [DEBUG] Not enough actors available for region ${settings.region}. Try another region.`);
      }
      
      // Step 2: Sort by popularity for better selection
      console.time("SortActors");
      const sortedActors = [...actorsWithProfiles].sort((a, b) => {
        return b.popularity - a.popularity;
      });
      console.timeEnd("SortActors");
      
      // Step 3: Select random start actor (from top 20 popular actors)
      const topCount = Math.min(20, sortedActors.length);
      const startIndex = Math.floor(Math.random() * topCount);
      const start = sortedActors[startIndex];
      console.log("✅ [DEBUG] Selected start actor:", start.name);
      
      // Step 4: Find a suitable target actor with verified path
      console.time("FindTargetActor");
      let target = null;
      let attempts = 0;
      const maxAttempts = 30; // Try harder to find a good match
      
      while (!target && attempts < maxAttempts) {
        attempts++;
        
        // Pick random index ensuring it's different from start
        let targetIndex;
        do {
          const poolSize = Math.min(50, sortedActors.length);
          targetIndex = Math.floor(Math.random() * poolSize);
        } while (targetIndex === startIndex);
        
        const candidate = sortedActors[targetIndex];
        
        // Double-check they're different actors
        if (candidate.id === start.id) continue;
        
        // For easy/normal modes, verify a path exists
        if (settings.difficulty !== 'hard') {
          // For performance, only check every other candidate
          if (attempts % 2 === 0 || attempts >= maxAttempts/2) {
            console.log(`🔍 [DEBUG] Verifying path attempt ${attempts}: ${start.name} → ${candidate.name}`);
            console.time(`VerifyPath${attempts}`);
            const pathExists = await Promise.resolve(
              verifyPathExists(start.id, candidate.id, 
                settings.difficulty === 'easy' ? 6 : 10)
            );
            console.timeEnd(`VerifyPath${attempts}`);
            
            if (pathExists) {
              target = candidate;
              console.log(`✅ [DEBUG] Found valid path between ${start.name} and ${candidate.name} in ${attempts} attempts`);
              break;
            } else {
              console.log(`❌ [DEBUG] No valid path between ${start.name} and ${candidate.name}, attempt ${attempts}`);
            }
          }
        } else {
          // For hard mode, don't verify path
          target = candidate;
          console.log("✅ [DEBUG] Hard mode - using candidate without path verification");
          break;
        }
      }
      console.timeEnd("FindTargetActor");
      
      // If no good target found after attempts, pick any actor
      if (!target) {
        console.log("⚠️ [DEBUG] Could not find target with verified path, picking any actor");
        const fallbackIndex = startIndex === 0 ? 1 : 0;
        target = sortedActors[fallbackIndex];
      }
      
      console.log(`✅ [DEBUG] Final selection: ${start.name} and ${target.name}`);
      
      // Set actors and game phase
      setStartActor(start);
      setTargetActor(target);
      setLoadingMessage("Setting up game...");
      setGamePhase('playing');
      
      // Calculate optimal path in background
      setLoadingProgress(95);
      setTimeout(() => {
        try {
          console.log("🔍 [DEBUG] Calculating optimal path...");
          console.time("OptimalPathCalc");
          const path = calculateOptimalPath(start.id, target.id);
          console.timeEnd("OptimalPathCalc");
          console.log("✅ [DEBUG] Optimal path calculation complete, length:", path.length);
          setOptimalPath(path);
        } catch (error) {
          console.error("❌ [DEBUG] Error calculating optimal path:", error);
          setOptimalPath([]);
        }
        
        // Finish loading
        setLoadingProgress(100);
        setLoadingMessage("Ready to play!");
        setTimeout(() => {
          setIsLoading(false);
          setLoading(false);
          setSelectingActors(false);
        }, 800);
      }, 500);
      
      console.timeEnd("ManualActorSelection");
      
    } catch (error) {
      console.error("❌ [DEBUG] Error in actor selection:", error);
      setError(`Failed to select actors: ${error.message}`);
      setIsLoading(false);
      setLoading(false);
      setSelectingActors(false);
    } finally {
      console.timeEnd("ActorSelection");
    }
  }, [actorData, calculateOptimalPath, selectingActors, settings.difficulty, settings.region, verifyPathExists]);

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
      setLoadingMessage("Loading actors from database...");
      setLoadingProgress(10);
      
      // Try to load from cache first
      const cacheKey = `actor-data-${settings.region}-${settings.difficulty}`;
      const cachedData = localStorage.getItem(cacheKey);
      
      if (cachedData) {
        try {
          setLoadingMessage("Loading from browser cache...");
          const parsedData = JSON.parse(cachedData);
          const cacheTimestamp = parsedData._timestamp || 0;
          const currentTime = new Date().getTime();
          
          // Use cache if less than 24 hours old
          if (currentTime - cacheTimestamp < 24 * 60 * 60 * 1000) {
            setLoadingMessage("Using cached actor data...");
            setLoadingProgress(50);
            
            const { _timestamp, _dataSource, ...actorDataOnly } = parsedData;
            
            setDataSource(`cache:${_dataSource}`);
            setActorData(actorDataOnly);
            setLoadingProgress(80);
            setLoadingMessage("Selecting actors for your game...");
            
            // Return true indicating successful load
            return true;
          } else {
            setLoadingMessage("Cache expired, fetching fresh data...");
          }
        } catch (err) {
          console.error("Error loading from cache:", err);
          setLoadingMessage("Cache corrupted, fetching fresh data...");
        }
      }
      
      // Initialize SQL.js
      setLoadingMessage("Initializing database engine...");
      
      let SQL;
      try {
        const initSqlJs = (await import('sql.js')).default;
        SQL = await initSqlJs({ locateFile: file => SQL_CDN_URL });
        setLoadingMessage("Database engine initialized!");
      } catch (err) {
        console.warn("Primary SQL init failed, trying fallback:", err);
        try {
          const initSqlJs = (await import('sql.js')).default;
          SQL = await initSqlJs({ locateFile: file => FALLBACK_CDN_URL });
          setLoadingMessage("Database engine initialized with fallback!");
        } catch (secondErr) {
          console.error("Fatal error initializing SQL.js:", secondErr);
          throw new Error("Could not initialize database engine. Please try a different browser.");
        }
      }
      
      // Attempt to load database from various sources
      setLoadingProgress(20);
      setLoadingMessage("Fetching actor database...");
      
      let response = null;
      let dataSourceName = '';
      
      const sources = [
        { url: DB_URLS.LOCAL, name: 'local' },
        { url: DB_URLS.GITHUB, name: 'github' },
        { url: DB_URLS.CONNECTION_DB, name: 'connection_local' },
        { url: DB_URLS.GITHUB_CONNECTION_DB, name: 'connection_github' }
      ];
      
      // Try each source in order until one works
      for (const source of sources) {
        try {
          console.log(`Trying to load from ${source.name}...`);
          response = await fetch(source.url);
          if (response.ok) {
            dataSourceName = source.name;
            setLoadingMessage(`Loading from ${source.name} database...`);
            break;
          }
        } catch (err) {
          console.log(`${source.name} not available:`, err.message);
        }
      }
      
      if (!response || !response.ok) {
        throw new Error("Could not load actor database from any source.");
      }
      
      setLoadingProgress(30);
      
      // Load database from response
      const arrayBuffer = await response.arrayBuffer();
      const uInt8Array = new Uint8Array(arrayBuffer);
      
      const db = new SQL.Database(uInt8Array);
      setLoadingMessage("Processing actor data...");
      setLoadingProgress(40);
      
      // Query actors for selected region
      const actorQuery = dataSourceName.includes('connection') 
        ? `SELECT * FROM actors WHERE id IN (
             SELECT actor_id FROM actor_regions WHERE region = '${settings.region}'
           )`
        : `SELECT a.* FROM actors a
           JOIN actor_regions ar ON a.id = ar.actor_id
           WHERE ar.region = '${settings.region}'`;
      
      let actorResult;
      try {
        actorResult = db.exec(actorQuery);
      } catch (error) {
        console.error("Error querying actors:", error);
        throw new Error(`Database error: Failed to query actors. ${error.message}`);
      }
      
      if (!actorResult[0] || !actorResult[0].values || actorResult[0].values.length === 0) {
        throw new Error(`No actors found for region ${settings.region}. Please try another region.`);
      }
      
      setLoadingMessage(`Found ${actorResult[0].values.length} actors for ${settings.region}. Loading credits...`);
      setLoadingProgress(60);
      
      // Process actors
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
      
      // Load movie credits
      setLoadingMessage("Loading movie credits...");
      const movieQuery = `
        SELECT * FROM movie_credits 
        WHERE actor_id IN (
          SELECT id FROM actors WHERE id IN (
            SELECT actor_id FROM actor_regions WHERE region = '${settings.region}'
          )
        )
      `;
      
      let movieResult;
      try {
        movieResult = db.exec(movieQuery);
      } catch (error) {
        console.error("Error querying movie credits:", error);
      }
      
      if (movieResult && movieResult[0] && movieResult[0].values) {
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
      
      // Load TV credits for hard mode
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
        
        let tvResult;
        try {
          tvResult = db.exec(tvQuery);
        } catch (error) {
          console.error("Error querying TV credits:", error);
        }
        
        if (tvResult && tvResult[0] && tvResult[0].values) {
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
      
      // Filter actors with sufficient credits
      setLoadingMessage("Processing data...");
      setLoadingProgress(70);
      
      const filteredActors = {};
      for (const [id, actor] of Object.entries(actors)) {
        // Only use actors with enough credits
        const minCredits = settings.difficulty === 'easy' ? 5 : 3;
        if (actor.movie_credits.length >= minCredits) {
          filteredActors[id] = actor;
        }
      }
      
      // Make sure we have enough actors
      const actorCount = Object.keys(filteredActors).length;
      if (actorCount < 2) {
        throw new Error(`Not enough actors with sufficient credits for region ${settings.region}. Found: ${actorCount}`);
      }
      
      console.log(`Found ${actorCount} usable actors`);
      
      // Cache data for future use
      try {
        setLoadingMessage("Caching data for future use...");
        const dataWithMetadata = {
          ...filteredActors,
          _timestamp: new Date().getTime(),
          _dataSource: dataSourceName
        };
        
        localStorage.setItem(cacheKey, JSON.stringify(dataWithMetadata));
      } catch (err) {
        console.warn("Failed to cache data:", err);
        // Try clearing other caches and try again
        try {
          for (let i = 0; i < localStorage.length; i++) {
            const key = localStorage.key(i);
            if (key?.startsWith('actor-data-') && key !== cacheKey) {
              localStorage.removeItem(key);
            }
          }
          
          const dataWithMetadata = {
            ...filteredActors,
            _timestamp: new Date().getTime(),
            _dataSource: dataSourceName
          };
          
          localStorage.setItem(cacheKey, JSON.stringify(dataWithMetadata));
        } catch (clearErr) {
          console.error("Cannot store data in localStorage even after clearing:", clearErr);
        }
      }
      
      setDataSource(dataSourceName);
      setActorData(filteredActors);
      setLoadingProgress(80);
      setLoadingMessage("Data loaded! Selecting actors for your game...");
      
      return true;
    } catch (error) {
      console.error("Error loading actor data:", error);
      setError(`Failed to load actor data: ${error.message}`);
      setLoading(false);
      setIsLoading(false);
      setSelectingActors(false);
      return false;
    }
  }, [settings.region, settings.difficulty]);

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