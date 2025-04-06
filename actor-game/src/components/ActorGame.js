import { useState, useEffect, useCallback } from 'react';
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
  const [error, setError] = useState(null);
  const [path, setPath] = useState([]);
  const [gamePhase, setGamePhase] = useState('initializing'); // 'initializing', 'playing', 'completed'
  const [targetActor, setTargetActor] = useState(null);
  const [startActor, setStartActor] = useState(null);
  const [hintAvailable, setHintAvailable] = useState(false);
  const [hint, setHint] = useState(null);
  const [optimalPath, setOptimalPath] = useState(null);
  const [showOptimalPath, setShowOptimalPath] = useState(false);
  const [hintTimer, setHintTimer] = useState(null);

  // TMDB API constants
  const BASE_IMG_URL = "https://image.tmdb.org/t/p/";
  const PROFILE_SIZE = "w185";
  const POSTER_SIZE = "w342";

  const calculateOptimalPath = useCallback((startId, targetId) => {
    if (!actorData) return;
    
    const queue = [];
    const visited = new Set();
    const previous = new Map();
    
    queue.push({
      id: startId,
      type: 'actor',
      steps: 0
    });
    visited.add(`actor-${startId}`);
    
    while (queue.length > 0) {
      const current = queue.shift();
      
      if (current.type === 'actor' && current.id === targetId) {
        const optimalPathResult = [];
        let currentNode = current;
        
        while (currentNode) {
          optimalPathResult.unshift(currentNode);
          currentNode = previous.get(`${currentNode.type}-${currentNode.id}`);
        }
        
        setOptimalPath(optimalPathResult);
        return;
      }
      
      if (current.type === 'actor') {
        const actor = actorData[current.id];
        
        const credits = [
          ...actor.movie_credits,
          ...(settings.difficulty === 'hard' ? actor.tv_credits : [])
        ];
        
        const filteredCredits = settings.excludeMcu
          ? credits.filter(credit => !credit.is_mcu)
          : credits;
        
        for (const credit of filteredCredits) {
          const creditId = `movie-${credit.id}`;
          if (!visited.has(creditId)) {
            queue.push({
              id: credit.id,
              type: 'movie',
              title: credit.title || credit.name,
              poster_path: credit.poster_path,
              steps: current.steps + 1
            });
            visited.add(creditId);
            previous.set(creditId, current);
          }
        }
      } else if (current.type === 'movie') {
        for (const actorId in actorData) {
          const actor = actorData[actorId];
          
          const wasInMovie = actor.movie_credits.some(credit => credit.id === current.id) ||
            (settings.difficulty === 'hard' ? 
              actor.tv_credits.some(credit => credit.id === current.id) : false);
          
          if (wasInMovie) {
            const actorNodeId = `actor-${actorId}`;
            if (!visited.has(actorNodeId)) {
              queue.push({
                id: actorId,
                type: 'actor',
                name: actor.name,
                profile_path: actor.profile_path,
                steps: current.steps + 1
              });
              visited.add(actorNodeId);
              previous.set(actorNodeId, current);
            }
          }
        }
      }
    }
    
    setOptimalPath([]);
  }, [actorData, settings]);

  useEffect(() => {
    async function loadFromFirebase() {
      try {
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

  useEffect(() => {
    if (actorData && !loading && !startActor) {
      // Filter actors with profile images (better UI)
      const filteredActors = Object.entries(actorData)
        .filter(([_, actor]) => actor.profile_path)
        .map(([id, actor]) => ({
          id,
          ...actor
        }));
      
      // Sort by popularity
      const sortedActors = [...filteredActors].sort((a, b) => b.popularity - a.popularity);
      
      // Get top 10% for start actor (always well-known)
      const topActors = sortedActors.slice(0, Math.floor(sortedActors.length * 0.1));
      const startIndex = Math.floor(Math.random() * topActors.length);
      const newStartActor = topActors[startIndex];
      
      // Select target actor based on difficulty
      let targetActorPool;
      switch(settings.difficulty) {
        case 'easy':
          // Well-known actors (top 20%)
          targetActorPool = sortedActors.slice(0, Math.floor(sortedActors.length * 0.2));
          break;
        case 'normal':
          // Medium popularity (20%-50%)
          targetActorPool = sortedActors.slice(
            Math.floor(sortedActors.length * 0.2),
            Math.floor(sortedActors.length * 0.5)
          );
          break;
        case 'hard':
          // Less known actors (50%-100%)
          targetActorPool = sortedActors.slice(Math.floor(sortedActors.length * 0.5));
          break;
        default:
          targetActorPool = sortedActors;
      }
      
      // Make sure we don't pick the same actor
      targetActorPool = targetActorPool.filter(actor => actor.id !== newStartActor.id);
      
      // Select random target actor from appropriate pool
      const targetIndex = Math.floor(Math.random() * targetActorPool.length);
      const newTargetActor = targetActorPool[targetIndex];
      
      setStartActor(newStartActor);
      setTargetActor(newTargetActor);
      setGamePhase('playing');
    }
  }, [actorData, loading, settings.difficulty, startActor]);

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
      
      calculateOptimalPath(startActor.id, targetActor.id);
    }
    
    return () => {
      if (hintTimer) {
        clearTimeout(hintTimer);
      }
    };
  }, [startActor, targetActor, actorData, hintTimer, calculateOptimalPath]);

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

  const generateHint = () => {
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
  };

  const showHint = () => {
    const newHint = generateHint();
    setHint(newHint);
    
    if (hintTimer) clearTimeout(hintTimer);
    setHintAvailable(false);
    
    const timer = setTimeout(() => {
      setHintAvailable(true);
    }, 120000);
    
    setHintTimer(timer);
  };

  const handleSelection = (selection) => {
    setPath([...path, selection]);
    
    if (selection.id === targetActor.id) {
      setGamePhase('completed');
    }
  };

  if (loading) {
    return (
      <div className="loading-container">
        <div className="loading-progress-bar">
          <div 
            className="loading-progress-fill" 
            style={{ width: `${loadingProgress}%` }}
          ></div>
        </div>
        <div className="loading-text">Loading actor data...</div>
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
            src={startActor?.profile_path ? `${BASE_IMG_URL}${PROFILE_SIZE}${startActor.profile_path}` : '/placeholder.png'} 
            alt={startActor?.name} 
            className="actor-image"
          />
          <div className="actor-name">{startActor?.name}</div>
          <div className="actor-label">START</div>
        </div>
        
        <div className="connection-arrow">→</div>
        
        <div className="target-actor">
          <img 
            src={targetActor?.profile_path ? `${BASE_IMG_URL}${PROFILE_SIZE}${targetActor.profile_path}` : '/placeholder.png'} 
            alt={targetActor?.name} 
            className="actor-image"
          />
          <div className="actor-name">{targetActor?.name}</div>
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
                              src={item.profile_path ? `${BASE_IMG_URL}${PROFILE_SIZE}${item.profile_path}` : '/placeholder.png'} 
                              alt={item.name} 
                            />
                            <div className="item-name">{item.name}</div>
                          </>
                        ) : (
                          <>
                            <img 
                              src={item.poster_path ? `${BASE_IMG_URL}${POSTER_SIZE}${item.poster_path}` : '/movie-placeholder.png'} 
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