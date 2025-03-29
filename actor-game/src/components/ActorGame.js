import { useState, useEffect } from 'react';
import initSqlJs from 'sql.js';
import '../css/ActorGame.css';
import ActorCard from './ActorCard';
import GameControls from './GameControls';
import PathDisplay from './PathDisplay';

function ActorGame({ settings }) {
  const [actorData, setActorData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState(null);

  // TMDB API constants
  const BASE_IMG_URL = "https://image.tmdb.org/t/p/";
  const PROFILE_SIZE = "w185";
  const POSTER_SIZE = "w342";

  useEffect(() => {
    async function loadActorData() {
      try {
        setLoading(true);
        
        // Use the correct initialization approach
        const SQL = await initSqlJs({
          // Use the full URL to the WASM file
          locateFile: file => `https://cdnjs.cloudflare.com/ajax/libs/sql.js/1.8.0/${file}`
        });
        
        // Fetch the database file
        const dbFile = await fetch(`/actors_${settings.region}.db`);
        
        if (!dbFile.ok) {
          throw new Error(`Failed to load actor database: ${dbFile.status} ${dbFile.statusText}`);
        }
        
        // Get binary data from response
        const buffer = await dbFile.arrayBuffer();
        
        // Create database from binary data
        const db = new SQL.Database(new Uint8Array(buffer));
        
        // Extract actors data
        const actors = {};
        
        // Query all actors
        const actorsResults = db.exec(`SELECT id, name, popularity, profile_path, place_of_birth FROM actors`);
        
        if (actorsResults.length > 0 && actorsResults[0].values.length > 0) {
          for (const [id, name, popularity, profile_path, place_of_birth] of actorsResults[0].values) {
            const actorId = id.toString();
            
            // Get regions for this actor
            const regionsResult = db.exec(`SELECT region FROM actor_regions WHERE actor_id = ${id}`);
            const regions = regionsResult[0]?.values.map(row => row[0]) || [];
            
            // Get movie credits (including MCU flag)
            const movieCreditsResult = db.exec(`
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
                is_mcu: !!is_mcu  // Convert to boolean
              })
            ) || [];
            
            // Get TV credits (including MCU flag)
            const tvCreditsResult = db.exec(`
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
                is_mcu: !!is_mcu  // Convert to boolean
              })
            ) || [];
            
            // Add actor to data object
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
          
          console.log(`Successfully loaded data with ${Object.keys(actors).length} actors`);
          setActorData(actors);
        } else {
          throw new Error("No actor data found in database");
        }
        
      } catch (error) {
        console.error("Error loading actor data:", error);
        setError(error.message);
      } finally {
        setLoading(false);
      }
    }
    
    loadActorData();
  }, [settings.region]);

  // Helper function to check if an actor has been in MCU
  const actorHasBeenInMcu = (actor) => {
    if (!actor) return false;
    
    // Check if any movie is MCU
    const mcuMovie = actor.movie_credits?.find(movie => movie.is_mcu);
    if (mcuMovie) return true;
    
    // Check if any TV show is MCU
    const mcuTv = actor.tv_credits?.find(tv => tv.is_mcu);
    if (mcuTv) return true;
    
    return false;
  };

  // Game state
  const [gameState, setGameState] = useState('setup'); // setup, playing, won, lost
  const [startActor, setStartActor] = useState(null); 
  const [targetActor, setTargetActor] = useState(null);
  const [currentPath, setCurrentPath] = useState([]);
  const [moves, setMoves] = useState(0);
  const [maxMoves, setMaxMoves] = useState(6); // Limit moves based on difficulty
  
  // Player input state
  const [actorGuess, setActorGuess] = useState('');
  const [mediaGuess, setMediaGuess] = useState('');
  const [guessError, setGuessError] = useState(null);
  
  // Start a new game
  const startNewGame = () => {
    if (!actorData) return;
    
    // Select random start and target actors
    const actors = Object.values(actorData);
    const popularActors = actors.filter(actor => actor.popularity > 10);
    
    const randomStart = popularActors[Math.floor(Math.random() * popularActors.length)];
    let randomTarget;
    do {
      randomTarget = popularActors[Math.floor(Math.random() * popularActors.length)];
    } while (randomStart.id === randomTarget.id);
    
    setStartActor(randomStart);
    setTargetActor(randomTarget);
    setCurrentPath([randomStart]);
    setMoves(0);
    setGameState('playing');
    setActorGuess('');
    setMediaGuess('');
    setGuessError(null);
  };
  
  // Helper functions for game logic
  const findActor = (name) => {
    return Object.values(actorData).find(actor => 
      actor.name.toLowerCase() === name.toLowerCase()
    );
  };
  
  const actorHasWorkedOn = (actorId, mediaId) => {
    const actor = actorData[actorId];
    if (!actor) return false;
    
    const movieIds = (actor.movie_credits || []).map(m => m.id);
    const tvIds = (actor.tv_credits || []).map(t => t.id);
    
    return movieIds.includes(mediaId) || tvIds.includes(mediaId);
  };
  
  const findMediaByTitle = (title) => {
    // Search through all movies and TV shows to find by title
    for (const actor of Object.values(actorData)) {
      for (const movie of (actor.movie_credits || [])) {
        if (movie.title.toLowerCase() === title.toLowerCase()) {
          return { ...movie, type: 'movie' };
        }
      }
      for (const show of (actor.tv_credits || [])) {
        if (show.name.toLowerCase() === title.toLowerCase()) {
          return { ...show, type: 'tv' };
        }
      }
    }
    return null;
  };
  
  // Handle player guess submission
  const handleSubmit = (e) => {
    e.preventDefault();
    setGuessError(null);
    
    let actor; // Declare actor variable at this scope
    
    if (settings.difficulty === 'normal') {
      // Normal mode code
      if (!actorGuess || !mediaGuess) {
        setGuessError("Please enter both an actor name and movie/show title");
        return;
      }
      
      actor = findActor(actorGuess); // Now this assigns to our declared variable
      if (!actor) {
        setGuessError("Actor not found in our database");
        return;
      }
      
      const media = findMediaByTitle(mediaGuess);
      if (!media) {
        setGuessError("Movie/TV show not found in our database");
        return;
      }
      
      // Check if current actor worked on this media
      const currentActor = currentPath[currentPath.length - 1];
      if (!actorHasWorkedOn(currentActor.id, media.id)) {
        setGuessError(`${currentActor.name} didn't work on ${mediaGuess}`);
        return;
      }
      
      // Check if guessed actor worked on this media
      if (!actorHasWorkedOn(actor.id, media.id)) {
        setGuessError(`${actor.name} didn't work on ${mediaGuess}`);
        return;
      }
      
      // Valid move: Add to path
      setCurrentPath([...currentPath, 
        { id: media.id, title: media.title || media.name, type: media.type },
        actor
      ]);
      
    } else {
      // Easy mode code
      if (!actorGuess) {
        setGuessError("Please enter an actor name");
        return;
      }
      
      actor = findActor(actorGuess); // Now this assigns to our declared variable
      if (!actor) {
        setGuessError("Actor not found in our database");
        return;
      }
      
      // Find a common work between current actor and guessed actor
      const currentActor = currentPath[currentPath.length - 1];
      const commonWorks = findCommonWorks(currentActor.id, actor.id);
      
      if (commonWorks.length === 0) {
        setGuessError(`${currentActor.name} hasn't worked with ${actor.name}`);
        return;
      }
      
      // Get the most popular common work
      const commonMedia = commonWorks[0]; // This needs refinement to actually get the most popular
      
      // Valid move: Add to path
      setCurrentPath([...currentPath, 
        { id: commonMedia.id, title: commonMedia.title || commonMedia.name, type: commonMedia.type },
        actor
      ]);
    }
    
    // Now 'actor' is available in this scope
    // Clear inputs for next guess
    setActorGuess('');
    setMediaGuess('');
    
    // Increment moves
    const newMoves = moves + 1;
    setMoves(newMoves);
    
    // Check if target reached
    if (actor.id === targetActor.id) {
      setGameState('won');
    } else if (newMoves >= maxMoves) {
      setGameState('lost');
    }
  };
  
  const findCommonWorks = (actor1Id, actor2Id) => {
    const actor1 = actorData[actor1Id];
    const actor2 = actorData[actor2Id];
    
    if (!actor1 || !actor2) return [];
    
    const actor1MovieIds = (actor1.movie_credits || []).map(m => m.id);
    const actor2MovieIds = (actor2.movie_credits || []).map(m => m.id);
    
    const actor1TvIds = (actor1.tv_credits || []).map(t => t.id);
    const actor2TvIds = (actor2.tv_credits || []).map(t => t.id);
    
    // Find common works
    const commonMovies = actor1MovieIds.filter(id => actor2MovieIds.includes(id));
    const commonTvShows = actor1TvIds.filter(id => actor2TvIds.includes(id));
    
    // Would need more work to get the actual media objects
    return [...commonMovies, ...commonTvShows];
  };

  if (loading) return <div className="loading">Loading game data...</div>;
  if (!actorData) return (
    <div className="error">
      <h2>Failed to load game data</h2>
      <p>Please check the following:</p>
      <ul>
        <li>Ensure you have a database file named <code>actors_{settings.region}.db</code> in the public folder</li>
        <li>Check browser console for specific error messages</li>
        <li>Verify the database file is properly formatted</li>
      </ul>
      <button onClick={() => window.location.reload()}>Try Again</button>
    </div>
  );
  
  return (
    <div className="actor-game">
      {gameState === 'setup' ? (
        <div className="game-setup">
          <h2>Actor to Actor Challenge</h2>
          <p>Connect one actor to another through shared movies and TV shows.</p>
          <GameControls 
            gameState={gameState} 
            onStartGame={startNewGame} 
          />
        </div>
      ) : gameState === 'playing' ? (
        <div className="game-board">
          <div className="game-status">
            <ActorCard actor={startActor} role="start" />
            
            <GameControls 
              gameState={gameState} 
              onResetGame={startNewGame} 
              movesLeft={maxMoves - moves} 
            />
            
            <ActorCard actor={targetActor} role="target" />
          </div>
          
          <PathDisplay path={currentPath} />
          
          <form onSubmit={handleSubmit} className="guess-form">
            <input 
              type="text" 
              placeholder="Actor name" 
              value={actorGuess} 
              onChange={(e) => setActorGuess(e.target.value)} 
            />
            
            {settings.difficulty === 'normal' && (
              <input 
                type="text" 
                placeholder="Movie/Show title" 
                value={mediaGuess} 
                onChange={(e) => setMediaGuess(e.target.value)} 
              />
            )}
            
            <button type="submit">Submit Guess</button>
            {guessError && <p className="error-message">{guessError}</p>}
          </form>
        </div>
      ) : (
        <div className={`game-result ${gameState}`}>
          <h2>{gameState === 'won' ? 'You Win!' : 'Game Over'}</h2>
          <p>
            {gameState === 'won' 
              ? `You connected ${startActor.name} to ${targetActor.name} in ${moves} moves.`
              : `You couldn't connect ${startActor.name} to ${targetActor.name} in ${maxMoves} moves.`}
          </p>
          <GameControls 
            gameState={gameState} 
            onStartGame={startNewGame} 
          />
        </div>
      )}
    </div>
  );
}

export default ActorGame;