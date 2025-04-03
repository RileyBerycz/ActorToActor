import { useState, useEffect } from 'react';
import initSqlJs from 'sql.js';
import { collection, query, where, limit, getDocs } from 'firebase/firestore';
import GameControls from './GameControls';
import PathDisplay from './PathDisplay';
import '../css/ActorGame.css'; 

function ActorGame({ settings, db }) {
  const [actorData, setActorData] = useState(null);
  const [loading, setLoading] = useState(true);
  const [loadingProgress, setLoadingProgress] = useState(0); // Add loading progress state
  const [error, setError] = useState(null);

  // TMDB API constants
  const BASE_IMG_URL = "https://image.tmdb.org/t/p/";
  const PROFILE_SIZE = "w185";
  const POSTER_SIZE = "w342";

  useEffect(() => {
    async function loadActorData() {
      try {
        setLoading(true);
        setLoadingProgress(10);
        
        // Try loading from Firebase first
        try {
          const actorsRef = collection(db, "actors");
          const q = query(
            actorsRef, 
            where("regions", "array-contains", settings.region),
            limit(500)
          );
          
          setLoadingProgress(30);
          const actorSnapshot = await getDocs(q);
          
          // If we got results from Firebase, use them
          if (!actorSnapshot.empty) {
            const actors = {};
            actorSnapshot.forEach(doc => {
              const data = doc.data();
              actors[doc.id] = {
                name: data.name,
                popularity: data.popularity,
                profile_path: data.profile_path,
                place_of_birth: data.place_of_birth,
                regions: data.regions,
                movie_credits: data.movie_credits,
                tv_credits: data.tv_credits
              };
            });
            setActorData(actors);
            setLoadingProgress(100);
            setLoading(false);
            return; // Success - exit the function
          }
          
          console.log("No data found in Firebase, falling back to SQLite");
        } catch (firebaseError) {
          console.warn("Firebase error, falling back to SQLite:", firebaseError);
        }
        
        // Fallback to SQLite if Firebase failed or returned no results
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
        
        const db = new SQL.Database(new Uint8Array(buffer));
        setLoadingProgress(90);
        
        const actors = {};
        
        const actorsResults = db.exec(`SELECT id, name, popularity, profile_path, place_of_birth FROM actors`);
        
        if (actorsResults.length > 0 && actorsResults[0].values.length > 0) {
          for (const [id, name, popularity, profile_path, place_of_birth] of actorsResults[0].values) {
            const actorId = id.toString();
            
            const regionsResult = db.exec(`SELECT region FROM actor_regions WHERE actor_id = ${id}`);
            const regions = regionsResult[0]?.values.map(row => row[0]) || [];
            
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
                is_mcu: !!is_mcu
              })
            ) || [];
            
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
          
          console.log(`Successfully loaded data with ${Object.keys(actors).length} actors`);
          setActorData(actors);
        } else {
          throw new Error("No actor data found in database");
        }
        
        setLoadingProgress(100);
        setTimeout(() => {
          setLoading(false);
        }, 500);
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
        <div className="loading-text">Loading actor data...</div>
      </div>
    );
  }
  
  if (error) {
    return <div className="error-message">Error: {error}</div>;
  }
  
  return (
    <div className="actor-game">
      <div className="game-controls-container">
        <GameControls /* your props */ />
      </div>
      
      <div className="path-display-container">
        <PathDisplay /* your props */ />
      </div>
    </div>
  );
}

export default ActorGame;