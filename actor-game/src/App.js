import './App.css';
import { useState, useEffect } from 'react';
import ActorGame from './components/ActorGame';
import ExamplePath from './components/ExamplePath';

function App() {
  const [gameSettings, setGameSettings] = useState({
    region: 'GLOBAL', // Default, will be updated based on location
    difficulty: 'normal',
    excludeMcu: false
  });
  
  const [gameStarted, setGameStarted] = useState(false);
  const [isLoading, setIsLoading] = useState(false);
  
  // Detect user's region on app load
  useEffect(() => {
    async function detectUserRegion() {
      try {
        // Use a free IP geolocation API
        const response = await fetch('https://ipapi.co/json/');
        const data = await response.json();
        
        // Log what we're getting back for debugging
        console.log("API returned country code:", data.country_code);
        
        // Map country code to your regions with GB → UK conversion
        let countryCode = data.country_code;
        
        // Special handling for UK (which comes as GB)
        if (countryCode === 'GB') {
          countryCode = 'UK';
        }
        
        let detectedRegion = 'OTHER';
        
        // Check against your available regions
        const availableRegions = ['US', 'UK', 'CA', 'AU', 'KR', 'CN', 'JP', 'IN', 'FR', 'DE'];
        
        if (availableRegions.includes(countryCode)) {
          detectedRegion = countryCode;
        }
        
        console.log("Setting region to:", detectedRegion);
        
        // Update the game settings with detected region
        setGameSettings(prev => ({
          ...prev,
          region: detectedRegion
        }));
        
      } catch (error) {
        console.error("Error detecting region:", error);
        // Keep default region on error
      }
    }
    
    detectUserRegion();
  }, []);
  
  // Add this function to wrap the game start action
  const handleStartGame = () => {
    setIsLoading(true);  // Set loading to true first
    setGameStarted(true); // Then start the game
  };
  
  return (
    <div className="App">
      <header className="App-header">
        <h1>Actor to Actor</h1>
        <p>Connect actors through a chain of shared movie and TV appearances</p>
      </header>
      
      {/* Settings panel - always visible */}
      <div className="settings-panel">
        <label className="region-selector">
          Region:
          <select 
            value={gameSettings.region} 
            onChange={(e) => setGameSettings({...gameSettings, region: e.target.value})}
            disabled={gameStarted}
            className={gameSettings.region === 'GLOBAL' ? 'global-selected' : ''}
          >
            <option value="GLOBAL">Global (Very Difficult)</option>
            <option value="US">United States</option>
            <option value="UK">United Kingdom</option>
            <option value="CA">Canada</option>
            <option value="AU">Australia</option>
            <option value="KR">South Korea</option>
            <option value="CN">China</option>
            <option value="JP">Japan</option>
            <option value="IN">India</option>
            <option value="FR">France</option>
            <option value="DE">Germany</option>
            <option value="OTHER">Other Regions</option>
          </select>
        </label>
        
        <label>
          Difficulty:
          <select 
            value={gameSettings.difficulty} 
            onChange={(e) => setGameSettings({...gameSettings, difficulty: e.target.value})}
            disabled={gameStarted}
          >
            <option value="easy">Easy (Actor Only)</option>
            <option value="normal">Normal (Actor + Movie)</option>
            <option value="hard">Hard (Actor + Movie or TV Show)</option>
          </select>
        </label>
        
        <div className="checkbox-container">
          <span className="checkbox-label-text">Exclude MCU Movies</span>
          <input 
            type="checkbox" 
            checked={gameSettings.excludeMcu} 
            onChange={(e) => setGameSettings({...gameSettings, excludeMcu: e.target.checked})}
            id="mcu-checkbox"
            disabled={gameStarted || gameSettings.difficulty === 'hard'}
          />
          <label htmlFor="mcu-checkbox" className="custom-checkbox"></label>
        </div>
      </div>
      
      {/* Start button or active game */}
      {!gameStarted ? (
        <div className="start-game-container">
          <button 
            className="start-game-button"
            onClick={handleStartGame}
          >
            Start Game
          </button>
        </div>
      ) : (
        <div className="active-game-container">
          <ActorGame 
            settings={gameSettings} 
            onReset={() => {
              setGameStarted(false);
              setIsLoading(false);
            }}
            initialLoading={true} // Always start with loading true
          />
        </div>
      )}
      
      {/* Game instructions - only show when game is not started */}
      {!gameStarted && (
        <div className="game-instructions">
          <h3>How to Play</h3>
          <p>Starting with one actor, find a path to connect them with another actor through movies and co-stars:</p>
          
          <ExamplePath />
          
          <div className="instructions-detail">
            <p>This example shows how you can connect Leonardo DiCaprio to David Tennant:</p>
            <ol>
              <li>Start with Leonardo DiCaprio</li>
              <li>DiCaprio was in The Departed with Matt Damon</li>
              <li>Damon was in The Monuments Men with Bill Murray</li>
              <li>Murray was in The Grand Budapest Hotel with Ralph Fiennes</li>
              <li>Fiennes was in Harry Potter and the Goblet of Fire with David Tennant (Fiennes played Lord Voldemort, while Tennant played Barty Crouch Jr.)</li>
            </ol>
            <p>The aim of the game is to find castmates that get you to your target actor!</p>
          </div>
        </div>
      )}
      
      <footer>
        <p>Data provided by TMDB</p>
      </footer>
    </div>
  );
}

export default App;
