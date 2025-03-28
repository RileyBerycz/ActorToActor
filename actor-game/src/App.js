import './App.css';
import { useState } from 'react';
import ActorGame from './components/ActorGame';
import ExamplePath from './components/ExamplePath';

function App() {
  const [gameSettings, setGameSettings] = useState({
    region: 'GLOBAL',
    difficulty: 'normal',
    excludeMcu: false  // New setting for MCU filtering
  });
  
  return (
    <div className="App">
      <header className="App-header">
        <h1>Actor to Actor</h1>
        <p>Connect actors through a chain of shared movie and TV appearances</p>
      </header>
      
      <div className="settings-panel">
        <label>
          Region:
          <select 
            value={gameSettings.region} 
            onChange={(e) => setGameSettings({...gameSettings, region: e.target.value})}
          >
            <option value="GLOBAL">Global</option>
            <option value="US">United States</option>
            <option value="UK">United Kingdom</option>
            <option value="KR">South Korea</option>
            {/* Add other regions */}
          </select>
        </label>
        
        <label>
          Difficulty:
          <select 
            value={gameSettings.difficulty} 
            onChange={(e) => setGameSettings({...gameSettings, difficulty: e.target.value})}
          >
            <option value="easy">Easy (Actor Only)</option>
            <option value="normal">Normal (Actor + Movie)</option>
          </select>
        </label>
        
        <div className="checkbox-container">
          <span className="checkbox-label-text">Exclude MCU Movies</span>
          <input 
            type="checkbox" 
            checked={gameSettings.excludeMcu} 
            onChange={(e) => setGameSettings({...gameSettings, excludeMcu: e.target.checked})}
            id="mcu-checkbox"
          />
          <label htmlFor="mcu-checkbox" className="custom-checkbox"></label>
        </div>
      </div>
      
      <div className="game-instructions">
        <h3>How to Play</h3>
        <p>Starting with one actor, find a path to connect them with another actor through movies and co-stars:</p>
        
        <ExamplePath />
      </div>
      
      <ActorGame settings={gameSettings} />
      
      <footer>
        <p>Data provided by TMDB</p>
      </footer>
    </div>
  );
}

export default App;
