import './App.css';
import { useState, useEffect, useCallback } from 'react';
import ActorGame from './components/ActorGame';

const API_BASE = process.env.NODE_ENV === 'development' ? 'http://localhost:5000/api' : '/api';

function App() {
  const [gameSettings, setGameSettings] = useState({ difficulty: 'normal', excludeMcu: false });
  const [gameStarted, setGameStarted] = useState(false);
  const [dailyConnection, setDailyConnection] = useState(null);
  const [dailyLoading, setDailyLoading] = useState(true);
  const [gameMode, setGameMode] = useState(null);

  useEffect(() => {
    fetch(`${API_BASE}/daily-connection`)
      .then(r => r.json())
      .then(data => { setDailyConnection(data); setDailyLoading(false); })
      .catch(() => setDailyLoading(false));
  }, []);

  const handleStartGame = (mode) => {
    setGameMode(mode);
    setGameStarted(true);
  };

  const handleReset = () => {
    setGameStarted(false);
    setGameMode(null);
  };

  if (gameStarted) {
    return (
      <div className="App">
        <ActorGame
          settings={gameSettings}
          onReset={handleReset}
          initialLoading={true}
          gameMode={gameMode}
          dailyConnection={gameMode === 'daily' ? dailyConnection : null}
        />
      </div>
    );
  }

  return (
    <div className="App">
      <header className="App-header">
        <h1>Actor to Actor</h1>
        <p>Connect actors through a chain of shared movie and TV appearances</p>
      </header>

      {/* Daily Connection Hero */}
      <div className="daily-hero" onClick={() => dailyConnection?.available ? handleStartGame('daily') : null}
           style={{ cursor: dailyConnection?.available ? 'pointer' : 'default' }}>
        <div className="daily-badge">
          {dailyLoading ? 'Loading...' : (dailyConnection?.available ? "Today's Connection" : 'Free Play')}
        </div>
        {dailyConnection?.available ? (
          <div className="daily-pair">
            <div className="daily-actor">
              <span className="daily-name">{dailyConnection.start_actor?.name}</span>
            </div>
            <div className="daily-vs">vs</div>
            <div className="daily-actor">
              <span className="daily-name">{dailyConnection.target_actor?.name}</span>
            </div>
          </div>
        ) : (
          <div className="daily-pair">
            <p style={{ color: '#94a3b8', fontSize: '0.95em' }}>
              {dailyLoading ? "Checking for today's puzzle..." : 'No daily puzzle set yet! Play random games instead.'}
            </p>
          </div>
        )}
        {dailyConnection?.available && <div className="daily-cta">Tap to play →</div>}
      </div>

      {/* Play Random */}
      <div className="random-section">
        <div className="settings-panel">
          <label>Difficulty:</label>
          <select value={gameSettings.difficulty}
            onChange={e => setGameSettings({ ...gameSettings, difficulty: e.target.value })}>
            <option value="easy">Easy</option>
            <option value="normal">Normal</option>
            <option value="hard">Hard</option>
          </select>
          <div className="checkbox-container">
            <span>Exclude MCU</span>
            <input type="checkbox" checked={gameSettings.excludeMcu}
              onChange={e => setGameSettings({ ...gameSettings, excludeMcu: e.target.checked })} id="mcu-cb" />
            <label htmlFor="mcu-cb" className="custom-checkbox"></label>
          </div>
        </div>
        <button className="start-game-button" onClick={() => handleStartGame('random')}>
          Play Random Game
        </button>
      </div>

      {/* How to Play */}
      <div className="game-instructions">
        <h3>How to Play</h3>
        <p>Starting with one actor, find a path to connect them with another actor through shared movies:</p>
        <ol>
          <li>You're given a <strong>start actor</strong> and a <strong>target actor</strong></li>
          <li>Pick a movie starring your current actor</li>
          <li>Choose a co-star from that movie as your next actor</li>
          <li>Repeat until you reach the target actor!</li>
        </ol>
        <p className="tip">If the target actor appears in a movie you select, the connection auto-completes!</p>
      </div>

      <footer>
        <p>Data provided by TMDB</p>
      </footer>
    </div>
  );
}

export default App;
