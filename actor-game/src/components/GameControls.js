import React from 'react';
import '../css/GameControls.css';

function GameControls({ gameState, onStartGame, onResetGame, movesLeft }) {
  return (
    <div className="game-controls">
      {gameState === 'setup' && (
        <button className="start-button" onClick={onStartGame}>
          Start New Game
        </button>
      )}
      
      {gameState === 'playing' && (
        <div className="playing-controls">
          <div className="moves-counter">
            <span>Moves left: {movesLeft}</span>
          </div>
          <button className="hint-button" onClick={() => console.log('Hint requested')}>
            Get Hint
          </button>
          <button className="reset-button" onClick={onResetGame}>
            Reset Game
          </button>
        </div>
      )}
      
      {(gameState === 'won' || gameState === 'lost') && (
        <button className="play-again-button" onClick={onStartGame}>
          Play Again
        </button>
      )}
    </div>
  );
}

export default GameControls;