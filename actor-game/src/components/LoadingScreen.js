import React from 'react';
import '../css/LoadingScreen.css';

function LoadingScreen({ progress }) {
  return (
    <div className="loading-screen">
      <div className="loading-content">
        <h2>Actor to Actor</h2>
        <div className="loading-animation">
          <div className="actor-icon left"></div>
          <div className="loading-dots">
            <span></span>
            <span></span>
            <span></span>
          </div>
          <div className="actor-icon right"></div>
        </div>
        <div className="loading-progress-bar">
          <div 
            className="loading-progress-fill" 
            style={{ width: `${progress || 0}%` }}
          ></div>
        </div>
        <p>Loading actor data... {progress ? `${Math.round(progress)}%` : ''}</p>
      </div>
    </div>
  );
}

export default LoadingScreen;