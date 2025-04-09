import React from 'react';
import '../css/ActorCard.css';

function ActorCard({ actor, role }) {
  const BASE_IMG_URL = "https://image.tmdb.org/t/p/";
  const PROFILE_SIZE = "w185";
  
  // Handle case where actor or actor.name might be undefined
  if (!actor) {
    return (
      <div className={`actor-card ${role || ''}`}>
        <div className="actor-image">
          <div className="placeholder-image">?</div>
        </div>
        <div className="actor-info">
          <h3>Loading...</h3>
        </div>
      </div>
    );
  }
  
  // Safely access actor name
  const actorName = actor.name || 'Unknown Actor';
  
  return (
    <div className={`actor-card ${role || ''}`}>
      <div className="actor-image">
        {actor.profile_path ? (
          <img 
            src={`${BASE_IMG_URL}${PROFILE_SIZE}/${actor.profile_path}`} 
            alt={actorName} 
            onError={(e) => e.target.src = '/placeholder-actor.png'} 
          />
        ) : (
          <div className="placeholder-image">{actorName.charAt(0)}</div>
        )}
      </div>
      
      <div className="actor-info">
        <h3>{actorName}</h3>
        {role && <span className="role-badge">{role}</span>}
        {actor.is_mcu && <span className="mcu-badge">MCU</span>}
      </div>
    </div>
  );
}

export default ActorCard;