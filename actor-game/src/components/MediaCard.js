import React from 'react';
import '../css/MediaCard.css';

function MediaCard({ media }) {
  const BASE_IMG_URL = "https://image.tmdb.org/t/p/";
  const POSTER_SIZE = "w342";
  
  // Handle missing media data
  if (!media) {
    return (
      <div className="media-card">
        <div className="media-image">
          <div className="placeholder-poster">?</div>
        </div>
        <div className="media-info">
          <h4>Loading...</h4>
        </div>
      </div>
    );
  }
  
  // Safe access to properties with fallbacks
  const title = media.title || media.name || "Unknown Title";
  const releaseDate = media.release_date || media.first_air_date || "";
  const year = releaseDate ? new Date(releaseDate).getFullYear() : '';
  
  return (
    <div className="media-card">
      <div className="media-image">
        {media.poster_path ? (
          <img 
            src={`${BASE_IMG_URL}${POSTER_SIZE}/${media.poster_path}`} 
            alt={title} 
            onError={(e) => e.target.src = '/placeholder-poster.png'} 
          />
        ) : (
          <div className="placeholder-poster">{title.substring(0, 30)}</div>
        )}
      </div>
      
      <div className="media-info">
        <h4>{title}</h4>
        {year && <span className="year">{year}</span>}
        <span className="media-type">
          {media.type === 'movie' || (!media.media_type && !media.first_air_date) ? 'Movie' : 'TV Show'}
        </span>
      </div>
    </div>
  );
}

export default MediaCard;