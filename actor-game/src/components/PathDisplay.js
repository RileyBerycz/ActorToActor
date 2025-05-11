import React from 'react';
import '../css/PathDisplay.css';
import ActorCard from './ActorCard';
import MediaCard from './MediaCard';

function PathDisplay({ path }) {
  return (
    <div className="path-display">
      {path.length === 0 ? (
        <div className="empty-path">
          <p>Start guessing to build your path!</p>
        </div>
      ) : (
        <div className="path-nodes">
          {path.map((node, index) => (
            <React.Fragment key={index}>
              {/* Render nodes with proper type checking */}
              {node.type === 'actor' ? (
                <div className="path-node actor-node">
                  <ActorCard actor={node} />
                </div>
              ) : (
                <div className="path-node media-node">
                  <MediaCard media={node} />
                </div>
              )}
              
              {/* Add connecting arrow except after the last item */}
              {index < path.length - 1 && (
                <div className="path-connector">
                  <i className="arrow-icon">→</i>
                </div>
              )}
            </React.Fragment>
          ))}
        </div>
      )}
    </div>
  );
}

export default PathDisplay;