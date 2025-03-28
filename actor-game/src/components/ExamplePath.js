import React from 'react';
import '../css/ExamplePath.css';

// Debug mode to visualize placeholders
const DEBUG_MODE = false;

// Update placeholders to always come after real nodes

function createSnakeRows(flatData, nodesPerRow = 5) {
  const rows = [];
  const totalNodes = flatData.length;
  
  // Calculate total rows needed
  const totalRows = Math.ceil(totalNodes / nodesPerRow);
  
  for (let rowIndex = 0; rowIndex < totalRows; rowIndex++) {
    // Start and end indices for this row
    const startIdx = rowIndex * nodesPerRow;
    const endIdx = Math.min(startIdx + nodesPerRow, totalNodes);
    
    // Get nodes for this row
    const chunk = flatData.slice(startIdx, endIdx);
    
    // Determine if this is an odd or even row
    const isOdd = rowIndex % 2 === 0;
    const rowClass = isOdd ? 'odd' : 'even';
    
    // Create row elements array
    let rowElements = [];
    
    // Add the real nodes first
    chunk.forEach((item, index) => {
      // Add the node
      rowElements.push(
        <div className="path-node" key={`node-${startIdx + index}`}>
          <img 
            src={item.image} 
            alt={item.name} 
            className={`${item.type}-image ${item.role || ''}`}
          />
          <div className="node-label">{item.name}</div>
          {item.role && <div className={`node-tag ${item.role}`}>{item.role.toUpperCase()}</div>}
        </div>
      );
      
      // Add a connector after each node except the last one
      if (index < chunk.length - 1) {
        rowElements.push(
          <div className="connector horizontal" key={`connector-${startIdx + index}`}>
            <svg viewBox="0 0 24 24" width="24" height="24">
              <path fill="#555" d="M12 4l-1.41 1.41L16.17 11H4v2h12.17l-5.58 5.59L12 20l8-8-8-8z"/>
            </svg>
          </div>
        );
      }
    });
    
    // Calculate placeholders needed for this row
    const placeholdersNeeded = nodesPerRow - chunk.length;
    
    // Add placeholders AFTER real nodes
    if (placeholdersNeeded > 0) {
      // Add connector between last real node and first placeholder
      if (chunk.length > 0) {
        rowElements.push(
          <div className={`connector horizontal ${DEBUG_MODE ? '' : 'transparent'}`} key={`last-connector-${startIdx}`}>
            <svg viewBox="0 0 24 24" width="24" height="24">
              <path fill="#555" d="M12 4l-1.41 1.41L16.17 11H4v2h12.17l-5.58 5.59L12 20l8-8-8-8z"/>
            </svg>
          </div>
        );
      }
      
      // Add placeholders
      for (let j = 0; j < placeholdersNeeded; j++) {
        rowElements.push(
          <div 
            className={`path-node ${DEBUG_MODE ? 'debug-placeholder' : 'transparent'}`} 
            key={`placeholder-${startIdx + chunk.length + j}`}
          >
            <div style={{ width: 80, height: 120, border: DEBUG_MODE ? '2px dashed #ccc' : 'none' }}></div>
            <div className="node-label">{DEBUG_MODE ? 'Placeholder' : ''}</div>
          </div>
        );
        
        // Add connectors between placeholders except after the last one
        if (j < placeholdersNeeded - 1) {
          rowElements.push(
            <div className={`connector horizontal ${DEBUG_MODE ? '' : 'transparent'}`} key={`placeholder-connector-${startIdx + chunk.length + j}`}>
              <svg viewBox="0 0 24 24" width="24" height="24">
                <path fill="#555" d="M12 4l-1.41 1.41L16.17 11H4v2h12.17l-5.58 5.59L12 20l8-8-8-8z"/>
              </svg>
            </div>
          );
        }
      }
    }
    
    // Create vertical connector and down arrow for all rows except the last
    const isLastRow = rowIndex === totalRows - 1;
    const verticalElements = !isLastRow ? [
      <div className="vertical-connector" key={`vertical-${startIdx}`}></div>,
      <div className="down-arrow" key={`arrow-${startIdx}`}>
        <svg viewBox="0 0 24 24" width="24" height="24">
          <path fill="#555" d="M12 4l-1.41 1.41L16.17 11H4v2h12.17l-5.58 5.59L12 20l8-8-8-8z"/>
        </svg>
      </div>
    ] : [];
    
    // Add the complete row
    rows.push(
      <div className={`path-row ${rowClass}`} key={`row-${startIdx}`}>
        {rowElements}
        {verticalElements}
      </div>
    );
  }
  
  return rows;
}

function ExamplePath() {
  // Define the path data
  const flatData = [
    // Row 1
    {
      type: 'actor',
      name: 'Leonardo DiCaprio',
      image: 'https://image.tmdb.org/t/p/w92/wo2hJpn04vbtmh0B9utCFdsQhxM.jpg',
      role: 'start'
    },
    {
      type: 'movie',
      name: 'The Departed',
      image: 'https://image.tmdb.org/t/p/original/nT97ifVT2J1yMQmeq20Qblg61T.jpg'
    },
    {
      type: 'actor',
      name: 'Matt Damon',
      image: 'https://image.tmdb.org/t/p/w92/https://media.themoviedb.org/t/p/w300_and_h450_bestv2/4KAxONjmVq7qcItdXo38SYtnpul.jpg'
    },
    {
      type: 'movie',
      name: 'The Monuments Men',
      image: 'https://image.tmdb.org/t/p/original/wiWAg4mKV2S4vImPxsPRIdj2R2B.jpg'
    },
    {
      type: 'actor',
      name: 'Bill Murray',
      image: 'https://media.themoviedb.org/t/p/w300_and_h450_bestv2/nnCsJc9x3ZiG3AFyiyc3FPehppy.jpg'
    },
    // Row 2
    {
      type: 'movie',
      name: 'The Grand Budapest Hotel',
      image: 'https://image.tmdb.org/t/p/w92/eWdyYQreja6JGCzqHWXpWHDrrPo.jpg'
    },
    {
      type: 'actor',
      name: 'Ralph Fiennes',
      image: 'https://media.themoviedb.org/t/p/w300_and_h450_bestv2/u29BOqiV5GCQ8k8WUJM50i9xlBf.jpg'
    },
    {
      type: 'movie',
      name: 'Harry Potter and the Goblet of Fire',
      image: 'https://image.tmdb.org/t/p/w92/fECBtHlr0RB3foNHDiCBXeg9Bv9.jpg'
    },
    {
      type: 'actor',
      name: 'David Tennant',
      image: 'https://media.themoviedb.org/t/p/w300_and_h450_bestv2/pQHLJEOEcKpPpyiIheh47AJ5INS.jpg',
      role: 'target'
    }
  ];

  return (
    <div className="example-container">
      <div className="snake-path">
        {createSnakeRows(flatData, 5)}
      </div>
      
      <div className="example-explanation">
        <p>This example shows how you can connect Leonardo DiCaprio to David Tennant:</p>
        <ol>
          <li>Start with <strong>Leonardo DiCaprio</strong></li>
          <li>DiCaprio was in <strong>The Departed</strong> with <strong>Matt Damon</strong></li>
          <li>Damon was in <strong>The Monuments Men</strong> with <strong>Bill Murray</strong></li>
          <li>Murray was in <strong>The Grand Budapest Hotel</strong> with <strong>Ralph Fiennes</strong></li>
          <li>Fiennes was in <strong>Harry Potter and the Goblet of Fire</strong> with <strong>David Tennant</strong> (Fiennes played Lord Voldemort, while Tennant played Barty Crouch Jr.)</li>
        </ol>
        <p>The aim of the game is to find castmates that get you to your target actor!</p>
      </div>
    </div>
  );
}

export default ExamplePath;
