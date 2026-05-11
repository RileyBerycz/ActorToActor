function ExamplePath() {
  const steps = [
    { type: 'actor', name: 'Leonardo DiCaprio', role: 'start' },
    { type: 'movie', name: 'The Departed' },
    { type: 'actor', name: 'Matt Damon' },
    { type: 'movie', name: 'The Monuments Men' },
    { type: 'actor', name: 'Bill Murray' },
    { type: 'movie', name: 'The Grand Budapest Hotel' },
    { type: 'actor', name: 'Ralph Fiennes' },
    { type: 'movie', name: 'Harry Potter and the Goblet of Fire' },
    { type: 'actor', name: 'David Tennant', role: 'target' },
  ];

  return (
    <div className="example-container">
      <div className="example-path">
        {steps.map((step, i) => (
          <div key={i} className="example-step">
            <div className={`example-node ${step.type}${step.role ? ' ' + step.role : ''}`}>
              <div className="example-icon">
                {step.type === 'actor' ? '🎭' : '🎬'}
              </div>
              <div className="example-name">{step.name}</div>
              {step.role && <div className="example-role">{step.role === 'start' ? 'START' : 'TARGET'}</div>}
            </div>
            {i < steps.length - 1 && <div className="example-arrow">→</div>}
          </div>
        ))}
      </div>
    </div>
  );
}

export default ExamplePath;