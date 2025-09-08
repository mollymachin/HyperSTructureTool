import React, { useState, useCallback, useEffect } from 'react';
import './TextInputProcessor.css';

interface TextInputProcessorProps {
  onDataLoaded: (data: any) => void;
  onLoadingStart: () => void;
  onLoadingComplete: () => void;
  spatiotemporalFilters?: {
    startTime: string | null;
    endTime: string | null;
    locationNames: string[] | null;
    locationCoordinates: number[][] | null;
    includeSpatiallyUnconstrained: boolean;
  };
  onLoadFilteredDataRef?: (loadFunction: () => void) => void;
}

const TextInputProcessor: React.FC<TextInputProcessorProps> = ({ 
  onDataLoaded, 
  onLoadingStart, 
  onLoadingComplete,
  spatiotemporalFilters, 
  onLoadFilteredDataRef 
}) => {
  const [textInput, setTextInput] = useState('');
  const [isProcessing, setIsProcessing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [realTimeUpdates, setRealTimeUpdates] = useState(true); // Default: ON
  const [progressMessage, setProgressMessage] = useState<string>('');

  // Check if submit button should be enabled
  const canSubmit = textInput.trim().length > 0 && !isProcessing;

  const loadDataFromNeo4j = useCallback(async (loadType: 'all' | 'filtered' = 'all') => {
    try {
      let url = 'http://localhost:8000/api/hyperstructure/data';
      let params = new URLSearchParams();

      if (loadType === 'filtered' && spatiotemporalFilters) {
        if (spatiotemporalFilters.startTime) {
          params.append('start_time', spatiotemporalFilters.startTime);
        }
        if (spatiotemporalFilters.endTime) {
          params.append('end_time', spatiotemporalFilters.endTime);
        }
        if (spatiotemporalFilters.locationNames && spatiotemporalFilters.locationNames.length > 0) {
          params.append('location_names', spatiotemporalFilters.locationNames.join(','));
        }
        if (spatiotemporalFilters.locationCoordinates && spatiotemporalFilters.locationCoordinates.length > 0) {
          params.append('location_coordinates', JSON.stringify(spatiotemporalFilters.locationCoordinates));
        }
        if (spatiotemporalFilters.includeSpatiallyUnconstrained) {
          params.append('include_spatially_unconstrained', 'true');
        }
      }

      if (params.toString()) {
        url += `?${params.toString()}`;
      }
      
      const response = await fetch(url);
      const result = await response.json();

      if (result.status === 'success' && result.hyperstructure_data) {
        onDataLoaded(result.hyperstructure_data);
      } else {
        console.error('Failed to load data from Neo4j:', result.message);
      }
    } catch (err) {
      console.error('Error loading data from Neo4j:', err);
    }
  }, [spatiotemporalFilters, onDataLoaded]);

  const processTextInput = useCallback(async () => {
    if (!textInput.trim() || isProcessing) return;

    setIsProcessing(true);
    setError(null);
    setProgressMessage('Starting text processing pipeline...');
    onLoadingStart();

    try {
      // Send text to backend for processing
      const response = await fetch('http://localhost:8000/api/process-text', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          text: textInput.trim(),
          chunk_size: 3
        }),
      });

      if (!response.ok) {
        throw new Error(`HTTP error! status: ${response.status}`);
      }

      const result = await response.json();
      
      if (result.status === 'success') {
        setProgressMessage(`Text processing completed successfully! Added ${result.facts_processed} facts to the graph.`);
        // Don't immediately load data here - let the auto-refresh handle it
        // This prevents duplicate data loading calls
      } else {
        throw new Error(result.message || 'Text processing failed');
      }
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Unknown error occurred';
      setError(`Failed to process text: ${errorMessage}`);
      console.error('Text processing error:', err);
    } finally {
      // Set processing to false immediately to stop auto-refresh
      setIsProcessing(false);
      onLoadingComplete();
      setProgressMessage('');
      
      // Load the final data once to ensure the visualisation is up to date
      // Use a longer delay to ensure backend has finished processing and auto-refresh has stopped
      setTimeout(() => {
        loadDataFromNeo4j();
      }, 1500); // 1.5 second delay to ensure processing is complete
    }
  }, [textInput, isProcessing, onLoadingStart, onLoadingComplete, loadDataFromNeo4j]);

  // Expose the loadFilteredData function through the ref callback
  const loadFilteredData = useCallback(() => {
    loadDataFromNeo4j('filtered');
  }, [loadDataFromNeo4j]);

  // Use effect to expose the function when component mounts
  useEffect(() => {
    if (onLoadFilteredDataRef) {
      onLoadFilteredDataRef(loadFilteredData);
    }
  }, [onLoadFilteredDataRef, loadFilteredData]);

  // Load initial data when component mounts
  useEffect(() => {
    loadDataFromNeo4j();
  }, [loadDataFromNeo4j]);

  return (
    <div className="text-input-processor-container">
      <h2>Process New Text</h2>
      <p>Enter text to process through the pipeline and add to the graph</p>
      
      {error && (
        <div className="error-message">
          {error}
        </div>
      )}
      
      {progressMessage && (
        <div className="progress-message">
          {progressMessage}
          {realTimeUpdates && isProcessing && (
            <div className="real-time-indicator">
              🔄 Auto-refreshing graph every second...
            </div>
          )}
        </div>
      )}
      
      <div className="text-input-section">
        <textarea
          value={textInput}
          onChange={(e) => setTextInput(e.target.value)}
          placeholder="Enter your text here... (will be split into chunks of ~3 sentences)"
          className="text-input-area"
          rows={6}
          disabled={isProcessing}
        />
      </div>
      
      <div className="controls-section">
        <div className="real-time-toggle">
          <label className="toggle-label">
            <button
              type="button"
              onClick={() => setRealTimeUpdates(!realTimeUpdates)}
              className={`real-time-button ${realTimeUpdates ? '' : 'off'}`}
            >
              {realTimeUpdates ? 'ON' : 'OFF'}
            </button>
            Real-time updates
          </label>
          <span className="toggle-description">
            {realTimeUpdates ? 'ON' : 'OFF'} - Graph will automatically refresh every second while processing
          </span>
        </div>
        
        <button 
          onClick={processTextInput}
          disabled={!canSubmit}
          className={`submit-button ${!canSubmit ? 'disabled' : ''}`}
        >
          {isProcessing ? 'Processing...' : 'Process Text'}
        </button>
      </div>
    </div>
  );
};

export default TextInputProcessor;
