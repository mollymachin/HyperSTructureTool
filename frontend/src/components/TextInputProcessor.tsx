import React, { useState, useCallback, useEffect, useRef } from 'react';
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
  const [progressVisible, setProgressVisible] = useState<boolean>(false);
  const [currentProgress, setCurrentProgress] = useState<string>('');
  const [progressQueue, setProgressQueue] = useState<string[]>([]);
  const eventSourceRef = useRef<EventSource | null>(null);
  const drainingRef = useRef<boolean>(false);
  const drainTimerRef = useRef<number | null>(null);

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
    setProgressVisible(true);
    setCurrentProgress('Starting text processing pipeline...');
    setProgressQueue([]);
    onLoadingStart();

    // Helper to enqueue and start draining if idle
    const enqueue = (msg: string) => {
      setProgressQueue(prev => {
        const next = [...prev, msg];
        // Start draining if not already
        if (!drainingRef.current) {
          drainingRef.current = true;
          const drainOnce = () => {
            setProgressQueue(curr => {
              if (curr.length === 0) {
                drainingRef.current = false;
                if (drainTimerRef.current) {
                  window.clearTimeout(drainTimerRef.current);
                  drainTimerRef.current = null;
                }
                return curr;
              }
              const [head, ...rest] = curr;
              setCurrentProgress(head);
              // Schedule next pop after 1s
              if (drainTimerRef.current) {
                window.clearTimeout(drainTimerRef.current);
              }
              drainTimerRef.current = window.setTimeout(drainOnce, 1000);
              return rest;
            });
          };
          // If no current message, kick off immediately, otherwise wait 1s
          if (!currentProgress) {
            drainOnce();
          } else {
            drainTimerRef.current = window.setTimeout(drainOnce, 1000);
          }
        }
        return next;
      });
    };

    try {
      // Open SSE stream
      const url = `http://localhost:8000/api/process-text/stream?text=${encodeURIComponent(textInput.trim())}&chunk_size=3`;
      const es = new EventSource(url);
      eventSourceRef.current = es;

      es.onmessage = (ev: MessageEvent) => {
        try {
          const data = JSON.parse(ev.data || '{}');
          const type = String(data.type || 'info');
          const msg = String(data.message || '');
          if (type === 'error') {
            setError(msg || 'An error occurred during processing');
          }
          if (type === 'complete') {
            if (msg) enqueue(msg);
            es.close();
            eventSourceRef.current = null;
            setIsProcessing(false);
            onLoadingComplete();
            // Load final data after a short delay to ensure completion
            setTimeout(() => {
              loadDataFromNeo4j();
              // Hide popup a moment later if no more messages
              setTimeout(() => {
                setProgressVisible(false);
                setCurrentProgress('');
                setProgressQueue([]);
              }, 800);
            }, 1200);
            return;
          }
          if (msg) {
            setProgressVisible(true);
            enqueue(msg);
          }
        } catch (e) {
          // Non-JSON or unexpected message
          const fallback = (ev.data || '').toString();
          if (fallback) {
            setProgressVisible(true);
            enqueue(fallback);
          }
        }
      };

      es.onerror = (e: any) => {
        console.error('SSE error:', e);
        setError('Connection lost while streaming progress.');
        try { es.close(); } catch {}
        eventSourceRef.current = null;
        setIsProcessing(false);
        onLoadingComplete();
      };
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'Unknown error occurred';
      setError(`Failed to start streaming: ${errorMessage}`);
      console.error('Text processing stream error:', err);
      setIsProcessing(false);
      onLoadingComplete();
    }
  }, [textInput, isProcessing, onLoadingStart, onLoadingComplete, loadDataFromNeo4j, currentProgress]);

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

  // Cleanup on unmount
  useEffect(() => {
    return () => {
      if (eventSourceRef.current) {
        try { eventSourceRef.current.close(); } catch {}
        eventSourceRef.current = null;
      }
      if (drainTimerRef.current) {
        window.clearTimeout(drainTimerRef.current);
        drainTimerRef.current = null;
      }
    };
  }, []);

  return (
    <div className="text-input-processor-container">
      <h2 className="subheader">Process New Text</h2>
      <p>Enter text to process through the pipeline and add to the graph</p>
      
      {error && (
        <div className="error-message">
          {error}
        </div>
      )}
      
      <div className="text-input-section">
        <textarea
          value={textInput}
          onChange={(e) => setTextInput(e.target.value)}
          placeholder="Enter your text here..."
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
              {`Real-Time Updates: ${realTimeUpdates ? 'ON' : 'OFF'}`}
            </button>
          </label>
        </div>
        <div className="submit-actions">
          <button 
            onClick={processTextInput}
            disabled={!canSubmit}
            className={`submit-button ${!canSubmit ? 'disabled' : ''}`}
          >
            {isProcessing ? 'Processing...' : 'Process Text'}
          </button>

          {textInput.trim().length > 0 && (
            <button
              type="button"
              onClick={() => setTextInput('')}
              disabled={isProcessing}
              className="clear-text-button"
              title="Clear the text input"
            >
              Clear Text
            </button>
          )}
        </div>

        {progressVisible && (currentProgress || progressQueue.length > 0) && (
          <div className="progress-popup">
            <div className="progress-item">{currentProgress}</div>
          </div>
        )}
      </div>
    </div>
  );
};

export default TextInputProcessor;
