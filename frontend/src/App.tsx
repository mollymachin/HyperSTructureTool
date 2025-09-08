import React, { useState, useCallback, useEffect, useRef } from 'react';
import './App.css';
import HyperstructureVisualisation from './components/HyperstructureVisualisation';
import TextInputProcessor from './components/TextInputProcessor';
import SpatiotemporalInput from './components/TemporalInput';
import MapboxMap, { type MapboxMapHandle } from './components/MapboxMap';
import QuestionBox from './components/QuestionBox';

function App() {
  const [hyperstructureData, setHyperstructureData] = useState<any>(null);
  const [isProcessing, setIsProcessing] = useState(false);
  const [spatialData, setSpatialData] = useState<any[]>([]);
  const [containmentMode, setContainmentMode] = useState<'overlap' | 'contained'>('overlap');
  const mapRef = useRef<MapboxMapHandle | null>(null);
  const [spatiotemporalFilters, setSpatiotemporalFilters] = useState<{
    startTime: string | null;
    endTime: string | null;
    locationNames: string[] | null;
    locationCoordinates: number[][] | null;
    includeSpatiallyUnconstrained: boolean;
  }>({
    startTime: null,
    endTime: null,
    locationNames: null,
    locationCoordinates: null,
    includeSpatiallyUnconstrained: false
  });

  // Ref for the auto-refresh interval
  const autoRefreshIntervalRef = useRef<NodeJS.Timeout | null>(null);
  // Ref to store the last update time to prevent rapid successive updates
  const lastUpdateTimeRef = useRef<number>(0);
  // Ref to debounce API calls
  const debounceTimeoutRef = useRef<NodeJS.Timeout | null>(null);

  const handleDataLoaded = useCallback((data: any) => {
    setHyperstructureData(data);
    setIsProcessing(false);
  }, []);

  const handleLoadingStart = useCallback(() => {
    setIsProcessing(true);
    // DON'T clear hyperstructureData here - keep showing the current graph
    // setHyperstructureData(null); // This was causing the graph to disappear
  }, []);

  const handleLoadingComplete = useCallback(() => {
    setIsProcessing(false);
  }, []);

  const handleSpatiotemporalChange = useCallback((
    startTime: string | null, 
    endTime: string | null,
    locationNames: string[] | null,
    locationCoordinates: number[][] | null,
    includeSpatiallyUnconstrained: boolean
  ) => {
    setSpatiotemporalFilters({ 
      startTime, 
      endTime, 
      locationNames, 
      locationCoordinates, 
      includeSpatiallyUnconstrained 
    });
  }, []);

  const [loadFilteredDataFunction, setLoadFilteredDataFunction] = useState<(() => void) | null>(null);

  const handleLoadFilteredDataRef = useCallback((loadFunction: () => void) => {
    setLoadFilteredDataFunction(() => loadFunction);
  }, []);

  const handleLoadFilteredData = useCallback(() => {
    if (loadFilteredDataFunction) {
      loadFilteredDataFunction();
    }
  }, [loadFilteredDataFunction]);

  // Function to load data from Neo4j
  const loadDataFromNeo4j = useCallback(async () => {
    // Prevent multiple simultaneous calls
    if (autoRefreshIntervalRef.current === null) {
      return; // Don't load if auto-refresh is not active
    }
    
    try {
      let url = 'http://localhost:8000/api/hyperstructure/data';
      let params = new URLSearchParams();

      if (spatiotemporalFilters.startTime || spatiotemporalFilters.endTime || 
          spatiotemporalFilters.locationNames || spatiotemporalFilters.locationCoordinates) {
        // Apply spatiotemporal filters
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
        // Only update if data has actually changed
        const currentData = hyperstructureData;
        const newData = result.hyperstructure_data;
        
        // More sophisticated change detection
        const hasChanged = !currentData || 
            currentData.hyperedge_count !== newData.hyperedge_count ||
            currentData.entities?.length !== newData.entities?.length ||
            currentData.hyperedges?.length !== newData.hyperedges?.length ||
            // Check if any specific hyperedges have changed
            (currentData.hyperedges && newData.hyperedges && 
             JSON.stringify(currentData.hyperedges.map((h: any) => h.id || `${h.subjects?.join('_')}_${h.relation_type}_${h.objects?.join('_')}`).sort()) !== 
             JSON.stringify(newData.hyperedges.map((h: any) => h.id || `${h.subjects?.join('_')}_${h.relation_type}_${h.objects?.join('_')}`).sort()));
        
        if (hasChanged) {
          console.log('Data changed, updating visualisation');
          setHyperstructureData(newData);
          // Update the last update time to prevent rapid successive updates
          lastUpdateTimeRef.current = Date.now();
        } else {
          console.log('Data unchanged, skipping visualisation update');
        }
      } else {
        console.error('Failed to load data from Neo4j:', result.message);
      }
    } catch (error) {
      console.error('Error loading data from Neo4j:', error);
    }
  }, [spatiotemporalFilters, hyperstructureData]);

  // Debounced version of loadDataFromNeo4j to prevent rapid successive calls
  const debouncedLoadData = useCallback(() => {
    // Clear any existing debounce timeout
    if (debounceTimeoutRef.current) {
      clearTimeout(debounceTimeoutRef.current);
    }
    
    // Set a new debounce timeout
    debounceTimeoutRef.current = setTimeout(() => {
      loadDataFromNeo4j();
    }, 200); // 200ms debounce delay
  }, [loadDataFromNeo4j]);

  // Auto-refresh logic: poll every 1 second when processing and real-time updates are enabled
  useEffect(() => {
    // Clear any existing interval
    if (autoRefreshIntervalRef.current) {
      clearInterval(autoRefreshIntervalRef.current);
      autoRefreshIntervalRef.current = null;
    }

    // Start auto-refresh if processing
    if (isProcessing) {
      console.log('Starting auto-refresh: polling every 1 second');
      
      // Set initial last update time
      lastUpdateTimeRef.current = Date.now();
      
      autoRefreshIntervalRef.current = setInterval(() => {
        const now = Date.now();
        const timeSinceLastUpdate = now - lastUpdateTimeRef.current;
        
        // Ensure at least 1 second has passed since last update
        if (timeSinceLastUpdate >= 1000) {
          console.log('Auto-refresh: loading updated data from Neo4j');
          
          // Use debounced function to prevent rapid successive calls
          debouncedLoadData();
        } else {
          console.log(`Auto-refresh: skipping update (only ${timeSinceLastUpdate}ms since last update)`);
        }
      }, 1000);
    }

    // Cleanup function
    return () => {
      if (autoRefreshIntervalRef.current) {
        clearInterval(autoRefreshIntervalRef.current);
        autoRefreshIntervalRef.current = null;
      }
      // Clear any pending debounce timeout
      if (debounceTimeoutRef.current) {
        clearTimeout(debounceTimeoutRef.current);
        debounceTimeoutRef.current = null;
      }
    };
  }, [isProcessing, debouncedLoadData]);

  // Load initial data when component mounts
  useEffect(() => {
    loadDataFromNeo4j();
  }, [loadDataFromNeo4j]);

  const loadAllExtractedHyperedges = useCallback(async () => {
    try {
      const response = await fetch('http://localhost:8000/api/hyperedge/extract_structured_data', {
        method: 'GET',
        headers: {
          'Content-Type': 'application/json',
        },
      });

      const result = await response.json();
      
      if (result.status === 'success') {
        // Extract all spatial contexts from all hyperedges
        const allSpatialData = [];
        
        for (const hyperedge of result.hyperedges) {
          const spatialContexts = hyperedge.spatial_contexts || [];
          
          for (const spatialCtx of spatialContexts) {
            allSpatialData.push({
              type: spatialCtx.type,
              name: spatialCtx.name,
              coordinates: spatialCtx.coordinates,
              hyperedge_id: `${hyperedge.subjects.join('_')}_${hyperedge.relation_type}_${hyperedge.objects.join('_')}`
            });
          }
        }
        
        setSpatialData(allSpatialData);
      } else {
        console.error('Failed to load extracted hyperedges:', result.message);
      }
    } catch (error) {
      console.error('Error loading extracted hyperedges:', error);
    }
  }, []);

  const hasActiveFilters = Boolean(spatiotemporalFilters && (
    spatiotemporalFilters.startTime || 
    spatiotemporalFilters.endTime || 
    (spatiotemporalFilters.locationNames && spatiotemporalFilters.locationNames.length > 0) || 
    (spatiotemporalFilters.locationCoordinates && spatiotemporalFilters.locationCoordinates.length > 0)
  ));

  const clearHyperstructure = useCallback(async () => {
    if (!window.confirm('Are you sure you want to clear all hyperstructure data? This action cannot be undone.')) {
      return;
    }

    try {
      const response = await fetch('http://localhost:8000/api/hyperstructure/clear', {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
      });

      const result = await response.json();
      
      if (result.status === 'success') {
        console.log('Successfully cleared hyperstructure data');
        // Clear the local state
        setHyperstructureData(null);
        setSpatialData([]);
        alert('Hyperstructure data cleared successfully!');
      } else {
        console.error('Failed to clear hyperstructure data:', result.message);
        alert(`Failed to clear hyperstructure data: ${result.message}`);
      }
    } catch (error) {
      console.error('Error clearing hyperstructure data:', error);
      alert('Error clearing hyperstructure data. Please try again.');
    }
  }, []);

  return (
    <div className="App">
      <header className="App-header">
        <h1>HyperSTructure Interaction Tool</h1>
        <p>Create, visualise, interact with and query hyper-Spatio-Temporal-structures!</p>
        {(spatiotemporalFilters.startTime || spatiotemporalFilters.endTime || 
          spatiotemporalFilters.locationNames || spatiotemporalFilters.locationCoordinates) ? (
          <p style={{fontSize: '12px', opacity: 0.7}}>
            Filters: {spatiotemporalFilters.startTime || 'No start'} to {spatiotemporalFilters.endTime || 'No end'}
            {spatiotemporalFilters.locationNames ? ` | Names: ${spatiotemporalFilters.locationNames.join(', ')}` : ''}
            {spatiotemporalFilters.locationCoordinates ? ` | Polygon area` : ''}
            {spatiotemporalFilters.includeSpatiallyUnconstrained ? ' (including unconstrained)' : ''}
          </p>
        ) : null}
      </header>
      
      <main className="App-main">
        <div className="input-section">
          <TextInputProcessor 
            onDataLoaded={handleDataLoaded}
            onLoadingStart={handleLoadingStart}
            onLoadingComplete={handleLoadingComplete}
            spatiotemporalFilters={spatiotemporalFilters}
            onLoadFilteredDataRef={handleLoadFilteredDataRef}
          />
        </div>
        
        <div className="visualisation-section">
          <div style={{ display: 'flex', justifyContent: 'space-between', alignItems: 'center', marginBottom: '10px' }}>
            <h2>HyperSTructure Visualisation</h2>
            {hyperstructureData && (
              <button 
                onClick={clearHyperstructure}
                className="clear-button"
              >
                Clear Data
              </button>
            )}
          </div>
          <HyperstructureVisualisation 
            data={hyperstructureData} 
            isProcessing={isProcessing} 
          />
        </div>
        
        <div className="temporal-section">
          <SpatiotemporalInput 
            startTime={spatiotemporalFilters.startTime || ''}
            endTime={spatiotemporalFilters.endTime || ''}
            locationNames={spatiotemporalFilters.locationNames}
            locationCoordinates={spatiotemporalFilters.locationCoordinates}
            includeSpatiallyUnconstrained={spatiotemporalFilters.includeSpatiallyUnconstrained}
            onSpatiotemporalChange={handleSpatiotemporalChange}
            onLoadFilteredData={handleLoadFilteredData}
            isLoading={isProcessing}
            hasActiveFilters={hasActiveFilters}
          />
        </div>
        
        <div className="map-section">
          <h2>Interactive Map</h2>
          <div style={{ marginBottom: '10px', display: 'flex', gap: '10px', flexWrap: 'wrap' }}>
            <button 
              onClick={loadAllExtractedHyperedges}
              style={{ padding: '8px 16px', backgroundColor: '#007bff', color: 'white', border: 'none', borderRadius: '4px', cursor: 'pointer' }}
            >
              Load All Extracted Hyperedges
            </button>
            <button 
              onClick={clearHyperstructure}
              className="clear-button"
            >
              Clear Hyperstructure
            </button>
            <button
              onClick={() => mapRef.current?.startDrawing()}
              className="clear-button"
              style={{ backgroundColor: '#6c757d' }}
            >
              Draw area
            </button>
            <button
              onClick={() => mapRef.current?.toggleContainmentMode()}
              className="clear-button"
              style={{ backgroundColor: '#6c757d' }}
            >
              Mode: {containmentMode === 'overlap' ? 'Overlap' : 'Contained'}
            </button>
            <button
              onClick={() => mapRef.current?.clearSelection()}
              className="clear-button"
              style={{ backgroundColor: '#6c757d' }}
            >
              Clear selection
            </button>
          </div>
          <div className="map-and-question-row">
            <div className="map-column">
              <MapboxMap
                ref={mapRef}
                spatialData={spatialData}
                showInternalControls={false}
                onContainmentModeChange={(m) => setContainmentMode(m)}
              />
            </div>
            <div className="question-column">
              <QuestionBox onSubmit={(q) => console.log('Question submitted:', q)} />
            </div>
          </div>
        </div>
      </main>
    </div>
  );
}

export default App;