import React, { useState, useCallback, useEffect } from 'react';
import './DataLoader.css';
const API_BASE_URL = (process.env.REACT_APP_API_BASE_URL || 'http://localhost:8000').replace(/\/+$/, '');

interface DataLoaderProps {
  onDataLoaded: (data: any) => void;
  onLoadingStart: () => void;
  spatiotemporalFilters?: {
    startTime: string | null;
    endTime: string | null;
    locationNames: string[] | null;
    locationCoordinates: number[][] | null;
    includeSpatiallyUnconstrained: boolean;
  };
  onLoadFilteredDataRef?: (loadFunction: () => void) => void;
}

const DataLoader: React.FC<DataLoaderProps> = ({ onDataLoaded, onLoadingStart, spatiotemporalFilters, onLoadFilteredDataRef }) => {
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [lastLoadType, setLastLoadType] = useState<'all' | 'filtered' | null>(null);

  const loadDataFromNeo4j = useCallback(async (loadType: 'all' | 'filtered') => {
    setIsLoading(true);
    setError(null);
    onLoadingStart();
    setLastLoadType(loadType);

    try {
      let url = `${API_BASE_URL}/api/hyperstructure/data`;
      let params = new URLSearchParams();

      if (loadType === 'filtered') {
        // Build query parameters for spatiotemporal filtering
        if (spatiotemporalFilters?.startTime) {
          params.append('start_time', spatiotemporalFilters.startTime);
        }
        if (spatiotemporalFilters?.endTime) {
          params.append('end_time', spatiotemporalFilters.endTime);
        }
        if (spatiotemporalFilters?.locationNames && spatiotemporalFilters.locationNames.length > 0) {
          params.append('location_names', spatiotemporalFilters.locationNames.join(','));
        }
        if (spatiotemporalFilters?.locationCoordinates && spatiotemporalFilters.locationCoordinates.length > 0) {
          params.append('location_coordinates', JSON.stringify(spatiotemporalFilters.locationCoordinates));
        }
        if (spatiotemporalFilters?.includeSpatiallyUnconstrained) {
          params.append('include_spatially_unconstrained', 'true');
        }
      }
      // If loadType === 'all', no parameters are added, so all data is loaded

      if (params.toString()) {
        url += `?${params.toString()}`;
      }
      
      console.log(`Loading ${loadType} data from:`, url);
      
      const response = await fetch(url);
      const result = await response.json();

      if (result.status === 'success' && result.hyperstructure_data) {
        onDataLoaded(result.hyperstructure_data);
      } else {
        setError(result.message || `Failed to load ${loadType} data from Neo4j`);
      }
    } catch (err) {
      setError(`Failed to connect to backend server. Check API base URL setting.`);
    } finally {
      setIsLoading(false);
    }
  }, [spatiotemporalFilters, onDataLoaded, onLoadingStart]);

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

  return (
    <div className="data-loader-container">
      <h2>Load Hyperstructure Data</h2>
      <p>Load existing hyperstructure data from Neo4j database</p>
      
      {error && (
        <div className="error-message">
          {error}
        </div>
      )}
      
      <div className="button-group">
        <button 
          onClick={() => loadDataFromNeo4j('all')}
          disabled={isLoading}
          className={`load-button ${lastLoadType === 'all' ? 'active' : ''}`}
        >
          {isLoading && lastLoadType === 'all' ? 'Loading...' : 'Load All Data'}
        </button>
        
        {/* Removed Load Filtered Data button */}
      </div>
      
      {lastLoadType && (
        <div className="load-status">
          <p><strong>Last loaded:</strong> {lastLoadType === 'all' ? 'All data' : 'Filtered data'}</p>
        </div>
      )}
    </div>
  );
};

export default DataLoader;
