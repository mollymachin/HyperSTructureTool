import React, { useState, useEffect, useCallback } from 'react';
import './TemporalInput.css';

interface SpatiotemporalInputProps {
  startTime: string;
  endTime: string;
  locationNames: string[] | null;
  locationCoordinates: number[][] | null;
  includeSpatiallyUnconstrained: boolean;
  onSpatiotemporalChange: (
    startTime: string | null,
    endTime: string | null,
    locationNames: string[] | null,
    locationCoordinates: number[][] | null,
    includeSpatiallyUnconstrained: boolean
  ) => void;
  onLoadFilteredData: () => void;
  isLoading: boolean;
  hasActiveFilters: boolean;
}

const SpatiotemporalInput: React.FC<SpatiotemporalInputProps> = ({
  startTime: initialStartTime,
  endTime: initialEndTime,
  locationNames: initialLocationNames,
  locationCoordinates: initialLocationCoordinates,
  includeSpatiallyUnconstrained: initialIncludeSpatiallyUnconstrained,
  onSpatiotemporalChange,
  onLoadFilteredData,
  isLoading,
  hasActiveFilters
}) => {
  const [startTime, setStartTime] = useState(initialStartTime);
  const [endTime, setEndTime] = useState(initialEndTime);
  const [locationNames, setLocationNames] = useState(initialLocationNames?.join(', ') || '');
  const [locationCoordinates, setLocationCoordinates] = useState(initialLocationCoordinates ? JSON.stringify(initialLocationCoordinates) : '');
  const [includeSpatiallyUnconstrained, setIncludeSpatiallyUnconstrained] = useState(initialIncludeSpatiallyUnconstrained);
  const [spatialFilterType, setSpatialFilterType] = useState<'none' | 'names' | 'coordinates'>('none');

  const handleStartTimeChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    console.log('handleStartTimeChange called with value:', value);
    setStartTime(value);
    console.log('startTime state set to:', value);
  };

  const handleEndTimeChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    console.log('handleEndTimeChange called with value:', value);
    setEndTime(value);
    console.log('endTime state set to:', value);
  };

  const handleLocationNamesChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    setLocationNames(value);
    setSpatialFilterType('names');
  };

  const handleLocationCoordinatesChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.value;
    setLocationCoordinates(value);
    setSpatialFilterType('coordinates');
  };

  const handleSpatialFilterTypeChange = (e: React.ChangeEvent<HTMLSelectElement>) => {
    const value = e.target.value as 'none' | 'names' | 'coordinates';
    setSpatialFilterType(value);
    
    if (value === 'none') {
      setLocationNames('');
      setLocationCoordinates('');
    } else if (value === 'names') {
      setLocationCoordinates('');
    } else if (value === 'coordinates') {
      setLocationNames('');
    }
  };

  const handleIncludeSpatiallyUnconstrainedChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const value = e.target.checked;
    setIncludeSpatiallyUnconstrained(value);
  };

  const clearAllFilters = () => {
    setStartTime('');
    setEndTime('');
    setLocationNames('');
    setLocationCoordinates('');
    setIncludeSpatiallyUnconstrained(false);
    setSpatialFilterType('none');
    updateSpatiotemporalFilter();
  };

  const updateSpatiotemporalFilter = useCallback(() => {
    console.log('updateSpatiotemporalFilter called with:');
    console.log('  startTime state:', startTime);
    console.log('  endTime state:', endTime);
    console.log('  spatialFilterType:', spatialFilterType);
    console.log('  locationNames:', locationNames);
    console.log('  locationCoordinates:', locationCoordinates);
    
    let names: string[] | null = null;
    let coordinates: number[][] | null = null;

    if (spatialFilterType === 'names' && locationNames.trim()) {
      names = locationNames.split(',').map(name => name.trim()).filter(name => name);
    } else if (spatialFilterType === 'coordinates' && locationCoordinates.trim()) {
      try {
        // Parse coordinates as JSON array of [lon, lat] pairs
        coordinates = JSON.parse(locationCoordinates);
        if (!Array.isArray(coordinates) || coordinates.length < 3) {
          coordinates = null; // Invalid polygon
        }
      } catch (e) {
        coordinates = null; // Invalid JSON
      }
    }

    const finalStartTime = startTime || null;
    const finalEndTime = endTime || null;
    
    console.log('Calling onSpatiotemporalChange with:');
    console.log('  finalStartTime:', finalStartTime);
    console.log('  finalEndTime:', finalEndTime);
    console.log('  names:', names);
    console.log('  coordinates:', coordinates);
    console.log('  includeSpatiallyUnconstrained:', includeSpatiallyUnconstrained);

    onSpatiotemporalChange(
      finalStartTime,
      finalEndTime,
      names,
      coordinates,
      includeSpatiallyUnconstrained
    );
  }, [startTime, endTime, locationNames, locationCoordinates, includeSpatiallyUnconstrained, spatialFilterType, onSpatiotemporalChange]);

  // Use useEffect to watch for state changes and update filters automatically
  useEffect(() => {
    console.log('useEffect triggered - updating spatiotemporal filter');
    updateSpatiotemporalFilter();
  }, [updateSpatiotemporalFilter]);

  return (
    <div className="temporal-input-container">
      <h3>Spatio-Temporal Filtering</h3>
      <p>Filter the visualisation by time and/or location. Time filtering uses UTC.</p>
      
      <div className="temporal-inputs">
        <div className="input-group">
          <label htmlFor="start-time">Start Time (optional):</label>
          <input
            id="start-time"
            type="datetime-local"
            value={startTime}
            onChange={handleStartTimeChange}
            className="temporal-input"
            placeholder="2020-01-01T00:00:00"
          />
        </div>
        
        <div className="input-group">
          <label htmlFor="end-time">End Time (optional):</label>
          <input
            id="end-time"
            type="datetime-local"
            value={endTime}
            onChange={handleEndTimeChange}
            className="temporal-input"
            placeholder="2023-12-31T23:59:59"
          />
        </div>
      </div>

      <div className="spatial-inputs">
        <div className="input-group">
          <label htmlFor="spatial-filter-type">Spatial Filter Type:</label>
          <select
            id="spatial-filter-type"
            value={spatialFilterType}
            onChange={handleSpatialFilterTypeChange}
            className="spatial-select"
          >
            <option value="none">No spatial filtering</option>
            <option value="names">Location names</option>
            <option value="coordinates">Polygon coordinates</option>
          </select>
        </div>

        {spatialFilterType === 'names' && (
          <div className="input-group">
            <label htmlFor="location-names">Location Names (comma-separated):</label>
            <input
              id="location-names"
              type="text"
              value={locationNames}
              onChange={handleLocationNamesChange}
              className="spatial-input"
              autoComplete="off"
              autoCorrect="off"
              spellCheck={false}
              autoCapitalize="none"
              placeholder="Boston, MIT, Cambridge"
            />
          </div>
        )}

        {spatialFilterType === 'coordinates' && (
          <div className="input-group">
            <label htmlFor="location-coordinates">Polygon Coordinates (JSON array of [lon, lat] pairs):</label>
            <input
              id="location-coordinates"
              type="text"
              value={locationCoordinates}
              onChange={handleLocationCoordinatesChange}
              className="spatial-input"
              autoComplete="off"
              autoCorrect="off"
              spellCheck={false}
              autoCapitalize="none"
              placeholder="[[-71.0589, 42.3601], [-71.0935, 42.3591], [-71.0935, 42.3591]]"
            />
            <div className="checkbox-group">
              <input
                id="include-spatially-unconstrained"
                type="checkbox"
                checked={includeSpatiallyUnconstrained}
                onChange={handleIncludeSpatiallyUnconstrainedChange}
              />
              <label htmlFor="include-spatially-unconstrained">
                Include spatially unconstrained hyperedges
              </label>
            </div>
          </div>
        )}
      </div>
      
      {/* Removed current filters box as requested */}
      
      <div className="load-filtered-section">
        <div className="button-row">
          <button 
            onClick={clearAllFilters}
            className="clear-filters-button"
            title="Clear all filters"
          >
            Clear All Filters
          </button>
          
          <button 
            onClick={onLoadFilteredData}
            disabled={isLoading || !hasActiveFilters}
            className="load-filtered-button"
            title={!hasActiveFilters ? 'Set filters first to enable this button' : 'Load data with current filters'}
          >
            {isLoading ? 'Loading...' : 'Load Filtered Data'}
          </button>
        </div>
      </div>
    </div>
  );
};

export default SpatiotemporalInput;
