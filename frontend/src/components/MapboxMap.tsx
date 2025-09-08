import React, { useEffect, useRef, useCallback, useState, forwardRef, useImperativeHandle } from 'react';
import mapboxgl from 'mapbox-gl';
import 'mapbox-gl/dist/mapbox-gl.css';
import './MapboxMap.css';

// Just added mapbox token to .env, now need to link here
const MAPBOX_ACCESS_TOKEN = process.env.REACT_APP_MAPBOX_TOKEN as string | undefined;


interface SpatialData {
  type: 'Point' | 'Polygon' | 'MultiPolygon';
  name: string;
  coordinates: number[] | number[][] | number[][][] | number[][][][];
  hyperedge_id: string;
}

interface MapboxMapProps {
  className?: string;
  spatialData?: SpatialData[];
  showInternalControls?: boolean;
  onContainmentModeChange?: (mode: 'overlap' | 'contained') => void;
  onSelectionChange?: (hasSelection: boolean) => void;
}

// Normalise helpers outside the component to avoid hook deps
const normalisePolygon = (coords: any): number[][][] | null => {
  if (!Array.isArray(coords) || coords.length === 0) return null;
  if (Array.isArray(coords[0]) && typeof coords[0][0] === 'number') {
    return [coords as number[][]];
  }
  if (Array.isArray(coords[0]) && Array.isArray(coords[0][0]) && typeof coords[0][0][0] === 'number') {
    return coords as number[][][];
  }
  return null;
};

const normaliseMultiPolygon = (coords: any): number[][][][] | null => {
  if (!Array.isArray(coords) || coords.length === 0) return null;
  if (Array.isArray(coords[0])) {
    if (Array.isArray(coords[0][0]) && typeof coords[0][0][0] === 'number') {
      return [coords as number[][][]];
    }
    if (Array.isArray(coords[0][0]) && Array.isArray(coords[0][0][0]) && typeof coords[0][0][0][0] === 'number') {
      return coords as number[][][][];
    }
  }
  return null;
};

export type MapboxMapHandle = {
  startDrawing: () => void;
  clearSelection: () => void;
  toggleContainmentMode: () => void;
  getContainmentMode: () => 'overlap' | 'contained';
};

const MapboxMap = forwardRef<MapboxMapHandle, MapboxMapProps>(({ className = '', spatialData = [], showInternalControls = true, onContainmentModeChange, onSelectionChange }, ref) => {
  const mapContainer = useRef<HTMLDivElement>(null);
  const map = useRef<mapboxgl.Map | null>(null);
  const markers = useRef<mapboxgl.Marker[]>([]);
  const polygonLayers = useRef<Array<{sourceId: string; layerId: string; borderLayerId: string}>>([]);
  const originalDataRef = useRef<SpatialData[]>([]);

  // Drawing state
  const [isDrawing, setIsDrawing] = useState<boolean>(false);
  const [drawCoords, setDrawCoords] = useState<number[][]>([]);
  const [containmentMode, setContainmentMode] = useState<'overlap' | 'contained'>('overlap');
  // Keep last polygon for potential re-filter on mode change
  const [lastDrawnPolygon, setLastDrawnPolygon] = useState<number[][] | null>(null);
  const drawSourceId = 'user-draw-poly-source';
  const drawFillLayerId = 'user-draw-poly-fill';
  const drawLineLayerId = 'user-draw-poly-line';
  const drawPointLayerId = 'user-draw-points';

  // Function to add markers for Point spatial data
  const addPointMarkers = useCallback((data: SpatialData[]) => {
    if (!map.current) return;

    data.forEach((item) => {
      if (item.type === 'Point' && Array.isArray(item.coordinates) && item.coordinates.length === 2) {
        // Type guard to ensure coordinates is a number array for Point type
        const coords = item.coordinates as number[];
        const [lon, lat] = coords;
        
        // Simple, crisp purple pin without glow or gradients
        const markerEl = document.createElement('div');
        markerEl.innerHTML = `
          <svg width="28" height="38" viewBox="-2 -2 32 42" xmlns="http://www.w3.org/2000/svg" style="overflow: visible">
            <path d="M14 0C6.82 0 1 5.82 1 13c0 8.5 10.5 24 13 24s13-15.5 13-24C27 5.82 21.18 0 14 0z" fill="#7B61FF" stroke="#4B2BD6" stroke-width="2" shape-rendering="geometricPrecision"/>
            <circle cx="14" cy="13" r="5" fill="#ffffff"/>
          </svg>
        `;
        markerEl.style.cursor = 'pointer';
        markerEl.title = item.name;

        // Create and add the marker
        const marker = new mapboxgl.Marker(markerEl)
          .setLngLat([lon, lat])
          .addTo(map.current!);
        
        markers.current.push(marker);

        // Add popup on click
        const popup = new mapboxgl.Popup({ offset: 25 })
          .setHTML(`
            <div>
              <h4>${item.name}</h4>
              <p>Type: ${item.type}</p>
              <p>Hyperedge ID: ${item.hyperedge_id}</p>
              <p>Coordinates: [${lon.toFixed(6)}, ${lat.toFixed(6)}]</p>
            </div>
          `);
        
        marker.setPopup(popup);
      }
    });
  }, []);

  // Function to add polygons for Polygon spatial data
  const addPolygonLayers = useCallback((data: SpatialData[]) => {
    if (!map.current) return;

    data.forEach((item, index) => {
      if ((item.type === 'Polygon' || item.type === 'MultiPolygon') && Array.isArray(item.coordinates) && (item.coordinates as any).length > 0) {
        let sourceData: any = null;
        if (item.type === 'Polygon') {
          const poly = normalisePolygon(item.coordinates);
          if (!poly) return;
          sourceData = {
            type: 'Feature',
            geometry: {
              type: 'Polygon',
              coordinates: poly
            },
            properties: {
              name: item.name,
              hyperedge_id: item.hyperedge_id
            }
          };
        } else {
          const mpoly = normaliseMultiPolygon(item.coordinates);
          if (!mpoly) return;
          sourceData = {
            type: 'Feature',
            geometry: {
              type: 'MultiPolygon',
              coordinates: mpoly
            },
            properties: {
              name: item.name,
              hyperedge_id: item.hyperedge_id
            }
          };
        }
        
        // Create a unique source ID for this polygon
        const sourceId = `polygon-source-${index}`;
        const layerId = `polygon-layer-${index}`;
        const borderLayerId = `polygon-border-${index}`;
        
        // Add the polygon source
        map.current!.addSource(sourceId, {
          type: 'geojson',
          data: sourceData
        });

        // Add the filled polygon layer
        map.current!.addLayer({
          id: layerId,
          type: 'fill',
          source: sourceId,
          paint: {
            'fill-color': '#4ecdc4',
            'fill-opacity': 0.3,
            'fill-outline-color': '#4ecdc4'
          }
        });

        // Add the border layer for better visibility
        map.current!.addLayer({
          id: borderLayerId,
          type: 'line',
          source: sourceId,
          paint: {
            'line-color': '#4ecdc4',
            'line-width': 2,
            'line-opacity': 0.8
          }
        });

        // Store layer IDs for cleanup
        polygonLayers.current.push({ sourceId, layerId, borderLayerId });

        // Add click handler for the polygon
        map.current!.on('click', layerId, (e) => {
          const coordinates = e.lngLat;
          const popup = new mapboxgl.Popup({ offset: 25 })
            .setLngLat(coordinates)
            .setHTML(`
              <div>
                <h4>${item.name}</h4>
                <p>Type: ${item.type}</p>
                <p>Hyperedge ID: ${item.hyperedge_id}</p>
                <p>Click coordinates: [${coordinates.lng.toFixed(6)}, ${coordinates.lat.toFixed(6)}]</p>
              </div>
            `);
          
          popup.addTo(map.current!);
        });

        // Change cursor on hover
        map.current!.on('mouseenter', layerId, () => {
          map.current!.getCanvas().style.cursor = 'pointer';
        });

        map.current!.on('mouseleave', layerId, () => {
          map.current!.getCanvas().style.cursor = '';
        });
      }
    });
  }, []);

  // Function to clear existing markers and polygons
  const clearMarkers = () => {
    markers.current.forEach(marker => marker.remove());
    markers.current = [];
    
    // Clear polygon layers
    if (map.current) {
      polygonLayers.current.forEach(({ sourceId, layerId, borderLayerId }) => {
        if (map.current!.getLayer(layerId)) {
          map.current!.removeLayer(layerId);
        }
        if (map.current!.getLayer(borderLayerId)) {
          map.current!.removeLayer(borderLayerId);
        }
        if (map.current!.getSource(sourceId)) {
          map.current!.removeSource(sourceId);
        }
      });
      polygonLayers.current = [];
    }
  };

  // Geometry helpers
  const pointInPolygon = useCallback((point: number[], polygon: number[][]): boolean => {
    if (!point || point.length !== 2 || !polygon || polygon.length < 3) return false;
    const x = point[0], y = point[1];
    let inside = false;
    for (let i = 0, j = polygon.length - 1; i < polygon.length; j = i++) {
      const xi = polygon[i][0], yi = polygon[i][1];
      const xj = polygon[j][0], yj = polygon[j][1];
      const intersect = ((yi > y) !== (yj > y)) && (x < (xj - xi) * (y - yi) / ((yj - yi) || 1e-12) + xi);
      if (intersect) inside = !inside;
    }
    return inside;
  }, []);

  const segmentsIntersect = (a1: number[], a2: number[], b1: number[], b2: number[]): boolean => {
    const ccw = (A: number[], B: number[], C: number[]) => (C[1] - A[1]) * (B[0] - A[0]) > (B[1] - A[1]) * (C[0] - A[0]);
    return (ccw(a1, b1, b2) !== ccw(a2, b1, b2)) && (ccw(a1, a2, b1) !== ccw(a1, a2, b2));
  };

  const polygonsIntersect = useCallback((poly1: number[][], poly2: number[][]): boolean => {
    const bbox = (poly: number[][]) => {
      let minx = Infinity, miny = Infinity, maxx = -Infinity, maxy = -Infinity;
      poly.forEach(([x,y]) => { minx = Math.min(minx,x); miny = Math.min(miny,y); maxx = Math.max(maxx,x); maxy = Math.max(maxy,y); });
      return {minx, miny, maxx, maxy};
    };
    const b1 = bbox(poly1), b2 = bbox(poly2);
    if (b1.maxx < b2.minx || b2.maxx < b1.minx || b1.maxy < b2.miny || b2.maxy < b1.miny) return false;
    if (poly1.some(p => pointInPolygon(p, poly2))) return true;
    if (poly2.some(p => pointInPolygon(p, poly1))) return true;
    for (let i = 0; i < poly1.length; i++) {
      const a1 = poly1[i];
      const a2 = poly1[(i + 1) % poly1.length];
      for (let j = 0; j < poly2.length; j++) {
        const b1p = poly2[j];
        const b2p = poly2[(j + 1) % poly2.length];
        if (segmentsIntersect(a1, a2, b1p, b2p)) return true;
      }
    }
    return false;
  }, [pointInPolygon]);

  const polygonFullyInside = useCallback((inner: number[][], outer: number[][]): boolean => {
    if (!inner || inner.length < 3 || !outer || outer.length < 3) return false;
    return inner.every(p => pointInPolygon(p, outer));
  }, [pointInPolygon]);

  // Drawing helpers
  const ensureDrawLayers = useCallback(() => {
    if (!map.current) return;
    const m = map.current;
    if (!m.getSource(drawSourceId)) {
      m.addSource(drawSourceId, {
        type: 'geojson',
        data: { type: 'FeatureCollection', features: [] }
      });
    }
    if (!m.getLayer(drawFillLayerId)) {
      m.addLayer({
        id: drawFillLayerId,
        type: 'fill',
        source: drawSourceId,
        paint: { 'fill-color': '#ff00aa', 'fill-opacity': 0.15 }
      });
    }
    if (!m.getLayer(drawLineLayerId)) {
      m.addLayer({
        id: drawLineLayerId,
        type: 'line',
        source: drawSourceId,
        paint: { 'line-color': '#ff00aa', 'line-width': 2 }
      });
    }
    if (!m.getLayer(drawPointLayerId)) {
      m.addLayer({
        id: drawPointLayerId,
        type: 'circle',
        source: drawSourceId,
        paint: { 'circle-radius': 4, 'circle-color': '#ff00aa' }
      });
    }
  }, []);

  const updateDrawData = useCallback((coords: number[][]) => {
    if (!map.current) return;
    const m = map.current;
    const closed = coords.length >= 3 ? [...coords, coords[0]] : coords;
    const features: any[] = [];
    if (closed.length >= 4) {
      features.push({
        type: 'Feature',
        geometry: { type: 'Polygon', coordinates: [closed] },
        properties: {}
      });
    }
    if (coords.length > 0) {
      features.push({
        type: 'Feature',
        geometry: { type: 'LineString', coordinates: coords },
        properties: { role: 'line' }
      });
      features.push({
        type: 'Feature',
        geometry: { type: 'MultiPoint', coordinates: coords },
        properties: { role: 'points' }
      });
    }
    const src = m.getSource(drawSourceId) as mapboxgl.GeoJSONSource;
    if (src) {
      src.setData({ type: 'FeatureCollection', features });
    }
  }, []);

  const startDrawing = useCallback(() => {
    if (!map.current) return;
    originalDataRef.current = spatialData.slice();
    setIsDrawing(true);
    setDrawCoords([]);
    clearMarkers();
    ensureDrawLayers();
    try { map.current.doubleClickZoom.disable(); } catch {}
  }, [spatialData, ensureDrawLayers]);

  const filterAndRender = useCallback((userPoly: number[][]) => {
    if (!map.current) return;
    const filtered: SpatialData[] = [];
    originalDataRef.current.forEach((item) => {
      if (item.type === 'Point') {
        const coords = item.coordinates as number[];
        const inside = pointInPolygon(coords, userPoly);
        if (inside) filtered.push(item);
      } else if (item.type === 'Polygon') {
        const polyNorm = normalisePolygon(item.coordinates);
        if (polyNorm && polyNorm.length > 0) {
          const ring = polyNorm[0];
          const match = containmentMode === 'contained' ? polygonFullyInside(ring, userPoly) : polygonsIntersect(ring, userPoly);
          if (match) filtered.push(item);
        }
      } else if (item.type === 'MultiPolygon') {
        const mpoly = normaliseMultiPolygon(item.coordinates);
        if (mpoly) {
          let matched = false;
          if (containmentMode === 'contained') {
            matched = mpoly.every(poly2 => polygonFullyInside(poly2[0], userPoly));
          } else {
            matched = mpoly.some(poly2 => polygonsIntersect(poly2[0], userPoly));
          }
          if (matched) filtered.push(item);
        }
      }
    });

    clearMarkers();
    addPointMarkers(filtered);
    addPolygonLayers(filtered);
  }, [addPointMarkers, addPolygonLayers, containmentMode, polygonFullyInside, polygonsIntersect, pointInPolygon]);

  const finishDrawing = useCallback(() => {
    if (!map.current) return;
    setIsDrawing(false);
    try { map.current.doubleClickZoom.enable(); } catch {}
    const poly = drawCoords.length >= 3 ? drawCoords : [];
    ensureDrawLayers();
    updateDrawData(poly); // persist the selection polygon overlay
    setLastDrawnPolygon(poly.length ? poly : null);
    if (poly.length) {
      filterAndRender(poly);
      if (onSelectionChange) onSelectionChange(true);
    }
  }, [drawCoords, ensureDrawLayers, updateDrawData, filterAndRender, onSelectionChange]);

  // Re-apply filter when containment mode changes if we have a drawn polygon
  useEffect(() => {
    if (lastDrawnPolygon) {
      filterAndRender(lastDrawnPolygon);
    }
    if (onContainmentModeChange) onContainmentModeChange(containmentMode);
  }, [containmentMode, lastDrawnPolygon, filterAndRender, onContainmentModeChange]);

  const clearSelection = useCallback(() => {
    if (!map.current) return;
    setIsDrawing(false);
    setDrawCoords([]);
    setLastDrawnPolygon(null);
    ensureDrawLayers();
    updateDrawData([]);
    // Restore original dataset without changing camera
    const base = originalDataRef.current && originalDataRef.current.length > 0
      ? originalDataRef.current
      : spatialData;
    clearMarkers();
    addPointMarkers(base);
    addPolygonLayers(base);
    if (onSelectionChange) onSelectionChange(false);
  }, [spatialData, addPointMarkers, addPolygonLayers, ensureDrawLayers, updateDrawData, onSelectionChange]);

  // Expose imperative API to parent
  useImperativeHandle(ref, () => ({
    startDrawing: () => startDrawing(),
    clearSelection: () => clearSelection(),
    toggleContainmentMode: () => setContainmentMode(m => (m === 'overlap' ? 'contained' : 'overlap')),
    getContainmentMode: () => containmentMode,
  }), [startDrawing, clearSelection, containmentMode]);

  // Click handling during drawing
  useEffect(() => {
    const m = map.current;
    if (!m) return;
    if (!isDrawing) return;

    const onClick = (e: mapboxgl.MapMouseEvent) => {
      const pt: number[] = [e.lngLat.lng, e.lngLat.lat];
      // Close polygon if clicking near first point
      if (drawCoords.length >= 3) {
        const first = drawCoords[0];
        const a = m.project({lng: first[0], lat: first[1]});
        const b = m.project(e.lngLat);
        const dx = a.x - b.x, dy = a.y - b.y;
        const dist = Math.sqrt(dx*dx + dy*dy);
        if (dist < 10) {
          finishDrawing();
          return;
        }
      }
      const next = [...drawCoords, pt];
      setDrawCoords(next);
      ensureDrawLayers();
      updateDrawData(next);
    };
    const onDblClick = () => {
      if (drawCoords.length >= 3) finishDrawing();
    };

    m.on('click', onClick);
    m.on('dblclick', onDblClick);
    return () => {
      m.off('click', onClick);
      m.off('dblclick', onDblClick);
    };
  }, [isDrawing, drawCoords, ensureDrawLayers, updateDrawData, finishDrawing]);

  // Effect to handle spatial data changes
  useEffect(() => {
    if (map.current && spatialData.length > 0) {
      // Reset base dataset reference to the latest provided data
      originalDataRef.current = spatialData.slice();

      clearMarkers();
      addPointMarkers(spatialData);
      addPolygonLayers(spatialData);

      // Auto-fit to bounds of all provided spatial data
      try {
        const b = new mapboxgl.LngLatBounds();
        let hasAny = false;

        spatialData.forEach((item) => {
          if (item.type === 'Point' && Array.isArray(item.coordinates) && item.coordinates.length === 2) {
            const [lon, lat] = item.coordinates as number[];
            if (Number.isFinite(lon) && Number.isFinite(lat)) {
              b.extend([lon, lat]);
              hasAny = true;
            }
          } else if (item.type === 'Polygon') {
            const poly = normalisePolygon(item.coordinates);
            if (poly) {
              poly.forEach((ring) => {
                ring.forEach(([lon, lat]) => {
                  if (Number.isFinite(lon) && Number.isFinite(lat)) {
                    b.extend([lon, lat]);
                    hasAny = true;
                  }
                });
              });
            }
          } else if (item.type === 'MultiPolygon') {
            const mpoly = normaliseMultiPolygon(item.coordinates);
            if (mpoly) {
              mpoly.forEach((poly) => {
                poly.forEach((ring) => {
                  ring.forEach(([lon, lat]) => {
                    if (Number.isFinite(lon) && Number.isFinite(lat)) {
                      b.extend([lon, lat]);
                      hasAny = true;
                    }
                  });
                });
              });
            }
          }
        });

        if (hasAny) {
          map.current.fitBounds(b, { padding: 40, duration: 800 });
        }
      } catch (e) {
        // Ignore bounds errors
      }
      // If there is an active selection polygon, re-apply filtering to the new dataset
      if (lastDrawnPolygon && lastDrawnPolygon.length >= 3) {
        filterAndRender(lastDrawnPolygon);
      }
    }
  }, [spatialData, addPointMarkers, addPolygonLayers, lastDrawnPolygon, filterAndRender]);

  useEffect(() => {
    if (map.current) return; // initialise map only once

    if (mapContainer.current) {
      try {
        if (!MAPBOX_ACCESS_TOKEN) {
          console.error('Missing Mapbox token. Set REACT_APP_MAPBOX_TOKEN in frontend/.env');
          return;
        }
        mapboxgl.accessToken = MAPBOX_ACCESS_TOKEN;
        
        map.current = new mapboxgl.Map({
          container: mapContainer.current,
          style: 'mapbox://styles/mapbox/streets-v12', 
          center: [-0.17928, 51.49827], // Lon, lat
          zoom: 15, // Increased zoom level for more detail
          pitch: 45, // Added pitch for 3D angle
          bearing: 0, // Initial bearing
          antialias: true // Enable antialiasing for smoother rendering
        });

        // Add navigation controls
        map.current.addControl(new mapboxgl.NavigationControl());

        // Enable 3D buildings when the map loads
        map.current.on('load', () => {
          if (map.current) {
            // Add 3D building layer
            map.current.addLayer({
              'id': '3d-buildings',
              'source': 'composite',
              'source-layer': 'building',
              'filter': ['==', 'extrude', 'true'],
              'type': 'fill-extrusion',
              'minzoom': 15,
              'paint': {
                'fill-extrusion-color': '#aaa',
                'fill-extrusion-height': [
                  'interpolate',
                  ['linear'],
                  ['zoom'],
                  15,
                  0,
                  15.05,
                  ['get', 'height']
                ],
                'fill-extrusion-base': [
                  'interpolate',
                  ['linear'],
                  ['zoom'],
                  15,
                  0,
                  15.05,
                  ['get', 'min_height']
                ],
                'fill-extrusion-opacity': 0.6
              }
            });
          }
        });

      } catch (error) {
        console.error('Error initializing Mapbox map:', error);
      }
    }

    return () => {
      if (map.current) {
        clearMarkers();
        map.current.remove();
        map.current = null;
      }
    };
  }, []); // This useEffect only handles map initialization, not spatial data

  return (
    <div className={`mapbox-map ${className}`}>
      {showInternalControls && (
        <div className="map-controls">
          <button onClick={startDrawing} disabled={isDrawing} className="map-btn">Draw area</button>
          <button onClick={() => setContainmentMode(m => m === 'overlap' ? 'contained' : 'overlap')} className="map-btn">
            Mode: {containmentMode === 'overlap' ? 'Overlap' : 'Contained'}
          </button>
          <button onClick={clearSelection} disabled={!lastDrawnPolygon && !isDrawing} className="map-btn">Clear selection</button>
          {isDrawing && (
            <span className="map-hint">Click to add points. Double-click or click first point to finish.</span>
          )}
        </div>
      )}
      <div ref={mapContainer} className="map-container" />
    </div>
  );
});

export default MapboxMap;
