"""
Neo4j storage backend for HyperSTructure.
Implements a spatiotemporal hypergraph model with:
- (:Node) entities (people, locations, events)
- (:Hyperedge) facts/relationships between entities
- (:Context) spatiotemporal validity contexts
- [:CONNECTS] relationships from hyperedges to nodes
- [:VALID_IN] relationships from hyperedges to contexts
"""

import asyncio
import json
import os
from dotenv import load_dotenv
from typing import Dict, List, Set, Optional, Any, Union, Tuple
from dataclasses import dataclass
from datetime import datetime, timezone
import logging
from neo4j import GraphDatabase, Driver, Session
from neo4j.exceptions import ServiceUnavailable, AuthError

logger = logging.getLogger(__name__)

env_path = os.path.join(os.path.dirname(os.path.dirname(__file__)), '.env')
# In production (e.g., Render), service env vars should win over repo .env
load_dotenv(env_path, override=False)

@dataclass
class Neo4jConfig:
    """Configuration for Neo4j connection."""
    uri: str = os.getenv("NEO4J_URI")
    username: str = os.getenv("NEO4J_USERNAME")
    password: str = os.getenv("NEO4J_PASSWORD")
    database: str = os.getenv("NEO4J_DATABASE") or "neo4j"


class Neo4jStorage:
    """
    Neo4j storage backend for HyperSTructure.
    
    Data Model:
    - (:Node) - Entities (people, locations, events)
    - (:Hyperedge) - Facts/relationships between entities
    - (:Context) - Spatiotemporal validity contexts
    - [:CONNECTS] - Hyperedge connects to nodes
    - [:VALID_IN] - Hyperedge is valid in context
    """
    
    def __init__(self, config: Neo4jConfig, namespace: str = "default"):
        """
        Initialise Neo4j storage.
        
        Args:
            config: Neo4j connection configuration
            namespace: Namespace for this storage instance
        """
        self.config = config
        self.namespace = namespace
        self.driver: Optional[Driver] = None
        self._connected = False
        
    async def connect(self) -> bool:
        """Connect to Neo4j database."""
        try:
            self.driver = GraphDatabase.driver(
                self.config.uri,
                auth=(self.config.username, self.config.password)
            )
            
            # Test connection
            with self.driver.session(database=self.config.database) as session:
                result = session.run("RETURN 1 as test")
                result.single()
            
            self._connected = True
            logger.info(f"Connected to Neo4j at {self.config.uri}")
            
            # Initialise schema
            await self._create_constraints()
            return True
            
        except (ServiceUnavailable, AuthError) as e:
            logger.error(f"Failed to connect to Neo4j: {e}")
            return False
    
    async def disconnect(self):
        """Disconnect from Neo4j database."""
        if self.driver:
            self.driver.close()
            self.driver = None
            self._connected = False
            logger.info("Disconnected from Neo4j")
    
    async def _create_constraints(self):
        """Create database constraints and indexes."""
        with self.driver.session(database=self.config.database) as session:
            # Create constraints for unique identifiers to ensure data integrity
            constraints = [
                # No 2 nodes, hypredges or contexts can have the same id
                "CREATE CONSTRAINT node_id_unique IF NOT EXISTS FOR (n:Node) REQUIRE n.id IS UNIQUE",
                "CREATE CONSTRAINT hyperedge_id_unique IF NOT EXISTS FOR (h:Hyperedge) REQUIRE h.id IS UNIQUE",
                "CREATE CONSTRAINT context_id_unique IF NOT EXISTS FOR (c:Context) REQUIRE c.id IS UNIQUE"
            ]
            
            # Create indexes for efficient queries improving performance
            # Not implemented for time as unlikely query will search for exact time a temporal interval begins/ends
            indexes = [
                "CREATE INDEX node_type_index IF NOT EXISTS FOR (n:Node) ON (n.type)", # e.g. fast lookup index on the type property of nodes
                "CREATE INDEX hyperedge_relation_index IF NOT EXISTS FOR (h:Hyperedge) ON (h.relation_type)",
                "CREATE INDEX context_spatial_index IF NOT EXISTS FOR (c:Context) ON (c.location_name)",
                "CREATE INDEX context_certainty_index IF NOT EXISTS FOR (c:Context) ON (c.certainty)",
                "CREATE INDEX context_coordinates_index IF NOT EXISTS FOR (c:Context) ON (c.coordinates)" # Spatial index for Point coordinates
            ]
            
            for constraint in constraints:
                try:
                    session.run(constraint)
                except Exception as e:
                    logger.warning(f"Constraint creation failed (may already exist): {e}")
            
            for index in indexes:
                try:
                    session.run(index)
                except Exception as e:
                    logger.warning(f"Index creation failed (may already exist): {e}")
    
    async def query_by_temporal_range(self, start_time: str, 
                                    end_time: Optional[str] = None) -> Set[str]:
        """
        Find hyperedges valid in a time range.
        
        Args:
            start_time: Start time in ISO format (e.g., "2020-01-01T00:00:00")
            end_time: End time in ISO format (e.g., "2021-12-31T23:59:59") (optional)
            
        Returns:
            Set of hyperedge IDs valid in the time range
        """
        if not self._connected:
            logger.error("Not connected to Neo4j")
            return set()
        
        if end_time is None:
            # Use current time if no end_time provided
            end_time = datetime(3000, 12, 31, tzinfo=timezone.utc).isoformat() # Far future date for open-ended queries
        
        try:
            with self.driver.session(database=self.config.database) as session:
                result = session.run("""
                    MATCH (h:Hyperedge)-[:VALID_IN]->(c:Context)
                    WHERE (c.from_time IS NULL OR c.from_time <= $end_time) 
                    AND (c.to_time IS NULL OR c.to_time >= $start_time)
                    RETURN DISTINCT h.id as hyperedge_id
                """, start_time=start_time, end_time=end_time)
                
                return {record["hyperedge_id"] for record in result}
                
        except Exception as e:
            logger.error(f"Failed to query by temporal range: {e}")
            return set()

    async def query_by_location_name(self, location_names: List[str]) -> Set[str]:
        """
        Find all hyperedges that match specified location names.
        
        Args:
            location_names: List of location names (e.g. ["Boston", "MIT"])
        """
        if not self._connected:
            logger.error("Not connected to Neo4j")
            return set()
        
        try:
            with self.driver.session(database=self.config.database) as session:
                # Query for hyperedges with contexts in any of the specified regions
                result = session.run("""
                    MATCH (h:Hyperedge)-[:VALID_IN]->(c:Context)
                    WHERE c.location_name IN $location_names
                    RETURN DISTINCT h.id as hyperedge_id
                """, location_names=location_names)
                
                return {record["hyperedge_id"] for record in result}
                
        except Exception as e:
            logger.error(f"Failed to query by location name: {e}")
            return set()
    
    async def query_by_spatial_area(self, areas) -> Set[str]:
        """
        Find hyperedges valid within specified spatial regions.
        
        Args:
            areas: either a Polygon from Mapbox or a list of coordinates in [lon, lat] format
            
        Returns:
            Set of hyperedge IDs valid in the specified areas
        """
        if not self._connected:
            logger.error("Not connected to Neo4j")
            return set()
        
        try:
            with self.driver.session(database=self.config.database) as session:
                # Query for hyperedges with contexts in any of the specified regions
                result = session.run("""
                    MATCH (h:Hyperedge)-[:VALID_IN]->(c:Context)
                                     ...
                    RETURN DISTINCT h.id as hyperedge_id
                """, areas=areas)
                
                return {record["hyperedge_id"] for record in result}
                
        except Exception as e:
            logger.error(f"Failed to query by spatial area: {e}")
            return set()
    
    async def query_by_spatial_distance(self, center_lat: float, center_lon: float, 
                                      radius_km: float) -> Set[str]:
        """
        Find hyperedges within a certain distance from a point.
        
        Args:
            center_lat: Latitude of the center point
            center_lon: Longitude of the center point
            radius_km: Radius in kilometers
            
        Returns:
            Set of hyperedge IDs within the specified radius
        """
        if not self._connected:
            logger.error("Not connected to Neo4j")
            return set()
        
        try:
            with self.driver.session(database=self.config.database) as session:
                # Query for hyperedges with contexts within the specified radius
                result = session.run("""
                    MATCH (h:Hyperedge)-[:VALID_IN]->(c:Context)
                    WHERE c.coordinates IS NOT NULL 
                    AND c.spatial_type = 'Point'
                    AND point.distance(c.coordinates, point({longitude: $center_lon, latitude: $center_lat})) <= $radius_meters
                    RETURN DISTINCT h.id as hyperedge_id
                """, center_lat=center_lat, center_lon=center_lon, radius_meters=radius_km * 1000)
                
                return {record["hyperedge_id"] for record in result}
                
        except Exception as e:
            logger.error(f"Failed to query by spatial distance: {e}")
            return set()
    
    async def query_by_certainty_threshold(self, min_certainty: float = 0.8) -> Set[str]:
        """
        Find hyperedges with contexts above a certainty threshold.
        
        Args:
            min_certainty: Minimum certainty threshold (0.0 to 1.0)
            
        Returns:
            Set of hyperedge IDs with high certainty contexts
        """
        if not self._connected:
            logger.error("Not connected to Neo4j")
            return set()
        
        try:
            with self.driver.session(database=self.config.database) as session:
                result = session.run("""
                    MATCH (h:Hyperedge)-[:VALID_IN]->(c:Context)
                    WHERE c.certainty >= $min_certainty
                    RETURN DISTINCT h.id as hyperedge_id
                """, min_certainty=min_certainty)
                
                return {record["hyperedge_id"] for record in result}
                
        except Exception as e:
            logger.error(f"Failed to query by certainty threshold: {e}")
            return set()
    
    async def query_spatiotemporal(self, start_time: Optional[str] = None, 
                                 end_time: Optional[str] = None,
                                 location_names: Optional[List[str]] = None,
                                 location_coordinates: Optional[List[List[float]]] = None,
                                 include_spatially_unconstrained: bool = False,
                                 include_temporally_unconstrained: bool = False) -> Set[str]:
        """
        Find hyperedges valid at certain time and/or location.
        
        Args:
            start_time: Start time in ISO format (e.g., "2020-01-01T00:00:00") (optional)
            end_time: End time in ISO format (e.g., "2021-12-31T23:59:59") (optional)
            location_names: List of location names (e.g., ["Boston", "MIT"]) (optional)
            location_coordinates: List of coordinate pairs defining a polygon area in [lon, lat] format (optional)
            include_spatially_unconstrained: If True and location_coordinates provided, include hyperedges with no spatial context (default: False)
            
        Returns:
            Set of hyperedge IDs matching spatiotemporal criteria
        """
        if not self._connected:
            logger.error("Not connected to Neo4j")
            return set()
        
        try:
            with self.driver.session(database=self.config.database) as session:
                # If no filters are provided, return all hyperedges
                if not start_time and not end_time and not location_names and not location_coordinates:
                    result = session.run("""
                        MATCH (h:Hyperedge)
                        RETURN DISTINCT h.id as hyperedge_id
                    """)
                    return {record["hyperedge_id"] for record in result} # extracts hyperedge ID from each row of result & adds to set
                
                # Build the base query
                query_parts = ["MATCH (h:Hyperedge)-[:VALID_IN]->(c:Context)"]
                where_conditions = []
                parameters = {}
                
                # Add temporal filtering
                if start_time or end_time:
                    # Store original parameter presence before setting defaults
                    has_start_time = start_time is not None
                    has_end_time = end_time is not None
                    
                    # if not end_time:
                    #     # Far future (maximum value), still valid in Python datetime
                    #     end_time = datetime(9999, 12, 31, tzinfo=timezone.utc).isoformat() # Far future date
                    # if not start_time:
                    #     # Far past (Python's minimum), still valid in Python datetime
                    #     start_time = datetime(1, 1, 1, tzinfo=timezone.utc).isoformat() # Far past date
                    
                    # Only write filter into the query if the parameter was actually set by user
                    if has_start_time:
                        if include_temporally_unconstrained:
                            where_conditions.append("(c.to_time IS NULL OR c.to_time >= $start_time)")
                        else:
                            where_conditions.append("(c.to_time IS NOT NULL AND c.to_time >= $start_time)")
                        parameters["start_time"] = start_time
                    if has_end_time:
                        # If only end_time is provided, include contexts that start before the end_time
                        if include_temporally_unconstrained:
                            where_conditions.append("(c.from_time IS NULL OR c.from_time <= $end_time)")
                        else:
                            where_conditions.append("(c.from_time IS NOT NULL AND c.from_time <= $end_time)")
                        parameters["end_time"] = end_time
                
                # Add spatial filtering
                if location_names:
                    where_conditions.append("c.location_name IN $location_names")
                    parameters["location_names"] = location_names
                elif location_coordinates:
                    if include_spatially_unconstrained:
                        # Include hyperedges with coordinates AND those with no spatial context
                        where_conditions.append("(c.coordinates IS NOT NULL OR c.spatial_type IS NULL)")
                    else:
                        # Only include hyperedges with coordinates (strict spatial filtering)
                        where_conditions.append("c.coordinates IS NOT NULL")
                
                # Build the actual query
                if where_conditions:
                    query_parts.append("WHERE " + " AND ".join(where_conditions))
                query_parts.append("RETURN DISTINCT h.id as hyperedge_id")
                query = "\n".join(query_parts)
                
                # Execute the query
                result = session.run(query, **parameters)
                hyperedge_ids = {record["hyperedge_id"] for record in result} # Returns hyperedges filtered by time and basic spatial constraints
                
                # If we have coordinate filtering, we need to do additional filtering in Python
                if location_coordinates:
                    # Get the full context data for coordinate checking with spatial filtering
                    if include_spatially_unconstrained:
                        # Query includes both coordinates and no spatial context
                        coordinate_query = """
                            MATCH (h:Hyperedge)-[:VALID_IN]->(c:Context)
                            WHERE h.id IN $hyperedge_ids AND (c.coordinates IS NOT NULL OR c.spatial_type IS NULL)
                            RETURN h.id as hyperedge_id, c.coordinates as coordinates, c.spatial_type as spatial_type
                        """
                    else:
                        # Query only includes coordinates (strict filtering)
                        coordinate_query = """
                            MATCH (h:Hyperedge)-[:VALID_IN]->(c:Context)
                            WHERE h.id IN $hyperedge_ids AND c.coordinates IS NOT NULL
                            RETURN h.id as hyperedge_id, c.coordinates as coordinates, c.spatial_type as spatial_type
                        """
                    
                    coordinate_result = session.run(coordinate_query, hyperedge_ids=list(hyperedge_ids))
                    
                    # Filter hyperedges based on spatial containment/intersection
                    matching_hyperedge_ids = set()
                    for record in coordinate_result:
                        hyperedge_id = record["hyperedge_id"]
                        context_coordinates = record["coordinates"]
                        spatial_type = record["spatial_type"]
                        
                        # If no spatial context, include it (spatially unconstrained)
                        if spatial_type is None or context_coordinates is None:
                            matching_hyperedge_ids.add(hyperedge_id)
                        else:
                            # Check spatial intersection for contexts with coordinates
                            if self._spatial_intersects(context_coordinates, spatial_type, location_coordinates):
                                matching_hyperedge_ids.add(hyperedge_id)
                    
                    # Return hyperedges that match the spatial criteria OR are spatially unconstrained
                    return matching_hyperedge_ids
                
                return hyperedge_ids

        except Exception as e:
            logger.error(f"Failed to query spatiotemporal: {e}")
            return set()
    
    def _spatial_intersects(self, context_coords, spatial_type: str, user_polygon: List[List[float]]) -> bool:
        """
        Helper function to check if context coordinates intersect with user-defined polygon area.
        
        Args:
            context_coords: Coordinates from the context (could be Point, Polygon, etc.)
            spatial_type: Type of spatial context ('Point', 'Polygon', etc.)
            user_polygon: List of coordinate pairs defining the user's polygon area in [lon, lat] format
            
        Returns:
            True if there's spatial intersection, False otherwise
        """
        if not context_coords or not user_polygon or len(user_polygon) < 3:
            return False
        
        try:
            if spatial_type == 'Point':
                # For Point contexts, check if the point is within the user polygon
                return self._point_in_polygon(context_coords, user_polygon)
            elif spatial_type == 'Polygon':
                # For Polygon contexts, check if the polygons intersect
                return self._polygons_intersect(context_coords, user_polygon)
            else:
                # For other types, default to False
                return False
        except Exception:
            # If any error occurs in spatial calculations, return False
            return False
    
    def _point_in_polygon(self, point: List[float], polygon: List[List[float]]) -> bool:
        """
        Check if a point is inside a polygon using ray casting algorithm.
        
        Args:
            point: [lon, lat] coordinates of the point
            polygon: List of [lon, lat] coordinate pairs defining the polygon
            
        Returns:
            True if point is inside polygon, False otherwise
        """
        if len(point) != 2 or len(polygon) < 3:
            return False
        
        x, y = point[0], point[1]  # lon, lat
        n = len(polygon)
        inside = False
        
        p1x, p1y = polygon[0]
        for i in range(n + 1):
            p2x, p2y = polygon[i % n]
            if y > min(p1y, p2y):
                if y <= max(p1y, p2y):
                    if x <= max(p1x, p2x):
                        if p1y != p2y:
                            xinters = (y - p1y) * (p2x - p1x) / (p2y - p1y) + p1x
                        if p1x == p2x or x <= xinters:
                            inside = not inside
            p1x, p1y = p2x, p2y
        
        return inside
    
    def _polygons_intersect(self, poly1: List[List[float]], poly2: List[List[float]]) -> bool:
        """
        Check if two polygons intersect using bounding box and edge intersection tests.
        
        Args:
            poly1: First polygon as list of [lon, lat] coordinate pairs
            poly2: Second polygon as list of [lon, lat] coordinate pairs
            
        Returns:
            True if polygons intersect, False otherwise
        """
        if len(poly1) < 3 or len(poly2) < 3:
            return False
        
        # Quick bounding box check first
        if not self._bounding_boxes_overlap(poly1, poly2):
            return False
        
        # Check if any point from poly1 is inside poly2
        for point in poly1:
            if self._point_in_polygon(point, poly2):
                return True
        
        # Check if any point from poly2 is inside poly1
        for point in poly2:
            if self._point_in_polygon(point, poly1):
                return True
        
        # Check for edge intersections
        for i in range(len(poly1)):
            edge1_start = poly1[i]
            edge1_end = poly1[(i + 1) % len(poly1)]
            
            for j in range(len(poly2)):
                edge2_start = poly2[j]
                edge2_end = poly2[(j + 1) % len(poly2)]
                
                if self._edges_intersect(edge1_start, edge1_end, edge2_start, edge2_end):
                    return True
        
        return False
    
    def _bounding_boxes_overlap(self, poly1: List[List[float]], poly2: List[List[float]]) -> bool:
        """
        Check if bounding boxes of two polygons overlap.
        
        Args:
            poly1: First polygon as list of [lon, lat] coordinate pairs
            poly2: Second polygon as list of [lon, lat] coordinate pairs
            
        Returns:
            True if bounding boxes overlap, False otherwise
        """
        if not poly1 or not poly2:
            return False
        
        # Calculate bounding box for poly1
        min_lon1, max_lon1 = min(p[0] for p in poly1), max(p[0] for p in poly1)
        min_lat1, max_lat1 = min(p[1] for p in poly1), max(p[1] for p in poly1)
        
        # Calculate bounding box for poly2
        min_lon2, max_lon2 = min(p[0] for p in poly2), max(p[0] for p in poly2)
        min_lat2, max_lat2 = min(p[1] for p in poly2), max(p[1] for p in poly2)
        
        # Check for overlap
        return not (max_lon1 < min_lon2 or max_lon2 < min_lon1 or 
                   max_lat1 < min_lat2 or max_lat2 < min_lat1)
    
    def _edges_intersect(self, edge1_start: List[float], edge1_end: List[float], 
                         edge2_start: List[float], edge2_end: List[float]) -> bool:
        """
        Check if two line segments intersect.
        
        Args:
            edge1_start, edge1_end: First line segment endpoints [lon, lat]
            edge2_start, edge2_end: Second line segment endpoints [lon, lat]
            
        Returns:
            True if line segments intersect, False otherwise
        """
        def ccw(A, B, C):
            """Returns true if points A, B, C are counter-clockwise oriented."""
            return (C[1] - A[1]) * (B[0] - A[0]) > (B[1] - A[1]) * (C[0] - A[0])
        
        A, B = edge1_start, edge1_end
        C, D = edge2_start, edge2_end
        
        # Check if line segments intersect
        return ccw(A, C, D) != ccw(B, C, D) and ccw(A, B, C) != ccw(A, B, D)
    
    
    
    
    async def get_hyperedge_details(self, hyperedge_id: str) -> Optional[Dict[str, Any]]:
        """
        Get detailed information about a hyperedge.
        
        Args:
            hyperedge_id: ID of the hyperedge
            
        Returns:
            Dictionary with hyperedge details or None if not found
        """
        if not self._connected:
            logger.error("Not connected to Neo4j")
            return None
        
        try:
            with self.driver.session(database=self.config.database) as session:
                # Get hyperedge details
                result = session.run("""
                    MATCH (h:Hyperedge {id: $id})
                    RETURN h
                """, id=hyperedge_id)
                
                hyperedge_record = result.single()
                if not hyperedge_record:
                    return None
                
                # Get connected nodes
                nodes_result = session.run("""
                    MATCH (h:Hyperedge {id: $id})-[r:CONNECTS]->(n:Node)
                    RETURN n, r.role as role
                """, id=hyperedge_id)
                
                nodes = []
                for record in nodes_result:
                    node_data = dict(record['n'])
                    node_data['role'] = record['role']
                    nodes.append(node_data)
                
                # Get contexts
                contexts_result = session.run("""
                    MATCH (h:Hyperedge {id: $id})-[:VALID_IN]->(c:Context)
                    RETURN c
                """, id=hyperedge_id)
                
                contexts = [dict(record['c']) for record in contexts_result]
                
                return {
                    'hyperedge': dict(hyperedge_record['h']),
                    'nodes': nodes,
                    'contexts': contexts
                }
                
        except Exception as e:
            logger.error(f"Failed to get hyperedge details: {e}")
            return None
    
    async def delete_hyperedge(self, hyperedge_id: str) -> bool:
        """
        Delete a hyperedge and its associated data.
        
        Args:
            hyperedge_id: ID of the hyperedge to delete
            
        Returns:
            True if successful, False otherwise
        """
        if not self._connected:
            logger.error("Not connected to Neo4j")
            return False
        
        try:
            with self.driver.session(database=self.config.database) as session:
                # Safely delete the hyperedge; remove contexts only if orphaned
                session.run("""
                    MATCH (h:Hyperedge {id: $id})
                    OPTIONAL MATCH (h)-[r:VALID_IN]->(c:Context)
                    DELETE r
                    WITH c
                    WHERE c IS NOT NULL AND NOT (c)<-[:VALID_IN]-()
                    DETACH DELETE c
                """, id=hyperedge_id)
                session.run("""
                    MATCH (h:Hyperedge {id: $id})
                    DETACH DELETE h
                """, id=hyperedge_id)
                return True
                
        except Exception as e:
            logger.error(f"Failed to delete hyperedge: {e}")
            return False
    
    async def get_statistics(self) -> Dict[str, Any]:
        """
        Get database statistics.
        
        Returns:
            Dictionary with node counts and other statistics
        """
        if not self._connected:
            logger.error("Not connected to Neo4j")
            return {}
        
        try:
            with self.driver.session(database=self.config.database) as session:
                # Get each statistic separately to avoid UNION issues
                stats = {}
                
                # Node count
                result = session.run("MATCH (n:Node) RETURN count(n) as count")
                stats['node_count'] = result.single()['count']
                
                # Hyperedge count
                result = session.run("MATCH (h:Hyperedge) RETURN count(h) as count")
                stats['hyperedge_count'] = result.single()['count']
                
                # Context count
                result = session.run("MATCH (c:Context) RETURN count(c) as count")
                stats['context_count'] = result.single()['count']
                
                # CONNECTS relationship count
                result = session.run("MATCH ()-[r:CONNECTS]->() RETURN count(r) as count")
                stats['connects_count'] = result.single()['count']
                
                # VALID_IN relationship count
                result = session.run("MATCH ()-[r:VALID_IN]->() RETURN count(r) as count")
                stats['valid_in_count'] = result.single()['count']
                
                return stats
                
        except Exception as e:
            logger.error(f"Failed to get statistics: {e}")
            return {} 