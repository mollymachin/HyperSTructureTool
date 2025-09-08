"""
Cypher query generator for converting structured LLM output to Neo4j queries.
"""

import uuid
import json
import sys
import os
import logging
import textwrap
from typing import Dict, List, Any, Optional
from datetime import datetime
import math
from typing import Any
import hashlib


def cypher_escape(value: Any) -> str:
    # Escape a value for safe inclusion in single-quoted Cypher string literals.
    # Standard Cypher/Neo4j escaping doubles single quotes.
    
    s = "" if value is None else str(value)
    return s.replace("'", "''")


def cypher_quote(value: Any) -> str:
    return "'" + cypher_escape(value) + "'"


def cypher_string_list(values: list[str]) -> str:
    return "[" + ", ".join(cypher_quote(v) for v in values) + "]"


def build_coordinates_cypher(spatial_type: Any, spatial_coordinates: Any, max_points: int = 1000) -> str:
    """Return a Cypher literal for coordinates with optional simplification.

    Points are emitted as Neo4j point({longitude, latitude}).
    Polygons/MultiPolygons are JSON-encoded and stored as strings.
    Very large geometries are simplified by sampling to the total capped points.
    """
    try:
        if spatial_coordinates is None:
            return 'null'

        stype = (str(spatial_type) if spatial_type is not None else '').lower()
        if stype == 'point' and isinstance(spatial_coordinates, list) and len(spatial_coordinates) == 2:
            lon, lat = spatial_coordinates
            if isinstance(lon, (int, float)) and isinstance(lat, (int, float)):
                return f"point({{longitude: {lon}, latitude: {lat}}})"
            return 'null'

        # Simplify polygons by sampling
        def count_points(coords: Any) -> int:
            if isinstance(coords, list) and coords and isinstance(coords[0], (int, float)):
                return 1
            if isinstance(coords, list):
                return sum(count_points(c) for c in coords)
            return 0

        total = count_points(spatial_coordinates)
        if total > max_points:
            # Compute a global sampling factor
            factor = max(2, math.ceil(total / max_points))

            def simplify(coords: Any) -> Any:
                if isinstance(coords, list) and coords and isinstance(coords[0], (int, float)):
                    # A single coordinate pair
                    return coords
                if (
                    isinstance(coords, list)
                    and coords
                    and isinstance(coords[0], list)
                    and coords[0]
                    and isinstance(coords[0][0], (int, float))
                ):
                    ring = coords
                    sampled = ring[::factor] if factor > 1 else ring[:]
                    # Ensure closed ring if original looked closed
                    if sampled and ring and (ring[0] == ring[-1]):
                        if sampled[0] != sampled[-1]:
                            sampled = sampled + [sampled[0]]
                    return sampled
                if isinstance(coords, list):
                    return [simplify(c) for c in coords]
                return coords

            spatial_coordinates = simplify(spatial_coordinates)

        coordinates_json = json.dumps(spatial_coordinates)
        # Guard against extremely long literals
        if len(coordinates_json) > 200000:
            return 'null'
        return f"'{coordinates_json}'"
    except Exception:
        return 'null'

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from utils.temporal_checking import extract_time_range_from_interval

logger = logging.getLogger(__name__)

class CypherGenerator:
    # Generates Cypher queries from structured output.
    
    # Async generator with immediate dispatch
    async def generate_cypher_from_structured_output(self, structured_data: List[Dict[str, Any]], neo4j_storage=None):
        """
        Convert structured data (e.g. LLM output) to Cypher CREATE, MERGE and MATCH statements.
        Async generator that yields each hyperedge query as it's created.
        
        Args:
        structured_data: List[Dict[str, Any]] - Each dict should have a "fact_type" field:
        
        Schema:
        For "temporal_fact" type:
        - subjects: List of subject entities
        - objects: List of object entities  
        - relation_type: The relationship between subjects and objects
        - temporal_intervals: List of time ranges in ISO 8601 format
        - spatial_contexts: List of spatial locations with coordinates
        
        For "state_change_event" type:
        - affected_fact: The fact whose state changes (subjects, objects, relation_type)
        - caused_by: List of lists of facts that cause this fact to be True:
          * OR logic between lists: Any one list can cause the state
          * AND logic within each list: All facts in a list must be true together
          * Empty list [] means no causes (e.g., initial state)
        - causes: List of facts this fact being True causes to happen:
          - triggers_state: Boolean state this will cause
          - additional_required_states: List of facts and the state they must be in for this cause to work
        
        Example structure:
            {
                "fact_type": "temporal_fact",
                "subjects": ["Will", "Molly"],
                "objects": ["cats"],
                "relation_type": "likes",
                "temporal_intervals": [{"start_time": "2020-01-01T00:00:00", "end_time": "2021-12-31T23:59:59"}], # Times in ISO 8601 format
                "spatial_contexts": [{
                    "name": "Imperial College London",
                    "type": "Point",
                    "coordinates": [-0.179359, 51.498711]  # [lon, lat] in degrees formatted to match the Neo4j Point type
                },
                {
                    "name": "MIT",
                    "type": "Polygon",
                    "coordinates": [[
                        [-71.0935, 42.3591],
                        [-71.0917, 42.3591],
                        [-71.0917, 42.3603],
                        [-71.0935, 42.3603],
                        [-71.0935, 42.3591]  # closed ring
                    ]]
                }
            },
            {
                "fact_type": "state_change_event",
                "affected_fact": {
                    "subjects": ["Professor Knottenbelt"],
                    "objects": ["Computer Science"],
                    "relation_type": "teaches"
                },
                "caused_by": [
                    [
                        {
                            "subjects": ["John", "Sarah"],
                            "objects": ["dogs"],
                            "relation_type": "owns",
                            "triggered_by_state": True
                        }
                    ]
                ],
                "causes": [
                    {
                        "subjects": ["students"],
                        "objects": ["Computer Science"],
                        "relation_type": "learns",
                        "triggers_state": True,
                        "additional_required_states": []
                    }
                ]
            },
            {
                "fact_type": "state_change_event",
                "affected_fact": {
                    "subjects": ["students"],
                    "objects": ["Computer Science"],
                    "relation_type": "learns"
                },
                "caused_by": [
                    [
                        {
                            "subjects": ["Professor Knottenbelt"],
                            "objects": ["Computer Science"],
                            "relation_type": "teaches",
                            "triggered_by_state": True
                        },
                        {
                            "subjects": ["students"],
                            "objects": ["motivation"],
                            "relation_type": "has",
                            "triggered_by_state": True
                        }
                    ],
                    [
                        {
                            "subjects": ["online_course"],
                            "objects": ["Computer Science"],
                            "relation_type": "covers",
                            "triggered_by_state": True
                        }
                    ]
                ],
                "causes": []
            }
        ]
        
        Yields:
            Cypher query strings (one per hyperedge)
        """
        try:
            if structured_data is not None:
                # Handle case where structured_data might be a single dict instead of list
                if isinstance(structured_data, dict):
                    structured_data = [structured_data]

                for hyperedge_data in structured_data:
                    fact_type = hyperedge_data.get('fact_type', 'unknown')
                    
                    # TEMPORAL FACTS
                    if fact_type == 'temporal_fact':
                        # Extract data from structured data
                        subjects = hyperedge_data.get('subjects', [])
                        objects = hyperedge_data.get('objects', [])
                        relation_type = hyperedge_data.get("relation_type", "unknown")
                        temporal_intervals = hyperedge_data.get('temporal_intervals', []) # List of ISO 8601 strings
                        spatial_contexts = hyperedge_data.get('spatial_contexts', [])
                    
                        # Check if this should be appended to an existing hyperedge
                        existing_hyperedge = await self._find_appendable_hyperedge(
                            subjects, objects, relation_type, temporal_intervals, spatial_contexts, neo4j_storage
                        )
                        
                        if existing_hyperedge:
                            # Append to existing hyperedge
                            yield await self._generate_append_cypher(
                                existing_hyperedge, subjects, objects, relation_type, 
                                temporal_intervals, spatial_contexts
                            )
                        else:
                            # Create new hyperedge (existing logic)
                            # Generate unique IDs
                            hyperedge_id = f"he_{uuid.uuid4().hex[:8]}"
                            
                            # Build Cypher query
                            cypher_parts = []
                            params: Dict[str, Any] = {}
                            
                            # 1. Create entity nodes for subjects (MERGE by id only)
                            for i, subject in enumerate(subjects):
                                param_key = f"subject_{i}_id"
                                params[param_key] = subject
                                cypher_parts.append(
                                    f"MERGE (subject_{i}:Node {{id: ${param_key}}})"
                                )
                                cypher_parts.append(
                                    f"SET subject_{i}.type = 'entity'"
                                )
                        
                            # 2. Create entity nodes for objects (if any exist)
                            if objects:
                                for i, obj in enumerate(objects):
                                    param_key = f"object_{i}_id"
                                    params[param_key] = obj
                                    cypher_parts.append(
                                        f"MERGE (object_{i}:Node {{id: ${param_key}}})"
                                    )
                                    cypher_parts.append(
                                        f"SET object_{i}.type = 'entity'"
                                    )
                        
                            # 3/4. Create context nodes for each temporal & spatial interval (Cartesian product)
                            # Want one context node per combination of temporal & spatial interval
                            context_nodes = []
                            # params declared above
                            context_ids_for_key: List[str] = []
                            for i, interval in enumerate(temporal_intervals):
                                for j, spatial_ctx in enumerate(spatial_contexts if spatial_contexts else [{'name': 'unknown', 'type': 'unknown', 'coordinates': None}]):
                                    # Deterministic global context id for de-duplication
                                    start_time = interval.get('start_time', 'null') # Neo4j optimises temporal queries on ISO strs
                                    end_time = interval.get('end_time', 'null')
                                    
                                    # New format: spatial_ctx is a dict with name, type, coordinates
                                    spatial_name = spatial_ctx.get('name', 'unknown')
                                    spatial_type = spatial_ctx.get('type', 'unknown')
                                    spatial_coordinates = spatial_ctx.get('coordinates', None)
                                    
                                    # Escape the name and type
                                    escaped_spatial_name = cypher_escape(spatial_name) if spatial_name else 'unknown'
                                    escaped_spatial_type = cypher_escape(spatial_type) if spatial_type else 'unknown'
                                    
                                    # Handle coordinates - use Neo4j Point type for simple coordinates, JSON for complex geometries
                                    if spatial_coordinates is not None:
                                        if spatial_type.lower() == 'point' and isinstance(spatial_coordinates, list) and len(spatial_coordinates) == 2:
                                            # Use Neo4j Point type for simple 2D points, json for polygons as not directly supported
                                            try:
                                                lon, lat = spatial_coordinates
                                                if isinstance(lon, (int, float)) and isinstance(lat, (int, float)):
                                                    coordinates_cypher = f"point({{longitude: {lon}, latitude: {lat}}})"
                                                else:
                                                    coordinates_cypher = 'null'
                                            except (ValueError, TypeError):
                                                coordinates_cypher = 'null'
                                        else:
                                            # Use JSON string for complex geometries (polygons, 3D points etc)
                                            try:
                                                def simplify_coords(coords: Any, max_points: int = 800) -> Any:
                                                    # Simplify nested coordinate arrays by sampling to cap total points
                                                    try:
                                                        # Count total coordinate pairs conservatively
                                                        def flatten_count(c):
                                                            if isinstance(c, list) and c and isinstance(c[0], (int, float)):
                                                                return 1
                                                            if isinstance(c, list):
                                                                return sum(flatten_count(x) for x in c)
                                                            return 0
                                                        total = flatten_count(coords)
                                                        if total <= max_points:
                                                            return coords
                                                        # Sampling function for a single ring (list of [lon, lat])
                                                        def sample_ring(ring):
                                                            step = max(1, math.ceil(len(ring) * 1.0 / max(1, max_points // 4)))
                                                            return ring[::step]
                                                        def recurse(c):
                                                            if isinstance(c, list) and c and isinstance(c[0], list) and c and len(c) > 0 and isinstance(c[0][0], (int, float)):
                                                                return sample_ring(c)
                                                            if isinstance(c, list):
                                                                return [recurse(x) for x in c]
                                                            return c
                                                        return recurse(coords)
                                                    except Exception:
                                                        return coords
                                                simplified = simplify_coords(spatial_coordinates)
                                                coordinates_json = json.dumps(simplified)
                                                if len(coordinates_json) > 200000:
                                                    coordinates_cypher = 'null'
                                                else:
                                                    coordinates_cypher = f"'{coordinates_json}'"
                                            except (TypeError, ValueError):
                                                coordinates_cypher = 'null'
                                    else:
                                        coordinates_cypher = 'null'
                                    
                                    spatial_name_quoted = f"'{escaped_spatial_name}'"
                                    spatial_type_quoted = f"'{escaped_spatial_type}'"
                                    
                                    
                                    # Properly quote temporal values for Cypher
                                    from_time_param = f"from_time_{i}_{j}"
                                    to_time_param = f"to_time_{i}_{j}"
                                    params[from_time_param] = None if (start_time in (None, '', 'null')) else start_time
                                    params[to_time_param] = None if (end_time in (None, '', 'null')) else end_time
                                    
                                    # Build deterministic id components including coordinates
                                    start_key = start_time if (start_time not in (None, '', 'null')) else '__NULL__'
                                    end_key = end_time if (end_time not in (None, '', 'null')) else '__NULL__'
                                    # Coordinates signature for identity
                                    if spatial_type.lower() == 'point' and spatial_coordinates is not None and isinstance(spatial_coordinates, list) and len(spatial_coordinates) == 2:
                                        try:
                                            lon_sig = round(float(spatial_coordinates[0]), 6)
                                            lat_sig = round(float(spatial_coordinates[1]), 6)
                                            coord_sig = f"pt:{lon_sig}:{lat_sig}"
                                        except Exception:
                                            coord_sig = 'geo:NULL'
                                    else:
                                        try:
                                            coords_min = json.dumps(spatial_coordinates, separators=(',', ':'), ensure_ascii=False)
                                        except Exception:
                                            coords_min = 'null'
                                        coord_sig = 'geo:' + hashlib.sha1((coords_min or 'null').encode('utf-8')).hexdigest()[:16]
                                    key_str = f"{start_key}|{end_key}|{escaped_spatial_name}|{escaped_spatial_type}|{coord_sig}"
                                    context_id = "ctx_" + hashlib.sha1(key_str.encode('utf-8')).hexdigest()[:16]
                                    cypher_parts.append(
                                        f"MERGE (context_{i}_{j}:Context {{id: '{context_id}'}})"
                                    )
                                    context_ids_for_key.append(context_id)
                                    # Parameterise polygon strings to avoid oversized Cypher message
                                    use_param = coordinates_cypher.startswith("'") and coordinates_cypher.endswith("'")
                                    # Parameterise location fields and times
                                    loc_param = f"loc_name_{i}_{j}"
                                    stype_param = f"stype_{i}_{j}"
                                    params[loc_param] = spatial_name
                                    params[stype_param] = spatial_type
                                    if use_param:
                                        param_key = f"coords_{i}_{j}"
                                        params[param_key] = coordinates_cypher.strip("'")
                                        cypher_parts.append(
                                            f"ON CREATE SET context_{i}_{j}.from_time = ${from_time_param}, "
                                            f"context_{i}_{j}.to_time = ${to_time_param}, "
                                            f"context_{i}_{j}.location_name = ${loc_param}, "
                                            f"context_{i}_{j}.spatial_type = ${stype_param}, "
                                            f"context_{i}_{j}.coordinates = ${param_key}, "
                                            f"context_{i}_{j}.certainty = 1.0"
                                        )
                                    else:
                                        cypher_parts.append(
                                            f"ON CREATE SET context_{i}_{j}.from_time = ${from_time_param}, "
                                            f"context_{i}_{j}.to_time = ${to_time_param}, "
                                            f"context_{i}_{j}.location_name = ${loc_param}, "
                                            f"context_{i}_{j}.spatial_type = ${stype_param}, "
                                            f"context_{i}_{j}.coordinates = {coordinates_cypher}, "
                                            f"context_{i}_{j}.certainty = 1.0"
                                        )
                                    context_nodes.append(f"context_{i}_{j}")

                            # 3. Create or reuse hyperedge node using deterministic content-based id (aka deduplication)
                            entity_count = len(subjects) + len(objects)
                            escaped_relation = cypher_escape(relation_type)
                            sorted_subjects = [cypher_escape(s) for s in sorted(subjects)]
                            sorted_objects = [cypher_escape(o) for o in sorted(objects)]
                            key_components = [
                                escaped_relation,
                                "|".join(sorted_subjects),
                                "|".join(sorted_objects),
                                "|".join(sorted(sorted(set(context_ids_for_key))))
                            ]
                            hyperedge_key_str = "||".join(key_components)
                            deterministic_he_id = "he_" + hashlib.sha1(hyperedge_key_str.encode('utf-8')).hexdigest()[:16]
                            cypher_parts.append(
                                f"MERGE (hyperedge:Hyperedge {{id: '{deterministic_he_id}'}})"
                            )
                            params['relation_type'] = relation_type
                            cypher_parts.append(
                                f"ON CREATE SET hyperedge.relation_type = $relation_type, hyperedge.entity_count = {entity_count}"
                            )
                            
                            # 5. Create CONNECTS relationships from hyperedge to subjects
                            for i, subject in enumerate(subjects):
                                cypher_parts.append(
                                    f"MERGE (hyperedge)-[:CONNECTS {{role: 'subject'}}]->(subject_{i})"
                                )
                            
                            # 6. Create CONNECTS relationships from hyperedge to objects (if any exist)
                            if objects:
                                for i, obj in enumerate(objects):
                                    cypher_parts.append(
                                        f"MERGE (hyperedge)-[:CONNECTS {{role: 'object'}}]->(object_{i})"
                                    )
                            
                            # 7. Create VALID_IN relationships from hyperedge to contexts
                            for context_node in context_nodes:
                                cypher_parts.append(
                                    f"MERGE (hyperedge)-[:VALID_IN]->({context_node})"
                                )
                            
                            # Yield the complete query for this hyperedge
                            complete_query = "\n".join(cypher_parts)
                            yield complete_query, params


                    # STATE CHANGE EVENTS    
                    elif fact_type == 'state_change_event':
                        try:
                            # Extract state change event data
                            affected_fact = hyperedge_data.get('affected_fact', {})
                            caused_by = hyperedge_data.get('caused_by', [])
                            causes = hyperedge_data.get('causes', [])
                            
                            # Validate required fields
                            if not affected_fact or 'subjects' not in affected_fact or 'objects' not in affected_fact or 'relation_type' not in affected_fact:
                                logger.warning(f"Invalid affected_fact structure for state change event: {affected_fact}")
                                continue
                            
                            if not affected_fact.get('subjects') or not affected_fact.get('relation_type'):
                                logger.warning(f"Empty subjects or relation_type in affected_fact: {affected_fact}")
                                continue
                            
                            state_change_id = f"sce_{uuid.uuid4().hex[:8]}"
                            
                            # Build Cypher query for state change event
                            cypher_parts = []
                            params: Dict[str, Any] = {}
                            
                            # 1. Find the affected fact by content-based matching
                            affected_subjects = affected_fact.get('subjects', [])
                            affected_objects = affected_fact.get('objects', [])
                            affected_relation = affected_fact.get('relation_type', '')
                            
                            # Parameterize values for Cypher
                            params['affected_relation'] = affected_relation
                            params['affected_subjects'] = affected_subjects
                            params['affected_objects'] = affected_objects
                            
                            # 2. Create the state change event and link it to the affected fact
                            if affected_objects:
                                # Case: objects exist, match both subjects and objects
                                cypher_parts.append(f"""
                                    MATCH (h:Hyperedge {{relation_type: $affected_relation}})
                                    MATCH (h)-[:CONNECTS {{role: 'subject'}}]->(s:Node)
                                    WITH h, collect(DISTINCT s.id) AS subjIds
                                    WHERE size(subjIds) = {len(affected_subjects)}
                                    AND all(x IN subjIds WHERE x IN $affected_subjects)
                                    AND all(x IN $affected_subjects WHERE x IN subjIds)
                                    MATCH (h)-[:CONNECTS {{role: 'object'}}]->(o:Node)
                                    WITH h, subjIds, collect(DISTINCT o.id) AS objIds
                                    WHERE size(objIds) = {len(affected_objects)}
                                    AND all(x IN objIds WHERE x IN $affected_objects)
                                    AND all(x IN $affected_objects WHERE x IN objIds)
                                    CREATE (sce:StateChangeEvent {{id: '{state_change_id}'}})
                                    CREATE (sce)-[:AFFECTS_FACT]->(h)
                                """.strip())
                            else:
                                # Case: no objects (intransitive verb like "dies")
                                cypher_parts.append(f"""
                                    MATCH (h:Hyperedge {{relation_type: $affected_relation}})
                                    MATCH (h)-[:CONNECTS {{role: 'subject'}}]->(s:Node)
                                    WITH h, collect(DISTINCT s.id) AS subjIds
                                    WHERE size(subjIds) = {len(affected_subjects)}
                                    AND all(x IN subjIds WHERE x IN $affected_subjects)
                                    AND all(x IN $affected_subjects WHERE x IN subjIds)
                                    AND NOT EXISTS((h)-[:CONNECTS {{role: 'object'}}]->())
                                    CREATE (sce:StateChangeEvent {{id: '{state_change_id}'}})
                                    CREATE (sce)-[:AFFECTS_FACT]->(h)
                                """.strip())
                            
                            # 3. Handle caused_by relationships (what causes this fact to be True)
                            # caused_by is a list of lists: [[A, B], [C]] means "(A AND B) OR C"
                            # Each inner list represents facts that must ALL be true together (AND logic)
                            # Different inner lists represent alternative ways to cause the state (OR logic)
                            if caused_by:
                                for cause_group_idx, cause_group in enumerate(caused_by):
                                    # Each cause_group is a list of facts that together cause this fact to be True
                                    # Empty list means no causes (e.g. initial state)
                                    if not cause_group:
                                        continue
                                        
                                    for cause_idx, cause in enumerate(cause_group):
                                        cause_subjects = cause.get('subjects', [])
                                        cause_objects = cause.get('objects', [])
                                        cause_relation = cause.get('relation_type', '')
                                        triggered_by_state = cause.get('triggered_by_state', True)
                                        
                                        # Parameterize cause matching
                                        p_rel = f"cause_rel_{cause_group_idx}_{cause_idx}"
                                        p_subj = f"cause_subjs_{cause_group_idx}_{cause_idx}"
                                        p_obj = f"cause_objs_{cause_group_idx}_{cause_idx}"
                                        params[p_rel] = cause_relation
                                        params[p_subj] = cause_subjects
                                        params[p_obj] = cause_objects
                                        
                                        if cause_objects:
                                            # Case: cause has objects
                                            cypher_parts.append(f"""
                                                WITH sce
                                                MATCH (hc_{cause_group_idx}_{cause_idx}:Hyperedge {{relation_type: ${p_rel}}})
                                                MATCH (hc_{cause_group_idx}_{cause_idx})-[:CONNECTS {{role: 'subject'}}]->(sc_{cause_group_idx}_{cause_idx}:Node)
                                                WITH sce, hc_{cause_group_idx}_{cause_idx}, collect(DISTINCT sc_{cause_group_idx}_{cause_idx}.id) AS causeSubjIds
                                                WHERE size(causeSubjIds) = {len(cause_subjects)}
                                                AND all(x IN causeSubjIds WHERE x IN ${p_subj})
                                                AND all(x IN ${p_subj} WHERE x IN causeSubjIds)
                                                MATCH (hc_{cause_group_idx}_{cause_idx})-[:CONNECTS {{role: 'object'}}]->(oc_{cause_group_idx}_{cause_idx}:Node)
                                                WITH sce, hc_{cause_group_idx}_{cause_idx}, causeSubjIds, collect(DISTINCT oc_{cause_group_idx}_{cause_idx}.id) AS causeObjIds
                                                WHERE size(causeObjIds) = {len(cause_objects)}
                                                AND all(x IN causeObjIds WHERE x IN ${p_obj})
                                                AND all(x IN ${p_obj} WHERE x IN causeObjIds)
                                                CREATE (hc_{cause_group_idx}_{cause_idx})-[:CAUSES_STATE {{required_state: {str(triggered_by_state).lower()}}}]->(sce)
                                            """.strip())
                                        else:
                                            # Case: cause has no objects (intransitive verb)
                                            cypher_parts.append(f"""
                                                WITH sce
                                                MATCH (hc_{cause_group_idx}_{cause_idx}:Hyperedge {{relation_type: ${p_rel}}})
                                                MATCH (hc_{cause_group_idx}_{cause_idx})-[:CONNECTS {{role: 'subject'}}]->(sc_{cause_group_idx}_{cause_idx}:Node)
                                                WITH sce, hc_{cause_group_idx}_{cause_idx}, collect(DISTINCT sc_{cause_group_idx}_{cause_idx}.id) AS causeSubjIds
                                                WHERE size(causeSubjIds) = {len(cause_subjects)}
                                                AND all(x IN causeSubjIds WHERE x IN ${p_subj})
                                                AND all(x IN ${p_subj} WHERE x IN causeSubjIds)
                                                AND NOT EXISTS((hc_{cause_group_idx}_{cause_idx})-[:CONNECTS {{role: 'object'}}]->())
                                                CREATE (hc_{cause_group_idx}_{cause_idx})-[:CAUSES_STATE {{required_state: {str(triggered_by_state).lower()}}}]->(sce)
                                            """.strip())
                            
                            # 4. Handle causes relationships (what this fact being True causes to happen)
                            if causes:
                                for cause_idx, effect in enumerate(causes):
                                    effect_subjects = effect.get('subjects', [])
                                    effect_objects = effect.get('objects', [])
                                    effect_relation = effect.get('relation_type', '')
                                    triggers_state = effect.get('triggers_state', True)
                                    additional_required_states = effect.get('additional_required_states', [])
                                    
                                    p_rel = f"effect_rel_{cause_idx}"
                                    p_subj = f"effect_subjs_{cause_idx}"
                                    p_obj = f"effect_objs_{cause_idx}"
                                    params[p_rel] = effect_relation
                                    params[p_subj] = effect_subjects
                                    params[p_obj] = effect_objects
                                    
                                    if effect_objects:
                                        # Case: effect has objects
                                        cypher_parts.append(f"""
                                            WITH sce
                                            MATCH (he_{cause_idx}:Hyperedge {{relation_type: ${p_rel}}})
                                            MATCH (he_{cause_idx})-[:CONNECTS {{role: 'subject'}}]->(se_{cause_idx}:Node)
                                            WITH sce, he_{cause_idx}, collect(DISTINCT se_{cause_idx}.id) AS effectSubjIds
                                            WHERE size(effectSubjIds) = {len(effect_subjects)}
                                            AND all(x IN effectSubjIds WHERE x IN ${p_subj})
                                            AND all(x IN ${p_subj} WHERE x IN effectSubjIds)
                                            MATCH (he_{cause_idx})-[:CONNECTS {{role: 'object'}}]->(oe_{cause_idx}:Node)
                                            WITH sce, he_{cause_idx}, effectSubjIds, collect(DISTINCT oe_{cause_idx}.id) AS effectObjIds
                                            WHERE size(effectObjIds) = {len(effect_objects)}
                                            AND all(x IN effectObjIds WHERE x IN ${p_obj})
                                            AND all(x IN ${p_obj} WHERE x IN effectObjIds)
                                            CREATE (sce)-[:CAUSES_STATE {{triggers_state: {str(triggers_state).lower()}}}]->(he_{cause_idx})
                                        """.strip())
                                    else:
                                        # Case: effect has no objects (intransitive verb)
                                        cypher_parts.append(f"""
                                            WITH sce
                                            MATCH (he_{cause_idx}:Hyperedge {{relation_type: ${p_rel}}})
                                            MATCH (he_{cause_idx})-[:CONNECTS {{role: 'subject'}}]->(se_{cause_idx}:Node)
                                            WITH sce, he_{cause_idx}, collect(DISTINCT se_{cause_idx}.id) AS effectSubjIds
                                            WHERE size(effectSubjIds) = {len(effect_subjects)}
                                            AND all(x IN effectSubjIds WHERE x IN ${p_subj})
                                            AND all(x IN ${p_subj} WHERE x IN effectSubjIds)
                                            AND NOT EXISTS((he_{cause_idx})-[:CONNECTS {{role: 'object'}}]->())
                                            CREATE (sce)-[:CAUSES_STATE {{triggers_state: {str(triggers_state).lower()}}}]->(he_{cause_idx})
                                        """.strip())
                                    
                                    # Handle additional required states if any
                                    if additional_required_states:
                                        for req_state_idx, req_state in enumerate(additional_required_states):
                                            req_subjects = req_state.get('subjects', [])
                                            req_objects = req_state.get('objects', [])
                                            req_relation = req_state.get('relation_type', '')
                                            req_state_value = req_state.get('state', True)
                                            
                                            pr_rel = f"req_rel_{cause_idx}_{req_state_idx}"
                                            pr_subj = f"req_subjs_{cause_idx}_{req_state_idx}"
                                            pr_obj = f"req_objs_{cause_idx}_{req_state_idx}"
                                            params[pr_rel] = req_relation
                                            params[pr_subj] = req_subjects
                                            params[pr_obj] = req_objects
                                            
                                            if req_objects:
                                                # Case: required state has objects
                                                cypher_parts.append(f"""
                                                    WITH sce, he_{cause_idx}
                                                    MATCH (req_{cause_idx}_{req_state_idx}:Hyperedge {{relation_type: ${pr_rel}}})
                                                    MATCH (req_{cause_idx}_{req_state_idx})-[:CONNECTS {{role: 'subject'}}]->(reqs_{cause_idx}_{req_state_idx}:Node)
                                                    WITH sce, he_{cause_idx}, req_{cause_idx}_{req_state_idx}, collect(DISTINCT reqs_{cause_idx}_{req_state_idx}.id) AS reqSubjIds
                                                    WHERE size(reqSubjIds) = {len(req_subjects)}
                                                    AND all(x IN reqSubjIds WHERE x IN ${pr_subj})
                                                    AND all(x IN ${pr_subj} WHERE x IN reqSubjIds)
                                                    MATCH (req_{cause_idx}_{req_state_idx})-[:CONNECTS {{role: 'object'}}]->(reqo_{cause_idx}_{req_state_idx}:Node)
                                                    WITH sce, he_{cause_idx}, req_{cause_idx}_{req_state_idx}, reqSubjIds, collect(DISTINCT reqo_{cause_idx}_{req_state_idx}.id) AS reqObjIds
                                                    WHERE size(reqObjIds) = {len(req_objects)}
                                                    AND all(x IN reqObjIds WHERE x IN ${pr_obj})
                                                    AND all(x IN ${pr_obj} WHERE x IN reqObjIds)
                                                    CREATE (sce)-[:REQUIRES_STATE {{required_state: {str(req_state_value).lower()}}}]->(req_{cause_idx}_{req_state_idx})
                                                """.strip())
                                            else:
                                                # Case: required state has no objects (intransitive verb)
                                                cypher_parts.append(f"""
                                                    WITH sce, he_{cause_idx}
                                                    MATCH (req_{cause_idx}_{req_state_idx}:Hyperedge {{relation_type: ${pr_rel}}})
                                                    MATCH (req_{cause_idx}_{req_state_idx})-[:CONNECTS {{role: 'subject'}}]->(reqs_{cause_idx}_{req_state_idx}:Node)
                                                    WITH sce, he_{cause_idx}, req_{cause_idx}_{req_state_idx}, collect(DISTINCT reqs_{cause_idx}_{req_state_idx}.id) AS reqSubjIds
                                                    WHERE size(reqSubjIds) = {len(req_subjects)}
                                                    AND all(x IN reqSubjIds WHERE x IN ${pr_subj})
                                                    AND all(x IN ${pr_subj} WHERE x IN reqSubjIds)
                                                    AND NOT EXISTS((req_{cause_idx}_{req_state_idx})-[:CONNECTS {{role: 'object'}}]->())
                                                    CREATE (sce)-[:REQUIRES_STATE {{required_state: {str(req_state_value).lower()}}}]->(req_{cause_idx}_{req_state_idx})
                                                """.strip())
                                                        
                            # Yield the complete query for this state change event
                            complete_query = "\n".join(cypher_parts)
                            yield complete_query, params
                            
                        except Exception as e:
                            logger.error(f"Error processing state change event: {e}")
                            continue
        
                    # MODIFICATIONS
                    elif fact_type == 'modification':
                        try:
                            # Expected schema from extract_structured_modifications
                            # {
                            #   "fact_type": "modification",
                            #   "affected_fact": {"fact_type": "temporal_fact", "subjects": [...], "objects": [...], "relation_type": "..."},
                            #   "modify_fields_to": { ... }
                            # }

                            affected_fact = hyperedge_data.get('affected_fact', {})
                            modify_fields_to = hyperedge_data.get('modify_fields_to', {})

                            if not affected_fact or not modify_fields_to:
                                logger.warning("Modification missing affected_fact or modify_fields_to therefore skipping")
                                continue

                            affected_subjects = affected_fact.get('subjects', [])
                            affected_objects = affected_fact.get('objects', [])
                            affected_relation = affected_fact.get('relation_type', '')

                            if not affected_subjects or not affected_relation:
                                logger.warning(f"Invalid affected_fact for modification: {affected_fact}")
                                continue

                            # Parameterize inputs
                            cypher_parts = []
                            params: Dict[str, Any] = {}

                            # Match the existing hyperedge by exact subjects/objects set and relation_type
                            if affected_objects:
                                # Case: affected fact has objects
                                params['mod_rel'] = affected_relation
                                params['mod_subjs'] = affected_subjects
                                params['mod_objs'] = affected_objects
                                cypher_parts.append(f"""
                                    MATCH (h:Hyperedge {{relation_type: $mod_rel}})
                                    MATCH (h)-[:CONNECTS {{role: 'subject'}}]->(s:Node)
                                    WITH h, collect(DISTINCT s.id) AS subjIds
                                    WHERE size(subjIds) = {len(affected_subjects)}
                                      AND all(x IN subjIds WHERE x IN $mod_subjs)
                                      AND all(x IN $mod_subjs WHERE x IN subjIds)
                                    MATCH (h)-[:CONNECTS {{role: 'object'}}]->(o:Node)
                                    WITH h, subjIds, collect(DISTINCT o.id) AS objIds
                                    WHERE size(objIds) = {len(affected_objects)}
                                      AND all(x IN objIds WHERE x IN $mod_objs)
                                      AND all(x IN $mod_objs WHERE x IN objIds)
                                """.strip())
                            else:
                                # Case: affected fact has no objects (intransitive verb)
                                params['mod_rel'] = affected_relation
                                params['mod_subjs'] = affected_subjects
                                cypher_parts.append(f"""
                                    MATCH (h:Hyperedge {{relation_type: $mod_rel}})
                                    MATCH (h)-[:CONNECTS {{role: 'subject'}}]->(s:Node)
                                    WITH h, collect(DISTINCT s.id) AS subjIds
                                    WHERE size(subjIds) = {len(affected_subjects)}
                                      AND all(x IN subjIds WHERE x IN $mod_subjs)
                                      AND all(x IN $mod_subjs WHERE x IN subjIds)
                                    AND NOT EXISTS((h)-[:CONNECTS {{role: 'object'}}]->())
                                """.strip())

                            # 1) relation_type change
                            if 'relation_type' in modify_fields_to:
                                params['new_rel'] = str(modify_fields_to['relation_type'])
                                cypher_parts.append(
                                    f"SET h.relation_type = $new_rel"
                                )

                            # 2) Context rewiring (safe for shared contexts)
                            # Extract potential new temporal values
                            new_from = None
                            new_to = None
                            if 'temporal_intervals' in modify_fields_to:
                                intervals = modify_fields_to.get('temporal_intervals') or []
                                for interval in intervals:
                                    if isinstance(interval, dict):
                                        if new_from is None and interval.get('start_time') not in (None, ''):
                                            new_from = interval.get('start_time')
                                        if new_to is None and interval.get('end_time') not in (None, ''):
                                            new_to = interval.get('end_time')

                            # Extract potential new spatial values
                            new_name = None
                            new_type = None
                            new_coords = None
                            if 'spatial_contexts' in modify_fields_to:
                                spatial_contexts = modify_fields_to.get('spatial_contexts') or []
                                if spatial_contexts and isinstance(spatial_contexts[0], dict):
                                    sc0 = spatial_contexts[0]
                                    new_name = sc0.get('name')
                                    new_type = sc0.get('type')
                                    new_coords = sc0.get('coordinates')

                            # If both temporal and spatial provided, rewire all contexts to a single new Context
                            if (new_from is not None or new_to is not None) and (new_name is not None or new_type is not None or new_coords is not None):
                                from_time_lit = 'null' if (new_from in (None, '', 'null')) else cypher_quote(new_from)
                                to_time_lit = 'null' if (new_to in (None, '', 'null')) else cypher_quote(new_to)
                                escaped_name = cypher_escape(new_name or 'unknown')
                                escaped_type = cypher_escape(new_type or 'unknown')
                                # Coordinates literal and signature
                                if isinstance(new_coords, list) and len(new_coords) == 2 and str(new_type or '').lower() == 'point' and isinstance(new_coords[0], (int, float)) and isinstance(new_coords[1], (int, float)):
                                    lon_sig = round(float(new_coords[0]), 6)
                                    lat_sig = round(float(new_coords[1]), 6)
                                    coordinates_lit = f"point({{longitude: {new_coords[0]}, latitude: {new_coords[1]}}})"
                                    coord_sig = f"pt:{lon_sig}:{lat_sig}"
                                else:
                                    try:
                                        coords_min = json.dumps(new_coords, separators=(',', ':'), ensure_ascii=False) if new_coords is not None else 'null'
                                    except Exception:
                                        coords_min = 'null'
                                    coordinates_lit = f"'{coords_min.replace("'", "\\'")}'" if new_coords is not None else 'null'
                                    coord_sig = 'geo:' + hashlib.sha1((coords_min or 'null').encode('utf-8')).hexdigest()[:16]
                                start_key = new_from if (new_from not in (None, '', 'null')) else '__NULL__'
                                end_key = new_to if (new_to not in (None, '', 'null')) else '__NULL__'
                                key_str = f"{start_key}|{escaped_name}|{escaped_type}|{coord_sig}|{end_key}"
                                new_ctx_id = "ctx_" + hashlib.sha1(key_str.encode('utf-8')).hexdigest()[:16]

                                # Create/attach new context and rewire
                                cypher_parts.append(f"MERGE (new_ctx:Context {{id: '{new_ctx_id}'}})")
                                cypher_parts.append(f"ON CREATE SET new_ctx.from_time = {from_time_lit}, new_ctx.to_time = {to_time_lit}, new_ctx.location_name = '{escaped_name}', new_ctx.spatial_type = '{escaped_type}', new_ctx.coordinates = {coordinates_lit}, new_ctx.certainty = 1.0")
                                cypher_parts.append("MERGE (h)-[:VALID_IN]->(new_ctx)")
                                cypher_parts.append("OPTIONAL MATCH (h)-[r_old:VALID_IN]->(oldC:Context) WHERE oldC <> new_ctx DELETE r_old")
                                cypher_parts.append("WITH oldC WHERE oldC IS NOT NULL AND NOT (oldC)<-[:VALID_IN]-() DETACH DELETE oldC")
                            else:
                                # Fallback: apply property updates to attached Contexts (legacy)
                                # Temporal-only update
                                if new_from is not None or new_to is not None:
                                    set_clauses = []
                                    cypher_parts.append("MATCH (h)-[:VALID_IN]->(c:Context)")
                                    if new_from is not None:
                                        params['new_from'] = new_from
                                        set_clauses.append(f"c.from_time = $new_from")
                                    if new_to is not None:
                                        if str(new_to).lower() == 'null':
                                            set_clauses.append("c.to_time = null")
                                        else:
                                            params['new_to'] = new_to
                                            set_clauses.append(f"c.to_time = $new_to")
                                    if set_clauses:
                                        cypher_parts.append("SET " + ", ".join(set_clauses))

                                # Spatial-only update
                                if new_name is not None or new_type is not None or new_coords is not None:
                                    assignments = []
                                    cypher_parts.append("MATCH (h)-[:VALID_IN]->(c2:Context)")
                                    if new_name is not None:
                                        params['sp_new_name'] = new_name
                                        assignments.append(f"c2.location_name = $sp_new_name")
                                    if new_type is not None:
                                        params['sp_new_type'] = new_type
                                        assignments.append(f"c2.spatial_type = $sp_new_type")
                                    if new_coords is not None:
                                        if isinstance(new_coords, list) and len(new_coords) == 2 and isinstance(new_coords[0], (int, float)) and isinstance(new_coords[1], (int, float)) and str(new_type or '').lower() == 'point':
                                            assignments.append(f"c2.coordinates = point({{longitude: {new_coords[0]}, latitude: {new_coords[1]}}})")
                                        else:
                                            try:
                                                coords_json = json.dumps(new_coords)
                                                params['sp_new_coords'] = coords_json
                                                assignments.append(f"c2.coordinates = $sp_new_coords")
                                            except Exception:
                                                assignments.append("c2.coordinates = null")
                                    if assignments:
                                        cypher_parts.append("SET " + ", ".join(assignments))

                            # 4) subjects/objects rewiring
                            rewire_subjects = 'subjects' in modify_fields_to
                            rewire_objects = 'objects' in modify_fields_to
                            if rewire_subjects or rewire_objects:
                                # Rewire subjects
                                if rewire_subjects:
                                    new_subjects = modify_fields_to.get('subjects') or []
                                    esc_new_subj = [cypher_escape(s) for s in new_subjects]
                                    cypher_parts.append("""
                                        OPTIONAL MATCH (h)-[r_sub:CONNECTS {role: 'subject'}]->(oldS:Node)
                                        DELETE r_sub
                                    """.strip())
                                    # MERGE new subject nodes and connect
                                    for i, s_val in enumerate(esc_new_subj):
                                        p = f"ns_{i}_id"
                                        params[p] = new_subjects[i]
                                        cypher_parts.append(
                                            f"MERGE (ns_{i}:Node {{id: ${p}}})"
                                        )
                                        cypher_parts.append(
                                            f"SET ns_{i}.type = 'entity'"
                                        )
                                        cypher_parts.append(
                                            f"CREATE (h)-[:CONNECTS {{role: 'subject'}}]->(ns_{i})"
                                        )
                                # Rewire objects
                                if rewire_objects:
                                    new_objects = modify_fields_to.get('objects') or []
                                    esc_new_obj = [cypher_escape(o) for o in new_objects]
                                    cypher_parts.append("""
                                        OPTIONAL MATCH (h)-[r_obj:CONNECTS {role: 'object'}]->(oldO:Node)
                                        DELETE r_obj
                                    """.strip())
                                    for i, o_val in enumerate(esc_new_obj):
                                        p = f"no_{i}_id"
                                        params[p] = new_objects[i]
                                        cypher_parts.append(
                                            f"MERGE (no_{i}:Node {{id: ${p}}})"
                                        )
                                        cypher_parts.append(
                                            f"SET no_{i}.type = 'entity'"
                                        )
                                        cypher_parts.append(
                                            f"CREATE (h)-[:CONNECTS {{role: 'object'}}]->(no_{i})"
                                        )

                                # Update entity_count to reflect current connections
                                cypher_parts.append("""
                                    WITH h
                                    MATCH (h)-[:CONNECTS]->(n:Node)
                                    WITH h, count(n) as ec
                                    SET h.entity_count = ec
                                """.strip())

                            complete_query = "\n".join(cypher_parts)
                            yield complete_query, params

                        except Exception as e:
                            logger.error(f"Error processing modification: {e}")
                            continue

        except Exception as e:
            logger.error(f"Error transforming structured data to Cypher: {e}")
            # Yield empty string as fallback
            yield ""
    
    async def _find_appendable_hyperedge(self, subjects, objects, relation_type, temporal_intervals, spatial_contexts, neo4j_storage=None):
        """
        Find an existing hyperedge that can be appended to based on matching criteria.
        Returns the hyperedge data if found, None otherwise.
        
        Matching criteria:
        1. (relation_type, objects, contexts) match - append new subjects
        2. (subjects, relation_type, objects) match - append new contexts
        3. (subjects, relation_type, contexts) match - append new objects
        """
        if not neo4j_storage:
            return None
            
        try:
            # Prepare parameterised inputs for Cypher
            relation_param = relation_type
            subjects_list = list(subjects) if subjects else []
            objects_list = list(objects) if objects else []
            
            # Extract context information for matching
            temporal_times = []
            for interval in temporal_intervals:
                start_time = interval.get('start_time', None)
                end_time = interval.get('end_time', None)
                temporal_times.append([start_time, end_time])

            # Coalesce lists to compare nulls safely in Cypher by mapping None -> '__NULL__'
            temporal_times_coalesced = [[(a if a is not None else '__NULL__'), (b if b is not None else '__NULL__')] for a, b in temporal_times]
            
            spatial_names = []
            for spatial_ctx in spatial_contexts:
                if isinstance(spatial_ctx, dict):
                    spatial_names.append(spatial_ctx.get('name', 'unknown'))
                else:
                    spatial_names.append(str(spatial_ctx))
            spatial_names_coalesced = [(n if n is not None else '__NULL__') for n in spatial_names]
            
            # Try each matching criterion
            for criterion in [1, 2, 3]:
                if criterion == 1:  # (relation_type, objects, contexts) match
                    # Build query to find hyperedges with matching relation_type, objects, and contexts
                    if objects_list:
                        # Case: new fact has objects - match existing hyperedges with same objects
                        query = f"""
                            MATCH (h:Hyperedge {{relation_type: $relation}})
                            MATCH (h)-[:CONNECTS {{role: 'object'}}]->(o:Node)
                            WITH h, collect(DISTINCT o.id) AS objIds
                            WHERE size(objIds) = size($objectsList)
                              AND all(x IN objIds WHERE x IN $objectsList)
                              AND all(x IN $objectsList WHERE x IN objIds)
                        """
                    else:
                        # Case: new fact has no objects - match existing hyperedges with no objects
                        query = f"""
                            MATCH (h:Hyperedge {{relation_type: $relation}})
                            WHERE NOT EXISTS((h)-[:CONNECTS {{role: 'object'}}]->())
                        """

                    # Add context matching
                    if temporal_times:
                        if objects_list:
                            query += """
                            MATCH (h)-[:VALID_IN]->(c:Context)
                            WITH h, objIds, collect(DISTINCT [coalesce(c.from_time, '__NULL__'), coalesce(c.to_time, '__NULL__')]) AS contextTimes
                            WHERE size(contextTimes) = size($temporalTimes)
                              AND all(x IN contextTimes WHERE x IN $temporalTimes)
                              AND all(x IN $temporalTimes WHERE x IN contextTimes)
                            """
                        else:
                            query += """
                            MATCH (h)-[:VALID_IN]->(c:Context)
                            WITH h, collect(DISTINCT [coalesce(c.from_time, '__NULL__'), coalesce(c.to_time, '__NULL__')]) AS contextTimes
                            WHERE size(contextTimes) = size($temporalTimes)
                              AND all(x IN contextTimes WHERE x IN $temporalTimes)
                              AND all(x IN $temporalTimes WHERE x IN contextTimes)
                            """

                    if spatial_names:
                        if objects_list:
                            query += """
                            MATCH (h)-[:VALID_IN]->(c2:Context)
                            WITH h, objIds, collect(DISTINCT coalesce(c2.location_name, '__NULL__')) AS contextNames
                            WHERE size(contextNames) = size($spatialNames)
                              AND all(x IN contextNames WHERE x IN $spatialNames)
                              AND all(x IN $spatialNames WHERE x IN contextNames)
                            """
                        else:
                            query += """
                            MATCH (h)-[:VALID_IN]->(c2:Context)
                            WITH h, collect(DISTINCT coalesce(c2.location_name, '__NULL__')) AS contextNames
                            WHERE size(contextNames) = size($spatialNames)
                              AND all(x IN contextNames WHERE x IN $spatialNames)
                              AND all(x IN $spatialNames WHERE x IN contextNames)
                            """

                    query += "RETURN h.id as hyperedge_id, h.relation_type as relation_type ORDER BY h.id LIMIT 1"
                    
                elif criterion == 2:  # (subjects, relation_type, objects) match
                    if not subjects_list:
                        continue

                    if objects_list:
                        # Case: new fact has objects - match existing hyperedges with same subjects and objects
                        query = f"""
                            MATCH (h:Hyperedge {{relation_type: $relation}})
                            MATCH (h)-[:CONNECTS {{role: 'subject'}}]->(s:Node)
                            WITH h, collect(DISTINCT s.id) AS subjIds
                            WHERE size(subjIds) = size($subjectsList)
                              AND all(x IN subjIds WHERE x IN $subjectsList)
                              AND all(x IN $subjectsList WHERE x IN subjIds)
                            MATCH (h)-[:CONNECTS {{role: 'object'}}]->(o:Node)
                            WITH h, subjIds, collect(DISTINCT o.id) AS objIds
                            WHERE size(objIds) = size($objectsList)
                              AND all(x IN objIds WHERE x IN $objectsList)
                              AND all(x IN $objectsList WHERE x IN objIds)
                            RETURN h.id as hyperedge_id, h.relation_type as relation_type
                            ORDER BY h.id LIMIT 1
                        """
                    else:
                        # Case: new fact has no objects - match existing hyperedges with same subjects and no objects
                        query = f"""
                            MATCH (h:Hyperedge {{relation_type: $relation}})
                            MATCH (h)-[:CONNECTS {{role: 'subject'}}]->(s:Node)
                            WITH h, collect(DISTINCT s.id) AS subjIds
                            WHERE size(subjIds) = size($subjectsList)
                              AND all(x IN subjIds WHERE x IN $subjectsList)
                              AND all(x IN $subjectsList WHERE x IN subjIds)
                              AND NOT EXISTS((h)-[:CONNECTS {{role: 'object'}}]->())
                            RETURN h.id as hyperedge_id, h.relation_type as relation_type
                            ORDER BY h.id LIMIT 1
                        """
                    
                elif criterion == 3:  # (subjects, relation_type, contexts) match
                    if not subjects_list:
                        continue
                    
                    query = f"""
                        MATCH (h:Hyperedge {{relation_type: $relation}})
                        MATCH (h)-[:CONNECTS {{role: 'subject'}}]->(s:Node)
                        WITH h, collect(DISTINCT s.id) AS subjIds
                        WHERE size(subjIds) = size($subjectsList)
                          AND all(x IN subjIds WHERE x IN $subjectsList)
                          AND all(x IN $subjectsList WHERE x IN subjIds)
                    """
                    
                    # Add context matching (same as criterion 1)
                    if temporal_times:
                        query += """
                        MATCH (h)-[:VALID_IN]->(c:Context)
                        WITH h, subjIds, collect(DISTINCT [coalesce(c.from_time, '__NULL__'), coalesce(c.to_time, '__NULL__')]) AS contextTimes
                        WHERE size(contextTimes) = size($temporalTimes)
                          AND all(x IN contextTimes WHERE x IN $temporalTimes)
                          AND all(x IN $temporalTimes WHERE x IN contextTimes)
                        """
                    
                    if spatial_names:
                        query += """
                        MATCH (h)-[:VALID_IN]->(c2:Context)
                        WITH h, subjIds, collect(DISTINCT coalesce(c2.location_name, '__NULL__')) AS contextNames
                        WHERE size(contextNames) = size($spatialNames)
                          AND all(x IN contextNames WHERE x IN $spatialNames)
                          AND all(x IN $spatialNames WHERE x IN contextNames)
                        """
                    
                    query += "RETURN h.id as hyperedge_id, h.relation_type as relation_type ORDER BY h.id LIMIT 1"
                
                # Execute the query
                try:
                    params = {
                        'relation': relation_param,
                        'subjectsList': subjects_list,
                        'objectsList': objects_list,
                        'temporalTimes': temporal_times_coalesced,
                        'spatialNames': spatial_names_coalesced,
                    }
                    with neo4j_storage.driver.session(database=neo4j_storage.config.database) as session:
                        result = session.run(query, **params)
                        record = result.single()
                        
                        if record:
                            # Found a matching hyperedge, return its data
                            hyperedge_id = record["hyperedge_id"]
                            
                            # Get the full hyperedge data
                            full_query = f"""
                                MATCH (h:Hyperedge {{id: '{hyperedge_id}'}})
                                MATCH (h)-[:CONNECTS {{role: 'subject'}}]->(s:Node)
                                MATCH (h)-[:CONNECTS {{role: 'object'}}]->(o:Node)
                                MATCH (h)-[:VALID_IN]->(c:Context)
                                RETURN h.relation_type as relation_type,
                                       collect(DISTINCT s.id) as subjects,
                                       collect(DISTINCT o.id) as objects,
                                       collect(DISTINCT {{
                                           start_time: c.from_time,
                                           end_time: c.to_time
                                       }}) as temporal_intervals,
                                       collect(DISTINCT {{
                                           name: c.location_name,
                                           type: c.spatial_type,
                                           coordinates: c.coordinates
                                       }}) as spatial_contexts
                            """
                            
                            full_result = session.run(full_query)
                            full_record = full_result.single()
                            
                            if full_record:
                                return {
                                    'id': hyperedge_id,
                                    'relation_type': full_record["relation_type"],
                                    'subjects': full_record["subjects"],
                                    'objects': full_record["objects"],
                                    'temporal_intervals': full_record["temporal_intervals"],
                                    'spatial_contexts': full_record["spatial_contexts"],
                                    'append_criterion': criterion
                                }
                                
                except Exception as e:
                    logger.warning(f"Query failed for criterion {criterion}: {e}")
                    continue
            
            return None
            
        except Exception as e:
            logger.error(f"Error in _find_appendable_hyperedge: {e}")
            return None
    
    async def _generate_append_cypher(self, existing_hyperedge, subjects, objects, relation_type, temporal_intervals, spatial_contexts):
        """
        Generate Cypher to append new elements to an existing hyperedge.
        """
        cypher_parts = []
        
        # Get the existing hyperedge ID
        hyperedge_id = existing_hyperedge.get('id')
        if not hyperedge_id:
            logger.error("No hyperedge ID found in existing_hyperedge data")
            return ""
        
        # Start with MATCH to find the existing hyperedge
        cypher_parts.append(f"MATCH (existing_hyperedge:Hyperedge {{id: '{hyperedge_id}'}})")
        
        # Determine what needs to be appended by comparing with existing hyperedge
        existing_subjects = existing_hyperedge.get('subjects', [])
        existing_objects = existing_hyperedge.get('objects', [])
        existing_temporal = existing_hyperedge.get('temporal_intervals', [])
        existing_spatial = existing_hyperedge.get('spatial_contexts', [])
        
        # Find new subjects to append
        new_subjects = [s for s in subjects if s not in existing_subjects]
        if new_subjects:
            for i, subject in enumerate(new_subjects):
                escaped_subject = cypher_escape(subject)
                cypher_parts.append(f"MERGE (new_subject_{i}:Node {{id: '{escaped_subject}'}})")
                cypher_parts.append(f"SET new_subject_{i}.type = 'entity'")
                cypher_parts.append(f"CREATE (existing_hyperedge)-[:CONNECTS {{role: 'subject'}}]->(new_subject_{i})")
        
        # Find new objects to append
        new_objects = [o for o in objects if o not in existing_objects]
        if new_objects:
            for i, obj in enumerate(new_objects):
                escaped_obj = cypher_escape(obj)
                cypher_parts.append(f"MERGE (new_object_{i}:Node {{id: '{escaped_obj}'}})")
                cypher_parts.append(f"SET new_object_{i}.type = 'entity'")
                cypher_parts.append(f"CREATE (existing_hyperedge)-[:CONNECTS {{role: 'object'}}]->(new_object_{i})")
        
        # Find new temporal intervals to append
        new_temporal = [t for t in temporal_intervals if t not in existing_temporal]
        if new_temporal:
            # Create new context nodes for the cartesian product of new temporal intervals with all spatial contexts
            all_spatial = existing_spatial + spatial_contexts
            for i, interval in enumerate(new_temporal):
                for j, spatial_ctx in enumerate(all_spatial if all_spatial else [{'name': 'unknown', 'type': 'unknown', 'coordinates': None}]):
                    # Deterministic global context id for de-duplication (includes time, name, type, coords)
                    start_time = interval.get('start_time', 'null')
                    end_time = interval.get('end_time', 'null')
                    
                    # Handle spatial context
                    spatial_name = spatial_ctx.get('name', 'unknown')
                    spatial_type = spatial_ctx.get('type', 'unknown')
                    spatial_coordinates = spatial_ctx.get('coordinates', None)
                    
                    # Escape values
                    escaped_spatial_name = cypher_escape(spatial_name) if spatial_name else 'unknown'
                    escaped_spatial_type = cypher_escape(spatial_type) if spatial_type else 'unknown'
                    
                    # Handle coordinates via helper
                    coordinates_cypher = build_coordinates_cypher(spatial_type, spatial_coordinates)
                    
                    # Quote temporal values
                    if start_time == 'null' or start_time is None or start_time == '':
                        from_time = 'null'
                    else:
                        from_time = f"'{start_time}'"
                    
                    if end_time == 'null' or end_time is None or end_time == '':
                        to_time = 'null'
                    else:
                        to_time = f"'{end_time}'"
                    
                    # Build identity including coordinates signature
                    if str(spatial_type).lower() == 'point' and spatial_coordinates is not None and isinstance(spatial_coordinates, list) and len(spatial_coordinates) == 2:
                        try:
                            lon_sig = round(float(spatial_coordinates[0]), 6)
                            lat_sig = round(float(spatial_coordinates[1]), 6)
                            coord_sig = f"pt:{lon_sig}:{lat_sig}"
                        except Exception:
                            coord_sig = 'geo:NULL'
                    else:
                        try:
                            coords_min = json.dumps(spatial_coordinates, separators=(',', ':'), ensure_ascii=False)
                        except Exception:
                            coords_min = 'null'
                        coord_sig = 'geo:' + hashlib.sha1((coords_min or 'null').encode('utf-8')).hexdigest()[:16]
                    start_key = start_time if (start_time not in (None, '', 'null')) else '__NULL__'
                    end_key = end_time if (end_time not in (None, '', 'null')) else '__NULL__'
                    key_str = f"{start_key}|{end_key}|{escaped_spatial_name}|{escaped_spatial_type}|{coord_sig}"
                    context_id = "ctx_" + hashlib.sha1(key_str.encode('utf-8')).hexdigest()[:16]
                    cypher_parts.append(f"MERGE (new_context_{i}_{j}:Context {{id: '{context_id}'}})")
                    cypher_parts.append(f"ON CREATE SET new_context_{i}_{j}.from_time = {from_time}, new_context_{i}_{j}.to_time = {to_time}, new_context_{i}_{j}.location_name = '{escaped_spatial_name}', new_context_{i}_{j}.spatial_type = '{escaped_spatial_type}', new_context_{i}_{j}.coordinates = {coordinates_cypher}, new_context_{i}_{j}.certainty = 1.0")
                    cypher_parts.append(f"MERGE (existing_hyperedge)-[:VALID_IN]->(new_context_{i}_{j})")
        
        # Find new spatial contexts to append
        new_spatial = [s for s in spatial_contexts if s not in existing_spatial]
        if new_spatial:
            # Create new context nodes for the cartesian product of all temporal intervals with new spatial contexts
            all_temporal = existing_temporal + temporal_intervals
            for i, interval in enumerate(all_temporal):
                for j, spatial_ctx in enumerate(new_spatial):
                    # Deterministic global context id for dedup (inc time, name, type, coords)
                    start_time = interval.get('start_time', 'null')
                    end_time = interval.get('end_time', 'null')
                    
                    # Handle spatial context (same logic as above)
                    spatial_name = spatial_ctx.get('name', 'unknown')
                    spatial_type = spatial_ctx.get('type', 'unknown')
                    spatial_coordinates = spatial_ctx.get('coordinates', None)
                    
                    escaped_spatial_name = spatial_name.replace("'", "\\'") if spatial_name else 'unknown'
                    escaped_spatial_type = spatial_type.replace("'", "\\'") if spatial_type else 'unknown'
                    
                    if spatial_coordinates is not None:
                        if spatial_type.lower() == 'point' and isinstance(spatial_coordinates, list) and len(spatial_coordinates) == 2:
                            try:
                                lon, lat = spatial_coordinates
                                if isinstance(lon, (int, float)) and isinstance(lat, (int, float)):
                                    coordinates_cypher = f"point({{longitude: {lon}, latitude: {lat}}})"
                                else:
                                    coordinates_cypher = 'null'
                            except (ValueError, TypeError):
                                coordinates_cypher = 'null'
                        else:
                            try:
                                def simplify_coords(coords: Any, max_points: int = 800) -> Any:
                                    try:
                                        def flatten_count(c):
                                            if isinstance(c, list) and c and isinstance(c[0], (int, float)):
                                                return 1
                                            if isinstance(c, list):
                                                return sum(flatten_count(x) for x in c)
                                            return 0
                                        total = flatten_count(coords)
                                        if total <= max_points:
                                            return coords
                                        def sample_ring(ring):
                                            step = max(1, math.ceil(len(ring) * 1.0 / max(1, max_points // 4)))
                                            return ring[::step]
                                        def recurse(c):
                                            if isinstance(c, list) and c and isinstance(c[0], list) and c and len(c) > 0 and isinstance(c[0][0], (int, float)):
                                                return sample_ring(c)
                                            if isinstance(c, list):
                                                return [recurse(x) for x in c]
                                            return c
                                        return recurse(coords)
                                    except Exception:
                                        return coords
                                simplified = simplify_coords(spatial_coordinates)
                                coordinates_json = json.dumps(simplified)
                                if len(coordinates_json) > 200000:
                                    coordinates_cypher = 'null'
                                else:
                                    coordinates_cypher = f"'{coordinates_json}'"
                            except (TypeError, ValueError):
                                coordinates_cypher = 'null'
                    else:
                        coordinates_cypher = 'null'
                    
                    if start_time == 'null' or start_time is None or start_time == '':
                        from_time = 'null'
                    else:
                        from_time = f"'{start_time}'"
                    
                    if end_time == 'null' or end_time is None or end_time == '':
                        to_time = 'null'
                    else:
                        to_time = f"'{end_time}'"
                    
                    # Build identity including coordinates signature
                    if str(spatial_type).lower() == 'point' and spatial_coordinates is not None and isinstance(spatial_coordinates, list) and len(spatial_coordinates) == 2:
                        try:
                            lon_sig = round(float(spatial_coordinates[0]), 6)
                            lat_sig = round(float(spatial_coordinates[1]), 6)
                            coord_sig = f"pt:{lon_sig}:{lat_sig}"
                        except Exception:
                            coord_sig = 'geo:NULL'
                    else:
                        try:
                            coords_min = json.dumps(spatial_coordinates, separators=(',', ':'), ensure_ascii=False)
                        except Exception:
                            coords_min = 'null'
                        coord_sig = 'geo:' + hashlib.sha1((coords_min or 'null').encode('utf-8')).hexdigest()[:16]
                    start_key = start_time if (start_time not in (None, '', 'null')) else '__NULL__'
                    end_key = end_time if (end_time not in (None, '', 'null')) else '__NULL__'
                    key_str = f"{start_key}|{end_key}|{escaped_spatial_name}|{escaped_spatial_type}|{coord_sig}"
                    context_id = "ctx_" + hashlib.sha1(key_str.encode('utf-8')).hexdigest()[:16]
                    cypher_parts.append(f"MERGE (new_spatial_context_{i}_{j}:Context {{id: '{context_id}'}})")
                    cypher_parts.append(f"ON CREATE SET new_spatial_context_{i}_{j}.from_time = {from_time}, new_spatial_context_{i}_{j}.to_time = {to_time}, new_spatial_context_{i}_{j}.location_name = '{escaped_spatial_name}', new_spatial_context_{i}_{j}.spatial_type = '{escaped_spatial_type}', new_spatial_context_{i}_{j}.coordinates = {coordinates_cypher}, new_spatial_context_{i}_{j}.certainty = 1.0")
                    cypher_parts.append(f"MERGE (existing_hyperedge)-[:VALID_IN]->(new_spatial_context_{i}_{j})")
        
        # Update entity count
        total_entities = len(existing_subjects) + len(new_subjects) + len(existing_objects) + len(new_objects)
        cypher_parts.append(f"WITH existing_hyperedge")
        cypher_parts.append(f"MATCH (existing_hyperedge)-[:CONNECTS]->(n:Node)")
        cypher_parts.append(f"WITH existing_hyperedge, count(n) as entity_count")
        cypher_parts.append(f"SET existing_hyperedge.entity_count = entity_count")
        
        return "\n".join(cypher_parts)