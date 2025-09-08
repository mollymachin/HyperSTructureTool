from typing import Any, Dict, List
from kh_core.neo4j_storage import Neo4jConfig


# OpenAI tool/function definitions (JSON schema)
TOOLS: List[Dict[str, Any]] = [
    {
        "type": "function",
        "function": {
            "name": "get_entities_by_relation",
            "description": "Return distinct entity IDs that participate in hyperedges whose relation_type matches the provided relation phrase (case-insensitive, substring allowed).",
            "parameters": {
                "type": "object",
                "properties": {
                    "relation": {
                        "type": "string",
                        "description": "The relation keyword or phrase to search for, e.g. 'study' or 'studies'."
                    }
                },
                "required": ["relation"],
                "additionalProperties": False
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "query_facts",
            "description": "Query hyperedges (facts) with optional filters for entities (subjects/objects/any), temporal validity, and spatial context by name or polygon area.",
            "parameters": {
                "type": "object",
                "properties": {
                    "subjects": { "type": "array", "items": {"type": "string"}, "description": "Subject entity IDs to include (any match)." },
                    "objects": { "type": "array", "items": {"type": "string"}, "description": "Object entity IDs to include (any match)." },
                    "entities": { "type": "array", "items": {"type": "string"}, "description": "Entity IDs appearing in either role (any match)." },
                    "start_time": { "type": ["string", "null"], "description": "Start of validity interval (ISO-8601)." },
                    "end_time": { "type": ["string", "null"], "description": "End of validity interval (ISO-8601)." },
                    "at_time": { "type": ["string", "null"], "description": "Instant that must lie within the fact's interval (ISO-8601)." },
                    "include_temporally_unconstrained": { "type": "boolean", "description": "When some temporal constraints are provided, this is whether to include facts with unknown temporal information." },
                    "location_names": { "type": "array", "items": {"type": "string"}, "description": "Location names for contexts (any match)." },
                    "area_coordinates": { "type": "array", "items": { "type": "array", "items": {"type": "number"}, "minItems": 2, "maxItems": 2 }, "description": "Polygon as list of [lon, lat] pairs (>=3)." },
                    "include_spatially_unconstrained": { "type": "boolean", "description": "When spatial filters are provided, include facts without spatial context." },
                    "include_temporally_unconstrained": { "type": "boolean", "description": "When temporal filters are provided, include facts without temporal context." },
                    "limit": { "type": "integer", "description": "Max number of facts to return (default 100)." }
                },
                "additionalProperties": False
            }
        }
    }
]


async def execute_tool(name: str, arguments: Dict[str, Any], text_to_cypher_pipeline) -> Dict[str, Any]:
    """
    Execute a named tool with provided arguments using the shared pipeline/storage.
    Returns a JSON-serialisable dict result.
    """
    if name == "get_entities_by_relation":
        relation = (arguments.get("relation") or "").strip()
        if not relation:
            return {"entities": [], "message": "Empty relation provided"}

        # Ensure Neo4j is connected
        if text_to_cypher_pipeline is None or text_to_cypher_pipeline.neo4j_storage is None:
            return {"entities": [], "message": "Neo4j not initialised"}

        try:
            entities: List[str] = []
            query = (
                """
                MATCH (h:Hyperedge)
                WHERE toLower(h.relation_type) CONTAINS toLower($rel)
                OPTIONAL MATCH (h)-[:CONNECTS]->(n:Node)
                WITH DISTINCT n WHERE n IS NOT NULL
                RETURN DISTINCT n.id AS entity_id
                ORDER BY entity_id
                """
            )
            with text_to_cypher_pipeline.neo4j_storage.driver.session(database=text_to_cypher_pipeline.neo4j_config.database) as session:
                result = session.run(query, rel=relation)
                entities = [record["entity_id"] for record in result if record and record.get("entity_id")]
            return {"entities": entities}
        except Exception as e:
            return {"entities": [], "error": f"Neo4j query failed: {str(e)}"}

    # Unknown tool fallback
    if name == "query_facts":
        # Normalise inputs
        subjects = arguments.get("subjects") or []
        objects = arguments.get("objects") or []
        entities = arguments.get("entities") or []
        start_time = arguments.get("start_time")
        end_time = arguments.get("end_time")
        at_time = arguments.get("at_time")
        location_names = arguments.get("location_names") or None
        area_coordinates = arguments.get("area_coordinates") or None
        include_spatially_unconstrained = bool(arguments.get("include_spatially_unconstrained") or False)
        include_temporally_unconstrained = bool(arguments.get("include_temporally_unconstrained") or False)
        limit = int(arguments.get("limit") or 100)

        if at_time and (not start_time and not end_time):
            # Use instant as both start/end to mean containment
            start_time = at_time
            end_time = at_time

        if text_to_cypher_pipeline is None or text_to_cypher_pipeline.neo4j_storage is None:
            return {"facts": [], "error": "Neo4j not initialised"}

        try:
            # Step 1: spatiotemporal pre-filter to get candidate hyperedge IDs
            filtered_ids = await text_to_cypher_pipeline.neo4j_storage.query_spatiotemporal(
                start_time=start_time,
                end_time=end_time,
                location_names=location_names,
                location_coordinates=area_coordinates,
                include_spatially_unconstrained=include_spatially_unconstrained,
                include_temporally_unconstrained=include_temporally_unconstrained
            )

            params: Dict[str, Any] = {}
            where_parts: List[str] = []

            if filtered_ids:
                where_parts.append("h.id IN $hyperedge_ids")
                params["hyperedge_ids"] = list(filtered_ids)

            # Entity filters
            if subjects:
                where_parts.append("EXISTS { MATCH (h)-[:CONNECTS {role:'subject'}]->(ns:Node) WHERE ns.id IN $subjects }")
                params["subjects"] = subjects
            if objects:
                where_parts.append("EXISTS { MATCH (h)-[:CONNECTS {role:'object'}]->(no:Node) WHERE no.id IN $objects }")
                params["objects"] = objects
            if entities:
                where_parts.append("EXISTS { MATCH (h)-[:CONNECTS]->(ne:Node) WHERE ne.id IN $entities }")
                params["entities"] = entities

            # Build cypher
            cypher = [
                "MATCH (h:Hyperedge)",
            ]
            if where_parts:
                cypher.append("WHERE " + " AND ".join(where_parts))
            cypher.extend([
                "OPTIONAL MATCH (h)-[:CONNECTS {role:'subject'}]->(s:Node)",
                "OPTIONAL MATCH (h)-[:CONNECTS {role:'object'}]->(o:Node)",
                "OPTIONAL MATCH (h)-[:VALID_IN]->(c:Context)",
                "WITH h, collect(DISTINCT s) as subject_nodes, collect(DISTINCT o) as object_nodes, collect(DISTINCT c) as contexts",
                "RETURN h, subject_nodes, object_nodes, contexts",
                "ORDER BY h.id",
                "LIMIT $limit"
            ])
            params["limit"] = limit

            facts = []
            with text_to_cypher_pipeline.neo4j_storage.driver.session(database=text_to_cypher_pipeline.neo4j_config.database) as session:
                result = session.run("\n".join(cypher), **params)
                for record in result:
                    h = record["h"]
                    s_nodes = record["subject_nodes"] or []
                    o_nodes = record["object_nodes"] or []
                    ctxs = record["contexts"] or []

                    subjects_list = [n["id"] for n in s_nodes if n and n.get("id")]
                    objects_list = [n["id"] for n in o_nodes if n and n.get("id")]

                    # Temporal intervals from contexts
                    temporal_intervals = []
                    spatial_contexts = []
                    for c in ctxs:
                        if c.get("from_time") or c.get("to_time"):
                            temporal_intervals.append({
                                "start_time": c.get("from_time"),
                                "end_time": c.get("to_time")
                            })
                        if c.get("location_name"):
                            spatial_contexts.append({"name": c.get("location_name")})
                        if c.get("coordinates") is not None:
                            spatial_contexts.append({"coordinates": c.get("coordinates")})

                    facts.append({
                        "id": h.get("id"),
                        "relation_type": h.get("relation_type", h.get("relation_label", "unknown")),
                        "subjects": subjects_list,
                        "objects": objects_list,
                        "temporal_intervals": temporal_intervals,
                        "spatial_contexts": spatial_contexts
                    })

            return {"facts": facts}
        except Exception as e:
            return {"facts": [], "error": f"Query failed: {str(e)}"}

    return {"error": f"Unknown tool: {name}"}


