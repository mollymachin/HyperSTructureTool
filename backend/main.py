# Test small change
from pathlib import Path
import sys
sys.path.append(str(Path(__file__).resolve().parent.parent))

from fastapi import FastAPI
from fastapi.responses import StreamingResponse
import asyncio
from fastapi.middleware.cors import CORSMiddleware
from typing import Dict, Any, List
from pydantic import BaseModel
import json
import os
from kh_core.openai_llm_interface import OpenAILLMInterface
from backend.tools import TOOLS, execute_tool
from utils.text_to_cypher import TextToHyperSTructurePipeline
from kh_core.neo4j_storage import Neo4jConfig

app = FastAPI(title="Neo4j Hyperstructure Visualisation API", version="1.0.0")

# Enable CORS for frontend (configurable via env FRONTEND_ORIGIN, comma-separated for multiple)
frontend_origin_env = os.getenv("FRONTEND_ORIGIN", "http://localhost:3000")
allowed_origins = [o.strip() for o in frontend_origin_env.split(",") if o.strip()]
app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Initialise text-to-cypher pipeline
text_to_cypher_pipeline = None
openai_client = None

# Pydantic models for API requests/responses
class HyperstructureResponse(BaseModel): # Response format for returning hyerstructure data
    status: str
    message: str
    hyperstructure_data: Dict[str, Any] = None

class AddHyperedgeRequest(BaseModel):
    """Request model for adding a new hyperedge"""
    subjects: List[str]
    objects: List[str]
    relation_type: str
    temporal_intervals: List[Dict[str, str]] = []
    spatial_contexts: List[Dict[str, Any]] = []

class AddHyperedgeResponse(BaseModel):
    """Response model for adding a new hyperedge"""
    status: str
    message: str
    hyperedge_id: str = None
    spatial_data: List[Dict[str, Any]] = []

class ProcessTextRequest(BaseModel):
    """Request model for processing text through the pipeline"""
    text: str
    chunk_size: int = 3

class ProcessTextResponse(BaseModel):
    """Response model for text processing"""
    status: str
    message: str
    facts_processed: int = 0

class AskQueryRequest(BaseModel):
    message: str
    max_loops: int = 3
    tools: list = None  # optional override

class AskQueryResponse(BaseModel):
    status: str
    valid: bool
    descriptor: str
    tool_trace: list

class MultiAskRequest(BaseModel):
    text: str
    max_loops: int = 3
    tools: list = None

class MultiAskItem(BaseModel):
    question: str
    valid: bool
    descriptor: str
    tool_trace: list

class MultiAskResponse(BaseModel):
    status: str
    results: List[MultiAskItem]

# API Endpoints
@app.get("/")
async def root():
    return {"message": "Neo4j Hyperstructure Visualisation API"}

async def _ensure_pipeline_and_openai():
    global text_to_cypher_pipeline, openai_client
    if text_to_cypher_pipeline is None:
        neo4j_config = Neo4jConfig()
        text_to_cypher_pipeline = TextToHyperSTructurePipeline(neo4j_config=neo4j_config)
        ok = await text_to_cypher_pipeline.initialise_neo4j_connection()
        if not ok:
            raise RuntimeError("Failed to connect to Neo4j")
    if openai_client is None:
        openai_client = OpenAILLMInterface(model="gpt-5-nano")

# Prompts for the looping function-calling querying design
def _build_system_prompt():
    return (
        "You are a function-calling assistant that can call tools to answer questions about a graph. "
        "Choose a single tool and provide arguments as needed. "
        "When deciding spatial/temporal unconstrained flags: If a question asks with certainty, e.g. 'Show me everyone in South Africa' or 'Who is alive in 2020?', then do NOT include unconstrained results (set include_spatially_unconstrained=false and include_temporally_unconstrained=false). "
        "If a question is hypothetical or possibility-based (e.g. 'Who could have been alive in 2020?' or 'Who might be in South Africa?'), then include unconstrained results as well (set include_spatially_unconstrained=true and include_temporally_unconstrained=true)."
    )

def _build_validation_system_prompt():
    return (
        "You validate whether the latest tool result answers the original user question. "
        "Respond strictly as JSON with keys: valid (boolean) and descriptor (string)."
    )

def _format_messages(user_message: str, intermediate: str, full_context: str = ""):
    messages = [
        {"role": "system", "content": _build_system_prompt()},
        {"role": "user", "content": user_message},
    ]
    if full_context:
        messages.append({"role": "system", "content": f"Full input context: {full_context}"})
    if intermediate:
        messages.append({"role": "system", "content": f"Intermediate guidance: {intermediate}"})
    return messages

def _format_validation_messages(user_message: str, tool_name: str, tool_args: dict, tool_result: dict, history: list):
    return [
        {"role": "system", "content": _build_validation_system_prompt()},
        {"role": "user", "content": user_message},
        {"role": "system", "content": json.dumps({"tool": tool_name, "args": tool_args, "result": tool_result})},
        {"role": "system", "content": json.dumps({"history": history})},
    ]

@app.post("/api/query/ask", response_model=AskQueryResponse)
async def ask_query(request: AskQueryRequest):
    try:
        await _ensure_pipeline_and_openai()
    except Exception as e:
        return AskQueryResponse(status="error", valid=False, descriptor=f"Init failed: {str(e)}", tool_trace=[])

    tools = request.tools if request.tools is not None else TOOLS
    result = await _run_function_calling_loop(message=request.message, tools=tools, max_loops=request.max_loops)
    return AskQueryResponse(status="success", valid=result["valid"], descriptor=result["descriptor"], tool_trace=result["trace"]) 

async def _run_function_calling_loop(message: str, tools: list, max_loops: int = 3, full_context: str = "") -> Dict[str, Any]:
    intermediate = ""
    trace: List[Dict[str, Any]] = []
    loops = max(1, min(5, max_loops))
    for loop_idx in range(loops):
        # Step 1: tool selection
        messages = _format_messages(message, intermediate, full_context)
        assistant_msg = await openai_client.chat_completion_full(messages, tools=tools, tool_choice="auto")
        tool_calls = assistant_msg.get("tool_calls") or []
        if not tool_calls:
            return {"valid": False, "descriptor": "Model did not select a tool", "trace": trace}
        tool_call = tool_calls[0]
        fn = tool_call.get("function", {})
        tool_name = fn.get("name")
        args_raw = fn.get("arguments") or "{}"
        try:
            tool_args = json.loads(args_raw) if isinstance(args_raw, str) else args_raw
        except Exception:
            tool_args = {}

        # Step 2: execute
        result = await execute_tool(tool_name, tool_args, text_to_cypher_pipeline)
        trace.append({"loop": loop_idx, "tool": tool_name, "args": tool_args, "result": result})

        # Step 3: validate
        validation_messages = _format_validation_messages(message, tool_name, tool_args, result, trace)
        validation_msg_content = await openai_client.chat_completion(validation_messages, model="gpt-5-nano", response_format={"type": "json_object"})
        try:
            validation = json.loads(validation_msg_content or "{}")
        except Exception:
            validation = {"valid": False, "descriptor": "Validator returned invalid JSON"}

        is_valid = bool(validation.get("valid"))
        descriptor = str(validation.get("descriptor") or "")
        if is_valid:
            return {"valid": True, "descriptor": descriptor, "trace": trace}
        intermediate = descriptor
    return {"valid": False, "descriptor": intermediate or "No valid answer found", "trace": trace}

def _split_into_sentences(text: str) -> List[str]:
    import re
    # Split on the most obvious sentence endings followed by whitespace
    parts = re.split(r"(?<=[\.!?])\s+", text.strip())
    sentences = [p.strip() for p in parts if p and p.strip()]
    return sentences

@app.post("/api/query/ask_multi", response_model=MultiAskResponse)
async def ask_multi(request: MultiAskRequest):
    try:
        await _ensure_pipeline_and_openai()
    except Exception as e:
        return MultiAskResponse(status="error", results=[])

    tools = request.tools if request.tools is not None else TOOLS
    sentences = _split_into_sentences(request.text)
    results: List[MultiAskItem] = []
    for s in sentences:
        loop_result = await _run_function_calling_loop(message=s, tools=tools, max_loops=request.max_loops, full_context=request.text)
        results.append(MultiAskItem(question=s, valid=loop_result["valid"], descriptor=loop_result["descriptor"], tool_trace=loop_result["trace"]))
    return MultiAskResponse(status="success", results=results)

@app.get("/api/hyperstructure/data")
async def get_hyperstructure_data(
    start_time: str = None, 
    end_time: str = None,
    location_names: str = None,
    location_coordinates: str = None,
    include_spatially_unconstrained: bool = False
):
    """
    Get all hyperstructure data from Neo4j in frontend-compatible format.
    Returns nodes, hyperedges, and relationships for visualisation.
    Optional spatiotemporal filtering using query_spatiotemporal function.
    """
    try:
        global text_to_cypher_pipeline
        
        # Initialise Neo4j connection if not already done
        if text_to_cypher_pipeline is None:
            try:
                neo4j_config = Neo4jConfig()
                text_to_cypher_pipeline = TextToHyperSTructurePipeline(neo4j_config=neo4j_config)
                
                if not await text_to_cypher_pipeline.initialise_neo4j_connection():
                    return {
                        "status": "error",
                        "message": "Failed to connect to Neo4j database",
                        "hyperstructure_data": None
                    }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to initialize Neo4j: {str(e)}",
                    "hyperstructure_data": None
                }
        
        try:
            # Parse spatial parameters
            parsed_location_names = None
            parsed_location_coordinates = None
            
            if location_names:
                parsed_location_names = [name.strip() for name in location_names.split(',') if name.strip()]
            
            if location_coordinates:
                try:
                    parsed_location_coordinates = json.loads(location_coordinates)
                    if not isinstance(parsed_location_coordinates, list) or len(parsed_location_coordinates) < 3:
                        return {
                            "status": "error",
                            "message": "Invalid location coordinates format. Must be a JSON array of at least 3 [lon, lat] pairs.",
                            "hyperstructure_data": None
                        }
                except json.JSONDecodeError:
                    return {
                        "status": "error",
                        "message": "Invalid JSON format for location coordinates",
                        "hyperstructure_data": None
                    }
            
            # Use query_spatiotemporal to get filtered hyperedge IDs
            filtered_hyperedge_ids = await text_to_cypher_pipeline.neo4j_storage.query_spatiotemporal(
                start_time=start_time,
                end_time=end_time,
                location_names=parsed_location_names,
                location_coordinates=parsed_location_coordinates,
                include_spatially_unconstrained=include_spatially_unconstrained
            )
            
            # If no filtering applied, get all hyperedges
            if not filtered_hyperedge_ids and not any([start_time, end_time, parsed_location_names, parsed_location_coordinates]):
                # No filters - get all hyperedges
                cypher_query = """
                MATCH (h:Hyperedge)
                OPTIONAL MATCH (h)-[:CONNECTS {role: 'subject'}]->(s:Node)
                OPTIONAL MATCH (h)-[:CONNECTS {role: 'object'}]->(o:Node)
                OPTIONAL MATCH (h)-[:VALID_IN]->(c:Context)
                RETURN h,
                       collect(DISTINCT s) as subject_nodes,
                       collect(DISTINCT o) as object_nodes,
                       collect(DISTINCT c) as contexts
                ORDER BY h.id
                """
                params = {}
            else:
                # Use filtered hyperedge IDs
                cypher_query = """
                MATCH (h:Hyperedge)
                WHERE h.id IN $hyperedge_ids
                OPTIONAL MATCH (h)-[:CONNECTS {role: 'subject'}]->(s:Node)
                OPTIONAL MATCH (h)-[:CONNECTS {role: 'object'}]->(o:Node)
                OPTIONAL MATCH (h)-[:VALID_IN]->(c:Context)
                RETURN h,
                       collect(DISTINCT s) as subject_nodes,
                       collect(DISTINCT o) as object_nodes,
                       collect(DISTINCT c) as contexts
                ORDER BY h.id
                """
                params = {"hyperedge_ids": list(filtered_hyperedge_ids)}
            
            hyperedges = []
            all_entities = set()
            
            with text_to_cypher_pipeline.neo4j_storage.driver.session(database=text_to_cypher_pipeline.neo4j_config.database) as session:
                result = session.run(cypher_query, **params)
                
                for record in result:
                    hyperedge = record["h"]
                    subject_nodes = record["subject_nodes"] or []
                    object_nodes = record["object_nodes"] or []
                    contexts = record["contexts"] or []
                    
                    # Extract subjects/objects and combined entities
                    subjects = []
                    objects = []
                    entities = []
                    
                    for node in subject_nodes:
                        if node and node.get("id"):
                            sid = node["id"]
                            subjects.append(sid)
                            entities.append(sid)
                            all_entities.add(sid)
                    for node in object_nodes:
                        if node and node.get("id"):
                            oid = node["id"]
                            objects.append(oid)
                            entities.append(oid)
                            all_entities.add(oid)
                    
                    # If no explicit roles, try fallback: treat first as subject, rest as objects
                    if not subjects and not objects and entities:
                        # Ensure uniqueness while preserving order
                        seen = set()
                        ordered = [e for e in entities if not (e in seen or seen.add(e))]
                        subjects = ordered[:1]
                        objects = ordered[1:]
                    
                    # Extract temporal intervals from contexts
                    temporal_intervals = []
                    for context in contexts:
                        if context and (context.get("from_time") or context.get("to_time")):
                            temporal_intervals.append({
                                "start_time": context.get("from_time"),
                                "end_time": context.get("to_time")
                            })
                    
                    # Extract spatial contexts
                    spatial_contexts = []
                    for context in contexts:
                        if context and context.get("spatial_context"):
                            spatial_contexts.append(context["spatial_context"])
                        elif context and context.get("region"):
                            spatial_contexts.append(context["region"])
                        elif context and context.get("location_name"):
                            spatial_contexts.append({"name": context["location_name"]})
                        elif context and context.get("coordinates"):
                            spatial_contexts.append({"coordinates": context["coordinates"]})
                    
                    # Extract explicit contexts for visualisation
                    context_nodes = []
                    for context in contexts:
                        if context and (context.get("id") or context.get("from_time") or context.get("to_time") or context.get("location_name")):
                            context_nodes.append({
                                "id": context.get("id"),
                                "from_time": context.get("from_time"),
                                "to_time": context.get("to_time"),
                                "location_name": context.get("location_name")
                            })

                    # Create hyperedge in frontend format
                    hyperedge_data = {
                        "id": hyperedge.get("id", None),
                        "entities": entities,
                        "relation_type": hyperedge.get("relation_type", hyperedge.get("relation_label", "unknown")),
                        "subjects": subjects,
                        "objects": objects,
                        "temporal_intervals": temporal_intervals,
                        "spatial_contexts": spatial_contexts,
                        "contexts": context_nodes
                    }
                    
                    hyperedges.append(hyperedge_data)
            
            # Create frontend-compatible data structure
            hyperstructure_data = {
                "name": "Neo4j Hyperstructure",
                "entities": list(all_entities),
                "hyperedges": hyperedges,
                "hyperedge_count": len(hyperedges)
            }

            # Optionally include state change events for the frontend causality view
            try:
                state_query = """
                MATCH (sce:StateChangeEvent)-[:AFFECTS_FACT]->(h:Hyperedge)
                OPTIONAL MATCH (hc:Hyperedge)-[c:CAUSES_STATE]->(sce)
                WITH sce, h, collect({hyperedge: hc, rel: c}) as causes
                RETURN sce.id as id,
                       h as affected_h,
                       [c IN causes WHERE c.hyperedge IS NOT NULL | {he: c.hyperedge, req: c.rel.required_state}] as caused_by
                ORDER BY id
                """
                state_events = []
                with text_to_cypher_pipeline.neo4j_storage.driver.session(database=text_to_cypher_pipeline.neo4j_config.database) as session:
                    sres = session.run(state_query)
                    for rec in sres:
                        sce_id = rec["id"]
                        aff = rec["affected_h"] or {}
                        cb = rec["caused_by"] or []
                        # Build affected_fact
                        aff_subjects = []
                        aff_objects = []
                        try:
                            # Fetch subjects/objects for the affected hyperedge
                            subq = """
                            MATCH (h:Hyperedge {id: $hid})
                            OPTIONAL MATCH (h)-[:CONNECTS {role:'subject'}]->(s:Node)
                            OPTIONAL MATCH (h)-[:CONNECTS {role:'object'}]->(o:Node)
                            RETURN collect(DISTINCT s.id) as subs, collect(DISTINCT o.id) as objs, h.relation_type as rel
                            """
                            subres = session.run(subq, hid=aff.get("id"))
                            srec = subres.single()
                            aff_subjects = (srec and (srec["subs"] or [])) or []
                            aff_objects = (srec and (srec["objs"] or [])) or []
                            aff_rel = (srec and srec["rel"]) or aff.get("relation_type") or ""
                        except Exception:
                            aff_rel = aff.get("relation_type") or ""
                        affected_fact = {
                            "subjects": aff_subjects,
                            "objects": aff_objects,
                            "relation_type": aff_rel
                        }
                        # Build caused_by groups as flat groups (no explicit OR grouping here)
                        caused_by = []
                        flat_group = []
                        for item in cb:
                            he = item.get("he") or {}
                            req = bool(item.get("req", True))
                            # Build subjects/objects for cause hyperedge
                            subq2 = """
                            MATCH (h:Hyperedge {id: $hid})
                            OPTIONAL MATCH (h)-[:CONNECTS {role:'subject'}]->(s:Node)
                            OPTIONAL MATCH (h)-[:CONNECTS {role:'object'}]->(o:Node)
                            RETURN collect(DISTINCT s.id) as subs, collect(DISTINCT o.id) as objs, h.relation_type as rel
                            """
                            s2 = session.run(subq2, hid=he.get("id")).single()
                            subs2 = (s2 and (s2["subs"] or [])) or []
                            objs2 = (s2 and (s2["objs"] or [])) or []
                            rel2 = (s2 and s2["rel"]) or he.get("relation_type") or ""
                            flat_group.append({
                                "subjects": subs2,
                                "objects": objs2,
                                "relation_type": rel2,
                                "triggered_by_state": req
                            })
                        if flat_group:
                            caused_by.append(flat_group)
                        state_events.append({
                            "id": sce_id,
                            "fact_type": "state_change_event",
                            "affected_fact": affected_fact,
                            "caused_by": caused_by,
                            "causes": []
                        })
                if state_events:
                    hyperstructure_data["state_events"] = state_events
            except Exception as _e:
                # Proceed without state events
                pass
            
            # Add filter info to response
            filter_info = ""
            if start_time or end_time or parsed_location_names or parsed_location_coordinates:
                filter_parts = []
                if start_time or end_time:
                    filter_parts.append(f"time: {start_time or 'no start'} to {end_time or 'no end'}")
                if parsed_location_names:
                    filter_parts.append(f"names: {', '.join(parsed_location_names)}")
                if parsed_location_coordinates:
                    filter_parts.append(f"polygon area")
                    if include_spatially_unconstrained:
                        filter_parts.append("(including unconstrained)")
                filter_info = f" (filtered: {'; '.join(filter_parts)})"
            
            return {
                "status": "success",
                "message": f"Retrieved {len(hyperedges)} hyperedges and {len(all_entities)} entities{filter_info}",
                "hyperstructure_data": hyperstructure_data
            }
            
        except Exception as neo4j_error:
            print(f"Neo4j query failed: {neo4j_error}")
            return {
                "status": "error",
                "message": f"Neo4j query failed: {str(neo4j_error)}",
                "hyperstructure_data": None
            }
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error retrieving hyperstructure data: {str(e)}",
            "hyperstructure_data": None
        }

@app.post("/api/hyperedge/add", response_model=AddHyperedgeResponse)
async def add_hyperedge(request: AddHyperedgeRequest):
    """
    Add a new hyperedge to the Neo4j database and return spatial data for map visualization.
    """
    try:
        global text_to_cypher_pipeline
        
        # Initialise Neo4j connection if not already done
        if text_to_cypher_pipeline is None:
            try:
                neo4j_config = Neo4jConfig()
                text_to_cypher_pipeline = TextToHyperSTructurePipeline(neo4j_config=neo4j_config)
                
                if not await text_to_cypher_pipeline.initialise_neo4j_connection():
                    return AddHyperedgeResponse(
                        status="error",
                        message="Failed to connect to Neo4j database"
                    )
            except Exception as e:
                return AddHyperedgeResponse(
                    status="error",
                    message=f"Failed to initialize Neo4j: {str(e)}"
                )
        
        try:
            # Convert request to structured data format expected by the pipeline
            structured_data = {
                "fact_type": "temporal_fact",
                "subjects": request.subjects,
                "objects": request.objects,
                "relation_type": request.relation_type,
                "temporal_intervals": request.temporal_intervals,
                "spatial_contexts": request.spatial_contexts
            }
            
            # Generate and execute Cypher query
            cypher_query = ""
            cypher_params = None
            async for item in text_to_cypher_pipeline.cypher_generator.generate_cypher_from_structured_output([structured_data], neo4j_storage=text_to_cypher_pipeline.neo4j_storage):
                if isinstance(item, tuple) and len(item) == 2:
                    cypher_query, cypher_params = item
                else:
                    cypher_query = str(item)
                break
            
            if not cypher_query:
                return AddHyperedgeResponse(
                    status="error",
                    message="Failed to generate Cypher query"
                )
            
            # Execute the query (with params if provided)
            success = await text_to_cypher_pipeline.execute_cypher(cypher_query, cypher_params)
            if not success:
                return AddHyperedgeResponse(
                    status="error",
                    message="Failed to execute Cypher query"
                )
            
            # Extract spatial data for map visualisation
            spatial_data = []
            for spatial_ctx in request.spatial_contexts:
                if spatial_ctx.get("type") == "Point" and spatial_ctx.get("coordinates"):
                    # For Point type, extract lat/lon coordinates
                    lon, lat = spatial_ctx["coordinates"]
                    spatial_data.append({
                        "type": "Point",
                        "name": spatial_ctx.get("name", "Unknown"),
                        "coordinates": [lon, lat],
                        "hyperedge_id": f"he_{hash(str(structured_data)) % 1000000:08x}"  # Hash-based IDs, simple but preserving uniqueness
                    })
                elif spatial_ctx.get("type") == "Polygon" and spatial_ctx.get("coordinates"):
                    # For Polygon type, extract all coordinate pairs
                    coordinates = spatial_ctx["coordinates"]
                    if coordinates and len(coordinates) > 0:
                        spatial_data.append({
                            "type": "Polygon",
                            "name": spatial_ctx.get("name", "Unknown"),
                            "coordinates": coordinates,
                            "hyperedge_id": f"he_{hash(str(structured_data)) % 1000000:08x}"
                        })
            
            return AddHyperedgeResponse(
                status="success",
                message=f"Successfully added hyperedge with {len(spatial_data)} spatial contexts",
                hyperedge_id=f"he_{hash(str(structured_data)) % 1000000:08x}",
                spatial_data=spatial_data
            )
            
        except Exception as neo4j_error:
            print(f"Neo4j operation failed: {neo4j_error}")
            return AddHyperedgeResponse(
                status="error",
                message=f"Neo4j operation failed: {str(neo4j_error)}"
            )
        
    except Exception as e:
        return AddHyperedgeResponse(
            status="error",
            message=f"Error adding hyperedge: {str(e)}"
        )

@app.get("/api/hyperedge/extract_structured_data")
async def get_extracted_structured_data():
    """
    Get hyperedges with spatial contexts from Neo4j for map visualisation.
    """
    try:
        # Ensure connection
        global text_to_cypher_pipeline
        if text_to_cypher_pipeline is None:
            neo4j_config = Neo4jConfig()
            text_to_cypher_pipeline = TextToHyperSTructurePipeline(neo4j_config=neo4j_config)
            ok = await text_to_cypher_pipeline.initialise_neo4j_connection()
            if not ok:
                return {"status": "error", "message": "Failed to connect to Neo4j"}

        # Query Neo4j for hyperedges and their spatial contexts
        cypher = """
        MATCH (h:Hyperedge)
        OPTIONAL MATCH (h)-[:CONNECTS {role:'subject'}]->(s:Node)
        OPTIONAL MATCH (h)-[:CONNECTS {role:'object'}]->(o:Node)
        OPTIONAL MATCH (h)-[:VALID_IN]->(c:Context)
        WITH h, collect(DISTINCT s.id) AS subjects, collect(DISTINCT o.id) AS objects, collect(c) AS contexts
        UNWIND contexts AS c
        WITH h, subjects, objects, c,
             CASE WHEN c.spatial_type = 'Point' AND c.coordinates IS NOT NULL THEN c.coordinates.longitude ELSE null END AS lon,
             CASE WHEN c.spatial_type = 'Point' AND c.coordinates IS NOT NULL THEN c.coordinates.latitude ELSE null END AS lat
        WITH h, subjects, objects,
             collect(DISTINCT {name: c.location_name, type: c.spatial_type, lon: lon, lat: lat, coords: c.coordinates}) AS spatial_contexts
        RETURN h.id AS hyperedge_id, h.relation_type AS relation_type, subjects, objects, spatial_contexts
        ORDER BY hyperedge_id
        """

        hyperedges = []
        with text_to_cypher_pipeline.neo4j_storage.driver.session(database=text_to_cypher_pipeline.neo4j_config.database) as session:
            records = session.run(cypher)
            for record in records:
                rel = record["relation_type"]
                subjects = record["subjects"] or []
                objects = record["objects"] or []
                scs = record["spatial_contexts"] or []
                spatial_contexts = []
                for sc in scs:
                    if not sc:
                        continue
                    name = sc.get("name")
                    stype = sc.get("type")
                    lon = sc.get("lon")
                    lat = sc.get("lat")
                    coords_val = sc.get("coords")
                    if stype == 'Point' and lon is not None and lat is not None:
                        spatial_contexts.append({"name": name, "type": "Point", "coordinates": [lon, lat]})
                    elif stype == 'Polygon' and coords_val is not None:
                        try:
                            # coords_val stored as JSON string
                            import json as _json
                            coords = _json.loads(coords_val) if isinstance(coords_val, str) else coords_val
                            spatial_contexts.append({"name": name, "type": "Polygon", "coordinates": coords})
                        except Exception:
                            continue
                    elif stype == 'MultiPolygon' and coords_val is not None:
                        try:
                            import json as _json
                            coords = _json.loads(coords_val) if isinstance(coords_val, str) else coords_val
                            spatial_contexts.append({"name": name, "type": "MultiPolygon", "coordinates": coords})
                        except Exception:
                            continue

                if spatial_contexts:
                    hyperedges.append({
                        "subjects": subjects,
                        "objects": objects,
                        "relation_type": rel,
                        "spatial_contexts": spatial_contexts
                    })

        return {
            "status": "success",
            "message": f"Retrieved {len(hyperedges)} hyperedges with spatial contexts from Neo4j",
            "hyperedges": hyperedges,
            "total_hyperedges": len(hyperedges)
        }
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error getting extracted structured data: {str(e)}"
        }

@app.post("/api/process-text")
async def process_text(request: ProcessTextRequest):
    """
    Process text through the chunking_streaming_pipeline and add facts to the graph.
    """
    try:
        if not request.text or not request.text.strip():
            return ProcessTextResponse(
                status="error",
                message="Text input is required"
            )
        
        global text_to_cypher_pipeline
        if text_to_cypher_pipeline is None:
            try:
                neo4j_config = Neo4jConfig()
                text_to_cypher_pipeline = TextToHyperSTructurePipeline(neo4j_config=neo4j_config)
                
                if not await text_to_cypher_pipeline.initialise_neo4j_connection():
                    return ProcessTextResponse(
                        status="error",
                        message="Failed to connect to Neo4j database"
                    )
            except Exception as e:
                return ProcessTextResponse(
                    status="error",
                    message=f"Failed to initialize Neo4j: {str(e)}"
                )
        
        # Process text through the pipeline
        facts_processed = 0
        try:
            from utils.process_text import chunking_streaming_pipeline
            
            async for fact in chunking_streaming_pipeline(request.text.strip(), request.chunk_size):
                facts_processed += 1
                # Each fact is automatically added to the graph by the pipeline
                # We simply count them for this response
        except Exception as pipeline_error:
            return ProcessTextResponse(
                status="error",
                message=f"Pipeline processing failed: {str(pipeline_error)}",
                facts_processed=facts_processed
            )
        
        return ProcessTextResponse(
            status="success",
            message=f"Successfully processed text and added {facts_processed} facts to the graph",
            facts_processed=facts_processed
        )
        
    except Exception as e:
        return ProcessTextResponse(
            status="error",
            message=f"Error processing text: {str(e)}"
        )

@app.get("/api/process-text/stream")
async def process_text_stream(text: str, chunk_size: int = 3):
    """
    Server-Sent Events (SSE) endpoint that streams human-readable progress messages
    while processing text. Messages are throttled per second
    """
    async def event_generator():
        # Helper to format SSE events
        def sse_event(data: dict) -> str:
            try:
                payload = json.dumps(data, ensure_ascii=False)
            except Exception:
                payload = json.dumps({"message": str(data)})
            return f"data: {payload}\n\n"

        # Ensure pipeline is ready
        global text_to_cypher_pipeline
        if text_to_cypher_pipeline is None:
            try:
                neo4j_config = Neo4jConfig()
                text_to_cypher_pipeline = TextToHyperSTructurePipeline(neo4j_config=neo4j_config)
                ok = await text_to_cypher_pipeline.initialise_neo4j_connection()
                if not ok:
                    yield sse_event({"type": "error", "message": "Failed to connect to Neo4j database"})
                    return
            except Exception as e:
                # Will still proceed without DB - pipeline handles no-graph mode
                pass

        # Import the pipeline and set up a queue for events
        from utils.process_text import chunking_streaming_pipeline
        queue: asyncio.Queue = asyncio.Queue()

        # Emit initial messages
        await queue.put({"type": "info", "message": "Starting text processing pipeline..."})
        sentences = _split_into_sentences(text)
        if sentences:
            await queue.put({"type": "info", "message": f"Detected {len(sentences)} sentences to process"})

        processed = 0
        done = asyncio.Event()

        async def progress_cb(evt: dict):
            try:
                # Ensure minimal shape
                if not isinstance(evt, dict):
                    return
                msg = evt.get("message")
                if msg:
                    await queue.put({"type": "stage", **evt})
            except Exception:
                pass

        async def run_pipeline():
            nonlocal processed
            try:
                async for fact in chunking_streaming_pipeline(text.strip(), chunk_size, progress_cb=progress_cb):
                    processed += 1
                    subj = fact.get("subjects") or []
                    obj = fact.get("objects") or []
                    rel = fact.get("relation_type") or ""
                    if subj or obj or rel:
                        subj_txt = ", ".join(subj) if subj else "(unknown)"
                        obj_txt = ", ".join(obj) if obj else "(none)"
                        preview = f"{subj_txt} {rel} {obj_txt}".strip()
                    else:
                        preview = "structured fact"
                    await queue.put({
                        "type": "stage",
                        "message": f"Extracted spatio-temporal fact #{processed}: {preview}",
                        "count": processed
                    })
                await queue.put({
                    "type": "complete",
                    "message": f"Processing complete. Added {processed} facts to the graph.",
                    "count": processed
                })
            except Exception as e:
                await queue.put({"type": "error", "message": f"Pipeline processing failed: {str(e)}"})
            finally:
                done.set()

        # Start the pipeline in the background
        task = asyncio.create_task(run_pipeline())

        # Drain the queue and stream out as SSE
        try:
            while True:
                try:
                    item = await asyncio.wait_for(queue.get(), timeout=0.25)
                except asyncio.TimeoutError:
                    if done.is_set() and queue.empty():
                        break
                    continue
                yield sse_event(item)
        finally:
            task.cancel()
            try:
                await task
            except Exception:
                pass

    headers = {
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "Content-Type": "text/event-stream",
        "X-Accel-Buffering": "no",
    }
    return StreamingResponse(event_generator(), headers=headers, media_type="text/event-stream")

@app.post("/api/hyperstructure/clear")
async def clear_hyperstructure():
    """
    Clear all hyperstructure data from the Neo4j database.
    This will remove all hyperedges, nodes, and contexts.
    """
    try:
        global text_to_cypher_pipeline
        
        # Initialise Neo4j connection if not already done
        if text_to_cypher_pipeline is None:
            try:
                neo4j_config = Neo4jConfig()
                text_to_cypher_pipeline = TextToHyperSTructurePipeline(neo4j_config=neo4j_config)
                
                if not await text_to_cypher_pipeline.initialise_neo4j_connection():
                    return {
                        "status": "error",
                        "message": "Failed to connect to Neo4j database"
                    }
            except Exception as e:
                return {
                    "status": "error",
                    "message": f"Failed to initialize Neo4j: {str(e)}"
                }
        
        try:
            # Clear all hyperstructure data
            cypher_query = """
            MATCH (n)
            DETACH DELETE n
            """
            
            with text_to_cypher_pipeline.neo4j_storage.driver.session(database=text_to_cypher_pipeline.neo4j_config.database) as session:
                result = session.run(cypher_query)
                # Consume the result to ensure the query executes
                list(result)
            
            return {
                "status": "success",
                "message": "Successfully cleared all hyperstructure data from the database"
            }
            
        except Exception as neo4j_error:
            print(f"Neo4j clear operation failed: {neo4j_error}")
            return {
                "status": "error",
                "message": f"Neo4j clear operation failed: {str(neo4j_error)}"
            }
        
    except Exception as e:
        return {
            "status": "error",
            "message": f"Error clearing hyperstructure: {str(e)}"
        }

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000) 