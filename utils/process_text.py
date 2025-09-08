from typing import List, Dict, Any, AsyncGenerator
import re
import time
import asyncio
from config import MODEL_NAME
from datetime import datetime, timezone


def expand_spatial_coordinates(structured_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Takes the output of extract_structure_no_coords and replaces the spatial_contexts 
    strings with the full spatial data from expand_spatial.
    
    Args:
        structured_data: Dictionary with spatial_contexts as list of strings
        
    Returns:
        Dictionary with spatial_contexts as list of spatial data dictionaries
        
    Example:
        Input: {
            "fact_type": "temporal_fact",
            "subjects": ["Will"],
            "objects": ["cats"],
            "relation_type": "likes",
            "temporal_intervals": [{"start_time": "2020-01-01T00:00:00", "end_time": "2021-12-31T23:59:59"}],
            "spatial_contexts": ["Imperial College London", "Cambridge University"]
        }
        
        Output: {
            "fact_type": "temporal_fact",
            "subjects": ["Will"],
            "objects": ["cats"],
            "relation_type": "likes",
            "temporal_intervals": [{"start_time": "2020-01-01T00:00:00", "end_time": "2021-12-31T23:59:59"}],
            "spatial_contexts": [
                {
                    "name": "Imperial College London",
                    "type": "Point",
                    "coordinates": [-0.179359, 51.498711]
                },
                {
                    "name": "Cambridge University",
                    "type": "Point", 
                    "coordinates": [0.121800, 52.204722]
                }
            ]
        }
    """
    
    if 'spatial_contexts' not in structured_data:
        print("Warning: No spatial_contexts field found")
        return structured_data
    
    # Process each spatial context string
    expanded_spatial_contexts = []
    
    for location_name in structured_data['spatial_contexts']:
        # Skip null values and placeholder text
        if location_name is None:
            continue
            
        if isinstance(location_name, str) and location_name.strip():
            # Skip placeholder text like "unknown", "none", "n/a" etc
            location_lower = location_name.strip().lower()
            if location_lower in ["unknown", "none", "n/a", "not specified", "unspecified"]:
                continue
                
            try:
                spatial_data = expand_spatial(location_name.strip())
                
                if spatial_data:
                    expanded_spatial_contexts.extend(spatial_data)
                else:
                    expanded_spatial_contexts.append({
                        "name": location_name.strip(),
                        "type": "Point",
                        "coordinates": None
                    })
            except Exception as e:
                print(f"Error expanding spatial context for '{location_name}': {e}")
                expanded_spatial_contexts.append({
                    "name": location_name.strip(),
                    "type": "Point",
                    "coordinates": None
                })
        else:
            if location_name:
                expanded_spatial_contexts.append({
                    "name": str(location_name),
                    "type": "Point",
                    "coordinates": None
                })
    
    # Replace the spatial_contexts with expanded data to meet the expected structure
    structured_data['spatial_contexts'] = expanded_spatial_contexts
    
    return structured_data


def expand_spatial(text: str) -> List[Dict[str, Any]]:
    """
    Takes the name of a location into structured data format of spatial_contexts.
    Convert a place name into structured spatial data (Point or Polygon).
    - Uses Mapbox Geocoding API for points.
    - Falls back to Nominatim (OSM) for polygons (boundaries).

    Returns: list of dicts with name, type, and coordinates.

    
    Example:
        input: "Boston robotics lab"
        output: [{
            "name": "Boston robotics lab",
            "type": "Point",
            "coordinates": [-71.059750, 42.359775]
        }]
    """
    # Geocoding of spatial names to coordinates (either point or polygon)
    import requests

    MAPBOX_ACCESS_TOKEN = 'pk.eyJ1IjoibW9sbHltb2xzIiwiYSI6ImNtZTJ6ZGh6ZjAxdjMycXF6MHZwb2tudDAifQ.a34RHi95buIDamPtR282sA'
    
    results = []

    # 1. Try Mapbox Geocoding API which can encode Points (polygons are paid)
    mapbox_url = (
        f"https://api.mapbox.com/geocoding/v5/mapbox.places/{requests.utils.quote(text)}.json"
        f"?access_token={MAPBOX_ACCESS_TOKEN}&limit=1"
    )
    resp = requests.get(mapbox_url)
    if resp.ok:
        data = resp.json()
        if data["features"]:
            feature = data["features"][0]
            coords = feature["geometry"]["coordinates"]
            results.append({
                "name": text,
                "type": "Point",
                "coordinates": coords
            })
            return results

    # 2. Fallback: Nominatim for polygons (free)
    # Request full-detail polygons (we will simplify client-side deterministically below)
    nominatim_url = (
        f"https://nominatim.openstreetmap.org/search"
        f"?format=json&polygon_geojson=1&polygon_threshold=0&q={requests.utils.quote(text)}"
    )
    resp = requests.get(nominatim_url, headers={"User-Agent": "spatial-expander/1.0"})
    if resp.ok:
        data = resp.json()
        if data:
            place = data[0]
            if "geojson" in place:  # polygon available - check if it's Polygon or MultiPolygon
                geom_type = place["geojson"]["type"]
                coords = place["geojson"].get("coordinates")
                if geom_type in ["Polygon", "MultiPolygon"] and coords is not None:
                    # Strictly cap polygon complexity to keep payload small e.g. in case of a large polygon like the UK
                    MAX_POINTS = 20

                    def ensure_closed(ring):
                        if not ring:
                            return ring
                        if ring[0] != ring[-1]:
                            return ring + [ring[0]]
                        return ring

                    def ring_unique_vertices(ring):
                        if ring and ring[0] == ring[-1]:
                            return ring[:-1]
                        return ring

                    def decimate_ring(ring, target_vertices):
                        ring = ensure_closed(ring)
                        unique = ring_unique_vertices(ring)
                        n = len(unique)
                        if n <= max(4, target_vertices):  # keep as-is (will close below)
                            out = unique[:]
                        else:
                            # Evenly sample indices across the ring
                            step = n / float(target_vertices)
                            indices = []
                            k = 0
                            while len(indices) < target_vertices and k * step < n:
                                idx = int(k * step)
                                if not indices or idx != indices[-1]:
                                    indices.append(idx)
                                k += 1
                            # Guarantee first included
                            if 0 not in indices:
                                indices = [0] + indices
                            # Trim to target
                            indices = indices[:target_vertices]
                            out = [unique[i] for i in indices]
                        # Close the ring
                        if not out or out[0] != out[-1]:
                            out = out + [out[0]]
                        return out

                    # Normalize into list of polygons → rings structure
                    if geom_type == "Polygon":
                        polygons = [coords]
                    else:  # MultiPolygon
                        polygons = coords

                    num_rings = sum(len(poly) for poly in polygons)
                    if num_rings == 0:
                        results.append({
                            "name": text,
                            "type": "Point",
                            "coordinates": [float(place["lon"]), float(place["lat"])]
                        })
                        return results

                    # If there are too many rings to represent minimally, fallback to a point
                    if num_rings * 4 > MAX_POINTS:
                        results.append({
                            "name": text,
                            "type": "Point",
                            "coordinates": [float(place["lon"]), float(place["lat"])]
                        })
                        return results

                    per_ring_cap = max(4, MAX_POINTS // num_rings)

                    simplified_polygons = []
                    for poly in polygons:
                        simplified_rings = []
                        for ring in poly:
                            simplified_rings.append(decimate_ring(ring, per_ring_cap))
                        simplified_polygons.append(simplified_rings)

                    simplified_coords = simplified_polygons[0] if geom_type == "Polygon" else simplified_polygons

                    results.append({
                        "name": text,
                        "type": geom_type,
                        "coordinates": simplified_coords
                    })
                else:  # If it's LineString or other, fallback to point
                    results.append({
                        "name": text,
                        "type": "Point",
                        "coordinates": [float(place["lon"]), float(place["lat"])]
                    })
            else:  # fallback to point
                results.append({
                    "name": text,
                    "type": "Point",
                    "coordinates": [float(place["lon"]), float(place["lat"])]
                })

    return results


async def expand_temporal_facts_for_sentence(sentence: str, full_context: str, openai_interface) -> str:
    """
    Expand a single sentence into explicit temporal facts using the full context for disambiguation.
    This function transforms a single sentence into simple, explicit sentences where each relationship
    is given its own sentence in a standardised format.
    
    Args:
        sentence: Single sentence to expand
        full_context: Full text context for entity disambiguation
        openai_interface: OpenAI interface instance
        
    Returns:
        Expanded text where each relationship is in its own sentence
        
    """
    try:
        system_prompt = """You are a text expansion agent. 
You transform a single sentence into simple, explicit sentences in a standardised format.

## Your steps
1. Identify all relationships in the input sentence, by finding verbs. Find all the entities involved with that specific relationship and combine them into one sentence if they have the same spatial-temporal information (or both unknown spatial and temporal information).
2. Break down each sentence into individual subject(s)-relation-object(s)-time(s)-location(s) statements (objects are optional).
3. Use the full context provided to resolve any pronouns or ambiguous references.
4. Rewrite everything into the strict format below.

## Entity Disambiguation Rules
**CRITICAL**: Use the full context to disambiguate entity names and give them the most standalone meaning possible. 

- **Pronoun Resolution**: When you see pronouns like "he", "she", "they", "it", "his", "her", "their", etc., use the full context to determine what they refer to.
- **Example**: If the sentence says "She won the prize" and the context shows "Marie Curie was a prizewinning scientist", use "Marie Curie" not "She".

- **Possession Disambiguation**: When something is owned by someone, use the most descriptive name possible.
- **Example**: "John likes his game" should become "John : likes : John's game" (not "his game").

- **Context Analysis**: Use the full context to understand which entity each pronoun or vague reference refers to, then use the most specific name available.
- **Context Usage Restriction**: Use the full context ONLY for disambiguation (e.g., resolving pronouns or choosing between entities with the same name). Do NOT import actions, verbs, or additional relations from other sentences into the current fact.

- **Entity Name Disambiguation**: When the same entity name appears multiple times but refers to different things (infer this from the context), add a category in parentheses after the entity name to distinguish them.
- **Example**: "Stanley cup" could refer to a trophy or a beverage container. Use context to determine the category:
  * "John wins the Stanley cup" → Use "Stanley cup (trophy)"
  * "John drinks from the Stanley cup" → Use "Stanley cup (flask)"
- **Ambiguous EntityFormat**: Entity (category)

- **Entity Canonicalization (CRITICAL)**: When different phrases clearly refer to the same entity, choose ONE canonical surface form and use it consistently across all expanded sentences. Pick the most explicit descriptive name that names the entity itself (keep leading articles), not scaffolding/appositive phrasing like "the X called Y".
  * Example: if "the train called the Venice Simplon-Orient-Express" and "the express train" are the same entity, then use "The Venice Simplon-Orient-Express" as the canonical surface form.
  - Do NOT create trivial self-naming facts like: "The Venice Simplon-Orient-Express : is called : The Venice Simplon-Orient-Express from unknown to unknown at unknown." This adds no information.
  - If a discarded phrase conveys a meaningful type/category (e.g., "the train"), add a separate type attribution fact using the canonical name and the type noun:
    * Example: "The Venice Simplon-Orient-Express : is : A train from unknown to unknown at unknown."

## Logical Inference Rules
**IMPORTANT**: Infer these specific types of additional guaranteed relationships that are not given in the input text, but that are logically certain, from the given sentence ONLY (NOT the whole context).

- **Life Status Inference**: If someone is born or dies, infer their life status during relevant periods.
- **Example**: "John was born in 2000" → Infer: "John : is : Alive from 2000-01-01T00:00:00 to unknown at unknown"
- **Example**: "Marie Curie died in 1934" → Infer: "Marie Curie : is : Alive from unknown to 1934-07-04T00:00:00 at unknown"

- **Keep Inferences Non-Trivial**: Only infer relationships that add meaningful information, not trivially-obvious, redundant or not necessarily true facts.
- **Good**: "John graduated from university in 2020" → Infer: "John : has : A university degree from 2020-01-01T00:00:00 to unknown at unknown"
- **Avoid**: "John ate lunch" → Don't infer: "John : is : Hungry before eating" (may not be true, not directly referenced) or "John : exists : from unknown to unknown at unknown" (trivial)

- **Ownership Inference**: If someone acquires or loses ownership, infer their ownership status during relevant periods.
- **Example**: "John bought a car in 2020" → Infer: "John : owns : A car from 2020-01-01T00:00:00 to unknown at unknown"

- **Symmetric Relations (CRITICAL)**: If the relation is symmetric (a relation between two entities necessarily implies the inverse), emit both directions as separate sentences by swapping the subject(s) and object(s) while keeping identical temporal intervals and locations. Examples include: "marries", "is sibling of", "is equal to", "is adjacent to".
  - **Example**: Input describes "Molly is the sibling of Heidi" → Emit both:
    "Molly : is sibling of : Heidi from unknown to unknown at unknown."
    "Heidi : is sibling of : Molly from unknown to unknown at unknown."

### STRICT SCOPE OF INFERENCE
- Only perform the allowed inferences above. Do NOT invent any other inferred facts.
- Do not generate extra actions, events, or relations that are not explicitly present in the input sentence unless they are one of the three allowed inferences above.
- The relation for a fact must come from a verb (or verb phrase) in the CURRENT input sentence (normalized to present tense singular). Do not combine or chain verbs from other sentences when forming a fact.
- Per-sentence focus (CRITICAL): Expand only the facts that this sentence primarily describes. If this sentence merely references a fact that is already fully described elsewhere in the context and adds no new information (no new subjects/objects, and no new time/location intervals), do NOT output that duplicate fact here. If it adds new intervals/locations for the same fact, output only the new intervals/locations without repeating ones already given elsewhere.

## Temporal handling rules
- Prefer ISO 8601 timestamps (YYYY-MM-DDTHH:MM:SS) for start and end times when they can be resolved.
- If only one side is given, set the other to unknown.
- If no concrete ISO time can be resolved but temporal information is present, use a concise descriptive string in place of the ISO time.
- Ambiguous phrases should be mapped to descriptive interval bounds:
  - "during X" → from "start of X" to "end of X"
  - "after X" → from "end of X" to unknown
  - "before X" → from unknown to "start of X"
- Examples of descriptors: "start of the wedding", "end of school term", "after sunrise". Keep them short and literal.
- An interval may mix ISO and a descriptor (e.g., start ISO, end descriptor).
- Time zone normalization: Emit all ISO timestamps in naive UTC (no trailing Z or +00:00). If a time zone is specified (e.g., CEST) or implied by the location (e.g., 9am in Paris), convert the local time to UTC before emitting. If neither time zone nor location is given, leave as given.
- Daylight saving time (DST): When converting local times to UTC, compute the offset for the specific date (account for DST transitions). Do not assume a fixed offset across different dates in the same month.
- BST (British Summer Time) is equivalent to UTC+1. BST ends on the 26th October 2025, after which British time is UTC+0. Tuesday 28th October is NOT in BST, therefore 11am on the 28th October is 11am UTC.

## Spatiotemporal grouping rules (CRITICAL)
- If the SAME subjects and objects share multiple times and/or locations that are meant to be combinable (cartesian product), write a SINGLE sentence and:
  - List each time interval as a separate "from ... to ..." phrase with NO "and" between time phrases.
  - List each location as a separate "at ..." phrase with NO "and" between location phrases.
  - Example (times combine with locations): "... from 2025-10-07T11:00:00 to unknown from 2025-10-14T11:00:00 to unknown at Imperial College London at Queen's Lawn."
- If time-location pairs are DISTINCT and must NOT be cross-combined, separate each pair with "and" by repeating the full pair "from ... to ... at ..." for each:
  - Example: "... from 2025-10-01T17:00:00 to 2025-10-01T18:00:00 at London and from 2025-10-01T22:00:00 to 2025-10-01T23:00:00 at Bristol."
- Multiple times for the SAME location: chain the time phrases then write a single "at ..." once at the end:
  - Example: "... from 2025-10-07T11:00:00 to unknown from 2025-10-14T11:00:00 to unknown at Imperial College London."
- Multiple locations for the SAME time: write the single time once then chain multiple "at ..." phrases:
  - Example: "... from 2025-10-14T11:00:00 to unknown at Imperial College London at Queen's Lawn."
- Use "and" ONLY between full pair blocks that should NOT be cartesian product-ed.

## Formatting rules
- Present tense only, can contain modal auxiliary: "likes" not "liked", "works as" not "worked as", "can buy" not "could buy". If the sentence includes a modal auxiliary (e.g., "can", "could"), keep the PRESENT tense version of the modal with the verb (e.g., "can buy"); do NOT collapse to "buys".
- Use colon separators between fields EXACTLY as: "[Subject(s)] : [relation] : [object(s)] ... from ... to ... from ... to ... ... at ... at ..." with optional "and" ONLY between non-combinable pair blocks ("from ... to ... at ... and from ... to ... at ...").
- For intransitive verbs (no objects), still include both colons and leave objects empty: "[Subject(s)] : [relation] : from ...". A time or location should NOT be treated as an object, but rather as a temporal interval or location name.
- Use "unknown" if a time or location is missing. If a descriptor is used, include the descriptor text directly in place of the time.
- Do NOT use "and" to join times or locations that should combine; use adjacency (no "and").
- Capitalization normalization: Always capitalize the first word of each subject entity and each object entity (e.g., "the farmers' market" should be written as "The farmers' market").
  * Example: "the farmers' market" → "The farmers' market"; "a fish" → "A fish".
 - Multiple entities formatting: If there are multiple subjects and/or objects for the same relation, list them within their field, with the first letter of each distinct entity capitalized, separated by "and" (preserving articles) — e.g., "Alice and Bob : likes : Cats and Dogs ...". If one entity contains the word "and" within it, e.g. "Food from China, India and Japan", then write it as "Food from China, India & Japan" to signify not separating the entity.
 - "and" usage scope (CRITICAL): Use "and" ONLY to separate distinct top-level subjects/objects. Do NOT insert "and" inside a single entity name or noun phrase list — keep internal lists as commas + "&" and treat them as ONE entity. Do not promote internal items (e.g., "Turkey") to separate subjects/objects.
   * Example (correct): "Students : can buy : Food that originates from India, Turkey, France & China ..." (ONE object entity, not four)
- Object phrase integrity: Objects must reflect the object span of the CURRENT sentence. Typically this is a noun phrase; verb phrases are allowed ONLY if they appear as the sentence's object (e.g., a quoted title). Never borrow a verb from another sentence or from context into the object. Do not include temporal or spatial phrases inside objects — those belong in the "from ... to ..." and "at ..." fields.
  * Good:  "The Abdus Salam Library : overlooks : The farmers' market ..."
  * Bad:   "The Abdus Salam Library : overlooks : The farmers' market set up ..." ("set up" comes from another action; do not import it into the object)
- CRITICAL GROUPING RULE: If multiple facts have the same subject(s), relation type, temporal intervals, and spatial contexts, combine ALL their objects into ONE fact sentence. 
  * Example: "John likes cats at home" and "John likes dogs at home" → "John : likes : cats and dogs from unknown to unknown at home"
  * This applies even when times/locations are "unknown" - if they match, combine the objects.
- IMPORTANT: If the subject(s), relationship, and object(s) are the same, combine multiple temporal intervals and/or locations into ONE fact sentence.
- Ignore causality ("because", "led to", etc).
- KEEP ARTICLES ("a", "an", "the") as part of object entities. For example: "John works as an optometrist and a doctor" should become ONE relationship: "John : works as : An optometrist and A doctor". Articles are part of the object and should be preserved.
- No duplicate facts: Do not output paraphrases or repeats of the same subject(s)-relation-object(s) with identical times/locations within the same output.

## Examples

Input sentence: "Marie Curie won the Nobel Prize for Physics in 1903 and 1911."
Full context: "Marie Curie was a pioneering scientist. Marie Curie won the Nobel Prize for Physics in 1903 and 1911. She also won the Nobel Prize for Chemistry in 1911."

Output:
"Marie Curie : wins : The Nobel Prize for Physics from 1903-01-01T00:00:00 to 1903-12-31T23:59:59 from 1911-01-01T00:00:00 to 1911-12-31T23:59:59 at unknown."

Input sentence: "John died in 1995 at the hospital."
Full context: "John was alive from unknown to 1995-01-01T00:00:00 at unknown. John died in 1995."

Output:
"John : dies : from 1995-01-01T00:00:00 to 1995-12-31T23:59:59 at the hospital."

Input sentence: "Molly (a farmer) and her sister Heidi began liking apples, pears and oranges in 1970 to 1999 and again in 2020 for a year. Her sister started liking them again from 2022 to 2025."
Full context: "Molly and Heidi are sisters. Molly and her sister Heidi began liking apples, pears and oranges in 1970 to 1999 and again in 2020 for a year. Her sister started liking them again from 2022 to 2025. They both like fruits."

Output:
"Molly : is : A farmer from unknown to unknown at unknown.
Molly and Heidi : likes : Apples and Pears and Oranges from 1970-01-01T00:00:00 to 1998-12-31T23:59:59 and from 2020-01-01T00:00:00 to 2020-12-31T23:59:59 at unknown.
Heidi : likes : Apples and Pears and Oranges from 2022-01-01T00:00:00 to 2024-12-31T23:59:59 at unknown."

Input sentence: "Students can buy a duck in the Isle of Wight. They like a book in Truro and in Fowey."
Full context: same.

Output:
"Students : can buy : A duck from unknown to unknown at the Isle of Wight.
Students : likes : A book from unknown to unknown at Truro at Fowey."

Input sentence: "The farmers' market sets up every Tuesday at Imperial College London in October 2025 (Tuesdays: 7th, 14th, 21st, 28th)."
Full context: same.

Output:
"The farmers' market : sets up : from 2025-10-07T10:00:00 to unknown from 2025-10-14T10:00:00 to unknown from 2025-10-21T10:00:00 to unknown from 2025-10-28T11:00:00 to unknown at Imperial College London."

Input sentence: "The train stops at London at 5-6pm and at Bristol at 10-11pm on the 1st of January 2025."
Full context: same.

Output:
"The train : stops : from 2025-01-01T17:00:00 to 2025-01-01T18:00:00 at London and from 2025-01-01T22:00:00 to 2025-01-01T23:00:00 at Bristol."

Input sentence: "Bob likes food that originates in China and Thailand."
Full context: "Bob likes food that originates in China and Thailand."

Output:
"Bob : likes : Food that originates in China & Thailand from unknown to unknown at unknown.
Food that originates in China & Thailand : originates : from unknown to unknown at China at Thailand."

Transform the following sentence into expanded, explicit sentences following the format above. Use the full context to resolve any ambiguous references. Write each relationship as a separate sentence. Do not add explanations or commentary - just return the expanded text string.
"""

        # Call OpenAI with configurable model, providing current time context for resolving phrases like "now" / "today"
        current_time_iso = datetime.now(timezone.utc).isoformat()
        response = await openai_interface.chat_completion(
            model="gpt-5-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "system", "content": f"Current time context (UTC): {current_time_iso}. Interpret relative temporal phrases like 'now', 'today', 'yesterday', 'this month/year' using this as the reference."},
                {"role": "user", "content": f"Full context:\n{full_context}\n\nSentence to expand:\n{sentence}"}
            ]
        )
        expanded_text = response.strip()
        
        # Clean up any common LLM formatting artifacts
        expanded_text = re.sub(r'^```\w*\n?', '', expanded_text)  # Remove code block markers
        expanded_text = re.sub(r'\n?```$', '', expanded_text)
        expanded_text = re.sub(r'^Output:\s*', '', expanded_text, flags=re.IGNORECASE)
        expanded_text = re.sub(r'^Expanded text:\s*', '', expanded_text, flags=re.IGNORECASE)
        
        return expanded_text.strip()
        
    except Exception as e:
        print(f"Error in expand_temporal_facts_for_sentence: {e}")
        print("Returning original sentence")
        # Return original sentence if expansion fails
        return sentence


def extract_partial_structured_state_facts(structured_temporal_facts: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Takes the structured temporal facts and procedurally extracts template fact state information from them.

    Input:
    structured_temporal_facts: [
            {
                "fact_type": "temporal_fact",
                "subjects": ["Will"],
                "objects": ["university"],
                "relation_type": "graduates from",
                "temporal_intervals": [{"start_time": "null", "end_time": "null"}],
                "spatial_contexts": []
            }
        ]

        Output: [
            {
                "fact_type": "state_change_event",
                "affected_fact": {
                    "subjects": ["Will"],
                    "objects": ["university"],
                    "relation_type": "graduates from"
                },
                "caused_by": [],
                "causes": []
            },
        ]
        """
    output_facts = []
    
    for fact in structured_temporal_facts:
        # Create the state change event fact with the exact template structure
        state_fact = {
            "fact_type": "state_change_event",
            "affected_fact": {
                "subjects": fact["subjects"],
                "objects": fact["objects"],
                "relation_type": fact["relation_type"]
            },
            "caused_by": [],
            "causes": []
        }
        output_facts.append(state_fact)
    
    return output_facts


async def extract_structured_state_facts(
    whole_text: str,
    partial_structured_state_facts: List[Dict[str, Any]],
    openai_interface
) -> List[Dict[str, Any]]:
    """
    Takes the whole input text and partial structured state facts and uses GPT-5-mini
    to fill in the causality fields (caused_by, causes).
    Uses JSON structured outputs for reliability.
    Includes a worked example in the system prompt for few-shot accuracy.
    """
    try:
        system_prompt = """You are a data extraction agent.
Your job is to complete the causality fields in the partial structured state facts
by analyzing the input text.

RULES:
1. Do not change the structure of the input facts — only fill in the empty caused_by and causes fields IF there is a genuine causality link.
2. Keep "affected_fact" exactly as provided.
3. Normalize to only positive causality (what makes facts True and what they cause when True). A certain fact being True can cause an arbitrary number of things to either happen (become True) or not happen (become False).
4. To reference a fact, use the exact subjects, objects, and relation_type from the input including capitalization if present.
5. Field definitions:
   - caused_by: list of lists. [[A], [B, C]] means "A alone OR (B and C together)".
       Each reference has: subjects, objects, relation_type, triggered_by_state (True/False).
   - causes: list of entries describing what this fact being True causes.
       Each entry has: subjects, objects, relation_type, triggers_state (True/False),
       additional_required_states (list of extra conditions, can be empty).
6. If no causes or causes found, return [] for those fields. You MUST leave caused_by and/or causes empty when there is no genuine causality link.
7. Intransitive verbs: If a fact has no objects (e.g., "John dies"), set objects to an empty array [] in affected_fact and in any fact references inside caused_by/causes. Do not omit the objects field.
8. Return ONLY the completed JSON array, no commentary.

EXAMPLE INPUT:
whole_text: "Graduating from university caused Will to work for the Imperial Department of Computing from 2020 until 2025."
partial_structured_state_facts: [
    {
        "fact_type": "state_change_event",
        "affected_fact": {
            "subjects": ["Will"],
            "objects": ["university"],
            "relation_type": "graduates from"
        },
        "caused_by": [],
        "causes": []
    },
    {
        "fact_type": "state_change_event",
        "affected_fact": {
            "subjects": ["Will"],
            "objects": ["Imperial Department of Computing"],
            "relation_type": "works for"
        },
        "caused_by": [],
        "causes": []
    }
]

EXAMPLE OUTPUT:
[
    {
        "fact_type": "state_change_event",
        "affected_fact": {
            "subjects": ["Will"],
            "objects": ["university"],
            "relation_type": "graduates from"
        },
        "caused_by": [[]],
        "causes": [  
            {
                "subjects": ["Will"],
                "objects": ["Imperial Department of Computing"],
                "relation_type": "works for",
                "triggers_state": True,
                "additional_required_states": []
            }
        ]
    },
    {
        "fact_type": "state_change_event",
        "affected_fact": {
            "subjects": ["Will"],
            "objects": ["Imperial Department of Computing"],
            "relation_type": "works for"
        },
        "caused_by": [
            [
                {
                    "subjects": ["Will"],
                    "objects": ["university"],
                    "relation_type": "graduates from",
                    "triggered_by_state": True
                }
            ]
        ],
        "causes": []
    }
]"""

        user_prompt = f"""Input text:\n{whole_text}\n
Partial structured state facts:\n{partial_structured_state_facts}"""

        response = await openai_interface.chat_completion(
            model="gpt-5-mini",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "state_change_event_schema",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "state_facts": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "fact_type": {"type": "string", "enum": ["state_change_event"]},
                                        "affected_fact": {
                                            "type": "object",
                                            "properties": {
                                                "subjects": {"type": "array", "items": {"type": "string"}},
                                                "objects": {"type": "array", "items": {"type": "string"}},
                                                "relation_type": {"type": "string"}
                                            },
                                            "required": ["subjects", "objects", "relation_type"]
                                        },
                                        "caused_by": {
                                            "type": "array",
                                            "items": {
                                                "type": "array",
                                                "items": {
                                                    "type": "object",
                                                    "properties": {
                                                        "subjects": {"type": "array", "items": {"type": "string"}},
                                                        "objects": {"type": "array", "items": {"type": "string"}},
                                                        "relation_type": {"type": "string"},
                                                        "triggered_by_state": {"type": "boolean"}
                                                    },
                                                    "required": ["subjects", "objects", "relation_type", "triggered_by_state"]
                                                }
                                            }
                                        },
                                        "causes": {
                                            "type": "array",
                                            "items": {
                                                "type": "object",
                                                "properties": {
                                                    "subjects": {"type": "array", "items": {"type": "string"}},
                                                    "objects": {"type": "array", "items": {"type": "string"}},
                                                    "relation_type": {"type": "string"},
                                                    "triggers_state": {"type": "boolean"},
                                                    "additional_required_states": {
                                                        "type": "array",
                                                        "items": {
                                                            "type": "object",
                                                            "properties": {
                                                                "subjects": {"type": "array", "items": {"type": "string"}},
                                                                "objects": {"type": "array", "items": {"type": "string"}},
                                                                "relation_type": {"type": "string"},
                                                                "state": {"type": "boolean"}
                                                            },
                                                            "required": ["subjects", "objects", "relation_type", "state"]
                                                        }
                                                    }
                                                },
                                                "required": ["subjects", "objects", "relation_type", "triggers_state", "additional_required_states"]
                                            }
                                        }
                                    },
                                    "required": ["fact_type", "affected_fact", "caused_by", "causes"]
                                }
                            }
                        },
                        "required": ["state_facts"]
                    }
                }
            }
        )

        response_content = response.strip()
        try:
            import json
            parsed = json.loads(response_content)
            return parsed.get('state_facts', [])
        except json.JSONDecodeError:
            print(f"Failed to parse JSON response: {response_content}")
            return partial_structured_state_facts

    except Exception as e:
        print(f"Error in extract_structured_state_facts: {e}")
        return partial_structured_state_facts

def clean_text(text: str) -> str:
    """
    Clean the text of non-text characters such as citations, diacritics, etc.
    Example:
        input: "Bjarne Stroustrup (/ˈbjɑːrnə ˈstrɒvstrʊp/ ⓘ; Danish: [ˈbjɑːnə ˈstʁʌwˀstʁɔp];[3][4]
                born 30 December 1950) is a Danish computer scientist, known for the development of the C++
                programming language.[5]"
        output: "Bjarne Stroustrup (/bjrn strvstrup/ ; Danish: [bjrn stwstp];
                born 30 December 1950) is a Danish computer scientist, known for the development of the C++ 
                programming language."
    """
    
    text = re.sub(r'\[\d+\]', '', text)
    # Remove combining diacritical marks (Unicode ranges 0300-036F)
    text = re.sub(r'[\u0300-\u036F]', '', text)
    pronunciation_symbols = r'[ˈˌːˑ˘˗˴˵˶˷˸˹˺˻˼˽˾˿ˀˉˊˋˌˍˎˏˑ˒˓˔˕˖˗˘˙˚˛˜˝˞˟ˠˡˢˣˤ˥˦˧˨˩˪˫ˬ˭ˮ˯˰˱˲˳˴˵˶˷˸˹˺˻˼˽˾˿]'
    text = re.sub(pronunciation_symbols, '', text)
    
    # Remove additional IPA vowels and consonants that could confuse LLMs
    ipa_vowels = r'[ɑɒʊəɜɨɯɵɶɷɸɹɺɻɼɽɾɿʀʁʂʃʄʅʆʇʈʉʊʋʌʍʎʏʐʑʒʓʕʖʗʘʙʚʛʜʝʞʟʠʡʢʣʤʥʦʧʨʩʪʫʬʭʮʯɔ]'
    text = re.sub(ipa_vowels, '', text)
    text = re.sub(r'[⁰¹²³⁴⁵⁶⁷⁸⁹₀₁₂₃₄₅₆₇₈₉]', '', text)
    text = re.sub(r'[ⓘⓐⓑⓒⓓⓔⓕⓖⓗⓘⓙⓚⓛⓜⓝⓞⓟⓠⓡⓢⓣⓤⓥⓦⓧⓨⓩ]', '', text)
    # Remove control characters but preserve newlines (\n = \u000A) and carriage returns (\r = \u000D)
    text = re.sub(r'[\u0000-\u0009\u000B-\u001F\u007F-\u009F]', '', text)
    
    # Additional cleaning to prevent malformed sentences
    # Remove standalone brackets and punctuation that could create invalid sentences
    text = re.sub(r'\s+[\[\]{}]\s+', ' ', text)  # Remove standalone brackets with spaces
    text = re.sub(r'^\s*[\[\]{}]\s*', '', text)  # Remove brackets at start
    text = re.sub(r'\s*[\[\]{}]\s*$', '', text)  # Remove brackets at end
    
    # Clean up multiple spaces and normalise whitespace
    text = re.sub(r'\s+', ' ', text)
    text = text.strip()
    
    return text


async def detect_modification_sentences(text: str, with_LLM_call=False, openai_interface=None) -> tuple[str, str]:
    """
    Detect modification sentences in the text and separate them from regular temporal fact sentences.
    
    A modification sentence is one that describes changes to existing facts in the graph, such as:
    - "Actually, John likes to read magazines, not books."
    - "The date Sally booked the race tickets was actually the 20th October"
    - "In fact, the meeting was on Tuesday, not Monday"
    
    Args:
        text: Input text to analyze
        openai_interface: Optional OpenAILLMInterface instance. If None, will try to create one.
    
    Returns:
        Tuple of (regular_text, modification_text) where:
        - regular_text: Text containing sentences that will become temporal facts
        - modification_text: Text containing sentences that describe modifications to existing facts
    """
    
        
    # First, do a quick check for obvious modification indicators
    modification_indicators = ['actually', 'in fact', 'oops', 'my mistake', 'update', 'correction', 'modification'] # Check all the keywords here
    text_lower = text.lower()
    
    # Split text into sentences and check each for modification indicators
    sentences = [s.strip() for s in text.split('.') if s.strip()]
    regular_sentences = []
    modification_sentences = []
    
    for sentence in sentences:
        sentence_lower = sentence.lower()
        is_modification = any(indicator in sentence_lower for indicator in modification_indicators)
        
        if is_modification:
            modification_sentences.append(sentence)
        else:
            regular_sentences.append(sentence)
    
    # If no LLM call requested, return keyword-based results
    if not with_LLM_call:
        regular_text = '. '.join(regular_sentences) if regular_sentences else text
        modification_text = '. '.join(modification_sentences) if modification_sentences else ""
        return regular_text, modification_text
    
    try:
        from kh_core.openai_llm_interface import OpenAILLMInterface, call_openai_once
        
        # Create OpenAI interface if not provided
        if openai_interface is None:
            openai_interface = OpenAILLMInterface()
        
        # Create the modification detection prompt
        system_prompt = """You are a text analysis agent that identifies modification sentences in text.
A modification sentence is one that describes changes to existing facts, corrections, or updates. Examples include:
- "To all intents and purposes, John runs the company, not Mike." (corrects subject)
- "Oops, Sally booked the race tickets on the 20th October instead of the 15th" (corrects time)
- "The meeting was on Tuesday, not Monday" (corrects time)
- "My mistake, the location of John's meeting was London" (corrects location)
- "Update: the relationship ended in 2021, not 2020" (corrects time and subject)

A regular temporal fact sentence is one that states new facts without correcting existing ones:
- "John really liked cats from 2020 onwards" (Assume "really" is for emphasis)
- "Sally booked race tickets on October 15th"
- "The meeting was on Monday at 2pm"

Your task is to classify each sentence in the input text as either:
1. REGULAR - a sentence that states new temporal facts
2. MODIFICATION - a sentence that corrects or updates existing facts

Return your response in this exact format:
REGULAR:
[list all regular sentences, one per line]

MODIFICATION:
[list all modification sentences, one per line]

If there are no modification sentences, just return:
REGULAR:
[all sentences]"""

        user_prompt = f"Text to analyze:\n{text}"

        messages = [
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt}
        ]

        response = await openai_interface.chat_completion(
            model=openai_interface.model,
            messages=messages
        )
        
        # If there is an error, just use the already found modification sentences and remaining text as regular text
        if response is None:
            regular_text = '. '.join(regular_sentences) if regular_sentences else text
            modification_text = '. '.join(modification_sentences) if modification_sentences else ""
            return regular_text, modification_text
        
        # Parse the response
        response_text = response.strip()
        
        # Extract regular and modification text from LLM response
        llm_regular_sentences = []
        llm_modification_sentences = []
        
        current_section = None
        for line in response_text.split('\n'):
            line = line.strip()
            if line == "REGULAR:":
                current_section = "regular"
            elif line == "MODIFICATION:":
                current_section = "modification"
            elif line and current_section:
                if current_section == "regular":
                    llm_regular_sentences.append(line)
                elif current_section == "modification":
                    llm_modification_sentences.append(line)
        
        # Use LLM results if available, otherwise fall back to keyword results
        if llm_regular_sentences or llm_modification_sentences:
            regular_text = '\n'.join(llm_regular_sentences) if llm_regular_sentences else text
            modification_text = '\n'.join(llm_modification_sentences) if llm_modification_sentences else ""
        else:
            regular_text = '. '.join(regular_sentences) if regular_sentences else text
            modification_text = '. '.join(modification_sentences) if modification_sentences else ""
    
        return regular_text, modification_text
        
    except Exception as e:
        print(f"Error in detect_modification_sentences: {e}")
        # Return keyword-based results if LLM fails
        regular_text = '. '.join(regular_sentences) if regular_sentences else text
        modification_text = '. '.join(modification_sentences) if modification_sentences else ""
        return regular_text, modification_text
    

async def extract_structured_modifications(modification_text: str, openai_interface) -> List[Dict[str, Any]]:
    """
    Extract modification events from a sentence or multiple sentences using GPT-5-mini.

    Return format:
    [
        {
            "fact_type": "modification",
            "affected_fact": {
                "fact_type": "temporal_fact",
                "subjects": [str],
                "objects": [str],
                "relation_type": str
            },
            "modify_fields_to": {
                field_name: new_value(s) (only the fields that change)
            }
        }
    ]
    """
    try:
        system_prompt = """You are a data extraction agent.
Your task is to parse sentences that describe corrections or changes to temporal facts
and output structured JSON describing the modification.

### TEMPORAL FACT STRUCTURE
A temporal_fact has these fields:
- type: always "temporal_fact"
- subjects: [string, ...]
- objects: [string, ...] (can be empty array [] if no objects are given. Times and locations are NOT objects.)
- relation_type: string (EXACTLY ONE, present tense singular form)
- temporal_intervals: list of {start_time, end_time}
- spatial_contexts: [string, ...] (location name(s))

### MODIFICATION RULES
1. Always set "fact_type" = "modification".
2. "affected_fact" should identify the original fact **only by subjects, objects, relation_type**.
   - Do NOT include temporal_intervals or spatial_contexts here.
3. "modify_fields_to" = dictionary of only the fields to modify, and the values to set them to.
   - Keys: any of ["subjects", "objects", "relation_type", "temporal_intervals", "spatial_contexts"].
   - Values: the corrected version(s) of those fields.
4. Only include the fields that actually change. Do not repeat unchanged fields.
   - Example: if only the end_time changes, include just {"end_time": "..."} inside temporal_intervals.
   - Do NOT set a field to null unless the change is explicitly "field_name becomes unknown".
5. relation_type must always be a single string.
6. subjects/objects can be one or more.
7. If multiple corrections appear, output multiple modification objects.
8. Always output an array of modification objects, even if there is only one.

### EXAMPLES
Input: "Actually, John likes magazines, not books."
Output:
[
    {
        "fact_type": "modification",
        "affected_fact": {
            "fact_type": "temporal_fact",
            "subjects": ["John"],
            "objects": ["books"],
            "relation_type": "likes"
        },
        "modify_fields_to": {
            "objects": ["magazines"]
        }
    }
]

Input: "Correction: John died in 1996, not 1995."
Output:
[
    {
        "fact_type": "modification",
        "affected_fact": {
            "fact_type": "temporal_fact",
            "subjects": ["John"],
            "objects": [],
            "relation_type": "died"
        },
        "modify_fields_to": {
            "temporal_intervals": [
                {"start_time": "1996-01-01T00:00:00", "end_time": "1996-12-31T23:59:59"}
            ]
        }
    }
]

Input: "Correction: Tom studies Physics until 2026, not 2025."
Output:
[
    {
        "fact_type": "modification",
        "affected_fact": {
            "fact_type": "temporal_fact",
            "subjects": ["Tom"],
            "objects": ["Physics"],
            "relation_type": "studies"
        },
        "modify_fields_to": {
            "temporal_intervals": [
                {"end_time": "2025-12-31T23:59:59"}
            ]
        }
    }
]"""

        response = await openai_interface.chat_completion(
            model=MODEL_NAME,
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Modification text:\n{modification_text}"}
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "modification_schema",
                    "schema": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "fact_type": {"type": "string", "enum": ["modification"]},
                                "affected_fact": {
                                    "type": "object",
                                    "properties": {
                                        "subjects": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        },
                                        "objects": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        },
                                        "relation_type": {"type": "string"}
                                    },
                                    "required": ["subjects", "objects", "relation_type"]
                                },
                                "modify_fields_to": {
                                    "type": "object",
                                    "additionalProperties": True
                                }
                            },
                            "required": ["fact_type", "affected_fact", "modify_fields_to"]
                        }
                    }
                }
            }
        )

        structured_modifications_no_coords = response

        # Expand spatial contexts to include name, type, and coordinates
        for modification in structured_modifications_no_coords:
            modify_fields_to = modification.get('modify_fields_to', {})
            
            # Check if spatial_contexts field is being modified
            if 'spatial_contexts' in modify_fields_to:
                spatial_contexts = modify_fields_to['spatial_contexts']
                expanded_spatial_contexts = []
                
                # Process each spatial context (expected to be string location name)
                for spatial_context in spatial_contexts:
                    if isinstance(spatial_context, str):
                        # Expand the location name to get full spatial data
                        expanded = expand_spatial(spatial_context)
                        expanded_spatial_contexts.extend(expanded)
                
                # Update the modification with expanded spatial contexts
                modification['modify_fields_to']['spatial_contexts'] = expanded_spatial_contexts
        
        return structured_modifications_no_coords

    except Exception as e:
        print(f"Error in extract_structured_modifications: {e}")
        return []



from typing import Optional, Callable, Awaitable, Dict, Any

async def chunking_streaming_pipeline(text: str, chunk_size: int = 3, progress_cb: Optional[Callable[[Dict[str, Any]], Awaitable[None]]] = None):
    """
    Full pipeline for processing text into structured data ready to send to cypher generation and execution.
    
    Pipeline flow:
    1. Split input text into chunks of ~3 sentences
    2. Clean each chunk concurrently for efficiency
    3. Expand temporal facts for each chunk as it completes
    4. Extract structured data directly using extract_structure_no_coords_from_chunk
    5. Expand spatial coordinates for each fact
    6. Immediately send each structured data Dict to the graph using cypher generation and execution
    7. Track completion of the entire text
    8. Once complete, extract partial state facts and structured state facts for the whole text
    9. Yield structured data Dicts one at a time as they are produced
    10. After successful temporal fact commits, also yield structured state facts
    """
    # Initialise timing for the entire pipeline run
    pipeline_start_time = time.time()
    
    # Initialise components for graph operations
    from utils.cypher_generator import CypherGenerator
    from utils.text_to_cypher import TextToHyperSTructurePipeline
    
    cypher_generator = CypherGenerator()
    text_to_cypher_pipeline = TextToHyperSTructurePipeline()
    
    # Initialise Neo4j connection
    try:
        await text_to_cypher_pipeline.initialise_neo4j_connection()
    except Exception as e:
        print(f"Warning: Could not initialise Neo4j connection: {e}")
        print("Graph operations will be skipped")
        text_to_cypher_pipeline = None
    
    # Detect modification sentences first
    print("Detecting modification sentences...")
    modification_start_time = time.time()
    regular_text, modification_text = await detect_modification_sentences(text)
    modification_end_time = time.time()
    modification_duration = modification_end_time - modification_start_time
    print(f"Text separation complete: {len(regular_text.split('.'))} regular sentences, {len(modification_text.split('.')) if modification_text else 0} modification sentences")
    print(f"Modification detection completed in {modification_duration:.2f} seconds (total time: {modification_end_time - pipeline_start_time:.2f}s)")
    
    if modification_text:
        print(f"Modification text detected: {modification_text[:100]}...")

    # Process the modification text
    if modification_text:
        from kh_core.openai_llm_interface import OpenAILLMInterface
        openai_interface = OpenAILLMInterface()
        modification_facts = await extract_structured_modifications(modification_text, openai_interface)
    else:
        modification_facts = []
    
    # Split regular text into chunks
    chunking_start_time = time.time()
    chunks = split_text_into_chunks(regular_text, chunk_size)
    chunking_end_time = time.time()
    chunking_duration = chunking_end_time - chunking_start_time
    print(f"Text chunking completed in {chunking_duration:.2f} seconds (total time: {chunking_end_time - pipeline_start_time:.2f}s)")

    # Track all structured data for state fact extraction
    all_structured_data = []
    
    # Track successful graph operations for temporal facts
    successful_temporal_facts = []
    failed_temporal_facts = []
    
    # Process each sentence concurrently for temporal fact expansion and immediate structure extraction
    async def process_sentence_end_to_end(chunk_index, sentence_index, sentence):
        temporal_expansion_start = time.time()
        print(f"  Expanding sentence {sentence_index + 1} from chunk {chunk_index}: {sentence}")
        if progress_cb:
            try:
                await progress_cb({
                    "type": "stage",
                    "stage": "temporal_start",
                    "chunk": chunk_index,
                    "sentence": sentence_index + 1,
                    "message": f"Expanding temporal facts for sentence {sentence_index + 1}: {sentence}"
                })
            except Exception:
                pass
        
        # Use the new per-sentence function with full context
        expanded_sentence = await expand_temporal_facts_for_sentence(sentence, regular_text, openai_interface)
        # Output expanded temporal facts immediately to terminal
        try:
            print(f"  --- Expanded temporal facts for chunk {chunk_index}, sentence {sentence_index + 1} ---")
            print(expanded_sentence)
            print(f"  --- End expanded temporal facts ---")
        except Exception as e:
            print(f"  Warning: Failed to print expanded temporal facts: {e}")
        
        temporal_expansion_end = time.time()
        temporal_expansion_duration = temporal_expansion_end - temporal_expansion_start
        print(f"  ✓ Sentence {sentence_index + 1} from chunk {chunk_index} temporal expansion complete in {temporal_expansion_duration:.2f} seconds (total time: {temporal_expansion_end - pipeline_start_time:.2f}s)")
        if progress_cb:
            try:
                await progress_cb({
                    "type": "stage",
                    "stage": "temporal_done",
                    "chunk": chunk_index,
                    "sentence": sentence_index + 1,
                    "message": f"Finished expanding the spatio-temporal facts for sentence {sentence_index + 1}!"
                })
            except Exception:
                pass
        
        # Immediately process this expanded sentence with structure extraction
        try:
            structure_extraction_start = time.time()
            structured_data_list = await extract_structure_no_coords_from_chunk(expanded_sentence, openai_interface)
            structure_extraction_end = time.time()
            structure_extraction_duration = structure_extraction_end - structure_extraction_start
            print(f"  ✓ Sentence {sentence_index + 1} from chunk {chunk_index} structure extraction complete in {structure_extraction_duration:.2f} seconds (total time: {structure_extraction_end - pipeline_start_time:.2f}s)")
            if progress_cb:
                try:
                    await progress_cb({
                        "type": "stage",
                        "stage": "structure_done",
                        "chunk": chunk_index,
                        "sentence": sentence_index + 1,
                        "message": f"Finished extracting the structured JSON for sentence {sentence_index + 1}!"
                    })
                except Exception:
                    pass
            
            # Process each structured data item from this sentence
            results = []
            # Helper to sanitise a single structured fact to avoid placeholder junk entering the graph
            def sanitise_fact(fact: Dict[str, Any]) -> Dict[str, Any] | None:
                try:
                    if not isinstance(fact, dict):
                        return None
                    # Clean relation_type
                    rel = str(fact.get('relation_type', '') or '').strip()
                    if not rel or rel.lower() in {'unknown', '?'}:
                        return None
                    fact['relation_type'] = rel
                    # Clean subjects
                    subs = [str(s).strip() for s in (fact.get('subjects') or []) if s is not None]
                    subs = [s for s in subs if s and s != '?' and s.lower() != 'unknown']
                    if not subs:
                        return None
                    fact['subjects'] = subs
                    # Clean objects (objects may be empty by design for intransitive verbs)
                    objs = [str(o).strip() for o in (fact.get('objects') or []) if o is not None]
                    objs = [o for o in objs if o and o != '?' and o.lower() != 'unknown']
                    fact['objects'] = objs
                    return fact
                except Exception:
                    return None

            for i, structured_data in enumerate(structured_data_list):
                if structured_data:
                    # Add fact_type if not present
                    if 'fact_type' not in structured_data:
                        structured_data['fact_type'] = 'temporal_fact'
                    
                    # Expand spatial coordinates for this fact
                    spatial_expansion_start = time.time()
                    structured_data_with_spatial = expand_spatial_coordinates(structured_data)
                    spatial_expansion_end = time.time()
                    spatial_expansion_duration = spatial_expansion_end - spatial_expansion_start
                    print(f"  ✓ Sentence {sentence_index + 1} from chunk {chunk_index} fact {i+1} spatial expansion complete in {spatial_expansion_duration:.2f} seconds (total time: {spatial_expansion_end - pipeline_start_time:.2f}s)")
                    # Clean up the fact to avoid placeholder entities/relations, avoiding 'unknown' in the visualisation
                    sanitised = sanitise_fact(structured_data_with_spatial)
                    if sanitised is None:
                        print(f"Skipping invalid/placeholder fact for sentence {sentence_index + 1} (e.g. unknown relation or '?' entity)")
                        continue
                    if progress_cb:
                        try:
                            await progress_cb({
                                "type": "stage",
                                "stage": "spatial_done",
                                "chunk": chunk_index,
                                "sentence": sentence_index + 1,
                                "message": f"Finished spatial context and coordinates extraction for sentence {sentence_index + 1} fact {i+1}"
                            })
                        except Exception:
                            pass
                    
                    # Store for state fact extraction
                    all_structured_data.append(sanitised)
                    
                    # Immediately send to graph using cypher generation and execution
                    if text_to_cypher_pipeline:
                        try:
                            # Generate cypher query for this structured data
                            cypher_generation_start = time.time()
                            async for item in cypher_generator.generate_cypher_from_structured_output([sanitised], text_to_cypher_pipeline.neo4j_storage):
                                # Support both legacy string and (query, params) tuple
                                if isinstance(item, tuple) and len(item) == 2:
                                    query, params = item
                                else:
                                    query, params = str(item), {}
                                if query and str(query).strip():
                                    try:
                                        lines = str(query).strip().splitlines()
                                        # Extract contiguous MATCH/WHERE/WITH lines as context preview
                                        match_block = []
                                        in_block = False
                                        for ln in lines:
                                            ln_stripped = ln.strip()
                                            if ln_stripped.startswith(("MATCH", "WITH", "WHERE")):
                                                in_block = True
                                                match_block.append(ln)
                                            elif in_block:
                                                # Stop when we leave the header block
                                                break
                                        preview = "\n".join(match_block or lines[:6])
                                        print("    Cypher MATCH preview:\n" + preview)
                                    except Exception:
                                        pass
                                    # Execute the cypher query immediately
                                    cypher_execution_start = time.time()
                                    success = await text_to_cypher_pipeline.execute_cypher(query, params)
                                    cypher_execution_end = time.time()
                                    cypher_execution_duration = cypher_execution_end - cypher_execution_start
                                    cypher_generation_duration = cypher_execution_start - cypher_generation_start
                                    
                                    if success:
                                        # Track successful temporal fact insertion
                                        successful_temporal_facts.append(sanitised)
                                        print(f"  ✓ Sentence {sentence_index + 1} from chunk {chunk_index} fact {i+1} successfully added to graph")
                                        print(f"    Cypher generation: {cypher_generation_duration:.2f}s, Execution: {cypher_execution_duration:.2f}s (total time: {cypher_execution_end - pipeline_start_time:.2f}s)")
                                        if progress_cb:
                                            try:
                                                await progress_cb({
                                                    "type": "stage",
                                                    "stage": "graph_done",
                                                    "chunk": chunk_index,
                                                    "sentence": sentence_index + 1,
                                                    "message": f"Fact from sentence {sentence_index + 1} successfully added to graph"
                                                })
                                            except Exception:
                                                pass
                                    else:
                                        # Track failed temporal fact insertion
                                        failed_temporal_facts.append(sanitised)
                                        print(f"  ✗ Sentence {sentence_index + 1} from chunk {chunk_index} fact {i+1} failed to execute cypher query")
                                        print(f"    Cypher generation: {cypher_generation_duration:.2f}s, Execution failed (total time: {cypher_execution_end - pipeline_start_time:.2f}s)")
                                else:
                                    print(f"  Warning: Empty cypher query generated for sentence {sentence_index + 1} from chunk {chunk_index}")
                        except Exception as e:
                            # Track failed temporal fact insertion due to exception
                            failed_temporal_facts.append(sanitised)
                            print(f"  ✗ Failed to process graph operations for sentence {sentence_index + 1} from chunk {chunk_index}: {e}")
                    else:
                        # If no graph connection, treat as successful for tracking purposes
                        successful_temporal_facts.append(sanitised)
                        print(f"  ✓ Sentence {sentence_index + 1} from chunk {chunk_index} fact {i+1} processed (no graph connection)")
                    
                    # Add the structured data to results
                    results.append(sanitised)
                    
        except Exception as e:
            print(f"  Error in structure extraction for sentence {sentence_index + 1} from chunk {chunk_index}: {e}")
            return []
        
        return results
    
    # Initialise OpenAI interface for processing chunks
    from kh_core.openai_llm_interface import OpenAILLMInterface
    openai_interface = OpenAILLMInterface()
    
    # Process ALL chunks and ALL sentences concurrently for maximum parallelism
    print(f"Starting concurrent processing of {len(chunks)} chunks with per-sentence concurrency...")
    
    # Create a flat list of all sentence tasks across all chunks
    all_sentence_tasks = []
    sentence_to_chunk_mapping = {}
    
    for chunk_idx, chunk_text in chunks:
        # Clean the chunk first
        cleaned_chunk = clean_text(chunk_text)
        
        # Split chunk into individual sentences
        sentences = split_into_sentences(cleaned_chunk)
        print(f"\n--- PREPARING CHUNK {chunk_idx} ({len(sentences)} sentences) ---")
        print(f"Original chunk: {cleaned_chunk}")
        
        # Create tasks for all sentences in this chunk
        for sentence_idx, sentence in enumerate(sentences):
            # Create the sentence processing task
            sentence_task = process_sentence_end_to_end(chunk_idx, sentence_idx, sentence)
            all_sentence_tasks.append(sentence_task)
            sentence_to_chunk_mapping[sentence_task] = chunk_idx
    
    # Process ALL sentences concurrently across ALL chunks
    print(f"Processing {len(all_sentence_tasks)} sentences concurrently across all chunks...")
    
    # Use asyncio.as_completed to process sentences as they finish
    for completed_sentence_task in asyncio.as_completed(all_sentence_tasks):
        sentence_results = await completed_sentence_task
        if sentence_results:
            # Yield each structured output immediately as it's produced
            for structured_output in sentence_results:
                yield structured_output
    
    print("--- ALL SPATIOTEMPORA CHUNKS AND SENTENCES PROCESSING COMPLETE ---\n")
    
    # Once the entire text has completed the spatial coordinate expansion stage
    # Extract partial state facts and structured state facts for the whole text
    if all_structured_data:
        try:
            print(f"Text processing complete. Extracting state facts for {len(all_structured_data)} temporal facts...")
            state_extraction_start_time = time.time()

            # Check that ALL temporal facts are successfully in the graph
            total_temporal_facts = len(all_structured_data)
            successful_count = len(successful_temporal_facts)
            failed_count = len(failed_temporal_facts)

            print(f"Graph operation summary:")
            print(f"  Total temporal facts: {total_temporal_facts}")
            print(f"  Successfully added to graph: {successful_count}")
            print(f"  Failed to add to graph: {failed_count}")

            if failed_count > 0:
                print(f"WARNING: {failed_count} temporal facts failed to be added to the graph!")
                print("State fact extraction will be skipped to prevent cypher matching failures.")
                print("Failed temporal facts:")
                for i, failed_fact in enumerate(failed_temporal_facts):
                    print(f"  {i+1}. {failed_fact.get('subjects', [])} {failed_fact.get('relation_type', '')} {failed_fact.get('objects', [])}")
                return

            if successful_count == 0:
                print("WARNING: No temporal facts were successfully added to the graph!")
                print("State fact extraction will be skipped.")
                return

            print(f"All temporal facts successfully committed to graph. Proceeding with state fact extraction")

            # Extract partial structured state facts for the whole text
            partial_start = time.time()
            partial_state_facts = extract_partial_structured_state_facts(all_structured_data)
            partial_duration = time.time() - partial_start
            print(f"Extracted {len(partial_state_facts)} partial state facts in {partial_duration:.2f} seconds")

            # Extract structured state facts from the partial ones
            from kh_core.openai_llm_interface import OpenAILLMInterface
            openai_interface = OpenAILLMInterface()

            llm_start = time.time()
            structured_state_facts = await extract_structured_state_facts(text, partial_state_facts, openai_interface)
            llm_duration = time.time() - llm_start
            print(f"Extracted {len(structured_state_facts)} structured state facts in {llm_duration:.2f} seconds")
            try:
                import json as _json
                preview_count = min(5, len(structured_state_facts))
                print(f"--- Structured State Facts (showing up to {preview_count}) ---")
                for i, sf in enumerate(structured_state_facts[:preview_count]):
                    print(_json.dumps(sf, indent=2, ensure_ascii=False))
                if len(structured_state_facts) > preview_count:
                    print(f"... {len(structured_state_facts) - preview_count} more not shown ...")
                print(f"--- End Structured State Facts Preview ---")
            except Exception as e:
                print(f"Warning: Failed to pretty-print structured state facts: {e}")

            # Send state facts to graph as well (safe as temporal facts are confirmed to be in the graph)
            if text_to_cypher_pipeline and structured_state_facts:
                try:
                    state_fact_success_count = 0
                    state_fact_fail_count = 0
                    graph_ops_start = time.time()

                    for state_fact in structured_state_facts:
                        # Generate and execute cypher for each state fact
                        async for item in cypher_generator.generate_cypher_from_structured_output([state_fact], text_to_cypher_pipeline.neo4j_storage):
                            if isinstance(item, tuple) and len(item) == 2:
                                query, params = item
                            else:
                                query, params = str(item), {}
                            if query and str(query).strip():
                                try:
                                    lines = str(query).strip().splitlines()
                                    match_block = []
                                    in_block = False
                                    for ln in lines:
                                        ln_stripped = ln.strip()
                                        if ln_stripped.startswith(("MATCH", "WITH", "WHERE")):
                                            in_block = True
                                            match_block.append(ln)
                                        elif in_block:
                                            break
                                    preview = "\n".join(match_block or lines[:6])
                                    print("Full Cypher MATCH preview (state fact):\n" + preview)
                                except Exception:
                                    pass
                                success = await text_to_cypher_pipeline.execute_cypher(query, params)
                                if success:
                                    state_fact_success_count += 1
                                    print(f"✓ State fact successfully added to graph")
                                else:
                                    state_fact_fail_count += 1
                                    print(f"✗ Failed to execute cypher query for state fact")
                                # Stream the state fact out as well for reporting purposes
                                try:
                                    yield state_fact
                                except Exception:
                                    pass

                    graph_ops_duration = time.time() - graph_ops_start
                    print(f"State fact graph operations complete:")
                    print(f"  Successfully added: {state_fact_success_count}")
                    print(f"  Failed to add: {state_fact_fail_count}")
                    print(f"  Graph ops duration: {graph_ops_duration:.2f} seconds")

                except Exception as e:
                    print(f"Error processing graph operations for state facts: {e}")

            # If no graph connection, still yield the structured state facts
            if not text_to_cypher_pipeline and structured_state_facts:
                for state_fact in structured_state_facts:
                    try:
                        yield state_fact
                    except Exception:
                        pass

            total_state_duration = time.time() - state_extraction_start_time
            print(f"State fact extraction and graph population complete in {total_state_duration:.2f} seconds!")

        except Exception as e:
            print(f"Error in state fact extraction: {e}")
    
    print("Pipeline complete!")
    
    # Final pipeline summary with timing
    pipeline_end_time = time.time()
    total_pipeline_duration = pipeline_end_time - pipeline_start_time
    
    print(f"\n{'='*60}")
    print("PIPELINE COMPLETION SUMMARY")
    print(f"{'='*60}")
    print(f"Total pipeline execution time: {total_pipeline_duration:.2f} seconds")
    print(f"Temporal facts processed: {len(all_structured_data)}")
    print(f"Successfully added to graph: {len(successful_temporal_facts)}")
    print(f"Failed to add to graph: {len(failed_temporal_facts)}")
    print(f"Pipeline complete!")


def split_text_into_chunks(text: str, chunk_size: int = 6):
    """
    Split text into chunks of specified sentence size.
    
    Args:
        text: Input text
        chunk_size: Number of sentences per chunk
        
    Returns:
        List of tuples (chunk_index, chunk_text)
    """
    # Split text into sentences first
    sentences = split_into_sentences(text)
    
    # Group sentences into chunks
    chunks = []
    for i in range(0, len(sentences), chunk_size):
        chunk_sentences = sentences[i:i + chunk_size]
        chunk_text = " ".join(chunk_sentences)
        chunks.append((i // chunk_size, chunk_text))
    
    return chunks


def split_into_sentences(text: str) -> List[str]:
    """
    Split text into sentences using simple punctuation-based splitting.
    
    Args:
        text: Input text
        
    Returns:
        List of sentences
    """
    # Simple sentence splitting - can be improved later
    # Split on periods, exclamation marks, and question marks
    import re
    
    # Split on sentence endings, but be careful about abbreviations
    sentences = re.split(r'(?<=[.!?])\s+', text)
    
    # Clean up sentences
    cleaned_sentences = []
    for sentence in sentences:
        sentence = sentence.strip()
        if sentence and len(sentence) > 3:  # Filter out very short fragments
            cleaned_sentences.append(sentence)
    
    return cleaned_sentences


def validate_structured_data(data_list: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Validate and clean up structured data, removing entries with missing required fields.
    """
    
    valid_data = []
    invalid_count = 0
    
    for i, data in enumerate(data_list):
        if not isinstance(data, dict):
            print(f"Warning: Item {i} is not a dictionary, skipping")
            invalid_count += 1
            continue
            
        # Check fact_type and validate accordingly
        fact_type = data.get('fact_type', 'temporal_fact')  # Default to temporal_fact for backward compatibility
        
        if fact_type == 'temporal_fact':
            # Temporal facts require temporal_intervals and spatial_contexts
            required_fields = ['subjects', 'objects', 'relation_type', 'temporal_intervals', 'spatial_contexts']
            missing_fields = [field for field in required_fields if field not in data]
            
            if missing_fields:
                print(f"Warning: Temporal fact {i} missing required fields: {missing_fields}")
                invalid_count += 1
                continue
                
            # Validate field types for temporal facts
            if not isinstance(data['subjects'], list) or not isinstance(data['objects'], list):
                print(f"Warning: Temporal fact {i} has invalid subjects or objects field type")
                invalid_count += 1
                continue
                
            # Allow empty objects array for intransitive verbs (have no object) (e.g. "John dies")
            if not data['subjects']:
                print(f"Warning: Temporal fact {i} must have at least one subject")
                invalid_count += 1
                continue
                
            if not isinstance(data['relation_type'], str) or not data['relation_type']:
                print(f"Warning: Temporal fact {i} has invalid or empty relation_type")
                invalid_count += 1
                continue
                
            if not isinstance(data['temporal_intervals'], list) or not isinstance(data['spatial_contexts'], list):
                print(f"Warning: Temporal fact {i} has invalid temporal_intervals or spatial_contexts field type")
                invalid_count += 1
                continue
                
            # Check for empty/null entries that might indicate parsing issues
            if (not data['subjects'] and not data['objects'] and 
                not data['relation_type'] and 
                all(t.get('start_time') is None and t.get('end_time') is None for t in data['temporal_intervals']) and
                all(s.get('name') is None for s in data['spatial_contexts'])):
                print(f"Warning: Temporal fact {i} appears to be empty/null, likely a parsing error - skipping")
                invalid_count += 1
                continue
                
        elif fact_type == 'state_change_event':
            # State change events require different fields
            required_fields = ['affected_fact', 'caused_by', 'causes']
            missing_fields = [field for field in required_fields if field not in data]
            
            if missing_fields:
                print(f"Warning: State change event {i} missing required fields: {missing_fields}")
                invalid_count += 1
                continue
                
            # Validate affected_fact structure
            affected_fact = data.get('affected_fact', {})
            if not isinstance(affected_fact, dict) or 'subjects' not in affected_fact or 'objects' not in affected_fact or 'relation_type' not in affected_fact:
                print(f"Warning: State change event {i} has invalid affected_fact structure")
                invalid_count += 1
                continue
                
            # Validate other fields
            if not isinstance(data['caused_by'], list) or not isinstance(data['causes'], list):
                print(f"Warning: State change event {i} has invalid caused_by or causes field type")
                invalid_count += 1
                continue
                
        else:
            print(f"Warning: Item {i} has unknown fact_type: {fact_type}")
            invalid_count += 1
            continue
            
        valid_data.append(data)
    
    if invalid_count > 0:
        print(f"Filtered out {invalid_count} invalid entries, keeping {len(valid_data)} valid entries")
    
    return valid_data


async def extract_structure_no_coords_from_chunk(chunk_text: str, openai_interface) -> List[Dict[str, Any]]:
    """
    Extract structured data from a chunk of text (multiple sentences) using OpenAI with configurable model.
    Uses JSON structured outputs with a schema to guarantee valid results.
    Coordinates are left as null.
    """
    try:
        system_prompt = """You are a data extraction agent.
Parse each sentence in the input text into structured temporal facts.

RULES:
1. Always set "fact_type" to "temporal_fact".
2. Extract all subjects (entities performing the action).
3. Extract all objects (entities receiving the action) - this can be an empty array [] if no object given e.g. for intransitive verbs in sentences like "John dies". Times and locations are NOT objects.
4. Extract the main verb (relation_type).
5. Subjects, relation and objects are colon-separated. You are given a sentence formatted as "[Subjects] : relation : [objects] ...":
   - Subjects are everything before the first colon.
   - relation_type is the text between the first and second colon.
   - Objects are everything after the second colon up to the first occurrence of " from " or " at " or sentence end. If nothing appears between the second colon and " from/at ", set objects to [].
   - Split multiple subjects/objects on the word "and" (the expansion format uses "and" to separate multiple entities) while preserving internal entity wording and articles. Do NOT split on "&"; treat "&" as part of an entity name and keep the entity intact (e.g., "Food from China, India & Japan" is ONE object string, not split by "&").
   - Do not treat temporal ("from ... to ...") or spatial ("at ...") phrases as objects.
5. For temporal_intervals:
   - Times can be ISO 8601 timestamps (YYYY-MM-DDTHH:MM:SS) or string descriptors, as given in input.
   - If only one side is given, set the other to null.
   - If no time info exists, set both start_time and end_time to null.
   - IMPORTANT: If a sentence mentions multiple time periods for the SAME action, combine them into ONE fact with multiple temporal intervals.
   - CRITICAL: Distinguish combinable vs paired intervals based on the presence of "and" between full pair blocks:
     - If times are listed consecutively without "and" (e.g., "from t1 ... from t2 ...") they are COMBINABLE with all listed locations.
     - If pairs are separated by "and" (e.g., "from t1 ... at L1 and from t2 ... at L2"), they are DISTINCT pairs that should NOT be cross-combined.
     - Assume all timestamps have been converted to UTC timezone already, do NOT convert timezones.
6. For spatial_contexts:
   - Extract each location explicitly mentioned after "at".
   - If no location is mentioned, return [null] (not "unknown").
   - Only include actual place names, never use placeholder text.
   - CRITICAL: If multiple locations are listed without "and" (e.g., "at L1 at L2"), treat them as COMBINABLE with all times in the sentence.
   - If full time-location pairs are separated by "and", keep them paired and do not cross-combine across the pairs.
7. One structured JSON object per sentence, even if the sentence mentions multiple time periods.
8. EXAMPLES:
   - "Marie Curie : wins : Nobel Prize for Physics from 1903 to 1911" → subjects: ["Marie Curie"], relation_type: "wins", objects: ["Nobel Prize for Physics"], temporal_intervals: two entries for 1903 and 1911
   - "Alice and Bob : faints : from 2020 to 2021 at the party" → subjects: ["Alice", "Bob"], objects: [], relation_type: "faints", spatial_contexts: ["the party"]
   - "The farmers' market : sets up : from 2025-10-07T11:00:00 to unknown from 2025-10-14T11:00:00 to unknown at Imperial College London" → temporal_intervals: two entries; spatial_contexts: ["Imperial College London"] (combinable)
   - "The lecture : can run : from 2025-10-01T17:00:00 to 2025-10-01T18:00:00 at London and from 2025-10-01T22:00:00 to 2025-10-01T23:00:00 at Bristol" → two DISTINCT pairs; represent as two context entries that must not be cross-combined.
"""

        response = await openai_interface.chat_completion(
            model="gpt-5-nano",
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": f"Chunk to process:\n{chunk_text}"}
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "name": "temporal_fact_schema",
                    "schema": {
                        "type": "object",
                        "properties": {
                            "facts": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "fact_type": {"type": "string", "enum": ["temporal_fact"]},
                                        "subjects": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        },
                                        "objects": {
                                            "type": "array",
                                            "items": {"type": "string"}
                                        },
                                        "relation_type": {"type": "string"},
                                        "temporal_intervals": {
                                            "type": "array",
                                            "items": {
                                                "type": "object",
                                                "properties": {
                                                    "start_time": {"type": ["string", "null"]},
                                                    "end_time": {"type": ["string", "null"]}
                                                },
                                                "required": ["start_time", "end_time"]
                                            }
                                        },
                                        "spatial_contexts": {
                                            "type": "array",
                                            "items": {"type": ["string", "null"]}
                                        }
                                    },
                                    "required": [
                                        "fact_type", "subjects", "relation_type", "temporal_intervals", "spatial_contexts"
                                    ]
                                }
                            }
                        },
                        "required": ["facts"]
                    }
                }
            }
        )

        response_content = response.strip()
        try:
            import json
            parsed = json.loads(response_content)
            return parsed.get('facts', [])
        except json.JSONDecodeError:
            print(f"Failed to parse JSON response: {response_content}")
            return []

    except Exception as e:
        print(f"Error in extract_structure_no_coords_from_chunk: {e}")
        return []