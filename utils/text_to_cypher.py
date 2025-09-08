# Main pipeline for converting plain text to cypher statements

import json
import logging
import sys
import os
from typing import Dict, Any, Optional, Tuple, List

# Add the project root to the Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if project_root not in sys.path:
    sys.path.insert(0, project_root)

from kh_core.neo4j_storage import Neo4jStorage, Neo4jConfig
from utils.cypher_generator import CypherGenerator
import asyncio

logger = logging.getLogger(__name__)


class TextToHyperSTructurePipeline:
    """
    Pipeline for converting plain text to hyperSTructure construction/update cypher statements and executing them.
    """
    
    def __init__(self, neo4j_config: Optional[Neo4jConfig] = None):
        """
        Initialise the pipeline.
        
        Args:
            neo4j_config: Neo4j configuration (optional, uses defaults)
        """
        self.cypher_generator = CypherGenerator()
        self.neo4j_config = neo4j_config or Neo4jConfig()
        self.neo4j_storage = None # Lazy-initialised - Object can be created even if db is temporarily unavailable
    
    # Async connection methods - much better performance
    async def initialise_neo4j_connection(self) -> bool:
        """Initialise Neo4j connection."""
        self.neo4j_storage = Neo4jStorage(self.neo4j_config)
        return await self.neo4j_storage.connect()
    
    async def close_neo4j_connection(self):
        """Close Neo4j connection."""
        if self.neo4j_storage:
            await self.neo4j_storage.disconnect()
    
    
    async def generate_cypher(self, structured_data: List[Dict[str, Any]]) -> str:
        """
        Generate Cypher query from structured data.
        
        Args:
            structured_data: List of structured data from LLM
            
        Returns:
            Cypher query string (concatenated for debugging)
        """
        try:
            # Handle async generator for immediate concurrent execution
            queries = [] # Stores query strings
            tasks = []  # Stores async tasks for execution
            
            async for item in self.cypher_generator.generate_cypher_from_structured_output(structured_data):
                # Support both legacy string and (query, params) tuples
                if isinstance(item, tuple) and len(item) == 2:
                    query, params = item
                else:
                    query, params = str(item), {}
                if query.strip():
                    queries.append(query)
                    task = asyncio.create_task(self.execute_cypher(query, params))
                    tasks.append(task)
            
            # Wait for all tasks to complete
            if tasks:
                results = await asyncio.gather(*tasks, return_exceptions=True)
                # Log any failures
                for i, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.error(f"Query {i} failed: {result}")
            
            # Return concatenated queries for debugging
            return "\n\n".join(queries)
            
        except Exception as e:
            logger.error(f"Failed to generate Cypher: {e}")
            raise
    
    async def execute_cypher(self, cypher_query: str, params: Optional[Dict[str, Any]] = None) -> bool:
        """
        Execute Cypher query in Neo4j.
        
        Args:
            cypher_query: Cypher query to execute
            
        Returns:
            True if successful, False otherwise
        """
        if not self.neo4j_storage:
            raise RuntimeError("Neo4j not initialised. Call initialize_neo4j_connection() first.")
        
        try:
            with self.neo4j_storage.driver.session(database=self.neo4j_config.database) as session:
                if params:
                    result = session.run(cypher_query, **params)
                else:
                    result = session.run(cypher_query)
                # Consume the result to ensure the query executes (non-blocking of event loop)
                await asyncio.get_event_loop().run_in_executor(None, lambda: list(result))
                logger.info("Query executed successfully")
                return True
                
        except Exception as e:
            logger.error(f"Failed to execute Cypher query: {e}")
            return False
    
    async def process_text_to_graph(self, text: str) -> Tuple[bool, str, str]:
        """
        Complete pipeline: text → structured data → Cypher → execute.
        
        Args:
            text: Plain text input
            
        Returns:
            Tuple of (success, structured_data, cypher_query)
        """
        try:
            # Step 1: Extract structured data
            structured_data = self.extract_structured_data(text)
            
            # Step 2: Generate Cypher and execute concurrently
            cypher_query = await self.generate_cypher(structured_data)
            
            # Step 3: Queries already executed in generate_cypher, just return success
            success = True  # Assuming success if no exceptions were raised
            
            return success, json.dumps(structured_data, indent=2), cypher_query
            
        except Exception as e:
            logger.error(f"Pipeline failed: {e}")
            return False, "", ""
    
    
    def validate_structured_data(self, data: Dict[str, Any]) -> bool:
        """
        Validate structured data from LLM.
        
        Args:
            data: Structured data to validate
            
        Returns:
            True if valid, False otherwise
        """
        # Check fact_type and validate accordingly
        fact_type = data.get('fact_type', 'temporal_fact')  # Default to temporal_fact for backward compatibility
        
        if fact_type == 'temporal_fact':
            # Temporal facts require temporal_intervals
            required_fields = ['subjects', 'objects', 'relation_type', 'temporal_intervals']
            
            for field in required_fields:
                if field not in data:
                    logger.error(f"Missing required field for temporal fact: {field}")
                    return False
            
            if not isinstance(data['subjects'], list) or not data['subjects']:
                logger.error("subjects must be a non-empty list")
                return False
            
            if not isinstance(data['objects'], list):
                logger.error("objects must be a list")
                return False
            
            if not isinstance(data['relation_type'], str) or not data['relation_type']:
                logger.error("relation_type must be a non-empty string")
                return False
            
            if not isinstance(data['temporal_intervals'], list) or not data['temporal_intervals']:
                logger.error("temporal_intervals must be a non-empty list")
                return False
                
        elif fact_type == 'state_change_event':
            # State change events require different fields
            required_fields = ['affected_fact', 'caused_by', 'causes']
            
            for field in required_fields:
                if field not in data:
                    logger.error(f"Missing required field for state change event: {field}")
                    return False
            
            # Validate affected_fact structure
            affected_fact = data.get('affected_fact', {})
            if not isinstance(affected_fact, dict) or 'subjects' not in affected_fact or 'objects' not in affected_fact or 'relation_type' not in affected_fact:
                logger.error("affected_fact must have subjects, objects, and relation_type")
                return False
            
            # Validate other fields
            if not isinstance(data['caused_by'], list):
                logger.error("caused_by must be a list")
                return False
                
            if not isinstance(data['causes'], list):
                logger.error("causes must be a list")
                return False
                
        else:
            logger.error(f"Unknown fact_type: {fact_type}")
            return False
        
        return True 
    
    async def clear_database(self) -> bool:
        """
        Clear all data from the database (for development/testing).
        
        Returns:
            True if successful, False otherwise
        """
        if not self.neo4j_storage:
            raise RuntimeError("Neo4j not initialized. Call initialize_neo4j_connection() first.")
        
        try:
            with self.neo4j_storage.driver.session(database=self.neo4j_config.database) as session:
                # Delete all nodes and relationships
                result = session.run("MATCH (n) DETACH DELETE n")
                list(result)  # Consume the result
                logger.info("Database cleared successfully")
                return True
                
        except Exception as e:
            logger.error(f"Failed to clear database: {e}")
            return False
    
# Run the pipeline by calling the process_text_to_graph method
if __name__ == "__main__":
    import asyncio
    
    async def main():
        pipeline = TextToHyperSTructurePipeline()
        
        # Test mode: just generate Cypher without connecting to Neo4j
        print("Testing pipeline without Neo4j connection...")
        try:
            # Test structured data extraction
            structured_data = pipeline.extract_structured_data("Test")
            print(f"Structured data extracted: {len(structured_data)} hyperedges")
            
            # Test Cypher generation (without execution)
            print("Testing Cypher generation...")
            queries = []
            async for query in pipeline.cypher_generator.generate_cypher_from_structured_output(structured_data):
                if query.strip():
                    queries.append(query)
                    print(f"Generated query: {query[:100]}...")
            
            print(f"Successfully generated {len(queries)} Cypher queries")
            
            # Execute queries if Neo4j is available
            if pipeline.neo4j_storage and pipeline.neo4j_storage._connected:
                for query in queries:
                    success = await pipeline.execute_cypher(query)
                    if not success:
                        print("Failed to connect to Neo4j - make sure Neo4j is running on localhost:7687")
                        break
            else:
                print("Neo4j not connected - queries not executed")
            
            return True, structured_data, "\n\n".join(queries)
            
        except Exception as e:
            print(f"Error: {e}")
            return False, str(e), ""
    
    # Run the async main function
    asyncio.run(main())