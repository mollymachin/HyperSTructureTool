#!/usr/bin/env python3
"""
Simple script to clear database, populate with extract_structured_data, and test visualization.
"""

import asyncio
import sys
from pathlib import Path

# Add the project root to the Python path
project_root = Path(__file__).parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from utils.text_to_cypher import TextToHyperSTructurePipeline
from kh_core.neo4j_storage import Neo4jConfig

async def main():
    """Simple database population and testing."""
    
    print("This script will clear and repopulate the database with hyperedges from extract_structured_data.")
    
    # Initialize pipeline
    neo4j_config = Neo4jConfig(
        uri="bolt://localhost:7687",
        username="neo4j",
        password="password",
        database="neo4j"
    )
    
    pipeline = TextToHyperSTructurePipeline(neo4j_config=neo4j_config)
    
    # Connect
    if not await pipeline.initialise_neo4j_connection():
        print("Failed to connect to Neo4j")
        return
    
    print("Connected to Neo4j")
    
    # Clear database
    try:
        await pipeline.clear_database()
        print("Db cleared!")
    except Exception as e:
        print(f"Clear failed: {e}")
        return
    
    # Add hyperedges
    try:
        # Get the structured data directly from extract_structured_data
        structured_data = pipeline.extract_structured_data("aaa")
        print(f"Retrieved {len(structured_data)} hyperedges from extract_structured_data")
        
        # Generate and execute Cypher queries for each hyperedge
        for i, hyperedge_data in enumerate(structured_data, 1):
            print(f"Processing hyperedge {i}: {hyperedge_data['subjects']} {hyperedge_data['relation_type']} {hyperedge_data['objects']}")
            
            # Generate Cypher query
            cypher_query = ""
            async for query in pipeline.cypher_generator.generate_cypher_from_structured_output([hyperedge_data]):
                cypher_query = query
                break
            
            if cypher_query:
                # Execute the query
                success = await pipeline.execute_cypher(cypher_query)
                if success:
                    print("Added successfully")
                else:
                    print("Failed to add")
            else:
                print("Failed to generate Cypher")
        
        print("All hyperedges processed!")
        
    except Exception as e:
        print(f"Add failed: {e}")
        return
    
    # Close connection
    await pipeline.close_neo4j_connection()
    
    # Instructions
    print("\n" + "Start the app and use 'Load all hyperedges' button to see the freshly generated hyperedges.")

if __name__ == "__main__":
    asyncio.run(main())
