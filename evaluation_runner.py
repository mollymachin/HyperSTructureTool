#!/usr/bin/env python3
"""
Minimal evaluation runner for chunking_streaming_pipeline stages.
Tests inputs from custom_dataset.txt to see pipeline outputs.
"""

import asyncio
import json
import time
import os
from datetime import datetime
from utils.process_text import chunking_streaming_pipeline
from config import MODEL_NAME
from testing_datasets.custom_dataset import TEST_INPUTS

# Change these to easily switch inputs for evaluation
CURRENT_SECTION = "development_test"  # Section name from custom_dataset.py
CURRENT_INDEX = 1

# Manual test input (for quick testing without using dataset)
MANUAL_TEST_INPUT = "Marie Curie won the Nobel Prize in the year 1903 at the Royal Swedish Academy of Sciences. Will is a professor at Imperial College London. Will works as a researcher. Molly likes cats, cars and crypto in South Kensington tube station."
USE_MANUAL_INPUT = False  # Set to True to use MANUAL_TEST_INPUT instead of dataset

def get_current_test_input():
    """Get the current test input based on section and index selection."""
    if USE_MANUAL_INPUT:
        return "manual_test", MANUAL_TEST_INPUT
    
    if CURRENT_SECTION not in TEST_INPUTS:
        available_sections = list(TEST_INPUTS.keys())
        raise ValueError(f"Section '{CURRENT_SECTION}' not found. Available sections: {available_sections}")
    
    section_inputs = TEST_INPUTS[CURRENT_SECTION]
    if CURRENT_INDEX >= len(section_inputs):
        raise ValueError(f"Index {CURRENT_INDEX} out of range for section '{CURRENT_SECTION}' (max index: {len(section_inputs) - 1})")
    
    test_name = f"{CURRENT_SECTION}[{CURRENT_INDEX}]"
    test_input = section_inputs[CURRENT_INDEX]
    return test_name, test_input

# Log file configuration
LOG_FILE = "evaluation_log.txt"

# Database clearing configuration
CLEAR_DATABASE_BEFORE_RUN = True  # Set to False to keep existing facts

def log_to_file(message: str, append: bool = True):
    """Write message to log file with timestamp."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    log_entry = f"[{timestamp}] {message}\n"
    
    mode = 'a' if append else 'w'
    with open(LOG_FILE, mode, encoding='utf-8') as f:
        f.write(log_entry)

def log_separator():
    """Write separator line to log file."""
    separator = "——" * 30 + "\n"
    with open(LOG_FILE, 'a', encoding='utf-8') as f:
        f.write(separator)

async def clear_graph_database():
    """Clear all facts from the Neo4j graph database."""
    try:
        from utils.text_to_cypher import TextToHyperSTructurePipeline
        
        # Initialise the pipeline to get database connection
        text_to_cypher_pipeline = TextToHyperSTructurePipeline()
        await text_to_cypher_pipeline.initialise_neo4j_connection()
        
        # Cypher query to clear all nodes and relationships
        clear_query = """
        MATCH (n)
        DETACH DELETE n
        """
        
        print("Clearing graph database...")
        log_to_file("Clearing graph database...")
        
        success = await text_to_cypher_pipeline.execute_cypher(clear_query)
        
        if success:
            print("✓ Graph database cleared successfully")
            log_to_file("✓ Graph database cleared successfully")
        else:
            print("✗ Failed to clear graph database")
            log_to_file("✗ Failed to clear graph database")
            
    except Exception as e:
        error_msg = f"Error clearing graph database: {e}"
        print(error_msg)
        log_to_file(error_msg)
        print("Continuing with existing database state...")

async def run_evaluation():
    """Run pipeline evaluation on test input."""
    test_name, test_input = get_current_test_input()
    
    # Clear database if configured to do so
    if CLEAR_DATABASE_BEFORE_RUN:
        await clear_graph_database()
    
    # Log start of evaluation
    log_to_file(f"Pipeline Evaluation Runner - Model: {MODEL_NAME}")
    log_to_file(f"Testing: {test_name}")
    log_to_file(f"Input: {test_input}")
    log_to_file("=" * 50)
    
    print("Pipeline Evaluation Runner")
    print("=" * 50)
    print(f"Testing: {test_name}")
    print(f"Input: {test_input}")
    print(f"Model: {MODEL_NAME}")
    if CLEAR_DATABASE_BEFORE_RUN:
        print("Database cleared before run")
    else:
        print("Using existing database state")
    print("=" * 50)
    
    all_outputs = []
    total_start_time = time.time()
    
    try:
        # Stage 1: Pipeline execution
        stage_start_time = time.time()
        log_to_file("Stage 1: Starting pipeline execution...")
        
        async for structured_output in chunking_streaming_pipeline(test_input, chunk_size=3):
            all_outputs.append(structured_output)
            
            # Log each output
            log_to_file(f"\n--- Structured Output #{len(all_outputs)} ---")
            log_to_file(f"Subjects: {structured_output.get('subjects', [])}")
            log_to_file(f"Objects: {structured_output.get('objects', [])}")
            log_to_file(f"Relation: {structured_output.get('relation_type', '')}")
            log_to_file(f"Temporal: {structured_output.get('temporal_intervals', [])}")
            log_to_file(f"Spatial: {structured_output.get('spatial_contexts', [])}")
            log_to_file(f"Fact Type: {structured_output.get('fact_type', '')}")
            
            # Also print to console
            print(f"\n--- Structured Output #{len(all_outputs)} ---")
            print(f"Subjects: {structured_output.get('subjects', [])}")
            print(f"Objects: {structured_output.get('objects', [])}")
            print(f"Relation: {structured_output.get('relation_type', '')}")
            print(f"Temporal: {structured_output.get('temporal_intervals', [])}")
            print(f"Spatial: {structured_output.get('spatial_contexts', [])}")
            print(f"Fact Type: {structured_output.get('fact_type', '')}")
        
        stage_time = time.time() - stage_start_time
        log_to_file(f"Stage 1 completed in {stage_time:.2f} seconds")
        
    except Exception as e:
        error_msg = f"Pipeline error: {e}"
        log_to_file(error_msg)
        print(error_msg)
        import traceback
        traceback.print_exc()
    
    # Calculate total time
    total_time = time.time() - total_start_time
    
    # Log completion summary
    log_to_file(f"\n{'='*50}")
    log_to_file(f"Total outputs: {len(all_outputs)}")
    log_to_file(f"Total execution time: {total_time:.2f} seconds")
    log_to_file(f"All outputs saved to 'evaluation_outputs.json'")
    
    # Save all outputs to JSON file
    with open('evaluation_outputs.json', 'w') as f:
        json.dump(all_outputs, f, indent=2, default=str)
    
    # Add separator for next run
    log_separator()
    
    # Print summary to console
    print(f"\n{'='*50}")
    print(f"Total outputs: {len(all_outputs)}")
    print(f"Total execution time: {total_time:.2f} seconds")
    print(f"All outputs saved to 'evaluation_outputs.json'")
    print(f"Log written to '{LOG_FILE}'")

if __name__ == "__main__":
    try:
        current_test_name, current_test_input = get_current_test_input()
        
        
        print(f"\nCurrent test: {current_test_name}")
        print(f"Input preview: {current_test_input[:100]}{'...' if len(current_test_input) > 100 else ''}")
        print(f"Model: {MODEL_NAME}")
        print(f"Clear database before run: {CLEAR_DATABASE_BEFORE_RUN}")
        print(f"Using manual input: {USE_MANUAL_INPUT}")
       
        
    except Exception as e:
        print(f"Error getting test input: {e}")
        print("Please check your CURRENT_SECTION and CURRENT_INDEX settings")
        exit(1)
    
    # Initialise log file if it doesn't exist
    if not os.path.exists(LOG_FILE):
        log_to_file("Evaluation Log Started", append=False)
        log_to_file(f"Model: {MODEL_NAME}")
        log_to_file("=" * 50)
    
    asyncio.run(run_evaluation())
