import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

"""
Schema Inspector Node - Retrieves and caches database schema information
"""
from typing import Dict, Any, Optional
import json
import os
from datetime import datetime, timedelta
from graphs.state import AgentState
from utils.constants import NodeNames, SCHEMA_CACHE_FILE, CACHE_TTL, CACHE_ENABLED
from utils.logger import get_logger
from utils.db_connection import get_db

logger = get_logger("SchemaInspectorNode")
db = get_db()


def load_schema_cache() -> Optional[Dict[str, Any]]:
    """
    Load schema from cache file if it exists and is not expired
    
    Returns:
        Cached schema dict or None
    """
    if not CACHE_ENABLED:
        return None
    
    try:
        if os.path.exists(SCHEMA_CACHE_FILE):
            with open(SCHEMA_CACHE_FILE, 'r') as f:
                cache_data = json.load(f)
            
            # Check if cache is expired
            cached_time = datetime.fromisoformat(cache_data.get('timestamp', '2000-01-01'))
            if datetime.now() - cached_time < timedelta(seconds=CACHE_TTL):
                logger.info("Loaded schema from cache")
                return cache_data.get('schema')
            else:
                logger.info("Schema cache expired")
    except Exception as e:
        logger.warning(f"Failed to load schema cache: {str(e)}")
    
    return None


def save_schema_cache(schema: Dict[str, Any]):
    """
    Save schema to cache file
    
    Args:
        schema: Schema dictionary to cache
    """
    if not CACHE_ENABLED:
        return
    
    try:
        os.makedirs(os.path.dirname(SCHEMA_CACHE_FILE), exist_ok=True)
        
        cache_data = {
            'timestamp': datetime.now().isoformat(),
            'schema': schema
        }
        
        with open(SCHEMA_CACHE_FILE, 'w') as f:
            json.dump(cache_data, f, indent=2)
        
        logger.info("Saved schema to cache")
    except Exception as e:
        logger.warning(f"Failed to save schema cache: {str(e)}")


def inspect_database_schema() -> tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Inspect the database and retrieve complete schema information
    
    Returns:
        Tuple of (schema_dict, error_message)
    """
    try:
        # Get all table names
        table_names, error = db.get_table_names()
        if error:
            return None, f"Failed to get table names: {error}"
        
        logger.info(f"Found {len(table_names)} tables in database")
        
        # Get schema for each table
        tables_schema = {}
        for table_name in table_names:
            # Get column information
            columns, error = db.get_table_schema(table_name)
            if error:
                logger.warning(f"Failed to get schema for table {table_name}: {error}")
                continue
            
            # Get sample data
            sample_data, error = db.get_sample_data(table_name, limit=2)
            if error:
                logger.warning(f"Failed to get sample data for {table_name}: {error}")
                sample_data = []
            
            tables_schema[table_name] = {
                'columns': columns,
                'sample_data': sample_data
            }
        
        # Get all relationships
        relationships, error = db.get_table_relationships()
        if error:
            logger.warning(f"Failed to get relationships: {error}")
            relationships = []
        
        schema = {
            'tables': tables_schema,
            'relationships': relationships,
            'table_names': table_names
        }
        
        logger.info(f"Successfully inspected database schema with {len(tables_schema)} tables")
        return schema, None
        
    except Exception as e:
        error_msg = f"Unexpected error during schema inspection: {str(e)}"
        logger.error(error_msg)
        return None, error_msg


def schema_inspector_node(state: AgentState) -> Dict[str, Any]:
    """
    Inspect database schema and cache it
    
    This node:
    1. Checks if schema is already cached in state
    2. Tries to load from cache file
    3. If not cached, inspects database
    4. Saves schema to cache
    5. Provides schema information for query planning
    
    Args:
        state: Current agent state
        
    Returns:
        Updated state dictionary
    """
    logger.node_entry(NodeNames.SCHEMA_INSPECTOR, state)
    
    schema_info = state.get("schema_info")
    schema_cached = state.get("schema_cached", False)
    
    # If schema already in state, use it
    if schema_info and schema_cached:
        logger.info("Using schema from state")
        logger.node_exit(NodeNames.SCHEMA_INSPECTOR, success=True)
        return {
            "node_history": state.get("node_history", []) + [NodeNames.SCHEMA_INSPECTOR]
        }
    
    # Try to load from cache file
    schema_info = load_schema_cache()
    
    # If no cache, inspect database
    if not schema_info:
        logger.info("Inspecting database schema...")
        schema_info, error = inspect_database_schema()
        
        if error:
            logger.error(f"Schema inspection failed: {error}")
            logger.node_exit(NodeNames.SCHEMA_INSPECTOR, success=False)
            
            return {
                "schema_info": None,
                "schema_cached": False,
                "error_info": {
                    "type": "schema_error",
                    "message": error,
                    "node": NodeNames.SCHEMA_INSPECTOR
                },
                "node_history": state.get("node_history", []) + [NodeNames.SCHEMA_INSPECTOR]
            }
        
        # Save to cache
        save_schema_cache(schema_info)
    
    # Generate schema summary for logging
    table_count = len(schema_info.get('table_names', []))
    relationship_count = len(schema_info.get('relationships', []))
    
    logger.info(f"Schema loaded: {table_count} tables, {relationship_count} relationships")
    
    # Update state
    updates = {
        "schema_info": schema_info,
        "schema_cached": True,
        "node_history": state.get("node_history", []) + [NodeNames.SCHEMA_INSPECTOR],
        "metadata": {
            **state.get("metadata", {}),
            "schema_table_count": table_count,
            "schema_relationship_count": relationship_count
        }
    }
    
    logger.node_exit(NodeNames.SCHEMA_INSPECTOR, success=True)
    
    return updates