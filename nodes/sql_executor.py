import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

"""
SQL Executor Node - Safely executes validated SQL queries
"""
from typing import Dict, Any
import time
from graphs.state import AgentState
from utils.constants import NodeNames
from utils.logger import get_logger
from utils.db_connection import get_db

logger = get_logger("SQLExecutorNode")
db = get_db()


def sql_executor_node(state: AgentState) -> Dict[str, Any]:
    """
    Execute validated SQL query against the database
    
    This node:
    1. Retrieves validated SQL query
    2. Executes query with timeout protection
    3. Measures execution time
    4. Returns results or error information
    
    Args:
        state: Current agent state
        
    Returns:
        Updated state dictionary
    """
    logger.node_entry(NodeNames.SQL_EXECUTOR, state)
    
    sql_query = state.get("sql_query")
    
    if not sql_query:
        logger.error("No SQL query to execute")
        logger.node_exit(NodeNames.SQL_EXECUTOR, success=False)
        
        return {
            "execution_success": False,
            "error_info": {
                "type": "execution_error",
                "message": "No SQL query provided for execution",
                "node": NodeNames.SQL_EXECUTOR
            },
            "node_history": state.get("node_history", []) + [NodeNames.SQL_EXECUTOR]
        }
    
    try:
        logger.info(f"Executing SQL query...")
        logger.debug(f"Query: {sql_query[:200]}...")
        
        # Record start time
        start_time = time.time()
        
        # Execute query
        results, error = db.execute_query(sql_query, fetch=True)
        
        # Calculate execution time
        execution_time = time.time() - start_time
        
        if error:
            logger.error_occurred("ExecutionError", error, NodeNames.SQL_EXECUTOR)
            logger.node_exit(NodeNames.SQL_EXECUTOR, success=False)
            
            return {
                "execution_success": False,
                "query_results": None,
                "execution_time": execution_time,
                "error_info": {
                    "type": "execution_error",
                    "message": error,
                    "node": NodeNames.SQL_EXECUTOR,
                    "sql_query": sql_query
                },
                "node_history": state.get("node_history", []) + [NodeNames.SQL_EXECUTOR]
            }
        
        # Successful execution
        result_count = len(results) if results else 0
        
        logger.info(f"Query executed successfully: {result_count} rows in {execution_time:.3f}s")
        logger.node_exit(NodeNames.SQL_EXECUTOR, success=True)
        
        # Update state
        updates = {
            "execution_success": True,
            "query_results": results,
            "execution_time": execution_time,
            "error_info": None,
            "node_history": state.get("node_history", []) + [NodeNames.SQL_EXECUTOR],
            "metadata": {
                **state.get("metadata", {}),
                "result_count": result_count,
                "execution_time_seconds": round(execution_time, 3),
                "query_executed": sql_query
            }
        }
        
        # Handle empty results
        if result_count == 0:
            updates["metadata"]["empty_results"] = True
            logger.info("Query returned no results")
        
        return updates
        
    except Exception as e:
        error_msg = f"Unexpected error during query execution: {str(e)}"
        logger.error_occurred("UnexpectedError", error_msg, NodeNames.SQL_EXECUTOR)
        logger.node_exit(NodeNames.SQL_EXECUTOR, success=False)
        
        return {
            "execution_success": False,
            "query_results": None,
            "error_info": {
                "type": "unexpected_error",
                "message": error_msg,
                "node": NodeNames.SQL_EXECUTOR
            },
            "node_history": state.get("node_history", []) + [NodeNames.SQL_EXECUTOR]
        }


def route_after_execution(state: AgentState) -> str:
    """
    Route to next node based on execution result
    
    Args:
        state: Current agent state
        
    Returns:
        Next node name
    """
    if state.get("execution_success", False):
        return NodeNames.OUTPUT_FORMATTER
    else:
        print("execution failure")