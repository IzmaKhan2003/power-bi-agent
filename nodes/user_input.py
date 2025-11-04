"""
User Input Node - Entry point for user queries
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))


from typing import Dict, Any
from graphs.state import AgentState
from utils.constants import EXIT_COMMANDS, NodeNames
from utils.logger import get_logger
from utils.memory_manager import get_memory_manager

logger = get_logger("UserInputNode")
memory = get_memory_manager()


def user_input_node(state: AgentState) -> Dict[str, Any]:
    """
    Process user input and determine if conversation should continue
    
    This node:
    1. Checks if user wants to exit
    2. Validates input
    3. Resolves pronoun references using conversation history
    4. Updates node history
    
    Args:
        state: Current agent state
        
    Returns:
        Updated state dictionary
    """
    logger.node_entry(NodeNames.USER_INPUT, state)
    
    user_query = state.get("user_query", "").strip()
    session_id = state.get("session_id")
    
    # Check for exit commands
    if user_query.lower() in EXIT_COMMANDS:
        logger.info("User requested to exit")
        logger.node_exit(NodeNames.USER_INPUT, success=True)
        
        return {
            "should_exit": True,
            "node_history": state.get("node_history", []) + [NodeNames.USER_INPUT]
        }
    
    # Validate input
    if not user_query:
        logger.warning("Empty user query received")
        return {
            "should_exit": False,
            "error_info": {
                "type": "validation_error",
                "message": "Please provide a query"
            },
            "node_history": state.get("node_history", []) + [NodeNames.USER_INPUT]
        }
    
    # Resolve pronoun references using conversation history
    original_query = user_query
    resolved_query = memory.resolve_pronoun_references(user_query, session_id)
    
    if resolved_query != original_query:
        logger.info(f"Resolved pronoun reference in query")
        logger.debug(f"Original: {original_query}")
        logger.debug(f"Resolved: {resolved_query}")
    
    # Update state
    updates = {
        "user_query": resolved_query,
        "should_exit": False,
        "error_info": None,
        "retry_count": 0,  # Reset retry count for new query
        "node_history": state.get("node_history", []) + [NodeNames.USER_INPUT],
        "metadata": {
            **state.get("metadata", {}),
            "original_query": original_query,
            "query_resolved": resolved_query != original_query
        }
    }
    
    logger.info(f"Processing query: {resolved_query[:100]}...")
    logger.node_exit(NodeNames.USER_INPUT, success=True)
    
    return updates


def should_exit(state: AgentState) -> str:
    """
    Routing function to determine if conversation should exit
    Args:
        state: Current agent state
    """
    if state.get("should_exit", False):
        return NodeNames.EXIT
    return NodeNames.CONTEXT_MANAGER