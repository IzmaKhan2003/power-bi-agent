import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

"""
Final Response and Exit Nodes
"""
from typing import Dict, Any
from graphs.state import AgentState
from utils.constants import NodeNames
from utils.logger import get_logger
from utils.memory_manager import get_memory_manager

logger = get_logger("FinalResponseNode")
memory = get_memory_manager()


def final_response_node(state: AgentState) -> Dict[str, Any]:
    """
    Prepare final response to user and update memory
    
    This node:
    1. Retrieves formatted output or error message
    2. Updates conversation memory
    3. Prepares response for display
    4. Logs completion
    
    Args:
        state: Current agent state
        
    Returns:
        Updated state dictionary
    """
    logger.node_entry(NodeNames.FINAL_RESPONSE, state)
    
    user_query = state.get("user_query", "")
    formatted_output = state.get("formatted_output", "")
    sql_query = state.get("sql_query")
    query_results = state.get("query_results", [])
    error_info = state.get("error_info")
    session_id = state.get("session_id")
    execution_success = state.get("execution_success", False)
    
    # Determine success
    success = execution_success and formatted_output and not error_info
    
    # Update memory with this conversation turn
    if session_id:
        memory.update_session(
            user_query=user_query,
            sql_query=sql_query,
            results=query_results,
            success=success,
            error_message=error_info.get("user_message") if error_info else None,
            session_id=session_id
        )
        logger.debug(f"Updated session memory for: {session_id}")
    
    # Prepare final response text
    if not formatted_output:
        if error_info:
            final_text = error_info.get("user_message", "An error occurred")
        else:
            final_text = "I apologize, but I couldn't process your query."
    else:
        final_text = formatted_output
    
    # Add helpful prompt for next query
    if success:
        final_text += "\n\nFeel free to ask another question or type 'exit' to quit."
    
    # Log completion
    if success:
        logger.info(f"Query completed successfully")
    else:
        logger.warning(f"Query completed with errors")
    
    logger.node_exit(NodeNames.FINAL_RESPONSE, success=True)
    
    # Update state
    updates = {
        "formatted_output": final_text,
        "node_history": state.get("node_history", []) + [NodeNames.FINAL_RESPONSE],
        "metadata": {
            **state.get("metadata", {}),
            "query_completed": True,
            "final_success": success
        }
    }
    
    return updates


def exit_node(state: AgentState) -> Dict[str, Any]:
    """
    Handle graceful exit from conversation
    
    This node:
    1. Generates session summary
    2. Saves session data if needed
    3. Prepares goodbye message
    4. Cleans up resources
    
    Args:
        state: Current agent state
        
    Returns:
        Updated state dictionary
    """
    logger.node_entry(NodeNames.EXIT, state)
    
    session_id = state.get("session_id")
    
    # Get session summary
    session_summary = ""
    if session_id:
        session_summary = memory.get_session_summary(session_id)
        logger.info(f"Generated session summary for {session_id}")
        
        # Optionally save session to file
        session = memory.get_session(session_id)
        if session:
            try:
                import os
                session_file = f"data/logs/session_{session_id}.json"
                os.makedirs(os.path.dirname(session_file), exist_ok=True)
                session.save_to_file(session_file)
                logger.info(f"Session saved to {session_file}")
            except Exception as e:
                logger.warning(f"Failed to save session: {str(e)}")
    
    # Generate goodbye message
    goodbye_message = f"""Thank you for using the NLP-to-SQL Agent!

{session_summary}

Goodbye!"""
    
    logger.info("User exited conversation")
    logger.node_exit(NodeNames.EXIT, success=True)
    
    # Update state
    updates = {
        "formatted_output": goodbye_message,
        "should_exit": True,
        "node_history": state.get("node_history", []) + [NodeNames.EXIT],
        "metadata": {
            **state.get("metadata", {}),
            "session_ended": True
        }
    }
    
    return updates