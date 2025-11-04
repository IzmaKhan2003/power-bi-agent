"""
Context Manager Node - Maintains conversational state and context
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))


from typing import Dict, Any
from graphs.state import AgentState
from utils.constants import NodeNames
from utils.logger import get_logger
from utils.memory_manager import get_memory_manager

logger = get_logger("ContextManagerNode")
memory = get_memory_manager()


def context_manager_node(state: AgentState) -> Dict[str, Any]:
    """
    Manage conversational context and retrieve relevant information
    
    This node:
    1. Retrieves session memory
    2. Gets conversation history
    3. Provides context for query understanding
    4. Tracks conversation flow
    
    Args:
        state: Current agent state
        
    Returns:
        Updated state dictionary
    """
    logger.node_entry(NodeNames.CONTEXT_MANAGER, state)
    
    session_id = state.get("session_id")
    user_query = state.get("user_query", "")
    
    # Get or create session
    session = memory.get_session(session_id)
    if not session:
        logger.info(f"Creating new session: {session_id}")
        memory.create_session(session_id)
        session = memory.get_session(session_id)
    
    # Get conversation context
    context = memory.get_context_for_query(session_id)
    
    # Build conversation history for state
    conversation_history = []
    if session:
        recent_turns = session.get_recent_context(n=5)
        conversation_history = [
            {
                "query": turn.user_query,
                "sql": turn.sql_query,
                "success": turn.success,
                "timestamp": turn.timestamp
            }
            for turn in recent_turns
        ]
    
    # Determine if this is a follow-up query
    is_followup = context.get("has_previous_queries", False)
    
    # Check if query references previous results
    query_lower = user_query.lower()
    references_previous = any(
        word in query_lower 
        for word in ['same', 'previous', 'last', 'that', 'it', 'them', 'those']
    )
    
    # Prepare contextual information
    contextual_hints = []
    
    if is_followup:
        contextual_hints.append("This is a follow-up to previous queries")
        if context.get("last_query"):
            contextual_hints.append(f"Previous query: {context['last_query']}")
        if context.get("last_sql"):
            contextual_hints.append(f"Previous SQL: {context['last_sql']}")
    
    if references_previous and not context.get("last_query"):
        contextual_hints.append("Query seems to reference previous results, but no history found")
    
    # Update state
    updates = {
        "conversation_history": conversation_history,
        "node_history": state.get("node_history", []) + [NodeNames.CONTEXT_MANAGER],
        "metadata": {
            **state.get("metadata", {}),
            "is_followup": is_followup,
            "references_previous": references_previous,
            "conversation_length": context.get("conversation_length", 0),
            "contextual_hints": contextual_hints,
            "last_query": context.get("last_query"),
            "last_sql": context.get("last_sql")
        }
    }
    
    logger.info(f"Context loaded: {len(conversation_history)} previous turns")
    if contextual_hints:
        logger.debug(f"Contextual hints: {contextual_hints}")
    
    logger.node_exit(NodeNames.CONTEXT_MANAGER, success=True)
    
    return updates