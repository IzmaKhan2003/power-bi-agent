"""
State definition for the NLP-to-SQL LangGraph
"""
from typing import TypedDict, List, Dict, Any, Optional, Annotated
from langgraph.graph import add_messages


class AgentState(TypedDict):
    """
    State that flows through the graph nodes
    
    This represents all the information that needs to be passed
    between nodes during query processing
    """
    # User interaction
    user_query: str
    should_exit: bool
    
    # Conversation context
    session_id: str
    conversation_history: List[Dict[str, Any]]
    
    # Schema information
    schema_info: Optional[Dict[str, Any]]
    schema_cached: bool
    
    # Business context
    business_context: Optional[Dict[str, Any]]
    identified_domains: List[str]
    suggested_tables: List[str]
    
    # Query planning
    sql_query: Optional[str]
    query_plan: Optional[Dict[str, Any]]
    planner_confidence: float
    
    # Validation
    validation_passed: bool
    validation_errors: List[str]
    
    # Execution
    query_results: Optional[List[Dict[str, Any]]]
    execution_success: bool
    execution_time: Optional[float]
    
    # Output
    formatted_output: Optional[str]
    output_table: Optional[str]
    
    # Error handling
    error_info: Optional[Dict[str, Any]]
    error_type: Optional[str]
    retry_count: int
    can_retry: bool
    
    # Metadata
    metadata: Dict[str, Any]
    node_history: List[str]
    
    # LangChain messages (for LLM interactions)
    messages: Annotated[List, add_messages]


def create_initial_state(
    user_query: str,
    session_id: str,
    schema_info: Optional[Dict[str, Any]] = None
) -> AgentState:
    """
    Create initial state for a new query
    
    Args:
        user_query: User's natural language query
        session_id: Current session identifier
        schema_info: Cached schema information if available
        
    Returns:
        Initial AgentState
    """
    return AgentState(
        # User interaction
        user_query=user_query,
        should_exit=False,
        
        # Conversation context
        session_id=session_id,
        conversation_history=[],
        
        # Schema information
        schema_info=schema_info,
        schema_cached=schema_info is not None,
        
        # Business context
        business_context=None,
        identified_domains=[],
        suggested_tables=[],
        
        # Query planning
        sql_query=None,
        query_plan=None,
        planner_confidence=0.0,
        
        # Validation
        validation_passed=False,
        validation_errors=[],
        
        # Execution
        query_results=None,
        execution_success=False,
        execution_time=None,
        
        # Output
        formatted_output=None,
        output_table=None,
        
        # Error handling
        error_info=None,
        error_type=None,
        retry_count=0,
        can_retry=True,
        
        # Metadata
        metadata={},
        node_history=[],
        
        # Messages
        messages=[]
    )


def update_state(
    state: AgentState,
    updates: Dict[str, Any]
) -> AgentState:
    """
    Update state with new values
    
    Args:
        state: Current state
        updates: Dictionary of updates to apply
        
    Returns:
        Updated state
    """
    return {**state, **updates}