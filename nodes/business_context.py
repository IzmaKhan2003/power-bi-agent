import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

"""
Business Context Node - Maps business queries to database schema
"""
from typing import Dict, Any
from graphs.state import AgentState
from utils.constants import NodeNames
from utils.logger import get_logger
from utils.business_ontology import get_ontology_mapper

logger = get_logger("BusinessContextNode")
ontology = get_ontology_mapper()


def business_context_node(state: AgentState) -> Dict[str, Any]:
    """
    Enrich query with business domain knowledge
    
    This node:
    1. Identifies business domains from query
    2. Suggests relevant tables
    3. Provides business hints for query construction
    4. Suggests aggregations and filters
    5. Recommends JOINs between tables
    
    Args:
        state: Current agent state
        
    Returns:
        Updated state dictionary
    """
    logger.node_entry(NodeNames.BUSINESS_CONTEXT, state)
    
    user_query = state.get("user_query", "")
    schema_info = state.get("schema_info", {})
    
    # Identify business domains
    identified_domains = ontology.identify_domains(user_query)
    logger.info(f"Identified domains: {identified_domains}")
    
    # Get relevant tables
    suggested_tables = ontology.get_relevant_tables(user_query)
    logger.info(f"Suggested tables: {suggested_tables}")
    
    # Enrich with business context
    business_context = ontology.enrich_query_context(user_query, schema_info)
    
    # Get aggregation suggestions
    suggested_aggregations = ontology.suggest_aggregations(user_query)
    if suggested_aggregations:
        logger.info(f"Suggested aggregations: {suggested_aggregations}")
    
    # Get filter suggestions
    suggested_filters = ontology.suggest_filters(user_query)
    if suggested_filters:
        logger.info(f"Suggested filters: {suggested_filters}")
    
    # Get JOIN suggestions
    join_suggestions = []
    if len(suggested_tables) > 1:
        join_suggestions = ontology.get_join_suggestions(suggested_tables, schema_info)
        if join_suggestions:
            logger.info(f"Suggested {len(join_suggestions)} JOIN operations")
    
    # Combine all business intelligence
    enhanced_context = {
        **business_context,
        "suggested_aggregations": suggested_aggregations,
        "suggested_filters": suggested_filters,
        "join_suggestions": join_suggestions,
        "query_type": _detect_query_type(user_query)
    }
    
    # Update state
    updates = {
        "business_context": enhanced_context,
        "identified_domains": identified_domains,
        "suggested_tables": suggested_tables,
        "node_history": state.get("node_history", []) + [NodeNames.BUSINESS_CONTEXT],
        "metadata": {
            **state.get("metadata", {}),
            "business_domains": identified_domains,
            "suggested_table_count": len(suggested_tables),
            "has_aggregations": len(suggested_aggregations) > 0,
            "has_filters": len(suggested_filters) > 0
        }
    }
    
    logger.node_exit(NodeNames.BUSINESS_CONTEXT, success=True)
    
    return updates


def _detect_query_type(query: str) -> str:
    """
    Detect the type of query being asked
    
    Args:
        query: User's natural language query
        
    Returns:
        Query type string
    """
    query_lower = query.lower()
    
    # Check for different query patterns
    if any(word in query_lower for word in ['how many', 'count', 'number of']):
        return 'count'
    
    if any(word in query_lower for word in ['total', 'sum', 'add up']):
        return 'sum'
    
    if any(word in query_lower for word in ['average', 'avg', 'mean']):
        return 'average'
    
    if any(word in query_lower for word in ['top', 'highest', 'maximum', 'best', 'largest']):
        return 'top_n'
    
    if any(word in query_lower for word in ['bottom', 'lowest', 'minimum', 'worst', 'smallest']):
        return 'bottom_n'
    
    if any(word in query_lower for word in ['list', 'show', 'display', 'get', 'find']):
        return 'list'
    
    if any(word in query_lower for word in ['compare', 'difference', 'versus', 'vs']):
        return 'comparison'
    
    if any(word in query_lower for word in ['group by', 'breakdown', 'by category', 'by type']):
        return 'group_by'
    
    return 'general'