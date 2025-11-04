import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
"""
Planner Node - Generates SQL queries from natural language using LLM
"""
from typing import Dict, Any
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import PromptTemplate   , ChatPromptTemplate
from graphs.state import AgentState
from utils.constants import NodeNames, GEMINI_API_KEY, GEMINI_MODEL, OPENAI_TEMPERATURE
from utils.logger import get_logger

logger = get_logger("PlannerNode")


# SQL Generation Prompt Template
SQL_GENERATION_PROMPT = """You are an expert SQL query generator for PostgreSQL. Your task is to convert natural language questions into valid SQL queries.

Database Schema:
{schema_info}

Business Context:
{business_context}

Conversation History (for context):
{conversation_history}

User Question: {user_query}

Important Guidelines:
1. Generate ONLY SELECT queries (no INSERT, UPDATE, DELETE, DROP, etc.)
2. Use proper JOIN syntax when multiple tables are needed
3. Include appropriate WHERE clauses for filtering
4. Use aggregation functions (COUNT, SUM, AVG, etc.) when needed
5. Add GROUP BY when using aggregations
6. Include ORDER BY and LIMIT for top/bottom queries
7. Use table aliases for better readability
8. Ensure all column names and table names match the schema exactly
9. For date comparisons, use PostgreSQL date functions
10. Always qualify column names with table names/aliases when joining tables

{additional_hints}

Generate a SQL query that answers the user's question. Return ONLY the SQL query without any explanation or markdown formatting."""


def build_schema_context(schema_info: Dict[str, Any], suggested_tables: list) -> str:
    """
    Build a concise schema description for the prompt
    
    Args:
        schema_info: Full schema information
        suggested_tables: List of relevant table names
        
    Returns:
        Formatted schema string
    """
    if not schema_info:
        return "Schema information not available"
    
    tables = schema_info.get('tables', {})
    relationships = schema_info.get('relationships', [])
    
    schema_parts = []
    
    # If we have suggested tables, focus on those
    tables_to_describe = suggested_tables if suggested_tables else list(tables.keys())
    
    for table_name in tables_to_describe:
        if table_name not in tables:
            continue
            
        table_info = tables[table_name]
        columns = table_info.get('columns', [])
        
        # Format columns
        column_descriptions = []
        for col in columns:
            col_name = col.get('column_name')
            col_type = col.get('data_type')
            nullable = 'NULL' if col.get('is_nullable') == 'YES' else 'NOT NULL'
            column_descriptions.append(f"  - {col_name} ({col_type}, {nullable})")
        
        schema_parts.append(f"\nTable: {table_name}")
        schema_parts.append("\n".join(column_descriptions))
    
    # Add relationships
    if relationships:
        schema_parts.append("\nRelationships:")
        for rel in relationships:
            from_table = rel.get('from_table')
            to_table = rel.get('to_table')
            from_col = rel.get('from_column')
            to_col = rel.get('to_column')
            
            # Only include relevant relationships
            if from_table in tables_to_describe or to_table in tables_to_describe:
                schema_parts.append(
                    f"  - {from_table}.{from_col} -> {to_table}.{to_col}"
                )
    
    return "\n".join(schema_parts)


def build_business_context_str(business_context: Dict[str, Any]) -> str:
    """
    Build business context string for the prompt
    
    Args:
        business_context: Business context dictionary
        
    Returns:
        Formatted context string
    """
    if not business_context:
        return "No specific business context"
    
    parts = []
    
    # Domains
    domains = business_context.get('identified_domains', [])
    if domains:
        parts.append(f"Business Domains: {', '.join(domains)}")
    
    # Hints
    hints = business_context.get('business_hints', [])
    if hints:
        parts.append("Business Hints:")
        for hint in hints:
            parts.append(f"  - {hint}")
    
    # Query type
    query_type = business_context.get('query_type', 'general')
    parts.append(f"Query Type: {query_type}")
    
    return "\n".join(parts) if parts else "General query"


def build_conversation_context(conversation_history: list, metadata: dict) -> str:
    """
    Build conversation context string
    
    Args:
        conversation_history: List of previous conversation turns
        metadata: Metadata with contextual information
        
    Returns:
        Formatted context string
    """
    if not conversation_history:
        return "This is the first query in the conversation"
    
    parts = ["Previous queries in this conversation:"]
    for i, turn in enumerate(conversation_history[-3:], 1):  # Last 3 turns
        query = turn.get('query', '')
        sql = turn.get('sql', '')
        parts.append(f"{i}. Q: {query[:100]}")
        if sql:
            parts.append(f"   SQL: {sql[:100]}")
    
    # Add contextual hints
    hints = metadata.get('contextual_hints', [])
    if hints:
        parts.append("\nContext Notes:")
        for hint in hints:
            parts.append(f"  - {hint}")
    
    return "\n".join(parts)


def build_additional_hints(business_context: Dict[str, Any]) -> str:
    """
    Build additional hints from business context
    
    Args:
        business_context: Business context with suggestions
        
    Returns:
        Formatted hints string
    """
    if not business_context:
        return ""
    
    parts = []
    
    # Aggregations
    aggs = business_context.get('suggested_aggregations', [])
    if aggs:
        parts.append(f"Consider using these aggregations: {', '.join(aggs)}")
    
    # Filters
    filters = business_context.get('suggested_filters', {})
    if filters:
        parts.append("Suggested filters:")
        for filter_type, filter_value in filters.items():
            parts.append(f"  - {filter_type}: {filter_value}")
    
    # JOINs
    joins = business_context.get('join_suggestions', [])
    if joins:
        parts.append("Suggested JOIN operations:")
        for join in joins:
            parts.append(
                f"  - JOIN {join['to_table']} ON {join['from_table']}.{join['from_column']} = {join['to_table']}.{join['to_column']}"
            )
    
    return "\n".join(parts) if parts else ""


def planner_node(state: AgentState) -> Dict[str, Any]:
    """
    Generate SQL query from natural language using LLM
    
    This node:
    1. Prepares context (schema, business context, conversation)
    2. Uses LLM to generate SQL query
    3. Extracts and cleans the generated query
    4. Estimates confidence in the generation
    
    Args:
        state: Current agent state
        
    Returns:
        Updated state dictionary
    """
    logger.node_entry(NodeNames.PLANNER, state)
    
    user_query = state.get("user_query", "")
    schema_info = state.get("schema_info", {})
    business_context = state.get("business_context", {})
    conversation_history = state.get("conversation_history", [])
    suggested_tables = state.get("suggested_tables", [])
    metadata = state.get("metadata", {})
    
    try:
        # Build context strings
        schema_context = build_schema_context(schema_info, suggested_tables)
        business_context_str = build_business_context_str(business_context)
        conversation_context = build_conversation_context(conversation_history, metadata)
        additional_hints = build_additional_hints(business_context)
        
        # Create prompt
        prompt = ChatPromptTemplate.from_template(SQL_GENERATION_PROMPT)
        
        # Initialize LLM
        llm = ChatGoogleGenerativeAI(
            model=GEMINI_MODEL,
            google_api_key=GEMINI_API_KEY
        )
        
        # Generate SQL
        logger.info("Generating SQL query with LLM...")
        
        messages = prompt.format_messages(
            schema_info=schema_context,
            business_context=business_context_str,
            conversation_history=conversation_context,
            user_query=user_query,
            additional_hints=additional_hints
        )
        
        response = llm.invoke(messages)
        sql_query = response.content.strip()
        
        # Clean up the SQL query
        sql_query = _clean_sql_query(sql_query)
        
        # Estimate confidence (basic heuristic)
        confidence = _estimate_confidence(sql_query, suggested_tables)
        
        logger.query_generated(sql_query)
        logger.info(f"Query generation confidence: {confidence:.2f}")
        
        # Update state
        updates = {
            "sql_query": sql_query,
            "planner_confidence": confidence,
            "query_plan": {
                "generated_sql": sql_query,
                "confidence": confidence,
                "suggested_tables_used": suggested_tables,
                "generation_successful": True
            },
            "node_history": state.get("node_history", []) + [NodeNames.PLANNER],
            "messages": state.get("messages", []) + [
                {"role": "user", "content": user_query},
                {"role": "assistant", "content": sql_query}
            ]
        }
        
        logger.node_exit(NodeNames.PLANNER, success=True)
        return updates
        
    except Exception as e:
        error_msg = f"Failed to generate SQL query: {str(e)}"
        logger.error_occurred("PlanningError", error_msg, NodeNames.PLANNER)
        logger.node_exit(NodeNames.PLANNER, success=False)
        
        return {
            "sql_query": None,
            "planner_confidence": 0.0,
            "error_info": {
                "type": "planning_error",
                "message": error_msg,
                "node": NodeNames.PLANNER
            },
            "node_history": state.get("node_history", []) + [NodeNames.PLANNER]
        }


def _clean_sql_query(sql: str) -> str:
    """
    Clean and format SQL query
    
    Args:
        sql: Raw SQL from LLM
        
    Returns:
        Cleaned SQL query
    """
    # Remove markdown code blocks
    sql = sql.replace('```sql', '').replace('```', '').strip()
    
    # Remove extra whitespace
    lines = [line.strip() for line in sql.split('\n')]
    sql = ' '.join(lines)
    
    # Ensure it ends with semicolon
    if not sql.endswith(';'):
        sql += ';'
    
    return sql


def _estimate_confidence(sql: str, suggested_tables: list) -> float:
    """
    Estimate confidence in generated SQL
    
    Args:
        sql: Generated SQL query
        suggested_tables: List of suggested table names
        
    Returns:
        Confidence score (0-1)
    """
    confidence = 0.5  # Base confidence
    
    sql_upper = sql.upper()
    
    # Check if it's a SELECT query
    if sql_upper.startswith('SELECT'):
        confidence += 0.2
    
    # Check if suggested tables are used
    if suggested_tables:
        tables_used = sum(1 for table in suggested_tables if table.lower() in sql.lower())
        confidence += (tables_used / len(suggested_tables)) * 0.2
    
    # Check for proper SQL structure
    if 'FROM' in sql_upper:
        confidence += 0.1
    
    return min(confidence, 1.0)
