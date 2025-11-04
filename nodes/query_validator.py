import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

"""
Query Validator Node - Validates SQL queries for safety and correctness
"""
from typing import Dict, Any, List, Tuple
import re
import sqlparse
from graphs.state import AgentState
from utils.constants import (
    NodeNames, 
    FORBIDDEN_SQL_KEYWORDS, 
    ALLOWED_SQL_KEYWORDS,
    MAX_QUERY_LENGTH
)
from utils.logger import get_logger

logger = get_logger("ValidatorNode")


def validate_sql_syntax(sql: str) -> Tuple[bool, List[str]]:
    """
    Validate SQL syntax
    
    Args:
        sql: SQL query string
        
    Returns:
        Tuple of (is_valid, error_messages)
    """
    errors = []
    
    try:
        # Parse SQL
        parsed = sqlparse.parse(sql)
        
        if not parsed:
            errors.append("Failed to parse SQL query")
            return False, errors
        
        # Check if we have statements
        if len(parsed) == 0:
            errors.append("No SQL statement found")
            return False, errors
        
        # Check for multiple statements (potential SQL injection)
        if len(parsed) > 1:
            errors.append("Multiple SQL statements detected. Only single queries allowed.")
            return False, errors
        
        return True, []
        
    except Exception as e:
        errors.append(f"Syntax validation error: {str(e)}")
        return False, errors


def validate_sql_safety(sql: str) -> Tuple[bool, List[str]]:
    """
    Validate SQL query for safety (no destructive operations)
    
    Args:
        sql: SQL query string
        
    Returns:
        Tuple of (is_safe, error_messages)
    """
    errors = []
    sql_upper = sql.upper()
    
    # Check for forbidden keywords
    for keyword in FORBIDDEN_SQL_KEYWORDS:
        if re.search(r'\b' + keyword + r'\b', sql_upper):
            errors.append(f"Forbidden SQL keyword detected: {keyword}")
    
    # Must be a SELECT query
    if not sql_upper.strip().startswith('SELECT'):
        errors.append("Only SELECT queries are allowed")
    
    # Check query length
    if len(sql) > MAX_QUERY_LENGTH:
        errors.append(f"Query too long. Maximum length is {MAX_QUERY_LENGTH} characters")
    
    # Check for comment-based SQL injection
    if '--' in sql or '/*' in sql or '*/' in sql:
        # Allow comments but flag suspicious patterns
        if re.search(r'--[^\n]*\b(DROP|DELETE|INSERT)\b', sql, re.IGNORECASE):
            errors.append("Suspicious SQL comment detected")
    
    is_safe = len(errors) == 0
    return is_safe, errors


def validate_schema_consistency(
    sql: str, 
    schema_info: Dict[str, Any]
) -> Tuple[bool, List[str]]:
    """
    Validate that tables and columns in SQL exist in schema
    
    Args:
        sql: SQL query string
        schema_info: Database schema information
        
    Returns:
        Tuple of (is_consistent, error_messages)
    """
    errors = []
    warnings = []
    
    if not schema_info or 'tables' not in schema_info:
        # Can't validate without schema
        warnings.append("Schema info not available for validation")
        return True, warnings
    
    try:
        tables = schema_info.get('tables', {})
        table_names = set(tables.keys())
        
        # Extract table names from SQL (basic regex)
        # This is a simplified approach - production would use proper SQL parsing
        from_pattern = r'\bFROM\s+(\w+)'
        join_pattern = r'\bJOIN\s+(\w+)'
        
        mentioned_tables = set()
        mentioned_tables.update(re.findall(from_pattern, sql, re.IGNORECASE))
        mentioned_tables.update(re.findall(join_pattern, sql, re.IGNORECASE))
        
        # Check if mentioned tables exist
        for table in mentioned_tables:
            if table.lower() not in [t.lower() for t in table_names]:
                errors.append(f"Table '{table}' not found in schema")
        
        # Basic column validation (checks if column references exist)
        # This is simplified - would need full AST parsing for complete validation
        for table in mentioned_tables:
            table_lower = table.lower()
            matching_table = next((t for t in table_names if t.lower() == table_lower), None)
            
            if matching_table and matching_table in tables:
                table_columns = [
                    col['column_name'].lower() 
                    for col in tables[matching_table].get('columns', [])
                ]
                
                # Look for table.column patterns
                column_pattern = rf'\b{table}\.(\w+)'
                mentioned_columns = re.findall(column_pattern, sql, re.IGNORECASE)
                
                for col in mentioned_columns:
                    if col.lower() not in table_columns:
                        errors.append(f"Column '{col}' not found in table '{table}'")
        
        is_consistent = len(errors) == 0
        return is_consistent, errors + warnings
        
    except Exception as e:
        warnings.append(f"Schema validation error: {str(e)}")
        return True, warnings  # Don't fail on validation errors


def query_validator_node(state: AgentState) -> Dict[str, Any]:
    """
    Validate SQL query for safety, syntax, and schema consistency
    
    This node:
    1. Validates SQL syntax
    2. Checks for dangerous operations
    3. Validates against database schema
    4. Returns validation results with detailed errors
    
    Args:
        state: Current agent state
        
    Returns:
        Updated state dictionary
    """
    logger.node_entry(NodeNames.VALIDATOR, state)
    
    sql_query = state.get("sql_query")
    schema_info = state.get("schema_info", {})
    
    if not sql_query:
        logger.error("No SQL query to validate")
        logger.node_exit(NodeNames.VALIDATOR, success=False)
        
        return {
            "validation_passed": False,
            "validation_errors": ["No SQL query provided for validation"],
            "node_history": state.get("node_history", []) + [NodeNames.VALIDATOR]
        }
    
    all_errors = []
    
    # 1. Syntax validation
    logger.debug("Validating SQL syntax...")
    syntax_valid, syntax_errors = validate_sql_syntax(sql_query)
    all_errors.extend(syntax_errors)
    
    if not syntax_valid:
        logger.error(f"Syntax validation failed: {syntax_errors}")
    
    # 2. Safety validation
    logger.debug("Validating SQL safety...")
    safety_valid, safety_errors = validate_sql_safety(sql_query)
    all_errors.extend(safety_errors)
    
    if not safety_valid:
        logger.error(f"Safety validation failed: {safety_errors}")
    
    # 3. Schema consistency validation
    logger.debug("Validating schema consistency...")
    schema_valid, schema_errors = validate_schema_consistency(sql_query, schema_info)
    all_errors.extend(schema_errors)
    
    if not schema_valid:
        logger.warning(f"Schema validation issues: {schema_errors}")
    
    # Determine overall validation result
    validation_passed = syntax_valid and safety_valid and schema_valid
    
    if validation_passed:
        logger.info("Query validation passed successfully")
        logger.node_exit(NodeNames.VALIDATOR, success=True)
    else:
        logger.error(f"Query validation failed with {len(all_errors)} errors")
        logger.node_exit(NodeNames.VALIDATOR, success=False)
    
    # Update state
    updates = {
        "validation_passed": validation_passed,
        "validation_errors": all_errors,
        "node_history": state.get("node_history", []) + [NodeNames.VALIDATOR],
        "metadata": {
            **state.get("metadata", {}),
            "syntax_valid": syntax_valid,
            "safety_valid": safety_valid,
            "schema_valid": schema_valid
        }
    }
    
    # If validation failed, set error info
    if not validation_passed:
        updates["error_info"] = {
            "type": "validation_error",
            "message": "; ".join(all_errors),
            "node": NodeNames.VALIDATOR,
            "errors": all_errors
        }
    
    return updates


def route_after_validation(state: AgentState) -> str:
    """
    Route to next node based on validation result
    
    Args:
        state: Current agent state
        
    Returns:
        Next node name
    """
    if state.get("validation_passed", False):
        return NodeNames.SQL_EXECUTOR
    else:
        return NodeNames.ERROR_HANDLER