"""
Output Formatter Node - Formats query results into user-friendly output
"""
from typing import Dict, Any, List
from tabulate import tabulate
from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_core.prompts import PromptTemplate, ChatPromptTemplate
from graphs.state import AgentState
from utils.constants import (
    NodeNames, 
    MAX_ROWS_DISPLAY, 
    TABLE_FORMAT,
    GEMINI_API_KEY,
    GEMINI_MODEL
)
from utils.logger import get_logger

logger = get_logger("OutputFormatterNode")


# Natural language explanation prompt
EXPLANATION_PROMPT = """You are a helpful data analyst assistant. You need to explain query results to the user in natural language.

User's Question: {user_query}

SQL Query Executed: {sql_query}

Number of Results: {result_count}

Sample Results (first few rows):
{sample_results}

Task: Provide a clear, concise natural language explanation of what the query found. Include:
1. A direct answer to the user's question
2. Key insights from the data
3. Any notable patterns or findings

Keep the explanation conversational and easy to understand. Be specific with numbers and facts."""


def format_results_as_table(results: List[Dict[str, Any]], max_rows: int = MAX_ROWS_DISPLAY) -> str:
    """
    Format query results as a text table
    
    Args:
        results: List of result dictionaries
        max_rows: Maximum rows to display
        
    Returns:
        Formatted table string
    """
    if not results:
        return "No results to display"
    
    # Limit rows
    display_results = results[:max_rows]
    truncated = len(results) > max_rows
    
    # Create table
    table = tabulate(
        display_results,
        headers="keys",
        tablefmt=TABLE_FORMAT,
        showindex=False
    )
    
    # Add truncation notice
    if truncated:
        remaining = len(results) - max_rows
        table += f"\n\n... and {remaining} more rows (showing first {max_rows} of {len(results)})"
    
    return table


def generate_natural_explanation(
    user_query: str,
    sql_query: str,
    results: List[Dict[str, Any]]
) -> str:
    """
    Generate natural language explanation of results using LLM
    
    Args:
        user_query: Original user question
        sql_query: SQL query executed
        results: Query results
        
    Returns:
        Natural language explanation
    """
    try:
        # Prepare sample results
        sample_size = min(5, len(results))
        sample_results = results[:sample_size]
        
        # Format sample for prompt
        sample_str = format_results_as_table(sample_results, max_rows=5)
        
        # Create prompt
        prompt = ChatPromptTemplate.from_template(EXPLANATION_PROMPT)
        
        # Initialize LLM
        llm = ChatGoogleGenerativeAI(
            model=GEMINI_MODEL,
            google_api_key=GEMINI_API_KEY
        )
        
        # Generate explanation
        messages = prompt.format_messages(
            user_query=user_query,
            sql_query=sql_query,
            result_count=len(results),
            sample_results=sample_str
        )
        
        response = llm.invoke(messages)
        explanation = response.content.strip()
        
        return explanation
        
    except Exception as e:
        logger.warning(f"Failed to generate natural explanation: {str(e)}")
        # Fallback to simple explanation
        return f"Found {len(results)} results for your query."


def create_summary_stats(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Create summary statistics for the results
    
    Args:
        results: Query results
        
    Returns:
        Dictionary with summary stats
    """
    if not results:
        return {"total_rows": 0}
    
    stats = {
        "total_rows": len(results),
        "columns": list(results[0].keys()) if results else [],
        "column_count": len(results[0]) if results else 0
    }
    
    # Try to identify numeric columns and compute basic stats
    numeric_stats = {}
    if results:
        for col in results[0].keys():
            try:
                # Try to extract numeric values
                values = [row[col] for row in results if row[col] is not None]
                numeric_values = [float(v) for v in values if isinstance(v, (int, float))]
                
                if numeric_values:
                    numeric_stats[col] = {
                        "min": min(numeric_values),
                        "max": max(numeric_values),
                        "avg": sum(numeric_values) / len(numeric_values)
                    }
            except (ValueError, TypeError):
                pass  # Not a numeric column
    
    if numeric_stats:
        stats["numeric_summaries"] = numeric_stats
    
    return stats


def output_formatter_node(state: AgentState) -> Dict[str, Any]:
    """
    Format query results into user-friendly output
    
    This node:
    1. Formats results as a table
    2. Generates natural language explanation
    3. Creates summary statistics
    4. Combines everything into final output
    
    Args:
        state: Current agent state
        
    Returns:
        Updated state dictionary
    """
    logger.node_entry(NodeNames.OUTPUT_FORMATTER, state)
    
    query_results = state.get("query_results", [])
    user_query = state.get("user_query", "")
    sql_query = state.get("sql_query", "")
    execution_time = state.get("execution_time", 0)
    
    try:
        # Handle empty results
        if not query_results:
            logger.info("No results to format")
            
            formatted_output = f"""
I executed your query but didn't find any matching results.

SQL Query: {sql_query}
Execution Time: {execution_time:.3f} seconds

This might mean:
- There's no data matching your criteria
- The filters might be too restrictive
- You might want to try rephrasing your question
"""
            
            updates = {
                "formatted_output": formatted_output.strip(),
                "output_table": "No results",
                "node_history": state.get("node_history", []) + [NodeNames.OUTPUT_FORMATTER]
            }
            
            logger.node_exit(NodeNames.OUTPUT_FORMATTER, success=True)
            return updates
        
        # Format as table
        logger.debug("Formatting results as table...")
        output_table = format_results_as_table(query_results)
        
        # Generate natural language explanation
        logger.debug("Generating natural language explanation...")
        explanation = generate_natural_explanation(user_query, sql_query, query_results)
        
        # Create summary stats
        summary_stats = create_summary_stats(query_results)
        
        # Combine into final output
        formatted_output = f"""{explanation}

{output_table}

Query Details:
- Total Results: {summary_stats['total_rows']} rows
- Columns: {summary_stats['column_count']}
- Execution Time: {execution_time:.3f} seconds
"""
        
        # Add numeric summaries if available
        if "numeric_summaries" in summary_stats:
            formatted_output += "\nNumeric Summaries:\n"
            for col, stats in summary_stats["numeric_summaries"].items():
                formatted_output += f"  {col}: min={stats['min']:.2f}, max={stats['max']:.2f}, avg={stats['avg']:.2f}\n"
        
        logger.info(f"Successfully formatted {len(query_results)} results")
        logger.node_exit(NodeNames.OUTPUT_FORMATTER, success=True)
        
        # Update state
        updates = {
            "formatted_output": formatted_output.strip(),
            "output_table": output_table,
            "node_history": state.get("node_history", []) + [NodeNames.OUTPUT_FORMATTER],
            "metadata": {
                **state.get("metadata", {}),
                "output_generated": True,
                "summary_stats": summary_stats
            }
        }
        
        return updates
        
    except Exception as e:
        error_msg = f"Error formatting output: {str(e)}"
        logger.error_occurred("FormattingError", error_msg, NodeNames.OUTPUT_FORMATTER)
        logger.node_exit(NodeNames.OUTPUT_FORMATTER, success=False)
        
        # Fallback to simple formatting
        fallback_output = f"Found {len(query_results)} results, but encountered an error while formatting. Here's the raw data:\n\n{query_results[:5]}"
        
        return {
            "formatted_output": fallback_output,
            "output_table": str(query_results[:10]),
            "node_history": state.get("node_history", []) + [NodeNames.OUTPUT_FORMATTER],
            "metadata": {
                **state.get("metadata", {}),
                "formatting_error": error_msg
            }
        }