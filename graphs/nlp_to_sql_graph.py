"""
Main LangGraph definition for NLP-to-SQL conversational agent
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))


from langgraph.graph import StateGraph, END
from graphs.state import AgentState
from utils.constants import NodeNames
from utils.logger import get_logger

# Import all node functions
from nodes.user_input import user_input_node, should_exit
from nodes.context_manager import context_manager_node
from nodes.schema_inspector import schema_inspector_node
# from nodes.business_context import business_context_node
# from nodes.planner import planner_node
# from nodes.query_validator import query_validator_node, route_after_validation
# from nodes.sql_executor import sql_executor_node, route_after_execution
# from nodes.output_formatter import output_formatter_node
# from nodes.error_handler import error_handler_node, route_after_error
# from nodes.final_response import final_response_node, exit_node

logger = get_logger("NLPToSQLGraph")


def create_nlp_to_sql_graph() -> StateGraph:
    """
    Create and configure the NLP-to-SQL conversational graph
    
    Returns:
        Configured StateGraph instance
    """
    logger.info("Building NLP-to-SQL graph...")
    
    # Initialize graph
    workflow = StateGraph(AgentState)
    
    # Add all nodes
    workflow.add_node(NodeNames.USER_INPUT, user_input_node)
    workflow.add_node(NodeNames.CONTEXT_MANAGER, context_manager_node)
    workflow.add_node(NodeNames.SCHEMA_INSPECTOR, schema_inspector_node)
    # workflow.add_node(NodeNames.BUSINESS_CONTEXT, business_context_node)
    # workflow.add_node(NodeNames.PLANNER, planner_node)
    # workflow.add_node(NodeNames.VALIDATOR, query_validator_node)
    # workflow.add_node(NodeNames.SQL_EXECUTOR, sql_executor_node)
    # workflow.add_node(NodeNames.OUTPUT_FORMATTER, output_formatter_node)
    # workflow.add_node(NodeNames.ERROR_HANDLER, error_handler_node)
    # workflow.add_node(NodeNames.FINAL_RESPONSE, final_response_node)
    # workflow.add_node(NodeNames.EXIT, exit_node)
    
    # Set entry point
    workflow.set_entry_point(NodeNames.USER_INPUT)
    
    # Add edges
    
    # From user input - check if should exit
    workflow.add_conditional_edges(
        NodeNames.USER_INPUT,
        should_exit,
        {
#            NodeNames.EXIT: NodeNames.EXIT,
            NodeNames.CONTEXT_MANAGER: NodeNames.CONTEXT_MANAGER
        }
    )
    
    # From context manager - always go to schema inspector
    workflow.add_edge(NodeNames.CONTEXT_MANAGER, NodeNames.SCHEMA_INSPECTOR)
    
    # # From schema inspector - always go to business context
    # workflow.add_edge(NodeNames.SCHEMA_INSPECTOR, NodeNames.BUSINESS_CONTEXT)
    
    # # From business context - always go to planner
    # workflow.add_edge(NodeNames.BUSINESS_CONTEXT, NodeNames.PLANNER)
    
    # # From planner - always go to validator
    # workflow.add_edge(NodeNames.PLANNER, NodeNames.VALIDATOR)
    
    # # From validator - conditional based on validation result
    # workflow.add_conditional_edges(
    #     NodeNames.VALIDATOR,
    #     route_after_validation,
    #     {
    #         NodeNames.SQL_EXECUTOR: NodeNames.SQL_EXECUTOR,
    #         NodeNames.ERROR_HANDLER: NodeNames.ERROR_HANDLER
    #     }
    # )
    
    # # From SQL executor - conditional based on execution result
    # workflow.add_conditional_edges(
    #     NodeNames.SQL_EXECUTOR,
    #     route_after_execution,
    #     {
    #         NodeNames.OUTPUT_FORMATTER: NodeNames.OUTPUT_FORMATTER,
    #         NodeNames.ERROR_HANDLER: NodeNames.ERROR_HANDLER
    #     }
    # )
    
    # # From output formatter - always go to final response
    # workflow.add_edge(NodeNames.OUTPUT_FORMATTER, NodeNames.FINAL_RESPONSE)
    
    # # From error handler - conditional based on recovery strategy
    # workflow.add_conditional_edges(
    #     NodeNames.ERROR_HANDLER,
    #     route_after_error,
    #     {
    #         NodeNames.PLANNER: NodeNames.PLANNER,
    #         NodeNames.FINAL_RESPONSE: NodeNames.FINAL_RESPONSE
    #     }
    # )
    
    # # From final response - end the current query, ready for next
    # workflow.add_edge(NodeNames.FINAL_RESPONSE, END)
    
    # # From exit - end the conversation
    # workflow.add_edge(NodeNames.EXIT, END)
    
    # Compile the graph
    app = workflow.compile()
    
    logger.info("NLP-to-SQL graph compiled successfully")
    
    return app


# Create the compiled graph instance
nlp_to_sql_app = create_nlp_to_sql_graph()


def get_nlp_to_sql_graph():
    """Get the compiled graph instance"""
    return nlp_to_sql_app