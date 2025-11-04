"""
CLI Chat Interface for the NLP-to-SQL Agent
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))


from datetime import datetime
from typing import Optional
from graphs.nlp_to_sql_graph import get_nlp_to_sql_graph
from graphs.state import create_initial_state
from utils.logger import get_logger, agent_logger
from utils.memory_manager import get_memory_manager
from utils.db_connection import get_db
from utils.constants import EXIT_COMMANDS

logger = get_logger("CLIChat")
memory = get_memory_manager()
db = get_db()


def print_banner():
    """Print welcome banner"""
    banner = """
╔═══════════════════════════════════════════════════════════╗
║                                                           ║
║          NLP-to-SQL Conversational Agent v2.0             ║
║          Powered by LangGraph & LangChain                 ║
║                                                           ║
║  Ask questions about your database in natural language!   ║
║  Type 'exit', 'quit', or 'bye' to end the conversation.   ║
║                                                           ║
╚═══════════════════════════════════════════════════════════╝
"""
    print(banner)


def print_separator():
    """Print visual separator"""
    print("\n" + "─" * 60 + "\n")


def get_user_input() -> Optional[str]:
    """
    Get user input from command line
    
    Returns:
        User input string or None if interrupted
    """
    try:
        user_input = input("\n💬 You: ").strip()
        return user_input
    except (KeyboardInterrupt, EOFError):
        print("\n\nInterrupted. Exiting...")
        return None


def display_response(response: str):
    """
    Display agent response
    
    Args:
        response: Response text to display
    """
    print(f"\n🤖 Assistant:\n{response}")


def test_database_connection() -> bool:
    """
    Test database connection before starting chat
    
    Returns:
        True if connection successful, False otherwise
    """
    print("Testing database connection...")
    success, error = db.test_connection()
    
    if success:
        print("✓ Database connection successful!\n")
        return True
    else:
        print(f"✗ Database connection failed: {error}\n")
        print("Please check your database configuration in .env file")
        return False


def run_query(graph, state: dict, session_id: str) -> Optional[str]:
    """
    Run a single query through the graph
    
    Args:
        graph: Compiled LangGraph instance
        state: Initial state for the query
        session_id: Current session ID
        
    Returns:
        Final response text or None if error
    """
    try:
        logger.info(f"Processing query: {state['user_query'][:50]}...")
        
        # Set session for logging
        agent_logger.set_session(session_id)
        
        # Run the graph
        final_state = None
        for s in graph.stream(state):
            final_state = s
            # Optionally print progress
            # print(f"  → {list(s.keys())}", end="\r")
        
        if final_state:
            # Extract the final state from the last node
            last_node_state = list(final_state.values())[-1]
            response = last_node_state.get("formatted_output", "No response generated")
            should_exit = last_node_state.get("should_exit", False)
            
            return response, should_exit
        else:
            return "Failed to process query", False
            
    except Exception as e:
        error_msg = f"Error processing query: {str(e)}"
        logger.error(error_msg)
        return f"I encountered an error: {str(e)}\nPlease try rephrasing your question.", False


def run_cli_chat():
    """
    Main CLI chat loop
    """
    # Print welcome banner
    print_banner()
    
    # Test database connection
    if not test_database_connection():
        return
    
    # Create session
    session_id = memory.create_session()
    logger.info(f"Started new session: {session_id}")
    print(f"Session ID: {session_id}\n")
    
    # Get the graph
    graph = get_nlp_to_sql_graph()
    
    # # Initial schema load (optional - can be cached)
    # print("Loading database schema...")
    # from nodes.schema_inspector import inspect_database_schema
    # schema_info, error = inspect_database_schema()
    
    # if error:
    #     print(f"Warning: Could not load schema: {error}")
    #     schema_info = None
    # else:
    #     print(f"✓ Schema loaded successfully\n")
    
    # print_separator()
    # print("Ready to answer your questions! Ask me anything about the database.")
    # print_separator()
    
    # Main conversation loop
    query_count = 0
    while True:
        # Get user input
        user_query = get_user_input()
        
        # Handle None (interrupted)
        if user_query is None:
            break
        
        # Handle empty input
        if not user_query:
            print("Please enter a question.")
            continue
        
        # Check for exit commands
        if user_query.lower() in EXIT_COMMANDS:
            print("\n👋 Goodbye!")
            
            # Get session summary
            summary = memory.get_session_summary(session_id)
            print(f"\n{summary}")
            break
        
        # Create initial state for this query
        state = create_initial_state(
            user_query=user_query,
            session_id=session_id,
            schema_info=schema_info
        )
        
        # Process query
        query_count += 1
        response, should_exit = run_query(graph, state, session_id)
        
        # Display response
        display_response(response)
        print_separator()
        
        # Check if we should exit
        if should_exit:
            break
    
    # Cleanup
    logger.info(f"Session ended. Processed {query_count} queries.")
    print(f"\nProcessed {query_count} queries in this session.")


def main():
    """Entry point for CLI chat"""
    try:
        run_cli_chat()
    except KeyboardInterrupt:
        print("\n\nChat interrupted. Goodbye!")
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")
        logger.error(f"Fatal error in CLI chat: {str(e)}")


if __name__ == "__main__":
    main()