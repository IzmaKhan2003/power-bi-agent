# """
# Main entry point for the NLP-to-SQL Agent
# """
# import sys
# import argparse
# from interfaces.cli_chat import run_cli_chat
# from utils.logger import get_logger
# from utils.db_connection import get_db

# logger = get_logger("Main")


# def test_connection():
#     """Test database connection"""
#     print("Testing database connection...")
#     db = get_db()
#     success, error = db.test_connection()
    
#     if success:
#         print("✓ Connection successful!")
        
#         # Get table count
#         tables, _ = db.get_table_names()
#         if tables:
#             print(f"✓ Found {len(tables)} tables in database")
#             print(f"  Tables: {', '.join(tables[:5])}" + (" ..." if len(tables) > 5 else ""))
#         return 0
#     else:
#         print(f"✗ Connection failed: {error}")
#         return 1


# def run_chat():
#     """Run the CLI chat interface"""
#     try:
#         run_cli_chat()
#         return 0
#     except Exception as e:
#         logger.error(f"Chat failed: {str(e)}")
#         print(f"Error: {str(e)}")
#         return 1


# def main():
#     """Main function with argument parsing"""
#     parser = argparse.ArgumentParser(
#         description="NLP-to-SQL Conversational Agent",
#         formatter_class=argparse.RawDescriptionHelpFormatter,
#         epilog="""
# Examples:
#   python main.py                    # Start chat interface
#   python main.py --test-connection  # Test database connection
#   python main.py --help             # Show this help message
#         """
#     )
    
#     parser.add_argument(
#         '--test-connection',
#         action='store_true',
#         help='Test database connection and exit'
#     )
    
#     parser.add_argument(
#         '--web',
#         action='store_true',
#         help='Launch web interface (Streamlit - if available)'
#     )
    
#     args = parser.parse_args()
    
#     # Handle test connection
#     if args.test_connection:
#         return test_connection()
    
#     # Handle web interface
#     if args.web:
#         try:
#             print("Launching web interface...")
#             # Import and run Streamlit app
#             import subprocess
#             subprocess.run(["streamlit", "run", "interfaces/web_chat.py"])
#             return 0
#         except ImportError:
#             print("Error: Streamlit not installed. Install with: pip install streamlit")
#             return 1
#         except Exception as e:
#             print(f"Error launching web interface: {str(e)}")
#             return 1
    
#     # Default: Run CLI chat
#     return run_chat()


# if __name__ == "__main__":
#     sys.exit(main())