"""
Constants and configuration settings for the NLP-to-SQL agent
"""
import os
from typing import Dict, List
from dotenv import load_dotenv

load_dotenv()

# Database Configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", 5432)),
    "database": os.getenv("DB_NAME", "northwind"),
    "user": os.getenv("DB_USER", "postgres"),
    "password": os.getenv("DB_PASSWORD", ""),
}

# OpenAI Configuration
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY")
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.0-flash")
OPENAI_TEMPERATURE = float(os.getenv("OPENAI_TEMPERATURE", 0.1))

# Agent Configuration
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))
QUERY_TIMEOUT = int(os.getenv("QUERY_TIMEOUT", 30))
CACHE_ENABLED = os.getenv("CACHE_ENABLED", "true").lower() == "true"
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")

# Graph Node Names
class NodeNames:
    USER_INPUT = "user_input"
    CONTEXT_MANAGER = "context_manager"
    SCHEMA_INSPECTOR = "schema_inspector"
    BUSINESS_CONTEXT = "business_context"
    PLANNER = "planner"
    VALIDATOR = "validator"
    SQL_EXECUTOR = "sql_executor"
    OUTPUT_FORMATTER = "output_formatter"
    ERROR_HANDLER = "error_handler"
    FINAL_RESPONSE = "final_response"
    EXIT = "exit_node"

# State Keys
class StateKeys:
    USER_QUERY = "user_query"
    CONVERSATION_HISTORY = "conversation_history"
    SCHEMA_INFO = "schema_info"
    BUSINESS_CONTEXT = "business_context"
    SQL_QUERY = "sql_query"
    QUERY_RESULTS = "query_results"
    FORMATTED_OUTPUT = "formatted_output"
    ERROR_INFO = "error_info"
    RETRY_COUNT = "retry_count"
    SESSION_ID = "session_id"
    SHOULD_EXIT = "should_exit"
    METADATA = "metadata"

# Exit Commands
EXIT_COMMANDS = ["exit", "quit", "bye", "goodbye", "stop"]

# Business Context Ontology - Maps business terms to database concepts
BUSINESS_ONTOLOGY: Dict[str, Dict[str, List[str]]] = {
    "sales": {
        "tables": ["orders", "order_details", "products"],
        "keywords": ["revenue", "sales", "selling", "purchase", "bought"]
    },
    "customers": {
        "tables": ["customers", "orders"],
        "keywords": ["customer", "client", "buyer", "consumer"]
    },
    "products": {
        "tables": ["products", "categories", "suppliers"],
        "keywords": ["product", "item", "merchandise", "goods", "inventory"]
    },
    "employees": {
        "tables": ["employees", "orders"],
        "keywords": ["employee", "staff", "worker", "personnel", "team"]
    },
    "shipping": {
        "tables": ["orders", "shippers"],
        "keywords": ["ship", "delivery", "freight", "transport", "carrier"]
    },
    "regions": {
        "tables": ["customers", "employees", "region", "territories"],
        "keywords": ["region", "territory", "location", "area", "geography"]
    }
}

# SQL Query Templates for common patterns
SQL_TEMPLATES = {
    "top_n": """
        SELECT {columns}
        FROM {table}
        {joins}
        {where}
        ORDER BY {order_by}
        LIMIT {limit}
    """,
    "aggregate": """
        SELECT {group_by}, {aggregates}
        FROM {table}
        {joins}
        {where}
        GROUP BY {group_by}
        {having}
        ORDER BY {order_by}
    """,
    "count": """
        SELECT COUNT(*) as count
        FROM {table}
        {joins}
        {where}
    """
}

# Error Messages
ERROR_MESSAGES = {
    "validation_failed": "The generated SQL query failed validation. Please rephrase your question.",
    "execution_failed": "Failed to execute the query. This might be due to database connection issues.",
    "no_results": "The query executed successfully but returned no results.",
    "schema_error": "Unable to inspect database schema. Please check database connection.",
    "planning_error": "Failed to generate SQL query from your question. Please try rephrasing.",
    "max_retries": "Maximum retry attempts reached. Please try a different question."
}

# Logging Configuration
LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
LOG_FILE = 'data/logs/agent_run.log'

# Cache Configuration
SCHEMA_CACHE_FILE = 'data/schemas_cache.json'
CACHE_TTL = 3600  # 1 hour in seconds

# Validation Rules
MAX_QUERY_LENGTH = 5000
ALLOWED_SQL_KEYWORDS = ['SELECT', 'FROM', 'WHERE', 'JOIN', 'GROUP BY', 'HAVING', 'ORDER BY', 'LIMIT']
FORBIDDEN_SQL_KEYWORDS = ['DROP', 'DELETE', 'TRUNCATE', 'UPDATE', 'INSERT', 'ALTER', 'CREATE']

# Output Configuration
MAX_ROWS_DISPLAY = 100
TABLE_FORMAT = "simple"  # Options: simple, grid, fancy_grid, pipe, etc.