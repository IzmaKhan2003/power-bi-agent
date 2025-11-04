"""
Custom logger with color support and file logging
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import logging
import colorlog
import os
from typing import Optional
from datetime import datetime
from utils.constants import LOG_LEVEL, LOG_FORMAT, LOG_FILE


def setup_logger(
    name: str,
    level: Optional[str] = None,
    log_to_file: bool = True
) -> logging.Logger:
    """
    Setup a logger with color formatting for console and optional file logging
    
    Args:
        name: Logger name
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        log_to_file: Whether to log to file
        
    Returns:
        Configured logger instance
    """
    logger = colorlog.getLogger(name)
    logger.setLevel(level or LOG_LEVEL)
    
    # Remove existing handlers to avoid duplicates
    logger.handlers = []
    
    # Console handler with colors
    console_handler = colorlog.StreamHandler()
    console_handler.setLevel(level or LOG_LEVEL)
    
    console_formatter = colorlog.ColoredFormatter(
        '%(log_color)s%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        log_colors={
            'DEBUG': 'cyan',
            'INFO': 'green',
            'WARNING': 'yellow',
            'ERROR': 'red',
            'CRITICAL': 'red,bg_white',
        }
    )
    console_handler.setFormatter(console_formatter)
    logger.addHandler(console_handler)
    
    # File handler (if enabled)
    if log_to_file:
        # Create logs directory if it doesn't exist
        os.makedirs(os.path.dirname(LOG_FILE), exist_ok=True)
        
        file_handler = logging.FileHandler(LOG_FILE)
        file_handler.setLevel(logging.DEBUG)  # Log everything to file
        
        file_formatter = logging.Formatter(LOG_FORMAT)
        file_handler.setFormatter(file_formatter)
        logger.addHandler(file_handler)
    
    return logger


class AgentLogger:
    """Specialized logger for agent operations with structured logging"""
    
    def __init__(self, name: str = "NLP-SQL-Agent"):
        self.logger = setup_logger(name)
        self.session_id: Optional[str] = None
        
    def set_session(self, session_id: str):
        """Set the current session ID for logging context"""
        self.session_id = session_id
        
    def _log_with_context(self, level: str, message: str, **kwargs):
        """Log with session context"""
        context = f"[Session: {self.session_id}] " if self.session_id else ""
        full_message = f"{context}{message}"
        
        if kwargs:
            full_message += f" | {kwargs}"
            
        getattr(self.logger, level.lower())(full_message)
    
    def debug(self, message: str, **kwargs):
        self._log_with_context("DEBUG", message, **kwargs)
        
    def info(self, message: str, **kwargs):
        self._log_with_context("INFO", message, **kwargs)
        
    def warning(self, message: str, **kwargs):
        self._log_with_context("WARNING", message, **kwargs)
        
    def error(self, message: str, **kwargs):
        self._log_with_context("ERROR", message, **kwargs)
        
    def critical(self, message: str, **kwargs):
        self._log_with_context("CRITICAL", message, **kwargs)
        
    def node_entry(self, node_name: str, state: dict):
        """Log entry into a graph node"""
        self.info(f"Entering node: {node_name}", state_keys=list(state.keys()))
        
    def node_exit(self, node_name: str, success: bool = True):
        """Log exit from a graph node"""
        status = "SUCCESS" if success else "FAILURE"
        self.info(f"Exiting node: {node_name} - {status}")
        
    def query_generated(self, query: str):
        """Log SQL query generation"""
        self.info(f"Generated SQL query", query=query[:200])  # Truncate for logging
        
    def query_executed(self, query: str, rows: int, duration: float):
        """Log SQL query execution"""
        self.info(
            f"Query executed successfully",
            rows_returned=rows,
            duration_sec=round(duration, 3)
        )
        
    def error_occurred(self, error_type: str, error_message: str, node: Optional[str] = None):
        """Log errors with context"""
        self.error(
            f"Error in {node or 'unknown node'}",
            error_type=error_type,
            error=error_message
        )


# Global logger instance
agent_logger = AgentLogger()


def get_logger(name: Optional[str] = None) -> AgentLogger:
    """Get the global agent logger or create a new one"""
    if name:
        return AgentLogger(name)
    return agent_logger