"""
Database connection and query execution utilities
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))


import psycopg2
from psycopg2 import sql, extras
from typing import List, Dict, Any, Optional, Tuple
from contextlib import contextmanager
import time
from utils.constants import DB_CONFIG, QUERY_TIMEOUT
from utils.logger import get_logger

logger = get_logger("DBConnection")


class DatabaseConnection:
    """Manages PostgreSQL database connections and query execution"""
    
    def __init__(self, config: Optional[Dict[str, Any]] = None):
        """
        Initialize database connection manager
        
        Args:
            config: Database configuration dict. Uses DB_CONFIG from constants if None
        """
        self.config = config or DB_CONFIG
        self._connection = None
        
    @contextmanager
    def get_connection(self):
        """
        Context manager for database connections
        
        Yields:
            psycopg2 connection object
        """
        conn = None
        try:
            conn = psycopg2.connect(**self.config)
            logger.debug("Database connection established")
            yield conn
        except psycopg2.Error as e:
            logger.error(f"Database connection error: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
                logger.debug("Database connection closed")
    
    def test_connection(self) -> Tuple[bool, Optional[str]]:
        """
        Test database connectivity
        
        Returns:
            Tuple of (success: bool, error_message: Optional[str])
        """
        try:
            with self.get_connection() as conn:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
            logger.info("Database connection test successful")
            return True, None
        except Exception as e:
            error_msg = f"Database connection test failed: {str(e)}"
            logger.error(error_msg)
            return False, error_msg
    
    def execute_query(
        self,
        query: str,
        params: Optional[tuple] = None,
        fetch: bool = True
    ) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
        """
        Execute a SQL query and return results
        
        Args:
            query: SQL query string
            params: Query parameters for parameterized queries
            fetch: Whether to fetch results (False for INSERT/UPDATE/DELETE)
            
        Returns:
            Tuple of (results: List[Dict], error: Optional[str])
        """
        start_time = time.time()
        
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=extras.RealDictCursor) as cursor:
                    # Set query timeout
                    cursor.execute(f"SET statement_timeout = {QUERY_TIMEOUT * 1000}")
                    
                    # Execute query
                    cursor.execute(query, params)
                    
                    if fetch:
                        results = cursor.fetchall()
                        # Convert RealDictRow to regular dict
                        results = [dict(row) for row in results]
                        
                        duration = time.time() - start_time
                        logger.query_executed(query, len(results), duration)
                        
                        return results, None
                    else:
                        conn.commit()
                        return None, None
                        
        except psycopg2.Error as e:
            error_msg = f"Query execution error: {str(e)}"
            logger.error_occurred("DatabaseError", error_msg)
            return None, error_msg
        except Exception as e:
            error_msg = f"Unexpected error during query execution: {str(e)}"
            logger.error_occurred("UnexpectedError", error_msg)
            return None, error_msg
    
    def get_table_names(self) -> Tuple[Optional[List[str]], Optional[str]]:
        """
        Get all table names in the database
        
        Returns:
            Tuple of (table_names: List[str], error: Optional[str])
        """
        query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public' 
            AND table_type = 'BASE TABLE'
            ORDER BY table_name
        """
        
        results, error = self.execute_query(query)
        
        if error:
            return None, error
            
        table_names = [row['table_name'] for row in results]
        logger.info(f"Retrieved {len(table_names)} tables from database")
        
        return table_names, None
    
    def get_table_schema(
        self,
        table_name: str
    ) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
        """
        Get schema information for a specific table
        
        Args:
            table_name: Name of the table
            
        Returns:
            Tuple of (schema_info: List[Dict], error: Optional[str])
            Each dict contains: column_name, data_type, is_nullable, column_default
        """
        query = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length
            FROM information_schema.columns
            WHERE table_schema = 'public' 
            AND table_name = %s
            ORDER BY ordinal_position
        """
        
        results, error = self.execute_query(query, (table_name,))
        
        if error:
            return None, error
            
        logger.debug(f"Retrieved schema for table: {table_name}")
        return results, None
    
    def get_table_relationships(
        self,
        table_name: Optional[str] = None
    ) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
        """
        Get foreign key relationships for tables
        
        Args:
            table_name: Specific table name, or None for all relationships
            
        Returns:
            Tuple of (relationships: List[Dict], error: Optional[str])
        """
        query = """
            SELECT
                tc.table_name as from_table,
                kcu.column_name as from_column,
                ccu.table_name as to_table,
                ccu.column_name as to_column,
                tc.constraint_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
                ON tc.constraint_name = kcu.constraint_name
                AND tc.table_schema = kcu.table_schema
            JOIN information_schema.constraint_column_usage ccu
                ON ccu.constraint_name = tc.constraint_name
                AND ccu.table_schema = tc.table_schema
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_schema = 'public'
        """
        
        if table_name:
            query += " AND tc.table_name = %s"
            results, error = self.execute_query(query, (table_name,))
        else:
            results, error = self.execute_query(query)
        
        if error:
            return None, error
            
        logger.debug(f"Retrieved relationships for {table_name or 'all tables'}")
        return results, None
    
    def get_sample_data(
        self,
        table_name: str,
        limit: int = 3
    ) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
        """
        Get sample rows from a table
        
        Args:
            table_name: Name of the table
            limit: Number of sample rows
            
        Returns:
            Tuple of (sample_data: List[Dict], error: Optional[str])
        """
        # Use sql.Identifier to prevent SQL injection
        query = sql.SQL("SELECT * FROM {} LIMIT %s").format(
            sql.Identifier(table_name)
        )
        
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=extras.RealDictCursor) as cursor:
                    cursor.execute(query, (limit,))
                    results = cursor.fetchall()
                    results = [dict(row) for row in results]
                    
            logger.debug(f"Retrieved {len(results)} sample rows from {table_name}")
            return results, None
            
        except Exception as e:
            error_msg = f"Error getting sample data: {str(e)}"
            logger.error(error_msg)
            return None, error_msg


# Global database connection instance
db = DatabaseConnection()


def get_db() -> DatabaseConnection:
    """Get the global database connection instance"""
    return db