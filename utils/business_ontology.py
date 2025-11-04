"""
Business ontology and domain knowledge mapping
Maps business concepts to database schema
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from typing import List, Dict, Set, Any, Optional
from utils.constants import BUSINESS_ONTOLOGY
from utils.logger import get_logger

logger = get_logger("BusinessOntology")


class BusinessOntologyMapper:
    """Maps business domain concepts to database schema elements"""
    
    def __init__(self, ontology: Optional[Dict[str, Dict[str, List[str]]]] = None):
        """
        Initialize the ontology mapper
        
        Args:
            ontology: Custom ontology dict, uses BUSINESS_ONTOLOGY if None
        """
        self.ontology = ontology or BUSINESS_ONTOLOGY
        self._keyword_to_domain_map = self._build_keyword_map()
        
    def _build_keyword_map(self) -> Dict[str, List[str]]:
        """Build reverse mapping from keywords to domains"""
        keyword_map = {}
        
        for domain, data in self.ontology.items():
            for keyword in data.get('keywords', []):
                if keyword not in keyword_map:
                    keyword_map[keyword] = []
                keyword_map[keyword].append(domain)
        
        return keyword_map
    
    def identify_domains(self, query: str) -> List[str]:
        """
        Identify relevant business domains from a natural language query
        
        Args:
            query: User's natural language query
            
        Returns:
            List of identified domain names
        """
        query_lower = query.lower()
        identified_domains = set()
        
        # Check for keyword matches
        for keyword, domains in self._keyword_to_domain_map.items():
            if keyword in query_lower:
                identified_domains.update(domains)
        
        # Check for direct domain mentions
        for domain in self.ontology.keys():
            if domain in query_lower:
                identified_domains.add(domain)
        
        result = list(identified_domains)
        logger.debug(f"Identified domains for query: {result}")
        
        return result
    
    def get_relevant_tables(self, query: str) -> List[str]:
        """
        Get relevant database tables based on query
        
        Args:
            query: User's natural language query
            
        Returns:
            List of relevant table names
        """
        domains = self.identify_domains(query)
        
        if not domains:
            logger.debug("No specific domains identified, returning empty table list")
            return []
        
        relevant_tables = set()
        for domain in domains:
            if domain in self.ontology:
                tables = self.ontology[domain].get('tables', [])
                relevant_tables.update(tables)
        
        result = list(relevant_tables)
        logger.debug(f"Relevant tables for query: {result}")
        
        return result
    
    def enrich_query_context(self, query: str, schema_info: Dict[str, Any]) -> Dict[str, Any]:
        """
        Enrich query with business context
        
        Args:
            query: User's natural language query
            schema_info: Database schema information
            
        Returns:
            Enriched context dictionary
        """
        domains = self.identify_domains(query)
        relevant_tables = self.get_relevant_tables(query)
        
        # Filter schema to relevant tables if any identified
        filtered_schema = {}
        if relevant_tables and 'tables' in schema_info:
            for table_name, table_data in schema_info.get('tables', {}).items():
                if table_name in relevant_tables:
                    filtered_schema[table_name] = table_data
        
        context = {
            "identified_domains": domains,
            "suggested_tables": relevant_tables,
            "filtered_schema": filtered_schema if filtered_schema else schema_info.get('tables', {}),
            "business_hints": self._get_business_hints(domains)
        }
        
        logger.debug(f"Enriched query context with {len(domains)} domains")
        return context
    
    def _get_business_hints(self, domains: List[str]) -> List[str]:
        """
        Get business hints for identified domains
        
        Args:
            domains: List of domain names
            
        Returns:
            List of hint strings
        """
        hints = []
        
        for domain in domains:
            if domain == 'sales':
                hints.append("Consider joining orders with order_details and products for sales analysis")
                hints.append("Use SUM(quantity * unit_price) for revenue calculations")
            elif domain == 'customers':
                hints.append("Customer information is in the customers table")
                hints.append("Link customers to orders via customer_id")
            elif domain == 'products':
                hints.append("Products have categories and suppliers")
                hints.append("Check product inventory using units_in_stock")
            elif domain == 'employees':
                hints.append("Employees have territories and report to other employees")
                hints.append("Join employees with orders to see sales performance")
        
        return hints
    
    def suggest_aggregations(self, query: str) -> List[str]:
        """
        Suggest appropriate aggregation functions based on query
        
        Args:
            query: User's natural language query
            
        Returns:
            List of suggested aggregation functions
        """
        query_lower = query.lower()
        suggestions = []
        
        # Detect aggregation keywords
        if any(word in query_lower for word in ['total', 'sum', 'add up']):
            suggestions.append('SUM')
        
        if any(word in query_lower for word in ['count', 'number of', 'how many']):
            suggestions.append('COUNT')
        
        if any(word in query_lower for word in ['average', 'avg', 'mean']):
            suggestions.append('AVG')
        
        if any(word in query_lower for word in ['maximum', 'max', 'highest', 'largest']):
            suggestions.append('MAX')
        
        if any(word in query_lower for word in ['minimum', 'min', 'lowest', 'smallest']):
            suggestions.append('MIN')
        
        if suggestions:
            logger.debug(f"Suggested aggregations: {suggestions}")
        
        return suggestions
    
    def suggest_filters(self, query: str) -> Dict[str, Any]:
        """
        Suggest WHERE clause conditions based on query
        
        Args:
            query: User's natural language query
            
        Returns:
            Dictionary with filter suggestions
        """
        query_lower = query.lower()
        filters = {}
        
        # Time-based filters
        if 'this year' in query_lower or 'current year' in query_lower:
            filters['time_filter'] = 'EXTRACT(YEAR FROM order_date) = EXTRACT(YEAR FROM CURRENT_DATE)'
        elif 'last year' in query_lower:
            filters['time_filter'] = 'EXTRACT(YEAR FROM order_date) = EXTRACT(YEAR FROM CURRENT_DATE) - 1'
        elif 'this month' in query_lower:
            filters['time_filter'] = 'EXTRACT(MONTH FROM order_date) = EXTRACT(MONTH FROM CURRENT_DATE)'
        
        # Status filters
        if 'active' in query_lower:
            filters['status_filter'] = "discontinued = FALSE"
        elif 'discontinued' in query_lower:
            filters['status_filter'] = "discontinued = TRUE"
        
        # Top N
        if 'top' in query_lower:
            import re
            match = re.search(r'top\s+(\d+)', query_lower)
            if match:
                filters['limit'] = int(match.group(1))
        
        if filters:
            logger.debug(f"Suggested filters: {filters}")
        
        return filters
    
    def get_join_suggestions(self, tables: List[str], schema_info: Dict[str, Any]) -> List[Dict[str, str]]:
        """
        Suggest JOIN clauses for given tables
        
        Args:
            tables: List of table names
            schema_info: Database schema with relationship information
            
        Returns:
            List of join suggestions
        """
        suggestions = []
        relationships = schema_info.get('relationships', [])
        
        # Find relationships between the specified tables
        for rel in relationships:
            from_table = rel.get('from_table')
            to_table = rel.get('to_table')
            
            if from_table in tables and to_table in tables:
                suggestions.append({
                    'from_table': from_table,
                    'from_column': rel.get('from_column'),
                    'to_table': to_table,
                    'to_column': rel.get('to_column'),
                    'join_type': 'INNER JOIN'  # Default to INNER JOIN
                })
        
        if suggestions:
            logger.debug(f"Suggested {len(suggestions)} joins")
        
        return suggestions


# Global ontology mapper instance
ontology_mapper = BusinessOntologyMapper()


def get_ontology_mapper() -> BusinessOntologyMapper:
    """Get the global ontology mapper instance"""
    return ontology_mapper