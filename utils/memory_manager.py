"""
Memory management for conversational context and session state
"""
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from typing import Dict, List, Any, Optional
from datetime import datetime
from dataclasses import dataclass, field, asdict
import json
from utils.logger import get_logger

logger = get_logger("MemoryManager")


@dataclass
class ConversationTurn:
    """Represents a single turn in the conversation"""
    timestamp: str
    user_query: str
    sql_query: Optional[str] = None
    results_count: Optional[int] = None
    success: bool = True
    error_message: Optional[str] = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return asdict(self)


@dataclass
class SessionMemory:
    """Stores all information for a conversation session"""
    session_id: str
    started_at: str
    conversation_history: List[ConversationTurn] = field(default_factory=list)
    schema_cache: Optional[Dict[str, Any]] = None
    last_query: Optional[str] = None
    last_sql: Optional[str] = None
    last_results: Optional[List[Dict[str, Any]]] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def add_turn(
        self,
        user_query: str,
        sql_query: Optional[str] = None,
        results_count: Optional[int] = None,
        success: bool = True,
        error_message: Optional[str] = None
    ):
        """Add a conversation turn to history"""
        turn = ConversationTurn(
            timestamp=datetime.now().isoformat(),
            user_query=user_query,
            sql_query=sql_query,
            results_count=results_count,
            success=success,
            error_message=error_message
        )
        self.conversation_history.append(turn)
        
        # Update last query/sql tracking
        if success:
            self.last_query = user_query
            self.last_sql = sql_query
    
    def get_recent_context(self, n: int = 3) -> List[ConversationTurn]:
        """Get the last N conversation turns"""
        return self.conversation_history[-n:]
    
    def get_conversation_summary(self) -> str:
        """Generate a summary of the conversation"""
        summary_parts = []
        
        for i, turn in enumerate(self.conversation_history[-5:], 1):
            status = "✓" if turn.success else "✗"
            summary_parts.append(
                f"{status} Turn {i}: {turn.user_query[:50]}..."
            )
        
        return "\n".join(summary_parts)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary"""
        return {
            "session_id": self.session_id,
            "started_at": self.started_at,
            "conversation_history": [turn.to_dict() for turn in self.conversation_history],
            "schema_cache": self.schema_cache,
            "last_query": self.last_query,
            "last_sql": self.last_sql,
            "metadata": self.metadata
        }
    
    def save_to_file(self, filepath: str):
        """Save session to JSON file"""
        try:
            with open(filepath, 'w') as f:
                json.dump(self.to_dict(), f, indent=2)
            logger.info(f"Session saved to {filepath}")
        except Exception as e:
            logger.error(f"Failed to save session: {str(e)}")


class MemoryManager:
    """Manages conversation memory and context across sessions"""
    
    def __init__(self):
        self.sessions: Dict[str, SessionMemory] = {}
        self.current_session_id: Optional[str] = None
        
    def create_session(self, session_id: Optional[str] = None) -> str:
        """
        Create a new conversation session
        
        Args:
            session_id: Optional session ID, generates one if not provided
            
        Returns:
            Session ID
        """
        if not session_id:
            session_id = f"session_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        session = SessionMemory(
            session_id=session_id,
            started_at=datetime.now().isoformat()
        )
        
        self.sessions[session_id] = session
        self.current_session_id = session_id
        
        logger.info(f"Created new session: {session_id}")
        return session_id
    
    def get_session(self, session_id: Optional[str] = None) -> Optional[SessionMemory]:
        """
        Get a session by ID or the current session
        
        Args:
            session_id: Optional session ID, uses current if not provided
            
        Returns:
            SessionMemory object or None
        """
        sid = session_id or self.current_session_id
        return self.sessions.get(sid)
    
    def update_session(
        self,
        user_query: str,
        sql_query: Optional[str] = None,
        results: Optional[List[Dict[str, Any]]] = None,
        success: bool = True,
        error_message: Optional[str] = None,
        session_id: Optional[str] = None
    ):
        """
        Update session with new conversation turn
        
        Args:
            user_query: User's natural language query
            sql_query: Generated SQL query
            results: Query results
            success: Whether the turn was successful
            error_message: Error message if failed
            session_id: Optional session ID
        """
        session = self.get_session(session_id)
        if not session:
            logger.warning("No active session found, creating new one")
            self.create_session()
            session = self.get_session()
        
        results_count = len(results) if results else None
        session.add_turn(
            user_query=user_query,
            sql_query=sql_query,
            results_count=results_count,
            success=success,
            error_message=error_message
        )
        
        # Store results for potential follow-up questions
        if success and results:
            session.last_results = results
        
        logger.debug(f"Updated session with new turn: {user_query[:50]}...")
    
    def get_context_for_query(
        self,
        session_id: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Get relevant context for query planning
        
        Args:
            session_id: Optional session ID
            
        Returns:
            Dictionary with context information
        """
        session = self.get_session(session_id)
        if not session:
            return {}
        
        recent_turns = session.get_recent_context(n=3)
        
        context = {
            "has_previous_queries": len(recent_turns) > 0,
            "last_query": session.last_query,
            "last_sql": session.last_sql,
            "recent_queries": [turn.user_query for turn in recent_turns],
            "conversation_length": len(session.conversation_history)
        }
        
        return context
    
    def resolve_pronoun_references(
        self,
        query: str,
        session_id: Optional[str] = None
    ) -> str:
        """
        Resolve pronouns and references in user query using conversation context
        
        Args:
            query: User's query potentially with pronouns
            session_id: Optional session ID
            
        Returns:
            Query with pronouns resolved
        """
        session = self.get_session(session_id)
        if not session or not session.last_query:
            return query
        
        query_lower = query.lower()
        
        # Check for reference words
        reference_words = ['it', 'that', 'this', 'them', 'those', 'these', 'same']
        has_reference = any(word in query_lower.split() for word in reference_words)
        
        if has_reference and session.last_query:
            # Simple reference resolution - append context
            resolved = f"{query} (referring to previous query: '{session.last_query}')"
            logger.debug(f"Resolved pronoun reference: {query} -> {resolved}")
            return resolved
        
        return query
    
    def clear_session(self, session_id: Optional[str] = None):
        """Clear a session from memory"""
        sid = session_id or self.current_session_id
        if sid in self.sessions:
            del self.sessions[sid]
            logger.info(f"Cleared session: {sid}")
            
            if sid == self.current_session_id:
                self.current_session_id = None
    
    def get_session_summary(self, session_id: Optional[str] = None) -> str:
        """Get a summary of the session"""
        session = self.get_session(session_id)
        if not session:
            return "No active session"
        
        total_turns = len(session.conversation_history)
        successful_turns = sum(1 for turn in session.conversation_history if turn.success)
        
        summary = f"""
Session Summary:
- Session ID: {session.session_id}
- Started: {session.started_at}
- Total queries: {total_turns}
- Successful: {successful_turns}
- Failed: {total_turns - successful_turns}

Recent conversation:
{session.get_conversation_summary()}
        """
        
        return summary.strip()


# Global memory manager instance
memory_manager = MemoryManager()


def get_memory_manager() -> MemoryManager:
    """Get the global memory manager instance"""
    return memory_manager