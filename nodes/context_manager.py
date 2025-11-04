# nodes/context_manager.py
from typing import Dict, Any, List
import os

# Simple in-memory dictionary for session->history
# For FYP you can replace this with a file, Redis, or DB-backed memory_manager.
_SESSION_STORE: Dict[str, List[Dict[str, Any]]] = {}

# configurable window size
HISTORY_WINDOW = int(os.getenv("CONTEXT_WINDOW", "6"))

def context_manager(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    Manage simple session memory. Appends current user query to session history,
    returns a trimmed context summary that other nodes (planner) can use.
    """
    session_id = state.get("session_id")
    user_query = state.get("user_query")

    if not session_id or not user_query:
        raise ValueError("context_manager expects 'session_id' and 'user_query' in state.")

    # initialize session history
    if session_id not in _SESSION_STORE:
        _SESSION_STORE[session_id] = []

    # add this turn to history
    _SESSION_STORE[session_id].append({
        "role": "user",
        "text": user_query,
        "ts": state.get("metadata", {}).get("received_at")
    })

    # optionally, keep assistant replies in history as they are produced (other node will add)
    # Trim to last N turns (both user+assistant)
    history = _SESSION_STORE[session_id][-HISTORY_WINDOW:]

    # Build a compact context summary for planner/prompt injection
    context_lines = []
    for turn in history:
        role = turn.get("role", "user")
        text = turn.get("text", "")
        context_lines.append(f"{role}: {text}")

    context_summary = "\n".join(context_lines)

    # Add helper flags
    q_lower = user_query.lower()
    should_exit = any(token in q_lower for token in ["exit", "quit", "goodbye", "stop", "bye"])

    out = {
        **state,
        "conversation_history": history,
        "context_summary": context_summary,
        "should_exit": should_exit
    }

    print(f"🗂️ Session {session_id} history length: {len(_SESSION_STORE[session_id])}")
    if should_exit:
        print("🚪 User requested exit (should_exit=True).")

    return out
