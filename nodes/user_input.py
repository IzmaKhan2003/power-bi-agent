# nodes/user_input.py
from datetime import datetime
import uuid
from typing import Dict, Any

def user_input(state: Dict[str, Any]) -> Dict[str, Any]:
    """
    LangGraph node: Normalize and return user_input + session metadata.
    Works for both {'input': {'user_query': '...'}} and {'user_query': '...'} formats.
    """

    # Ensure state is a dict
    # Convert the incoming Pydantic state (AgentState) to a dict safely
    if hasattr(state, "dict"):
        incoming = state.dict()
    elif isinstance(state, dict):
        incoming = state
    else:
        incoming = {}
    print(f"user_input received state")


    # Detect user query from either 'input' or top-level
    if isinstance(incoming.get("input"), dict):
        user_query = incoming["input"].get("user_query") or incoming["input"].get("query")
    else:
        user_query = incoming.get("user_query") or incoming.get("query")

    # If missing, prompt user interactively (important for continuous CLI chat)
    if not user_query:
        user_query = input("Enter your question: ").strip()

    # If still missing after prompt, throw an error
    if not user_query:
        raise ValueError("❌ No user query provided to user_input node.")

    # Create or reuse session id
    session_id = incoming.get("session_id") or str(uuid.uuid4())

    # Add metadata for traceability
    metadata = {
        "received_at": datetime.utcnow().isoformat() + "Z",
        "session_id": session_id,
        "input_method": incoming.get("input_method", "cli"),
    }

    # Preserve conversation history if exists
    conversation_history = incoming.get("conversation_history", [])
    conversation_history.append({"role": "user", "content": user_query})

    print(f"🧠 Received user query: {user_query}")

    return {
        "user_query": user_query,
        "session_id": session_id,
        "metadata": metadata,
        "conversation_history": conversation_history
    }
