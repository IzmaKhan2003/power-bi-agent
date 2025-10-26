def user_input(state):
    """
    First node: receives user query and validates it.
    """
    query = getattr(state, "user_query", None)
    
    if not query:
        raise ValueError(f"User query not found in state: {state}")

    print(f"🧠 Received user query: {query}")
    return {"user_query": query}
