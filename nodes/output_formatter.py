# nodes/output_formatter.py
import json

def output_formatter(state):
    # Extract safely whether it's a dict or a Pydantic model
    if hasattr(state, "dict"):
        state = state.dict()

    user_query = state.get("user_query")
    sql_query = state.get("sql_query")
    sql_plan = state.get("sql_plan")
    query_result = state.get("query_result")

    print("\n🪄 Formatting final output...\n")

    if query_result is None:
        message = "❌ No results were returned (or query failed)."
    elif isinstance(query_result, list):
        message = f"✅ Query returned {len(query_result)} row(s):\n" + json.dumps(query_result, indent=2)
    else:
        message = f"✅ Query Result: {query_result}"

    formatted_output = {
        "user_query": user_query,
        "sql_plan": sql_plan,
        "sql_query": sql_query,
        "query_result": query_result,
        "message": message
    }

    print(message)
    return formatted_output
