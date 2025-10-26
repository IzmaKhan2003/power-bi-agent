from langgraph.graph import StateGraph
from pydantic import BaseModel
from nodes.user_input import user_input
from nodes.schema_inspector import schema_inspector
from nodes.planner import planner

class AgentState(BaseModel):
    user_query: str | None = None
    db_schema: list | None = None

builder = StateGraph(AgentState)

builder.add_node("user_input", user_input)
builder.add_node("schema_inspector", schema_inspector)
builder.add_node("planner", planner)

builder.add_edge("user_input", "schema_inspector")

builder.set_entry_point("user_input")

builder.add_edge("user_input", "schema_inspector")
builder.add_edge("schema_inspector", "planner")   
builder.set_finish_point("planner") 

app = builder.compile()
 

print("🤖 IntelliQuery (Stage 1) Started")

query = input("Enter your question: ")
input_state = {"user_query": query}
result = app.invoke(input_state)

# print("\n✅ Final Output:")
# if hasattr(result, "dict"):
#     result_data = result.dict()
# else:
#     result_data = result

# # Extract only the useful info
# useful_keys = ["user_query", "reasoning_plan", "sql_query"]
# filtered_result = {k: v for k, v in result_data.items() if k in useful_keys and v}

# for k, v in filtered_result.items():
#     print(f"{k}: {v}")