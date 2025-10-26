from langgraph.graph import StateGraph
from pydantic import BaseModel
from nodes.user_input import user_input
from nodes.schema_inspector import schema_inspector


class AgentState(BaseModel):
    user_query: str | None = None
    db_schema: list | None = None

builder = StateGraph(AgentState)

builder.add_node("user_input", user_input)
builder.add_node("schema_inspector", schema_inspector)

builder.add_edge("user_input", "schema_inspector")

builder.set_entry_point("user_input")

app = builder.compile()

print("🤖 IntelliQuery (Stage 1) Started")

query = input("Enter your question: ")
input_state = {"user_query": query}
result = app.invoke(input_state)

print("\n✅ Final Output:")
print(result)
