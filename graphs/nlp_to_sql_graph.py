import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))


from langgraph.graph import StateGraph
from pydantic import BaseModel

from nodes.user_input import user_input
from nodes.context_manager import context_manager
# from nodes.schema_inspector import schema_inspector
# from nodes.planner import planner
# from nodes.query_validator import query_validator
# from nodes.sql_executor import sql_executor
# from nodes.output_formatter import output_formatter

from typing import Optional, List, Any


class AgentState(BaseModel):
    user_query: Optional[str] = None
    db_schema: Optional[List[Any]] = None
    sql_plan: Optional[str] = None
    sql_query: Optional[str] = None
    query_result: str | None = None

builder = StateGraph(AgentState)

builder.add_node("user_input", user_input)
builder.add_node("context_manager", context_manager)
# builder.add_node("schema_inspector", schema_inspector)
# builder.add_node("planner", planner)
# builder.add_node("query_validator", query_validator)
# builder.add_node("sql_executor", sql_executor)
# builder.add_node("output_formatter", output_formatter)

builder.set_entry_point("user_input")

builder.add_edge("user_input", "context_manager")
# builder.add_edge("context_manager", "schema_inspector")
# builder.add_edge("schema_inspector", "planner")   
# builder.add_edge("planner", "query_validator")
# builder.add_edge("query_validator", "sql_executor")
# builder.add_edge("sql_executor", "output_formatter")

builder.set_finish_point("context_manager")

app = builder.compile()
 

print("IntelliQuery (Stage 1) Started")

# query = input("Enter your question: ")
# input_state = {"input": {"user_query": query}}
# result = app.invoke(input_state)


