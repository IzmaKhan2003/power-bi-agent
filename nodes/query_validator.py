# nodes/query_validator.py
import re
from langchain_community.utilities import SQLDatabase

def query_validator(state):
    # Works whether state is dict or Pydantic model
    sql_query = getattr(state, "sql_query", None) or (state.get("sql_query") if isinstance(state, dict) else None)
    print(f"🔍 Validator received SQL: {sql_query}")

    if not sql_query:
        raise ValueError(f"No SQL query provided for validation. State received: {state}")

    print(f"🧹 Validating SQL: {sql_query}")

    # Basic safety rules
    forbidden = ["drop", "delete", "update", "insert"]
    if any(keyword in sql_query.lower() for keyword in forbidden):
        raise ValueError("❌ Unsafe SQL detected (DML/DLL not allowed).")

    if not sql_query.strip().lower().startswith("select"):
        raise ValueError("❌ Only SELECT queries are allowed.")

    # Validate referenced tables
    db = SQLDatabase.from_uri("postgresql+psycopg2://postgres:3118@localhost:5432/northwind")
    schema = db.get_table_info().lower()

    tables_in_query = re.findall(r"from\s+(\w+)", sql_query.lower())
    missing = [t for t in tables_in_query if t not in schema]
    if missing:
        raise ValueError(f"❌ Invalid table(s) referenced: {', '.join(missing)}")

    print("✅ SQL validation passed.")
    return {**state.dict()} if hasattr(state, "dict") else state
