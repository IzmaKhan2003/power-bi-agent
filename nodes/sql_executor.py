# nodes/sql_executor.py
from langchain_community.utilities import SQLDatabase
from dotenv import load_dotenv
import os

load_dotenv()

def sql_executor(state):
    sql_query = getattr(state, "sql_query", None) or (state.get("sql_query") if isinstance(state, dict) else None)
    if not sql_query:
        raise ValueError("❌ No SQL query provided for execution.")

    print(f"Executing SQL: {sql_query}")

    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise ValueError("DATABASE_URL not set in .env")

    try:
        db = SQLDatabase.from_uri(db_url)
        result = db.run(sql_query)
        # print("Query result:", result)
    except Exception as e:
        print(f"SQL execution failed: {e}")
        result = None

    return {
        "user_query": getattr(state, "user_query", None),
        "db_schema": getattr(state, "db_schema", None),
        "sql_plan": getattr(state, "sql_plan", None),
        "sql_query": sql_query,
        "query_result": result
    }
