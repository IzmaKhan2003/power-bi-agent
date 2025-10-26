# nodes/planner.py
import os
from langchain_google_genai import ChatGoogleGenerativeAI
from dotenv import load_dotenv
import json, re

load_dotenv()

def planner(state):
    """Generate reasoning plan + SQL query from schema and question."""
    user_query = state.user_query
    schema = state.db_schema

    if not user_query or not schema:
        raise ValueError("Planner requires both user_query and db_schema.")

    print("🪄 Planner: generating SQL plan and query...")

    llm = ChatGoogleGenerativeAI(
        model=os.getenv("GEMINI_MODEL", "gemini-2.0-flash"),
        google_api_key=os.getenv("GEMINI_API_KEY")
    )

    prompt = f"""
You are a data planning assistant.
Schema (list of tables and columns): {state.db_schema}
User question: {state.user_query}

Generate a JSON object with:
- "plan": short explanation of what steps you will take
- "sql": SQL query to answer the question
"""

    response = llm.invoke(prompt)
    text = response if isinstance(response, str) else getattr(response, "text", str(response))
    text = text.replace("```json", "").replace("```", "").strip()

    try:
        parsed = json.loads(text)
        plan = parsed.get("plan")
        sql_query = parsed.get("sql")
    except:
        plan, sql_query = "Failed to parse", text

    print(f"✅ SQL Plan: {plan}")
    print(f"🧩 SQL Query: {sql_query}")

    return {
        "user_query": state.user_query,
        "db_schema": state.db_schema,
        "sql_plan": plan,
        "sql_query": sql_query
    }
