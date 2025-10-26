# nodes/planner.py
import os
from langchain_google_genai import ChatGoogleGenerativeAI
from dotenv import load_dotenv

load_dotenv()

def planner(state):
    """Generate a reasoning plan + SQL query from schema + user question."""
    user_query = state.user_query
    schema = state.db_schema

    if not user_query or not schema:
        raise ValueError("Planner requires both user_query and db_schema.")

    llm = ChatGoogleGenerativeAI(
        model=os.getenv("GEMINI_MODEL", "gemini-2.0-flash"),
        google_api_key=os.getenv("GEMINI_API_KEY")
    )

    prompt = f"""
    You are a data analyst assistant.  
    The database schema is:

    {schema}

    The user question is:
    "{user_query}"

    Step 1 – Plan the reasoning steps to answer the query.  
    Step 2 – Generate a valid SQL query for PostgreSQL (Northwind schema).  
    Respond in JSON with keys:
    {{
      "plan": "text",
      "sql": "SELECT ..."
    }}
    """

    response = llm.invoke(prompt)
    text = response.content[0].text if hasattr(response, "content") else str(response)

    return {"reasoning_plan": text, "sql_query": text}
