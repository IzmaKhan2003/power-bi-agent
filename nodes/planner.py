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
    You are a data analyst assistant.  
    Database schema:
    {schema}

    The user question:
    "{user_query}"

    Step 1 – Create reasoning steps for how you'd find this answer.  
    Step 2 – Generate a valid SQL query for PostgreSQL (Northwind schema).

    Return JSON like:
    {{
      "plan": "your reasoning here",
      "sql": "SELECT ..."
    }}
    """

    response = llm.invoke(prompt)

    if hasattr(response, "content") and isinstance(response.content, list):
        text = response.content[0].text
    elif hasattr(response, "text"):
        text = response.text
    else:
        text = str(response)


    try:
        json_text = re.search(r"\{.*\}", text, re.DOTALL).group(0)
        parsed = json.loads(json_text)
    except Exception:
        parsed = {"plan": "Could not parse reasoning.", "sql": text[:200]}

    print("✅ SQL Plan:", parsed.get("plan"))
    print("🧩 SQL Query:", parsed.get("sql"))

    return {"reasoning_plan": parsed.get("plan"), "sql_query": parsed.get("sql")}

