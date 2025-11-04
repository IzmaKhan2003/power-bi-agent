from prettytable import PrettyTable
import re
import ast
from langchain_community.utilities import SQLDatabase
import os
from dotenv import load_dotenv

load_dotenv()

def output_formatter(state):
    print("💥 output_formatter node reached!")
    result = getattr(state, "query_result", None)
    sql_query = getattr(state, "sql_query", None)
    user_query = getattr(state, "user_query", "your question")

    if not result:
        print("❌ No results to display.")
        return state

    print("✨ Formatting query result...")

    # Handle stringified results
    if isinstance(result, str):
        try:
            result = ast.literal_eval(result)
            print("🪄 Converted stringified result back to list.")
        except Exception as e:
            print(f"⚠️ Failed to parse result string: {e}")
            return state

    db_url = os.getenv("DATABASE_URL")
    db = SQLDatabase.from_uri(db_url)

    # Extract column names
    columns = []
    if sql_query:
        try:
            match = re.search(r"from\s+([a-zA-Z_][a-zA-Z0-9_]*)", sql_query, re.IGNORECASE)
            if match:
                table_name = match.group(1)
                schema_info = db.run(
                    f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'"
                )
                columns = [col[0] for col in schema_info]

            if re.search(r"select\s+(.*?)\s+from", sql_query, re.IGNORECASE):
                cols = re.findall(r"select\s+(.*?)\s+from", sql_query, re.IGNORECASE | re.DOTALL)[0]
                if cols.strip() != "*":
                    columns = [c.strip().split()[-1].replace(";", "") for c in cols.split(",")]
        except Exception as e:
            print(f"⚠️ Failed to extract column names: {e}")

    if not columns and result:
        num_cols = len(result[0])
        columns = [f"Column {i+1}" for i in range(num_cols)]

    # 🧠 Detect if result is short enough for conversational reply
    num_rows = len(result)
    num_cols = len(result[0]) if num_rows > 0 else 0

    # Case 1: single scalar value → conversational response
    if num_rows == 1 and num_cols == 1:
        answer = str(result[0][0])
        q = user_query.strip()
        q_lower = q.lower()

        if "how many" in q_lower or "number of" in q_lower:
            answer_text = f"There are **{answer}** results based on your question."
        elif "who" in q_lower or "which" in q_lower or "what" in q_lower:
            answer_text = f"The answer is **{answer}**."
        elif "average" in q_lower or "total" in q_lower or "sum" in q_lower:
            answer_text = f"The calculated value is **{answer}**."
        else:
            answer_text = f"According to the data, the answer is **{answer}**."

        formatted_output = f"🧠 **Question:** {q}\n✅ **Answer:** {answer_text}"

    # Case 2: small list of results (≤5 rows, ≤3 columns)
    elif num_rows <= 5 and num_cols <= 3:
        rows = [" | ".join(map(str, r)) for r in result]
        formatted_output = (
            f"🧠 **Question:** {user_query}\n\nHere’s what I found:\n\n"
            + "\n".join([f"• {row}" for row in rows])
        )

    # Case 3: larger datasets → pretty table
    else:
        table = PrettyTable()
        table.field_names = columns
        for row in result:
            clean_row = [
                r.tobytes().decode("utf-8", "ignore") if isinstance(r, memoryview) else r
                for r in row
            ]
            table.add_row(clean_row)
        formatted_output = (
            f"🧠 **Question:** {user_query}\n\n✅ **Query Result (Formatted):**\n\n"
            + table.get_string()
        )

    print("\n✅ Query Result (Formatted):\n")
    print(formatted_output)

    return {
        "formatted_output": formatted_output,
        "query_result": result,
        "sql_query": sql_query,
    }
