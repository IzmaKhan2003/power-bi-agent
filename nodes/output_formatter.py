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

    if not result:
        print("⚠️ No results to display.")
        return state

    print("✨ Formatting query result...")

    # Convert stringified result if necessary
    if isinstance(result, str):
        try:
            result = ast.literal_eval(result)
            print("🪄 Converted stringified result back to list.")
        except Exception as e:
            print(f"⚠️ Failed to parse result string: {e}")
            return state

    # Get DB URL from .env
    db_url = os.getenv("DATABASE_URL")
    db = SQLDatabase.from_uri(db_url)

    # Try to detect actual column names
    columns = []
    if sql_query:
        try:
            # Extract table name from query (after FROM)
            match = re.search(r"from\s+([a-zA-Z_][a-zA-Z0-9_]*)", sql_query, re.IGNORECASE)
            if match:
                table_name = match.group(1)
                schema_info = db.run(f"SELECT column_name FROM information_schema.columns WHERE table_name = '{table_name}'")
                # schema_info returns list of tuples like [('id',), ('name',), ...]
                columns = [col[0] for col in schema_info]
                print(f"🧩 Extracted actual columns from {table_name}: {columns}")

            # If query selects specific columns, override the full list
            if re.search(r"select\s+(.*?)\s+from", sql_query, re.IGNORECASE):
                cols = re.findall(r"select\s+(.*?)\s+from", sql_query, re.IGNORECASE | re.DOTALL)[0]
                if cols.strip() != "*":
                    columns = [c.strip().split()[-1].replace(";", "") for c in cols.split(",")]
        except Exception as e:
            print(f"⚠️ Failed to extract column names: {e}")

    if not columns:
        # Fallback if no columns were found
        num_cols = len(result[0]) if result else 0
        columns = [f"Column {i+1}" for i in range(num_cols)]

    # Build PrettyTable
    table = PrettyTable()
    table.field_names = columns

    for row in result:
        clean_row = [r.tobytes().decode('utf-8', 'ignore') if isinstance(r, memoryview) else r for r in row]
        table.add_row(clean_row)

    formatted_output = table.get_string()

    print("\n✅ Query Result (Formatted):\n")
    print(formatted_output)

    return {
        "formatted_output": formatted_output,
        "query_result": result,
        "sql_query": sql_query
    }
