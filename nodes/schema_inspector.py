import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()

def schema_inspector(state):
    try:
        conn = psycopg2.connect(os.getenv("DATABASE_URL"))
        cur = conn.cursor()
        cur.execute("""
            SELECT table_name, column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'public';
        """)
        schema_info = cur.fetchall()
        cur.close()
        conn.close()

        print(f"📘 Found {len(schema_info)} columns in database.")
        return {"db_schema": schema_info}

    except Exception as e:
        print(f"❌ Schema inspector failed: {e}")
        return {"db_schema": None}
