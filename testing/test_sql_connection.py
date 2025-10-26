from langchain_community.utilities import SQLDatabase
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

db_url = os.getenv("DATABASE_URL")

# Connect to database
db = SQLDatabase.from_uri(db_url)

# Quick schema check
print("✅ Connected to DB")
print("Tables:", db.get_usable_table_names())
