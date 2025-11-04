from langchain_community.utilities import SQLDatabase
from dotenv import load_dotenv

# Load environment variables
load_dotenv()
import os
DATABASE_URL = os.getenv("DATABASE_URL")

# Connect using SQLAlchemy URL
db = SQLDatabase.from_uri(
    DATABASE_URL
)
# Test connection
print("Tables:", db.get_usable_table_names())
