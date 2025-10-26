from langchain_google_genai import ChatGoogleGenerativeAI
from langchain_community.utilities import SQLDatabase
from langchain_community.agent_toolkits import create_sql_agent, SQLDatabaseToolkit
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Connect to PostgreSQL (Northwind)
db = SQLDatabase.from_uri(os.getenv("DATABASE_URL"))

# Initialize Gemini model
llm = ChatGoogleGenerativeAI(
    model=os.getenv("GEMINI_MODEL", "gemini-2.0-flash"),
    google_api_key=os.getenv("GEMINI_API_KEY"),
    temperature=0
)

# Create SQL toolkit and agent
toolkit = SQLDatabaseToolkit(db=db, llm=llm)
agent = create_sql_agent(llm=llm, toolkit=toolkit, verbose=True)

# Run a test query
question = "Which product has the highest unit price?"
response = agent.run(question)

print("💬 Question:", question)
print("🧾 Answer:", response)
