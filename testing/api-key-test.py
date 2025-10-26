import os
import requests
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("GEMINI_API_KEY")
url = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key={API_KEY}"

payload = {
    "contents": [{"parts": [{"text": "Hello from Gemini!"}]}]
}

response = requests.post(url, json=payload)
print(response.status_code)
print(response.text)
