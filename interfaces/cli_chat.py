# interfaces/cli_chat.py
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from graphs.nlp_to_sql_graph import app  # assumes you move/compile graph there
import os

def run_cli():
    print("🤖 IntelliQuery Chat (type 'exit' to quit)")
    session_id = None
    while True:
        query = input("\nEnter your question: ").strip()
        if not query:
            continue
        # pass input in the shape our user_input node accepts
        input_state = {"input": {"user_query": query, "input_method": "cli", "session_id": session_id}}
        # invoke graph — it will pass through user_input -> context_manager -> ...
        try:
            response = app.invoke(input_state)
        except Exception as e:
            print("❌ Agent error:", e)
            # continue loop rather than exit
            continue

        # Check if context_manager set should_exit
        should_exit = False
        if isinstance(response, dict):
            should_exit = response.get("should_exit", False)
            session_id = response.get("session_id", session_id)

        # Example: if output_formatter returns 'formatted_output' print it
        if isinstance(response, dict) and response.get("formatted_output"):
            print("\n" + response["formatted_output"])
        else:
            print("\nAgent response:", response)

        if should_exit:
            print("Goodbye 👋")
            break

if __name__ == "__main__":
    run_cli()
