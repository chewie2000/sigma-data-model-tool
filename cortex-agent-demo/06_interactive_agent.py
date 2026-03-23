"""
Step 6 — Interactive Cortex Agent REPL
=======================================
Ask free-form questions to the Cortex Agent from your terminal.
Useful for live demos or ad-hoc exploration.

Usage:
    python 06_interactive_agent.py

Same env vars as 05_cortex_agent_demo.py.
Type 'exit' or 'quit' to stop.
"""

import json
import os
import sys
import time

import requests
import snowflake.connector

SNOWFLAKE_ACCOUNT  = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_USER     = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_ROLE     = os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN")

DATABASE  = "CORTEX_AGENT_DEMO"
SCHEMA    = "TPCH"
WAREHOUSE = "CORTEX_DEMO_WH"

SEMANTIC_MODEL_STAGE = f"@{DATABASE}.{SCHEMA}.SEMANTIC_MODELS/02_semantic_model.yaml"
SEARCH_SERVICE       = f"{DATABASE}.{SCHEMA}.BUSINESS_KNOWLEDGE_SEARCH"
AGENT_ENDPOINT = (
    f"https://{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com"
    "/api/v2/cortex/agent:run"
)

# Conversation history (multi-turn support)
conversation_history: list[dict] = []


def get_snowflake_token() -> str:
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        role=SNOWFLAKE_ROLE,
        database=DATABASE,
        schema=SCHEMA,
        warehouse=WAREHOUSE,
    )
    token = conn.rest.token
    conn.close()
    return token


def ask_agent(question: str, token: str) -> tuple[str, list[str], list[str]]:
    """
    Send a question (with conversation history) to the agent.
    Returns (answer_text, sql_list, tools_used).
    """
    # Append the new user turn
    conversation_history.append({
        "role": "user",
        "content": [{"type": "text", "text": question}],
    })

    payload = {
        "model": "claude-3-5-sonnet",
        "messages": conversation_history,
        "tools": [
            {
                "tool_spec": {
                    "type": "cortex_analyst_text_to_sql",
                    "name": "tpch_analyst",
                },
                "tool_resources": {
                    "semantic_model_file": SEMANTIC_MODEL_STAGE,
                },
            },
            {
                "tool_spec": {
                    "type": "cortex_search",
                    "name": "business_knowledge_search",
                },
                "tool_resources": {
                    "name": SEARCH_SERVICE,
                    "max_results": 3,
                },
            },
        ],
        "tool_choice": "auto",
    }

    headers = {
        "Authorization": f'Snowflake Token="{token}"',
        "Content-Type":  "application/json",
        "Accept":        "text/event-stream",
        "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
    }

    answer   = ""
    sql_list = []
    tools_used = []

    with requests.post(
        AGENT_ENDPOINT,
        headers=headers,
        json=payload,
        stream=True,
        timeout=120,
    ) as resp:
        resp.raise_for_status()

        for raw_line in resp.iter_lines(decode_unicode=True):
            if not raw_line or not raw_line.startswith("data: "):
                continue
            data_str = raw_line[6:].strip()
            if data_str == "[DONE]":
                break
            try:
                event = json.loads(data_str)
            except json.JSONDecodeError:
                continue

            etype = event.get("event", "")
            data  = event.get("data", {})

            if etype == "content.delta":
                delta = data.get("delta", {})
                if delta.get("type") == "text":
                    chunk = delta.get("text", "")
                    answer += chunk
                    print(chunk, end="", flush=True)

            elif etype == "tool_call":
                tool_name  = data.get("tool_use", {}).get("name", "")
                tool_input = data.get("tool_use", {}).get("input", {})
                if tool_name not in tools_used:
                    tools_used.append(tool_name)
                if tool_name == "tpch_analyst":
                    sql = tool_input.get("query", "")
                    if sql:
                        sql_list.append(sql)

            elif etype == "tool_result":
                tool_name    = data.get("name", "")
                tool_content = data.get("content", [])
                if tool_name == "tpch_analyst":
                    for item in tool_content:
                        if isinstance(item, dict) and item.get("type") == "text":
                            sql_list.append(item.get("text", ""))

    print()  # newline after streamed answer

    # Store assistant turn in history for multi-turn context
    if answer:
        conversation_history.append({
            "role": "assistant",
            "content": [{"type": "text", "text": answer}],
        })

    return answer, sql_list, tools_used


def main():
    print("=" * 60)
    print(" Snowflake Cortex Agent — Interactive Demo")
    print(" Data: TPC-H via SNOWFLAKE_SAMPLE_DATA")
    print("=" * 60)
    print("Type your question and press Enter.")
    print("Type 'clear' to reset conversation history.")
    print("Type 'exit' or 'quit' to stop.\n")

    print("Authenticating ...")
    try:
        token = get_snowflake_token()
        token_acquired = time.time()
    except Exception as exc:
        print(f"Authentication failed: {exc}")
        sys.exit(1)
    print("Ready.\n")

    while True:
        try:
            question = input("You: ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nGoodbye.")
            break

        if not question:
            continue

        if question.lower() in ("exit", "quit"):
            print("Goodbye.")
            break

        if question.lower() == "clear":
            conversation_history.clear()
            print("Conversation history cleared.\n")
            continue

        # Refresh token every 9 minutes
        if time.time() - token_acquired > 540:
            try:
                token = get_snowflake_token()
                token_acquired = time.time()
            except Exception:
                pass  # keep old token if refresh fails

        print("\nAgent: ", end="", flush=True)
        start = time.time()

        try:
            answer, sql_list, tools_used = ask_agent(question, token)
        except requests.HTTPError as exc:
            print(f"\nHTTP ERROR {exc.response.status_code}: {exc.response.text[:400]}")
            continue
        except Exception as exc:
            print(f"\nERROR: {exc}")
            continue

        elapsed = time.time() - start

        if tools_used:
            print(f"\n  [Tools: {', '.join(tools_used)} | {elapsed:.1f}s]")

        if sql_list:
            print("\n  Generated SQL:")
            for sql in sql_list:
                for line in sql.strip().splitlines():
                    print(f"    {line}")

        print()


if __name__ == "__main__":
    main()
