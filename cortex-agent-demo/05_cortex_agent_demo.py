"""
Step 5 — End-to-end Cortex Agent Demo
======================================
Demonstrates a Snowflake Cortex Agent that orchestrates two tools:

  1. CortexAnalyst  — natural-language-to-SQL over the TPC-H semantic model
  2. CortexSearch   — RAG over business knowledge documents

The agent is called via the Snowflake REST API using SSE streaming.

Usage:
    python 05_cortex_agent_demo.py

Environment variables required:
    SNOWFLAKE_ACCOUNT    e.g. xy12345.us-east-1
    SNOWFLAKE_USER
    SNOWFLAKE_PASSWORD
    SNOWFLAKE_ROLE       (optional, defaults to SYSADMIN)

Dependencies:
    pip install snowflake-connector-python requests
"""

import json
import os
import sys
import time
from typing import Generator

import requests
import snowflake.connector

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------
SNOWFLAKE_ACCOUNT  = os.environ["SNOWFLAKE_ACCOUNT"]
SNOWFLAKE_USER     = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_ROLE     = os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN")

DATABASE  = "CORTEX_AGENT_DEMO"
SCHEMA    = "TPCH"
WAREHOUSE = "CORTEX_DEMO_WH"

# Cortex services
SEMANTIC_MODEL_STAGE = f"@{DATABASE}.{SCHEMA}.SEMANTIC_MODELS/02_semantic_model.yaml"
SEARCH_SERVICE       = f"{DATABASE}.{SCHEMA}.BUSINESS_KNOWLEDGE_SEARCH"

# Cortex Agent REST endpoint
# Format: https://<account>.snowflakecomputing.com/api/v2/cortex/agent:run
AGENT_ENDPOINT = (
    f"https://{SNOWFLAKE_ACCOUNT}.snowflakecomputing.com"
    "/api/v2/cortex/agent:run"
)


# ---------------------------------------------------------------------------
# Demo questions
# ---------------------------------------------------------------------------
DEMO_QUESTIONS = [
    # Analyst questions (answered via SQL on TPC-H)
    "What is the total net revenue for each year?",
    "Which are the top 5 customer market segments by revenue?",
    "Compare revenue across shipping modes — which mode generates the most?",
    "What is the revenue breakdown by geographic region?",
    "Show me the number of orders and total order value per order priority level.",
    # Knowledge questions (answered via Cortex Search RAG)
    "How is net revenue calculated in our data model?",
    "What do the order status codes F, O, and P mean?",
    "Which customer segments typically have the highest order values?",
    # Mixed — needs both tools
    "What is the revenue formula and which region is performing best?",
]


# ---------------------------------------------------------------------------
# Authentication helpers
# ---------------------------------------------------------------------------

def get_snowflake_token() -> str:
    """Exchange Snowflake username+password for a short-lived JWT/session token."""
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


# ---------------------------------------------------------------------------
# Cortex Agent API call
# ---------------------------------------------------------------------------

def build_agent_request(question: str) -> dict:
    """Build the request body for the Cortex Agent REST API."""
    return {
        "model": "claude-3-5-sonnet",   # LLM backing the agent
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": question,
                    }
                ],
            }
        ],
        "tools": [
            # ---- Tool 1: Cortex Analyst (NL → SQL) -------------------------
            {
                "tool_spec": {
                    "type": "cortex_analyst_text_to_sql",
                    "name": "tpch_analyst",
                },
                "tool_resources": {
                    "semantic_model_file": SEMANTIC_MODEL_STAGE,
                },
            },
            # ---- Tool 2: Cortex Search (vector RAG) ------------------------
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
        "tool_choice": "auto",   # let the agent decide which tool(s) to use
    }


def stream_agent_response(question: str, token: str) -> Generator[dict, None, None]:
    """Send a question to the Cortex Agent and yield SSE events as dicts."""
    headers = {
        "Authorization": f'Snowflake Token="{token}"',
        "Content-Type":  "application/json",
        "Accept":        "text/event-stream",
        "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
    }

    payload = build_agent_request(question)

    with requests.post(
        AGENT_ENDPOINT,
        headers=headers,
        json=payload,
        stream=True,
        timeout=120,
    ) as resp:
        resp.raise_for_status()

        buffer = ""
        for raw_line in resp.iter_lines(decode_unicode=True):
            if not raw_line:
                continue
            if raw_line.startswith("data: "):
                data_str = raw_line[6:].strip()
                if data_str == "[DONE]":
                    break
                try:
                    yield json.loads(data_str)
                except json.JSONDecodeError:
                    pass


# ---------------------------------------------------------------------------
# Response parsing
# ---------------------------------------------------------------------------

def parse_agent_response(events: list[dict]) -> dict:
    """
    Walk through the SSE event stream and extract:
      - final text answer
      - SQL queries generated (if any)
      - search results used (if any)
      - tool calls made
    """
    result = {
        "answer": "",
        "sql_queries": [],
        "search_results": [],
        "tool_calls": [],
    }

    for event in events:
        event_type = event.get("event", "")
        data       = event.get("data", {})

        if event_type == "content.delta":
            delta = data.get("delta", {})
            if delta.get("type") == "text":
                result["answer"] += delta.get("text", "")

        elif event_type == "tool_call":
            tool_name   = data.get("tool_use", {}).get("name", "")
            tool_input  = data.get("tool_use", {}).get("input", {})
            result["tool_calls"].append({"tool": tool_name, "input": tool_input})

            if tool_name == "tpch_analyst":
                sql = tool_input.get("query", "")
                if sql:
                    result["sql_queries"].append(sql)

            elif tool_name == "business_knowledge_search":
                results = tool_input.get("results", [])
                result["search_results"].extend(results)

        # Some versions surface SQL in tool_result events
        elif event_type == "tool_result":
            tool_name   = data.get("name", "")
            tool_content = data.get("content", [])
            if tool_name == "tpch_analyst":
                for item in tool_content:
                    if isinstance(item, dict) and item.get("type") == "text":
                        result["sql_queries"].append(item.get("text", ""))

    return result


# ---------------------------------------------------------------------------
# Pretty printer
# ---------------------------------------------------------------------------

SEPARATOR = "=" * 72

def print_demo_result(question: str, result: dict, elapsed: float):
    print(f"\n{SEPARATOR}")
    print(f"  QUESTION: {question}")
    print(SEPARATOR)

    if result["tool_calls"]:
        print(f"\n  Tools used: {', '.join(tc['tool'] for tc in result['tool_calls'])}")

    if result["sql_queries"]:
        print("\n  Generated SQL:")
        for sql in result["sql_queries"]:
            # Indent the SQL nicely
            for line in sql.strip().splitlines():
                print(f"    {line}")

    print(f"\n  Answer:\n")
    for line in result["answer"].strip().splitlines():
        print(f"    {line}")

    print(f"\n  [{elapsed:.1f}s]")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    print("Snowflake Cortex Agent Demo — TPC-H Sales & Operations")
    print(SEPARATOR)

    # Get auth token once (valid for ~10 minutes)
    print("Authenticating with Snowflake ...")
    try:
        token = get_snowflake_token()
    except Exception as exc:
        print(f"ERROR: Could not authenticate: {exc}")
        sys.exit(1)
    print("Authentication successful.\n")

    # Run through demo questions
    for i, question in enumerate(DEMO_QUESTIONS, 1):
        print(f"\n[{i}/{len(DEMO_QUESTIONS)}] Asking: {question}")
        start = time.time()

        try:
            events = list(stream_agent_response(question, token))
            result = parse_agent_response(events)
            elapsed = time.time() - start
            print_demo_result(question, result, elapsed)

        except requests.HTTPError as exc:
            print(f"  HTTP ERROR {exc.response.status_code}: {exc.response.text[:500]}")
        except Exception as exc:
            print(f"  ERROR: {exc}")

        # Small pause between requests
        time.sleep(1)

    print(f"\n{SEPARATOR}")
    print("Demo complete.")


if __name__ == "__main__":
    main()
