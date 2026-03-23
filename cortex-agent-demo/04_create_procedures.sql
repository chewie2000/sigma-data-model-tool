-- =============================================================================
-- STEP 4: Create Stored Procedures for Cortex Analyst & Cortex Agent
--
-- Both procedures run inside Snowflake (Snowpark Python) and call the
-- Cortex REST APIs using the session's own auth token — no external
-- credentials needed.
--
-- Requires ACCOUNTADMIN (or equivalent) to create the network rule and
-- external access integration.  Run once, then any role with USAGE on the
-- procedures can call them.
-- =============================================================================

USE DATABASE MARKO;
USE SCHEMA   ANALYTICS;
USE WAREHOUSE CORTEX_DEMO_WH;

-- ---------------------------------------------------------------------------
-- 4a. Network Rule — allow egress to this account's Snowflake endpoint
--     (needed so the stored procedure can call the REST API)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE NETWORK RULE CORTEX_REST_RULE
    TYPE       = HOST_PORT
    MODE       = EGRESS
    VALUE_LIST = (
        -- <account_locator>.snowflakecomputing.com  resolved at runtime
        'snowflakecomputing.com'
    )
    COMMENT = 'Allows outbound HTTPS to Snowflake Cortex REST APIs';

-- ---------------------------------------------------------------------------
-- 4b. External Access Integration
-- ---------------------------------------------------------------------------

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION CORTEX_REST_INTEGRATION
    ALLOWED_NETWORK_RULES = (MARKO.ANALYTICS.CORTEX_REST_RULE)
    ENABLED = TRUE
    COMMENT = 'External access for Cortex Analyst and Cortex Agent REST APIs';

-- ---------------------------------------------------------------------------
-- 4c. Helper: SEARCH_KNOWLEDGE
--     Wraps SNOWFLAKE.CORTEX.SEARCH_PREVIEW — pure SQL, no HTTP needed.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE SEARCH_KNOWLEDGE(
    QUERY       VARCHAR,
    MAX_RESULTS INT DEFAULT 3
)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python')
HANDLER = 'run'
AS $$
import json

def run(session, query: str, max_results: int = 3):
    query_obj = json.dumps({
        "query":   query,
        "columns": ["DOC_ID", "CATEGORY", "TITLE", "CONTENT"],
        "limit":   max_results,
    })
    # Escape any single quotes in the JSON before embedding in SQL
    safe = query_obj.replace("'", "''")
    row = session.sql(
        f"SELECT PARSE_JSON(SNOWFLAKE.CORTEX.SEARCH_PREVIEW("
        f"'MARKO.ANALYTICS.BUSINESS_KNOWLEDGE_SEARCH', '{safe}')):results AS results"
    ).collect()[0]
    return row['RESULTS']
$$;


-- ---------------------------------------------------------------------------
-- 4d. ASK_CORTEX_ANALYST
--     Sends a natural-language question to Cortex Analyst (NL→SQL) and
--     returns the generated SQL + answer text as a VARIANT.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE ASK_CORTEX_ANALYST(QUESTION VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests')
EXTERNAL_ACCESS_INTEGRATIONS = (CORTEX_REST_INTEGRATION)
HANDLER = 'run'
AS $$
import json
import requests

SEMANTIC_MODEL = '@MARKO.ANALYTICS.SEMANTIC_MODELS/SemModel.yml'

def run(session, question: str):
    # Resolve account URL at runtime
    account_locator = (
        session.sql("SELECT LOWER(CURRENT_ACCOUNT_LOCATOR())").collect()[0][0]
    )
    url = f"https://{account_locator}.snowflakecomputing.com/api/v2/cortex/analyst/message"

    # Re-use the session's auth token (no extra credentials needed)
    token = session._conn._conn.rest.token

    headers = {
        "Authorization": f'Snowflake Token="{token}"',
        "Content-Type":  "application/json",
        "Accept":        "application/json",
        "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
    }
    payload = {
        "messages": [
            {
                "role":    "user",
                "content": [{"type": "text", "text": question}],
            }
        ],
        "semantic_model_file": SEMANTIC_MODEL,
    }

    resp = requests.post(url, headers=headers, json=payload, timeout=120)
    resp.raise_for_status()
    body = resp.json()

    # Extract the generated SQL and text answer from the response
    sql_text    = None
    answer_text = None
    for msg in body.get("message", {}).get("content", []):
        if msg.get("type") == "sql":
            sql_text = msg.get("statement")
        elif msg.get("type") == "text":
            answer_text = msg.get("text")

    return {
        "question":    question,
        "answer":      answer_text,
        "sql":         sql_text,
        "raw_response": body,
    }
$$;


-- ---------------------------------------------------------------------------
-- 4e. ASK_CORTEX_AGENT
--     SQL wrapper to invoke the TPCH_SALES_AGENT defined in step 3.
--     The agent decides which tool(s) to use (Analyst, Search, or both).
-- ---------------------------------------------------------------------------

CREATE OR REPLACE PROCEDURE ASK_CORTEX_AGENT(QUESTION VARCHAR)
RETURNS VARIANT
LANGUAGE PYTHON
RUNTIME_VERSION = '3.11'
PACKAGES = ('snowflake-snowpark-python', 'requests')
EXTERNAL_ACCESS_INTEGRATIONS = (CORTEX_REST_INTEGRATION)
HANDLER = 'run'
AS $$
import json
import requests

SEMANTIC_MODEL  = '@MARKO.ANALYTICS.SEMANTIC_MODELS/SemModel.yml'
SEARCH_SERVICE  = 'MARKO.ANALYTICS.BUSINESS_KNOWLEDGE_SEARCH'
AGENT_LLM       = 'claude-3-5-sonnet'

def run(session, question: str):
    account_locator = (
        session.sql("SELECT LOWER(CURRENT_ACCOUNT_LOCATOR())").collect()[0][0]
    )
    url   = f"https://{account_locator}.snowflakecomputing.com/api/v2/cortex/agent:run"
    token = session._conn._conn.rest.token

    headers = {
        "Authorization": f'Snowflake Token="{token}"',
        "Content-Type":  "application/json",
        "Accept":        "text/event-stream",          # SSE streaming
        "X-Snowflake-Authorization-Token-Type": "KEYPAIR_JWT",
    }
    payload = {
        "model": AGENT_LLM,
        "messages": [
            {
                "role":    "user",
                "content": [{"type": "text", "text": question}],
            }
        ],
        "tools": [
            {
                "tool_spec": {
                    "type": "cortex_analyst_text_to_sql",
                    "name": "tpch_analyst",
                },
                "tool_resources": {
                    "semantic_model_file": SEMANTIC_MODEL,
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

    # Collect SSE stream into a list of events
    answer     = ""
    sql_list   = []
    tools_used = []

    with requests.post(url, headers=headers, json=payload, stream=True, timeout=120) as resp:
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
                    answer += delta.get("text", "")

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

    return {
        "question":  question,
        "answer":    answer,
        "sql":       sql_list,
        "tools_used": tools_used,
    }
$$;


-- ---------------------------------------------------------------------------
-- 4f. Run quick smoke tests
-- ---------------------------------------------------------------------------

-- Test 1: Cortex Search — returns VARIANT with results array
CALL SEARCH_KNOWLEDGE('how is revenue calculated');

-- Test 2: Cortex Analyst (NL → SQL)
CALL ASK_CORTEX_ANALYST('What is the total net revenue per year?');

-- Test 3: Cortex Agent (full orchestration)
CALL ASK_CORTEX_AGENT('What is the revenue formula and which region performs best?');
