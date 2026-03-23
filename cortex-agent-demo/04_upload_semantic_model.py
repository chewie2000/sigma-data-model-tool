"""
Step 4 — Upload semantic model YAML to the Snowflake internal stage.

Usage:
    python 04_upload_semantic_model.py

Reads connection parameters from environment variables (or a .env file).
"""

import os
import sys
from pathlib import Path

import snowflake.connector

# ---------------------------------------------------------------------------
# Connection config — set these as env vars or edit directly
# ---------------------------------------------------------------------------
SNOWFLAKE_ACCOUNT  = os.environ["SNOWFLAKE_ACCOUNT"]   # e.g. xy12345.us-east-1
SNOWFLAKE_USER     = os.environ["SNOWFLAKE_USER"]
SNOWFLAKE_PASSWORD = os.environ["SNOWFLAKE_PASSWORD"]
SNOWFLAKE_ROLE     = os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN")
SNOWFLAKE_DATABASE = "CORTEX_AGENT_DEMO"
SNOWFLAKE_SCHEMA   = "TPCH"
SNOWFLAKE_WAREHOUSE= "CORTEX_DEMO_WH"

YAML_FILE   = Path(__file__).parent / "02_semantic_model.yaml"
STAGE_NAME  = "@CORTEX_AGENT_DEMO.TPCH.SEMANTIC_MODELS"


def main():
    if not YAML_FILE.exists():
        print(f"ERROR: semantic model file not found: {YAML_FILE}")
        sys.exit(1)

    print(f"Connecting to Snowflake account: {SNOWFLAKE_ACCOUNT}")
    conn = snowflake.connector.connect(
        account=SNOWFLAKE_ACCOUNT,
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        role=SNOWFLAKE_ROLE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA,
        warehouse=SNOWFLAKE_WAREHOUSE,
    )
    cur = conn.cursor()

    print(f"Uploading {YAML_FILE.name} to stage {STAGE_NAME} ...")
    result = cur.execute(
        f"PUT file://{YAML_FILE.resolve()} {STAGE_NAME} AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
    ).fetchall()
    print("PUT result:", result)

    print("\nFiles currently in stage:")
    rows = cur.execute(f"LIST {STAGE_NAME}").fetchall()
    for row in rows:
        print(" ", row)

    cur.close()
    conn.close()
    print("\nDone. Semantic model uploaded successfully.")


if __name__ == "__main__":
    main()
