-- =============================================================================
-- STEP 3: Set up Cortex Search + Stage for Semantic Model
-- Run after 01_setup_database.sql
-- =============================================================================

USE DATABASE MARKO;
USE SCHEMA ANALYTICS;
USE WAREHOUSE CORTEX_DEMO_WH;

-- ---------------------------------------------------------------------------
-- 3a. Internal stage to hold the semantic model YAML
-- ---------------------------------------------------------------------------

CREATE OR REPLACE STAGE SEMANTIC_MODELS
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stores Cortex Analyst semantic model YAML files';

-- After running this script, upload the YAML from your local machine:
--   PUT file://02_semantic_model.yaml @SEMANTIC_MODELS AUTO_COMPRESS=FALSE;
-- Or via Snowsight: Data > Databases > MARKO > ANALYTICS > Stages > SEMANTIC_MODELS

-- Verify the stage after upload:
-- LIST @SEMANTIC_MODELS;

-- ---------------------------------------------------------------------------
-- 3b. Cortex Search Service on business knowledge documents
--     This gives the agent a RAG tool to look up KPI definitions,
--     business rules, and context documents.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE CORTEX SEARCH SERVICE BUSINESS_KNOWLEDGE_SEARCH
    ON CONTENT
    ATTRIBUTES CATEGORY, TITLE, DOC_ID
    WAREHOUSE = CORTEX_DEMO_WH
    TARGET_LAG = '1 hour'
    AS (
        SELECT
            DOC_ID,
            CATEGORY,
            TITLE,
            CONTENT
        FROM MARKO.ANALYTICS.BUSINESS_KNOWLEDGE
    );

-- Check service status
SHOW CORTEX SEARCH SERVICES;

-- Test the search service directly
SELECT PARSE_JSON(
    SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
        'MARKO.ANALYTICS.BUSINESS_KNOWLEDGE_SEARCH',
        '{
            "query": "how is revenue calculated",
            "columns": ["CATEGORY", "TITLE", "CONTENT"],
            "limit": 2
        }'
    )
) AS search_results;

-- ---------------------------------------------------------------------------
-- 3c. Grant permissions (adjust role as needed)
-- ---------------------------------------------------------------------------

-- Grant usage to SYSADMIN or a dedicated role
-- GRANT USAGE ON DATABASE MARKO TO ROLE SYSADMIN;
-- GRANT USAGE ON SCHEMA MARKO.ANALYTICS TO ROLE SYSADMIN;
-- GRANT ALL PRIVILEGES ON ALL VIEWS IN SCHEMA MARKO.ANALYTICS TO ROLE SYSADMIN;
-- GRANT ALL PRIVILEGES ON TABLE MARKO.ANALYTICS.BUSINESS_KNOWLEDGE TO ROLE SYSADMIN;
-- GRANT ALL PRIVILEGES ON STAGE MARKO.ANALYTICS.SEMANTIC_MODELS TO ROLE SYSADMIN;
-- GRANT ALL PRIVILEGES ON CORTEX SEARCH SERVICE MARKO.ANALYTICS.BUSINESS_KNOWLEDGE_SEARCH TO ROLE SYSADMIN;
-- GRANT USAGE ON WAREHOUSE CORTEX_DEMO_WH TO ROLE SYSADMIN;

-- ---------------------------------------------------------------------------
-- 3d. Quick sanity-check queries before running the agent
-- ---------------------------------------------------------------------------

-- Confirm views are accessible
SELECT COUNT(*) FROM ORDERS;
SELECT COUNT(*) FROM LINEITEM;

-- Test Cortex Analyst manually via SQL (SQL API alternative to REST):
-- SELECT SNOWFLAKE.CORTEX.COMPLETE(
--     'mistral-large',
--     'What is the total revenue for the year 1995?'
-- );

-- Confirm stage has the YAML (run after PUT command)
-- LIST @SEMANTIC_MODELS;
