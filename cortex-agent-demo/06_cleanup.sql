-- =============================================================================
-- STEP 6: Cleanup — removes all objects created by this demo
-- Run only when you want to tear down the demo environment
-- =============================================================================

USE ROLE ACCOUNTADMIN;

-- Drop External Access Integration and Network Rule (created in step 4)
DROP EXTERNAL ACCESS INTEGRATION IF EXISTS CORTEX_REST_INTEGRATION;
DROP NETWORK RULE IF EXISTS MARKO.ANALYTICS.CORTEX_REST_RULE;

-- Drop Cortex Agent
DROP CORTEX AGENT IF EXISTS MARKO.ANALYTICS.TPCH_SALES_AGENT;

-- Drop Cortex Search Service
DROP CORTEX SEARCH SERVICE IF EXISTS MARKO.ANALYTICS.BUSINESS_KNOWLEDGE_SEARCH;

-- Drop Stage
DROP STAGE IF EXISTS MARKO.ANALYTICS.SEMANTIC_MODELS;

-- Drop the demo database (cascades to all schemas, tables, views)
DROP DATABASE IF EXISTS MARKO;

-- Drop the warehouse
DROP WAREHOUSE IF EXISTS CORTEX_DEMO_WH;

-- Confirm
SHOW DATABASES LIKE 'MARKO';
SHOW WAREHOUSES LIKE 'CORTEX_DEMO_WH';
