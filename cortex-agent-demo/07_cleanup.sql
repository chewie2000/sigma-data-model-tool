-- =============================================================================
-- STEP 7: Cleanup — removes all objects created by this demo
-- Run only when you want to tear down the demo environment
-- =============================================================================

USE ROLE SYSADMIN;

-- Drop Cortex Search Service
DROP CORTEX SEARCH SERVICE IF EXISTS CORTEX_AGENT_DEMO.TPCH.BUSINESS_KNOWLEDGE_SEARCH;

-- Drop Stage
DROP STAGE IF EXISTS CORTEX_AGENT_DEMO.TPCH.SEMANTIC_MODELS;

-- Drop the demo database (cascades to all schemas, tables, views)
DROP DATABASE IF EXISTS CORTEX_AGENT_DEMO;

-- Drop the warehouse
DROP WAREHOUSE IF EXISTS CORTEX_DEMO_WH;

-- Confirm
SHOW DATABASES LIKE 'CORTEX_AGENT_DEMO';
SHOW WAREHOUSES LIKE 'CORTEX_DEMO_WH';
