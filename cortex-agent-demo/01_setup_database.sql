-- =============================================================================
-- STEP 1: Database & Schema Setup
-- Uses SNOWFLAKE_SAMPLE_DATA.TPCH_SF1 (TPC-H benchmark tables)
-- =============================================================================

-- Create a dedicated database/schema for the demo
CREATE DATABASE IF NOT EXISTS MARKO;
CREATE SCHEMA IF NOT EXISTS MARKO.ANALYTICS;
USE SCHEMA MARKO.ANALYTICS;

-- Create a warehouse for the demo
CREATE WAREHOUSE IF NOT EXISTS CORTEX_DEMO_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    COMMENT = 'Warehouse for Cortex Agent Demo';

USE WAREHOUSE CORTEX_DEMO_WH;

-- ---------------------------------------------------------------------------
-- Create views over the TPC-H sample data (SF1 = ~1GB scale factor)
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW ORDERS AS
    SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.ORDERS;

CREATE OR REPLACE VIEW CUSTOMER AS
    SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.CUSTOMER;

CREATE OR REPLACE VIEW LINEITEM AS
    SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.LINEITEM;

CREATE OR REPLACE VIEW PART AS
    SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.PART;

CREATE OR REPLACE VIEW SUPPLIER AS
    SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.SUPPLIER;

CREATE OR REPLACE VIEW PARTSUPP AS
    SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.PARTSUPP;

CREATE OR REPLACE VIEW NATION AS
    SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.NATION;

CREATE OR REPLACE VIEW REGION AS
    SELECT * FROM SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.REGION;

-- ---------------------------------------------------------------------------
-- Create a "business knowledge" table for Cortex Search (unstructured RAG)
-- This simulates business documents / KPI definitions the agent can reference
-- ---------------------------------------------------------------------------

CREATE OR REPLACE TABLE BUSINESS_KNOWLEDGE (
    DOC_ID      NUMBER AUTOINCREMENT PRIMARY KEY,
    CATEGORY    VARCHAR(100),
    TITLE       VARCHAR(500),
    CONTENT     TEXT
);

INSERT INTO BUSINESS_KNOWLEDGE (CATEGORY, TITLE, CONTENT) VALUES
('KPI Definition', 'Revenue',
 $$Revenue is defined as the extended price of line items minus any discounts applied. Formula: SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT)). This is the net revenue before tax. To include tax use: SUM(L_EXTENDEDPRICE * (1 - L_DISCOUNT) * (1 + L_TAX)). Revenue is the primary top-line metric used by the finance team.$$),

('KPI Definition', 'Order Status Codes',
 $$Order status codes in the ORDERS table: F = Fulfilled/Completed, O = Open/Pending, P = Partially fulfilled. The majority of historical orders will be in status F. Open orders (O) represent active demand. P orders need attention from fulfillment teams.$$),

('KPI Definition', 'Customer Segments',
 $$The MKTSEGMENT column in the CUSTOMER table classifies customers into market segments: AUTOMOBILE, BUILDING, FURNITURE, MACHINERY, HOUSEHOLD. These segments drive targeted marketing campaigns and pricing strategies. The HOUSEHOLD and FURNITURE segments typically have the highest average order values.$$),

('KPI Definition', 'Supplier Performance',
 $$Supplier performance is measured by: 1. Fill rate: percentage of part-supply combinations available (PARTSUPP.PS_AVAILQTY > 0). 2. Supply cost: PS_SUPPLYCOST in PARTSUPP — lower is better. 3. Comment flags in SUPPLIER.S_COMMENT may indicate certified or preferred supplier status. The procurement team reviews supplier performance quarterly.$$),

('Business Context', 'Priority Levels',
 $$Order priority in O_ORDERPRIORITY: 1-URGENT, 2-HIGH, 3-MEDIUM, 4-NOT SPECIFIED, 5-LOW. SLA commitments require urgent and high priority orders to ship within 24 hours. Medium and below have a 72-hour ship window. Breached SLAs are escalated to the VP of Operations.$$),

('Business Context', 'Fiscal Calendar',
 $$The company operates on a January-December fiscal year. Q1: Jan-Mar, Q2: Apr-Jun, Q3: Jul-Sep, Q4: Oct-Dec. Peak season is Q4 (holiday period). The TPC-H dataset spans orders from 1992 to 1998. Year-over-year comparisons should account for data completeness at year boundaries.$$),

('Business Context', 'Regional Structure',
 $$Sales regions map to the REGION table: AFRICA, AMERICA, ASIA, EUROPE, MIDDLE EAST. Each region contains multiple nations (NATION table). Regional VPs own P&L for their respective geographies. AMERICA and EUROPE are the two largest revenue-generating regions.$$),

('Technical Reference', 'Key Table Joins',
 $$Common join patterns in TPC-H: Orders to Customer: ORDERS.O_CUSTKEY = CUSTOMER.C_CUSTKEY. LineItem to Orders: LINEITEM.L_ORDERKEY = ORDERS.O_ORDERKEY. LineItem to Part: LINEITEM.L_PARTKEY = PART.P_PARTKEY. LineItem to Supplier: LINEITEM.L_SUPPKEY = SUPPLIER.S_SUPPKEY. Supplier to Nation to Region: S_NATIONKEY = N_NATIONKEY, N_REGIONKEY = R_REGIONKEY. Customer to Nation to Region: C_NATIONKEY = N_NATIONKEY, N_REGIONKEY = R_REGIONKEY.$$);

-- Verify setup
SELECT 'ORDERS' AS tbl, COUNT(*) AS row_count FROM ORDERS
UNION ALL SELECT 'CUSTOMER', COUNT(*) FROM CUSTOMER
UNION ALL SELECT 'LINEITEM', COUNT(*) FROM LINEITEM
UNION ALL SELECT 'SUPPLIER', COUNT(*) FROM SUPPLIER
UNION ALL SELECT 'PART', COUNT(*) FROM PART
UNION ALL SELECT 'NATION', COUNT(*) FROM NATION
UNION ALL SELECT 'REGION', COUNT(*) FROM REGION
UNION ALL SELECT 'BUSINESS_KNOWLEDGE', COUNT(*) FROM BUSINESS_KNOWLEDGE;
