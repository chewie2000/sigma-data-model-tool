-- =============================================================================
-- STEP 5: End-to-End Demo Queries
--
-- Run these in Snowsight (or snowsql) after completing steps 1-4.
-- Every interaction with Cortex is driven from SQL.
--
-- Sections:
--   A. Direct SQL analytics on TPC-H views
--   B. Cortex Search — RAG over business knowledge docs
--   C. Cortex Analyst — natural language → SQL
--   D. Cortex Agent — full orchestration (Analyst + Search)
-- =============================================================================

USE DATABASE CORTEX_AGENT_DEMO;
USE SCHEMA   TPCH;
USE WAREHOUSE CORTEX_DEMO_WH;


-- =============================================================================
-- SECTION A: Direct SQL Analytics
-- These are the baseline queries the agent will generate automatically.
-- Run them directly to see the data the Cortex tools reason over.
-- =============================================================================

-- A1. Annual revenue trend
SELECT
    YEAR(o.O_ORDERDATE)                               AS order_year,
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT))       AS net_revenue,
    COUNT(DISTINCT o.O_ORDERKEY)                       AS order_count,
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT))
        / COUNT(DISTINCT o.O_ORDERKEY)                 AS avg_order_revenue
FROM ORDERS   o
JOIN LINEITEM  l ON l.L_ORDERKEY = o.O_ORDERKEY
GROUP BY 1
ORDER BY 1;

-- A2. Revenue by region
SELECT
    r.R_NAME                                           AS region,
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT))       AS net_revenue,
    COUNT(DISTINCT c.C_CUSTKEY)                        AS unique_customers
FROM LINEITEM  l
JOIN ORDERS    o  ON l.L_ORDERKEY  = o.O_ORDERKEY
JOIN CUSTOMER  c  ON o.O_CUSTKEY   = c.C_CUSTKEY
JOIN NATION    n  ON c.C_NATIONKEY = n.N_NATIONKEY
JOIN REGION    r  ON n.N_REGIONKEY = r.R_REGIONKEY
GROUP BY 1
ORDER BY net_revenue DESC;

-- A3. Revenue by customer market segment
SELECT
    c.C_MKTSEGMENT                                     AS market_segment,
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT))       AS net_revenue,
    COUNT(DISTINCT c.C_CUSTKEY)                        AS customer_count,
    COUNT(DISTINCT o.O_ORDERKEY)                       AS order_count
FROM LINEITEM  l
JOIN ORDERS    o ON l.L_ORDERKEY = o.O_ORDERKEY
JOIN CUSTOMER  c ON o.O_CUSTKEY  = c.C_CUSTKEY
GROUP BY 1
ORDER BY net_revenue DESC;

-- A4. Shipping mode performance
SELECT
    l.L_SHIPMODE,
    COUNT(*)                                           AS line_items,
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT))       AS net_revenue,
    AVG(l.L_DISCOUNT)                                  AS avg_discount,
    AVG(DATEDIFF('day', l.L_SHIPDATE, l.L_RECEIPTDATE)) AS avg_transit_days
FROM LINEITEM l
GROUP BY 1
ORDER BY net_revenue DESC;

-- A5. Order priority breakdown
SELECT
    O_ORDERPRIORITY,
    COUNT(*)                AS order_count,
    SUM(O_TOTALPRICE)       AS total_order_value,
    AVG(O_TOTALPRICE)       AS avg_order_value
FROM ORDERS
GROUP BY 1
ORDER BY order_count DESC;

-- A6. Top 10 customers by revenue
SELECT
    c.C_NAME                                           AS customer_name,
    c.C_MKTSEGMENT                                     AS segment,
    n.N_NAME                                           AS nation,
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT))       AS net_revenue,
    COUNT(DISTINCT o.O_ORDERKEY)                       AS orders
FROM LINEITEM  l
JOIN ORDERS    o ON l.L_ORDERKEY  = o.O_ORDERKEY
JOIN CUSTOMER  c ON o.O_CUSTKEY   = c.C_CUSTKEY
JOIN NATION    n ON c.C_NATIONKEY = n.N_NATIONKEY
GROUP BY 1, 2, 3
ORDER BY net_revenue DESC
LIMIT 10;

-- A7. Supplier performance by nation
SELECT
    n.N_NAME                                           AS nation,
    COUNT(DISTINCT s.S_SUPPKEY)                        AS supplier_count,
    SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT))       AS supplied_revenue,
    AVG(ps.PS_SUPPLYCOST)                              AS avg_supply_cost
FROM LINEITEM l
JOIN SUPPLIER  s  ON l.L_SUPPKEY   = s.S_SUPPKEY
JOIN PARTSUPP  ps ON l.L_PARTKEY   = ps.PS_PARTKEY
                  AND l.L_SUPPKEY  = ps.PS_SUPPKEY
JOIN NATION    n  ON s.S_NATIONKEY = n.N_NATIONKEY
GROUP BY 1
ORDER BY supplied_revenue DESC
LIMIT 15;

-- A8. Monthly revenue with year-over-year growth
WITH monthly AS (
    SELECT
        YEAR(o.O_ORDERDATE)                            AS yr,
        MONTH(o.O_ORDERDATE)                           AS mo,
        SUM(l.L_EXTENDEDPRICE * (1 - l.L_DISCOUNT))   AS net_revenue
    FROM ORDERS o
    JOIN LINEITEM l ON l.L_ORDERKEY = o.O_ORDERKEY
    GROUP BY 1, 2
)
SELECT
    yr,
    mo,
    net_revenue,
    LAG(net_revenue) OVER (PARTITION BY mo ORDER BY yr)    AS prev_yr_revenue,
    ROUND(
        (net_revenue - LAG(net_revenue) OVER (PARTITION BY mo ORDER BY yr))
        / NULLIF(LAG(net_revenue) OVER (PARTITION BY mo ORDER BY yr), 0) * 100
    , 2)                                                   AS yoy_growth_pct
FROM monthly
ORDER BY yr, mo;


-- =============================================================================
-- SECTION B: Cortex Search — RAG over Business Knowledge
-- Uses SNOWFLAKE.CORTEX.SEARCH_PREVIEW directly in SQL (no stored proc needed)
-- =============================================================================

-- B1. Search using SEARCH_PREVIEW directly
SELECT
    value:DOC_ID::NUMBER       AS doc_id,
    value:CATEGORY::VARCHAR    AS category,
    value:TITLE::VARCHAR       AS title,
    LEFT(value:CONTENT::TEXT, 300) AS content_preview
FROM TABLE(
    FLATTEN(
        input => PARSE_JSON(
            SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                'CORTEX_AGENT_DEMO.TPCH.BUSINESS_KNOWLEDGE_SEARCH',
                '{ "query": "how is revenue calculated",
                   "columns": ["DOC_ID","CATEGORY","TITLE","CONTENT"],
                   "limit": 3 }'
            )
        ):results
    )
);

-- B2. Search for order status codes
SELECT
    value:TITLE::VARCHAR        AS title,
    value:CONTENT::TEXT         AS content
FROM TABLE(
    FLATTEN(
        input => PARSE_JSON(
            SNOWFLAKE.CORTEX.SEARCH_PREVIEW(
                'CORTEX_AGENT_DEMO.TPCH.BUSINESS_KNOWLEDGE_SEARCH',
                '{ "query": "order status codes meaning",
                   "columns": ["TITLE","CONTENT"],
                   "limit": 2 }'
            )
        ):results
    )
);

-- B3. Call via the convenience stored procedure (from step 4)
CALL SEARCH_KNOWLEDGE('which customer segments have highest order values', 3);

CALL SEARCH_KNOWLEDGE('supplier performance metrics', 2);

CALL SEARCH_KNOWLEDGE('regional structure VP geography', 2);


-- =============================================================================
-- SECTION C: Cortex Analyst — Natural Language → SQL
-- CALL ASK_CORTEX_ANALYST('<question>') returns the generated SQL + answer.
-- =============================================================================

-- C1. Revenue questions
CALL ASK_CORTEX_ANALYST('What is the total net revenue for each year?');

CALL ASK_CORTEX_ANALYST('Which are the top 5 customer market segments by revenue?');

CALL ASK_CORTEX_ANALYST('Compare total revenue across shipping modes.');

CALL ASK_CORTEX_ANALYST('What is the revenue breakdown by geographic region?');

-- C2. Operational questions
CALL ASK_CORTEX_ANALYST(
    'How many orders and what is the total value per order priority level?'
);

CALL ASK_CORTEX_ANALYST(
    'Which nation has the most customers and what is their average account balance?'
);

CALL ASK_CORTEX_ANALYST(
    'Show the average discount rate and average transit days by shipping mode.'
);

-- C3. Year-over-year analysis
CALL ASK_CORTEX_ANALYST(
    'Show monthly revenue for 1995 and 1996 side by side.'
);

-- C4. Inspect the generated SQL from a Cortex Analyst call
-- (Useful for auditing / understanding what SQL was produced)
WITH analyst_result AS (
    CALL ASK_CORTEX_ANALYST('What is the average order value per market segment?')
)
SELECT
    $1:question::VARCHAR   AS question,
    $1:answer::VARCHAR     AS answer,
    $1:sql::VARCHAR        AS generated_sql;


-- =============================================================================
-- SECTION D: Cortex Agent — Full Orchestration
-- The agent picks Cortex Analyst, Cortex Search, or both, automatically.
-- =============================================================================

-- D1. Pure analytics question (agent will use Cortex Analyst)
CALL ASK_CORTEX_AGENT('What is the total net revenue for each year?');

-- D2. Pure knowledge question (agent will use Cortex Search)
CALL ASK_CORTEX_AGENT('How is net revenue defined and calculated?');

-- D3. Mixed question (agent may invoke both tools)
CALL ASK_CORTEX_AGENT(
    'Explain the revenue formula we use, then show me which region earns the most.'
);

CALL ASK_CORTEX_AGENT(
    'What do the order priority SLA commitments say, and how many open urgent orders are there?'
);

CALL ASK_CORTEX_AGENT(
    'Which shipping modes exist per our business docs, and what is the revenue split across them?'
);

-- D4. Inspect agent results — extract SQL and answer separately
WITH agent_result AS (
    CALL ASK_CORTEX_AGENT('Show top 3 nations by customer count and explain the regional structure.')
)
SELECT
    $1:question::VARCHAR            AS question,
    $1:answer::VARCHAR              AS answer,
    $1:tools_used::VARCHAR          AS tools_invoked,
    $1:sql[0]::VARCHAR              AS first_generated_sql;
