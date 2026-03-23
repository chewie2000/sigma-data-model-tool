# Snowflake Cortex Agent Demo — TPC-H Sales & Operations

End-to-end demo of a **Snowflake Cortex Agent** driven entirely from SQL.

| Tool | What it does |
|------|-------------|
| **Cortex Analyst** | Natural-language → SQL over the TPC-H semantic model |
| **Cortex Search** | Vector RAG over business knowledge documents |
| **Cortex Agent** | Orchestrates both tools automatically per question |

---

## Architecture

```
                    SQL:  CALL ASK_CORTEX_AGENT('...')
                                      │
                          ┌───────────▼────────────┐
                          │   Cortex Agent         │
                          │   (claude-3-5-sonnet)  │
                          └───────┬────────┬───────┘
                                  │        │
              ┌───────────────────┘        └────────────────────┐
              │                                                   │
  ┌───────────▼──────────────┐              ┌────────────────────▼──────┐
  │   Cortex Analyst         │              │   Cortex Search            │
  │   (NL → SQL)             │              │   SEARCH_PREVIEW()         │
  │                          │              │                            │
  │   02_semantic_model.yaml │              │   BUSINESS_KNOWLEDGE table │
  │   on internal stage      │              │   (KPI defs, biz rules)    │
  │                          │              │                            │
  │   TPC-H views            │              │   Vector index             │
  │   (TPCH_SF1 SF=1)        │              │   (auto-maintained)        │
  └──────────────────────────┘              └────────────────────────────┘
```

---

## Prerequisites

- Snowflake account with access to `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1`
- `ACCOUNTADMIN` role (for network rule + external access integration in step 4)
- Cortex Analyst and Cortex Search enabled for your account/region

---

## Setup — step by step

### Step 1 — Database & data (`01_setup_database.sql`)

Creates `MARKO.ANALYTICS`, views over `TPCH_SF1`, and seeds
the `BUSINESS_KNOWLEDGE` table with 8 documents.

```sql
-- Run in Snowsight or snowsql
SOURCE 01_setup_database.sql;
```

### Step 2 — Semantic model (`02_semantic_model.yaml`)

The Cortex Analyst semantic model. Upload it to the stage created in step 3.

### Step 3 — Cortex services (`03_setup_cortex_services.sql`)

Creates the internal stage and the Cortex Search service.

```sql
SOURCE 03_setup_cortex_services.sql;
```

Upload the YAML to the stage (one-time, via snowsql CLI):

```bash
snowsql -a $SNOWFLAKE_ACCOUNT -u $SNOWFLAKE_USER \
  -q "PUT file://02_semantic_model.yaml \
       @MARKO.ANALYTICS.SEMANTIC_MODELS \
       AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
```

Or drag-and-drop in Snowsight:
`Data > Databases > MARKO > ANALYTICS > Stages > SEMANTIC_MODELS`

Verify:
```sql
LIST @MARKO.ANALYTICS.SEMANTIC_MODELS;
```

### Step 4 — Stored procedures (`04_create_procedures.sql`)

Creates a network rule, external access integration, and three procedures:

| Procedure | What it calls |
|-----------|-------------|
| `SEARCH_KNOWLEDGE(query, limit)` | `SNOWFLAKE.CORTEX.SEARCH_PREVIEW` — pure SQL |
| `ASK_CORTEX_ANALYST(question)` | Cortex Analyst REST API — returns SQL + answer |
| `ASK_CORTEX_AGENT(question)` | Cortex Agent REST API — full orchestration |

```sql
SOURCE 04_create_procedures.sql;
```

> Note: requires `ACCOUNTADMIN` for the network rule and integration.

### Step 5 — Run the demo (`05_demo_queries.sql`)

Everything is SQL from here. Four sections:

| Section | What it shows |
|---------|--------------|
| **A** | Direct TPC-H analytics (baseline SQL) |
| **B** | `SNOWFLAKE.CORTEX.SEARCH_PREVIEW` inline + `CALL SEARCH_KNOWLEDGE(...)` |
| **C** | `CALL ASK_CORTEX_ANALYST(...)` — NL questions → SQL |
| **D** | `CALL ASK_CORTEX_AGENT(...)` — agent picks tools automatically |

```sql
SOURCE 05_demo_queries.sql;
```

---

## Sample questions

### Structured analytics → Cortex Analyst
```sql
CALL ASK_CORTEX_ANALYST('What is the total net revenue for each year?');
CALL ASK_CORTEX_ANALYST('Which are the top 5 customer segments by revenue?');
CALL ASK_CORTEX_ANALYST('Compare revenue across shipping modes.');
CALL ASK_CORTEX_ANALYST('Show me revenue by geographic region.');
```

### Knowledge lookup → Cortex Search
```sql
CALL SEARCH_KNOWLEDGE('how is revenue calculated');
CALL SEARCH_KNOWLEDGE('order status codes meaning');
CALL SEARCH_KNOWLEDGE('fiscal calendar quarter dates');
```

### Full agent orchestration
```sql
CALL ASK_CORTEX_AGENT('What is the revenue formula and which region performs best?');
CALL ASK_CORTEX_AGENT('Explain order priority SLAs, then count open urgent orders.');
CALL ASK_CORTEX_AGENT('Which shipping modes exist and what is revenue per mode?');
```

---

## File reference

| File | Purpose |
|------|---------|
| `01_setup_database.sql` | DB, schema, TPC-H views, seed knowledge docs |
| `02_semantic_model.yaml` | Cortex Analyst semantic model |
| `03_setup_cortex_services.sql` | Internal stage + Cortex Search service |
| `04_create_procedures.sql` | Network rule, external access integration, stored procedures |
| `05_demo_queries.sql` | All demo queries (direct SQL, search, analyst, agent) |
| `06_cleanup.sql` | Tear down all demo objects |

---

## TPC-H schema

```
REGION (5)
  └── NATION (25)
        ├── CUSTOMER (150k)  ── ORDERS (1.5M) ── LINEITEM (6M)
        └── SUPPLIER (10k)  ─┘                       │
                                                    PART (200k)
PARTSUPP (800k) — links PART + SUPPLIER
```

Scale factor 1 ≈ 1 GB, available in `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1`.

---

## Cleanup

```sql
SOURCE 06_cleanup.sql;
```
