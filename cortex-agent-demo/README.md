# Snowflake Cortex Agent Demo — TPC-H Sales & Operations

End-to-end demo of a **Snowflake Cortex Agent** that combines:

| Tool | Type | What it does |
|------|------|--------------|
| **Cortex Analyst** | `cortex_analyst_text_to_sql` | Translates natural-language questions into SQL against the TPC-H semantic model |
| **Cortex Search** | `cortex_search` | RAG over business knowledge docs (KPI definitions, business rules) |

The agent automatically decides which tool(s) to invoke for each question.

---

## Architecture

```
User question
      │
      ▼
┌─────────────────────────────────────────────┐
│         Cortex Agent (claude-3-5-sonnet)    │
│                                             │
│  ┌──────────────────┐  ┌──────────────────┐ │
│  │  Cortex Analyst  │  │  Cortex Search   │ │
│  │  (NL → SQL)      │  │  (vector RAG)    │ │
│  │                  │  │                  │ │
│  │  Semantic Model  │  │  Business        │ │
│  │  (YAML on stage) │  │  Knowledge table │ │
│  │                  │  │                  │ │
│  │  TPC-H views     │  │  KPI definitions │ │
│  │  (TPCH_SF1)      │  │  business rules  │ │
│  └──────────────────┘  └──────────────────┘ │
└─────────────────────────────────────────────┘
      │
      ▼
Natural language answer + generated SQL
```

---

## Prerequisites

- Snowflake account with:
  - Access to `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1`
  - `CORTEX_USER` privilege (or `SYSADMIN`)
  - Cortex Analyst & Cortex Search enabled for your region
- Python 3.9+

---

## Setup — step by step

### 1. Install Python dependencies

```bash
pip install -r requirements.txt
```

### 2. Set environment variables

```bash
export SNOWFLAKE_ACCOUNT="xy12345.us-east-1"   # your account identifier
export SNOWFLAKE_USER="your_user"
export SNOWFLAKE_PASSWORD="your_password"
export SNOWFLAKE_ROLE="SYSADMIN"               # optional, defaults to SYSADMIN
```

### 3. Run the database setup SQL

Open a Snowsight worksheet (or `snowsql`) and execute:

```
01_setup_database.sql
```

This creates:
- `CORTEX_AGENT_DEMO` database + `TPCH` schema
- Views pointing at `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1.*`
- `BUSINESS_KNOWLEDGE` table with 8 seed documents
- `CORTEX_DEMO_WH` warehouse

### 4. Set up Cortex services + stage

```
03_setup_cortex_services.sql
```

This creates:
- `SEMANTIC_MODELS` internal stage
- `BUSINESS_KNOWLEDGE_SEARCH` Cortex Search service

### 5. Upload the semantic model YAML

```bash
python 04_upload_semantic_model.py
```

Or manually in Snowsight:
`Data > Databases > CORTEX_AGENT_DEMO > TPCH > Stages > SEMANTIC_MODELS > Upload`

### 6. Run the demo

**Scripted demo** (runs through 9 pre-defined questions):

```bash
python 05_cortex_agent_demo.py
```

**Interactive REPL** (ask your own questions, supports multi-turn):

```bash
python 06_interactive_agent.py
```

---

## Sample questions to try

### Structured analytics (→ Cortex Analyst)
- *"What is the total net revenue for each year?"*
- *"Which are the top 5 customer market segments by revenue?"*
- *"Compare revenue across shipping modes."*
- *"Show revenue by geographic region."*
- *"How many orders per priority level and what is their total value?"*
- *"Which nation has the highest number of customers?"*
- *"What is the average discount rate by ship mode?"*

### Business knowledge lookup (→ Cortex Search)
- *"How is net revenue calculated?"*
- *"What do the order status codes mean?"*
- *"What are our fiscal quarter dates?"*

### Multi-tool (agent uses both)
- *"What is the revenue formula and which region performs best?"*
- *"Explain the order priority SLAs and show me counts per priority."*

---

## File reference

| File | Purpose |
|------|---------|
| `01_setup_database.sql` | Create DB, schema, views, seed data |
| `02_semantic_model.yaml` | Cortex Analyst semantic model (dimensions, measures, relationships, verified queries) |
| `03_setup_cortex_services.sql` | Create stage & Cortex Search service |
| `04_upload_semantic_model.py` | Upload YAML to internal stage |
| `05_cortex_agent_demo.py` | Scripted end-to-end demo |
| `06_interactive_agent.py` | Interactive multi-turn REPL |
| `07_cleanup.sql` | Tear down all demo objects |
| `requirements.txt` | Python dependencies |

---

## TPC-H schema overview

```
REGION (5 rows)
  └── NATION (25 rows)
        ├── CUSTOMER (150,000 rows)  ─── ORDERS (1.5M) ─── LINEITEM (6M)
        └── SUPPLIER (10,000 rows)  ─┘                         │
                                                               PART (200,000)
PARTSUPP (800,000 rows)  ←── links PART + SUPPLIER
```

Scale factor 1 = ~1 GB raw data, all available in `SNOWFLAKE_SAMPLE_DATA.TPCH_SF1`.

---

## Cleanup

```
07_cleanup.sql
```
