# AI Assistant Instructions — olist-data-platform

This file provides instructions for AI assistants (Claude, Cursor, GitHub Copilot) working on this project.
Read this before making any changes.

---

## Project Context

**Name:** olist-data-platform
**Author:** Vicky Costa — Data Analyst / Data Science student
**GitHub:** https://github.com/srxkatsumi/olist-data-platform
**Status:** Phase 1 complete (pipeline operational). Phase 2 in progress (analysis layer).

**Stack:**

| Tool | Version | Role |
|---|---|---|
| Python | 3.x | DAGs, data loading scripts |
| Apache Airflow | 2.11.2 | Orchestration |
| Airbyte | 2.0.1 | Data extraction and loading |
| dbt | 1.11.8 | SQL transformations |
| Snowflake | — | Cloud data warehouse |
| PostgreSQL | 16 | Legacy source simulation |
| Docker | — | Local containerized environment |
| Metabase | latest | BI and dashboards |

**Data warehouse layout (Phase 1 — current):**
- `OLIST_DB.RAW` — raw tables loaded by Airbyte
- `OLIST_DB.RAW` (views) — dbt staging models
- `OLIST_DB.RAW` (tables) — dbt mart models

> Note: staging and marts currently share the RAW schema. This is a known Phase 1
> simplification. Phase 3 will introduce dedicated schemas (STAGING and MARTS)
> to enforce layer separation. Do not create new schemas before that milestone.

---

## Git Rules

**Never commit directly to `main`.** Always create a branch first.

```bash
git checkout -b feat/your-feature-name
```

Branch naming:
- `feat/` — new models, DAGs, or features
- `fix/` — bug fixes
- `docs/` — documentation changes
- `chore/` — dependencies, config, infrastructure

Commit messages must follow Conventional Commits:
```
feat(marts): add mart_revenue_by_category
fix(staging): handle null timestamps in stg_orders
docs(readme): update phase 2 status
```

---

## dbt Rules

### Always create or update schema.yml

When you create or modify a dbt model, you must create or update its `schema.yml` entry.
No model is complete without it.

Required for every model:
- `name` and `description`
- `description` for every column
- `not_null` and `unique` tests on primary keys
- `accepted_values` test on any categorical column (e.g., `order_status`)

Example:
```yaml
version: 2

models:
  - name: mart_example
    description: "Aggregates delivered orders by customer for lifetime value analysis."
    columns:
      - name: customer_id
        description: "Unique identifier for the customer."
        tests:
          - not_null
          - unique
      - name: order_status
        description: "Final status of the order at the time of aggregation."
        tests:
          - accepted_values:
              values: ['delivered', 'shipped', 'canceled', 'invoiced', 'processing']
```

### Run dbt test before marking work as complete

```bash
cd dbt/olist
dbt run --select <model_name> --profiles-dir .
dbt test --select <model_name> --profiles-dir .
```

Do not consider a model done until `dbt test` passes with zero failures.

---

## Security Rules

Never expose credentials, even in comments or examples.

| Forbidden | Use instead |
|---|---|
| Hardcoded passwords | `${SNOWFLAKE_PASSWORD}` (docker-compose) or `os.environ.get()` (Python) |
| Real Snowflake account IDs | Placeholder like `YOUR_ACCOUNT_HERE` |
| Real IP addresses | Generic descriptions like "your local network IP" |
| Committing `.env` | Only `.env.example` with empty values is committed |
| Committing `profiles.yml` | Only `profiles.yml.example` with placeholders is committed |

If you need to show a configuration example, always use the `.example` files as reference.

---

## Language Rules

- All code comments must be in **English**
- All `schema.yml` descriptions must be in **English**
- All commit messages must be in **English**
- README content is in **English**

---

## README Update Requirements

Update the README whenever you:
- Add a new dbt model — add it to the "Mart Models" table
- Add a new DAG — update the project structure and How to Run sections
- Add a new tool to the stack — update the Tech Stack table and badges
- Complete a phase — update the status banner

---

## File Structure Reference

```
olist-data-platform/
├── .env.example                 <- safe to read, never edit with real values
├── docker-compose.yml
├── dags/
│   ├── seed_dag.py              <- loads CSVs into PostgreSQL (runs once)
│   └── dbt_dag.py               <- runs staging then marts in sequence
├── dbt/olist/
│   ├── profiles.yml.example     <- safe to read, never edit with real values
│   ├── dbt_project.yml
│   └── models/
│       ├── staging/
│       │   ├── sources.yml
│       │   └── stg_orders.sql
│       └── marts/
│           ├── mart_sazonalidade.sql
│           ├── mart_clientes_top.sql
│           ├── mart_horarios_pico.sql
│           └── mart_perfil_cliente.sql
├── docs/
│   └── coding-standards.md
└── skills/
    └── SKILL.md                 <- this file
```

---

## What Not to Do

- Do not commit to `main` directly
- Do not create files unless the task requires it
- Do not add features beyond what was asked
- Do not add error handling for scenarios that cannot happen
- Do not refactor code that was not part of the task
- Do not add comments that restate what the code already says
- Do not expose credentials in any form in any tracked file
