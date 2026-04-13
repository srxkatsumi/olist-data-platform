# Coding Standards — olist-data-platform

## dbt Standards

### Every model must have a schema.yml entry

No model ships without a corresponding `schema.yml` block. This is not optional.

```yaml
version: 2

models:
  - name: mart_example
    description: "One clear sentence describing what this model answers."
    columns:
      - name: order_id
        description: "Unique identifier for the order."
        tests:
          - not_null
          - unique

      - name: order_status
        description: "Current status of the order."
        tests:
          - not_null
          - accepted_values:
              values: ['delivered', 'shipped', 'canceled', 'invoiced', 'processing', 'approved', 'unavailable', 'created']
```

### Column documentation rules

- All descriptions must be in **English**
- Every column must have a `description` — no exceptions
- Use plain language: describe what the value means, not what the column is named
- Avoid redundant descriptions like `"The order_id column"` — write `"Unique identifier for the order"`

### Required tests by column type

| Column type | Required tests |
|---|---|
| Primary key | `not_null`, `unique` |
| Foreign key | `not_null` |
| Categorical / status field | `not_null`, `accepted_values` |
| Numeric metric | `not_null` |

### Materialization defaults

| Layer | Materialization |
|---|---|
| Staging | `view` (default) |
| Marts | `table` (set via `{{ config(materialized='table') }}`) |

### Model file header

Every SQL file must start with a comment block:

```sql
-- model_name.sql
-- Question: <the business question this model answers>
```

---

## SQL Standards

### Naming conventions

| Object | Convention | Example |
|---|---|---|
| CTE names | lowercase, descriptive | `orders`, `payments`, `result` |
| Column aliases | UPPER_SNAKE_CASE | `TOTAL_ORDERS`, `AVG_DELIVERY_DAYS` |
| Model files | lowercase with underscores | `mart_revenue_by_category.sql` |
| Source references | match exact source table name | `OLIST_ORDERS_DATASET` |

### CTE structure

Always decompose complex queries into named CTEs. Final `SELECT` must reference a named CTE.

```sql
with orders as (
    select * from {{ ref('stg_orders') }}
),

payments as (
    select * from {{ source('raw', 'OLIST_ORDER_PAYMENTS_DATASET') }}
),

result as (
    select
        o.ORDER_ID,
        sum(p.PAYMENT_VALUE) as TOTAL_VALUE
    from orders o
    left join payments p on o.ORDER_ID = p.ORDER_ID
    group by o.ORDER_ID
)

select * from result
```

### Inline comments

Use comments to explain logic, not to restate what the code does.

```sql
-- Good: explains the decision
round(
    count(case when ORDER_STATUS = 'canceled' then 1 end) * 100.0
    / nullif(count(ORDER_ID), 0), 2
)  as CANCELLATION_RATE_PCT   -- nullif prevents division by zero

-- Bad: restates the code
count(ORDER_ID) as TOTAL_ORDERS  -- counts orders
```

---

## Security Rules

### Credentials

| Rule | Detail |
|---|---|
| Never hardcode credentials | No passwords, tokens, or account IDs in any tracked file |
| Always use environment variables | Reference `${SNOWFLAKE_PASSWORD}` in docker-compose, `os.environ` in Python |
| `.env` is always gitignored | Verify before every push |
| Only `.env.example` is committed | Contains keys with empty values — never real values |
| `profiles.yml` is always gitignored | Only `profiles.yml.example` is committed |

### What never goes into git

```
.env
dbt/olist/profiles.yml
logs/
data/*.csv
target/
```

If you accidentally commit a credential, rotate it immediately — do not just delete it from the latest commit.

### Sensitive data in code

- No real IP addresses in tracked files
- No internal hostnames or account identifiers
- No personal data (names, emails, document numbers) in code or comments

---

## README Update Requirements

The README must be updated whenever any of the following changes are made:

| Change | What to update |
|---|---|
| New dbt model added | Add row to "Mart Models" table |
| New DAG added | Update project structure and How to Run |
| New tool added to the stack | Update Tech Stack table and badges |
| Phase completed | Update status banner |
| Known issue found or resolved | Update Known Issues table |

The README is the public face of this project. Keep it accurate.
