# Contributing to olist-data-platform

## Branching Rules

Never commit directly to `main`. All work must happen on a dedicated branch.

```bash
git checkout -b feat/your-feature-name
```

### Branch Naming

| Prefix | Use for |
|---|---|
| `feat/` | New models, DAGs, connectors, or features |
| `fix/` | Bug fixes in existing logic |
| `docs/` | README, CONTRIBUTING, schema descriptions |
| `chore/` | Dependencies, config, infrastructure, CI |

Examples:
```
feat/mart-revenue-by-category
fix/stg-orders-null-timestamps
docs/update-readme-phase2
chore/pin-dbt-version
```

---

## Commit Messages

Follow [Conventional Commits](https://www.conventionalcommits.org/).

```
<type>(<scope>): <short description>
```

| Type | Use for |
|---|---|
| `feat` | New functionality |
| `fix` | Bug fix |
| `docs` | Documentation only |
| `refactor` | Code restructure without behavior change |
| `test` | Adding or updating tests |
| `chore` | Maintenance, dependencies, config |

Examples:
```
feat(marts): add mart_revenue_by_category model
fix(staging): handle null ORDER_APPROVED_AT timestamps
docs(readme): add phase 2 dashboard screenshots
chore(docker): pin dbt-snowflake to 1.9.0
```

Rules:
- Use the imperative mood: "add", not "added" or "adds"
- Keep the subject line under 72 characters
- Reference issues when applicable: `fix(dag): correct schedule interval (#12)`

---

## Pull Request Checklist

Before opening a PR, confirm all of the following:

**dbt models**
- [ ] `schema.yml` created or updated for every touched model
- [ ] All columns documented in English
- [ ] At least `not_null` and `unique` tests on primary keys
- [ ] `accepted_values` test on any categorical column
- [ ] `dbt run` completes without errors
- [ ] `dbt test` passes with no failures

**DAGs**
- [ ] DAG has a `tags` list
- [ ] `schedule` is intentional (use `None` for manual-only)
- [ ] No credentials hardcoded — all values from environment variables

**General**
- [ ] No `.env`, `profiles.yml`, or log files staged
- [ ] README updated if a new model, DAG, or major feature was added
- [ ] Branch is up to date with `main`

---

## Code Review Guidelines

**As the author:**
- Keep PRs focused — one concern per PR
- Add a description explaining what changed and why, not just what
- Link to relevant issues or context

**As the reviewer:**
- Approve only when the PR checklist is complete
- Comment on logic, not style — style is enforced by standards
- If you request changes, be specific about what needs to change

**Merge policy:**
- Squash and merge for `feat/` and `fix/` branches
- Merge commit for `docs/` and `chore/` branches
- Delete the branch after merging
