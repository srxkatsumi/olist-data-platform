# Security Rules — olist-data-platform

This file defines the security rules for AI assistants working on this project.
Read this before making any change. These rules override everything else.

## Absolute rules — never violate

- NEVER expose passwords, tokens, API keys, or credentials in any form
- NEVER read, display, or reference the contents of `.env` or `profiles.yml`
- NEVER suggest hardcoding credentials in any tracked file
- NEVER commit or stage `.env`, `profiles.yml`, or any file containing real credentials
- NEVER reproduce or echo back any value that looks like a password, token, or secret key
- NEVER include real IP addresses, account identifiers, or internal hostnames in tracked files
- NEVER bypass or disable the pre-commit hook
- NEVER commit directly to main

## Credential handling

All credentials must be handled through environment variables only:

| Context | How to reference credentials |
|---|---|
| Docker Compose | `${SNOWFLAKE_PASSWORD}` via `.env` file |
| Python / DAGs | `os.environ.get('SNOWFLAKE_PASSWORD')` |
| dbt | `profiles.yml` (gitignored — never committed) |
| GitHub Actions | `${{ secrets.SNOWFLAKE_PASSWORD }}` via repository secrets |

Safe files (committed): `.env.example`, `profiles.yml.example`
Never committed: `.env`, `profiles.yml`, `logs/`, `data/*.csv`

## Pre-commit and CI/CD

Every commit triggers `dbt compile` locally via pre-commit hook.
Every pull request triggers `dbt test` via GitHub Actions.
A commit or merge must never bypass these checks.

## How this file relates to SKILL.md

`SKILL.md` defines how to work on the project.
`SECURITY.md` defines what must never happen under any circumstance.
When in conflict, `SECURITY.md` always wins.

## If you detect a potential security issue

- Stop immediately
- Do not proceed with the task
- Alert the user with a clear explanation of the risk
- Suggest the correct secure alternative
