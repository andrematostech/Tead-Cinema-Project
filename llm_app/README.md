# LLM Add-on

This folder contains the separate LLM analytics add-on for the TEAD movie lakehouse.

## Goal

The add-on will let a user:

- ask a natural-language analytics question
- generate a safe SQL query
- run the query against Trino
- inspect the results
- optionally read a short explanation

## Scope for the first version

The first version will query only this Gold table:

- `iceberg.gold.movie_performance`

This keeps the add-on isolated from the core Bronze, Silver, and Gold pipeline.

## Planned files

- `app.py`
- `openai_client.py`
- `trino_client.py`
- `prompts.py`
- `sql_guard.py`
- `schema_context.py`
- `requirements.txt`
- `.env.example`
- `.env`

## Environment variables

Copy `.env.example` to a local `.env` file and set:

- `OPENAI_API_KEY`
- `OPENAI_MODEL`
- `TRINO_HOST`
- `TRINO_PORT`
- `TRINO_USER`
- `TRINO_CATALOG`
- `TRINO_SCHEMA`

## Notes

- The add-on should stay read-only.
- The add-on should use the official OpenAI Python SDK.
- The add-on should use the Responses API.
- The add-on should validate SQL before execution.
- The local `.env` file should never be committed.
- The default model for this add-on is `gpt-5-mini`.
