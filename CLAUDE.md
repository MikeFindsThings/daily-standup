# daily-standup

Public repo. A Prefect flow drafts short tech-news LinkedIn posts in the owner's voice; a FastMCP server on FastMCP Gateway lets Claude show drafts and send back an approval. Nothing is ever posted automatically.

## Secrets: hard rules

This repo is public. Anything committed is published permanently, including in history.

- Never read, print, or modify `.env`, `.env.*`, `*.swp`, or any token or credential file.
- Never write a real secret value anywhere in the repo: not in code, config, docs, tests, comments, or commit messages. Refer to secrets by name only.
- Never accept a secret value pasted into the conversation as something to store in a file. Tell the owner where to set it instead.
- Secrets live in two places, set by the owner:
  - Prefect Secret blocks: `anthropic-api-key`, `giphy-api-key`
  - FastMCP Gateway server environment: `PREFECT_API_URL`, `PREFECT_API_KEY`
- Account and workspace IDs, API URLs containing them, and server URLs with tokens count as secrets here. Use placeholders like `<account-id>`.
- Tests use fake values that do not match real key formats, and never call live APIs with real keys.

## Before every commit

1. Run `git status --short` and read the staged file list. Stage files by name; do not use `git add -A` or `git add .` without reading that list first.
2. The pre-commit hook in `.githooks/` runs gitleaks and blocks credential-type filenames. If `git config core.hooksPath` is not `.githooks`, set it before committing.
3. Never bypass the hook (`--no-verify`) and never add a gitleaks allowlist entry without the owner's explicit approval.
4. If a secret is ever committed or pushed: stop, tell the owner immediately, and treat the secret as leaked. It must be rotated; rewriting history is not enough.

## Other rules

- No auto-posting to LinkedIn. Output only.
- No personal data, nothing from the owner's other projects, nothing from their employer.
- Work in stages and stop after each one to report.
- Check current Prefect, and FastMCP (including FastMCP Gateway) docs before relying on memory.

## Naming

The hosting product is called **FastMCP Gateway** (confirmed in its dashboard, November 2026). It was formerly Horizon and before that FastMCP Cloud; the sign-in address is still horizon.prefect.io. Use "FastMCP Gateway" in all docs and code comments.
