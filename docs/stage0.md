# Stage 0: connectivity spike

Goal: prove Claude → FastMCP server → Prefect Cloud → Prefect Managed compute all connect, before building anything real.

You set every secret yourself. Names expected:

| Where | Name | What it is |
| --- | --- | --- |
| FastMCP Gateway server environment | `PREFECT_API_URL` | `https://api.prefect.cloud/api/accounts/<account-id>/workspaces/<workspace-id>` |
| FastMCP Gateway server environment | `PREFECT_API_KEY` | A Prefect Cloud API key |

Stage 1 adds two Prefect Secret blocks: `anthropic-api-key` and `giphy-api-key`.

## 1. GitHub repo (done)

The code is at https://github.com/MikeFindsThings/daily-standup (public). Prefect Managed clones it without credentials.

## 2. Prefect Cloud

```bash
uv run prefect cloud login
uv run prefect work-pool create managed-pool --type prefect:managed
uv run prefect deploy --all
uv run prefect deployment run hello/stage0 --watch
```

Pass: the run reaches `Completed` and the log shows the hello message.

## 3. FastMCP Gateway

1. Sign in at https://horizon.prefect.io with GitHub and select the `daily-standup` repo.
2. Entrypoint: `mcp_server/server.py:mcp`
3. Authentication: **on**.
4. Add `PREFECT_API_URL` and `PREFECT_API_KEY` in the server's environment settings.
5. Look for a preview-deployment setting. The repo is public, so pull requests from strangers should not get preview deployments that can see these variables. Turn previews off or limit them to your own branches if the option exists, and tell me what you find.
6. Deploy Server. Note the URL (documented form: `https://<server-name>.fastmcp.app/mcp`).
7. In the Inspector, run `ping`, then `trigger_hello_flow`.

Pass: `ping` returns a timestamp, and `trigger_hello_flow` returns a flow run ID that shows up in the Prefect UI.

## 4. Claude custom connector

1. https://claude.ai/customize/connectors → **+ Add** → **Add custom connector**.
2. Name it, paste the server URL including `/mcp`, Continue.
3. Review the detected auth settings, Continue.
4. Authentication: **Sign in now**. OAuth client: try **Use Claude's published identity** first; if sign-in fails, remove the connector and re-add it with **Register automatically**.
5. Add, then complete the FastMCP Gateway sign-in.
6. In a new chat, turn the connector on (+ → Connectors) and ask: "Call ping, then trigger the hello flow."

## 5. Claude Duet

Ask Duet to call `ping` on the connector. If it can't see custom connectors, we use the regular Claude app as the client and move on.

## What to report back

- Did step 2 complete? (If the work pool create fails with a plan limit, tell me the message.)
- Where did FastMCP Gateway put environment variables, and did the auth toggle work with Claude's sign-in?
- Could Duet call `ping`?

## Results (2026-11-14)

| Link | Result |
| --- | --- |
| Flow on Prefect Managed compute | Works. About 45 seconds of container start-up per run; the flow itself takes under a second. |
| FastMCP server on FastMCP Gateway | Works. Auto-redeploys on push to `main`. |
| Server triggers a Prefect run | Works, after a manual redeploy to pick up newly added environment variables. |
| Claude custom connector | Works in the regular Claude app. Adding the connector required a FastMCP Gateway sign-in, so auth is enforced. |
| Claude Duet | Not tested yet. |

Notes:

- Creating a Prefect Managed work pool on the Hobby plan worked with no plan-limit error.
- Changing environment variables in FastMCP Gateway does not take effect until the server is redeployed.
- No preview-deployment setting was found in the FastMCP Gateway dashboard, so whether pull requests from forks get a deployment is still unconfirmed.
