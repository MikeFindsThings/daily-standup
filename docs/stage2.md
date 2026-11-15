# Stage 2: the FastMCP server

`mcp_server/server.py` runs on FastMCP Gateway and gives an AI client controlled access to the daily-standup flow. It is a thin layer: it starts runs, reads the artifacts the flow published, and sends the human's reply to a suspended run. All real work stays in the flow.

## Tools

| Tool | What it does |
| --- | --- |
| `generate_post(topic)` | Starts a run and returns its ID immediately. Drafts are ready about two minutes later. |
| `get_drafts(run_id)` | Drafts for a run waiting on approval, or the run's status. With no ID, finds the run currently waiting, such as the scheduled morning run. |
| `approve_post(run_id, choice, edit, gif)` | Sends the pick to the suspended run and returns the final post immediately. |
| `regenerate(run_id)` | Asks for new drafts on the next-best story. |
| `skip(run_id)` | Ends the run with no post. |
| `recent_posts(n)` | Past approved posts, newest first, so the client can avoid repeats. |
| `ping()` | Health check. |

## Why every tool returns immediately

A run takes about 85 to 100 seconds to reach the approval step, mostly container start-up, and each resume takes about a minute. Holding a connector call open that long risks a client timeout that looks like a failure while the run is fine. So `generate_post` and `regenerate` hand back a run ID and say when to call `get_drafts`.

`approve_post` can still return the final post at once: the server works it out with the same function the flow uses (`standup/approval.py`), then resumes the run, which records the same result as an artifact about a minute later.

## Guard rails

- Auth is enforced by FastMCP Gateway; only signed-in members of the organization can connect.
- Tools only act on runs of the `daily-standup` flow. A run ID from any other flow is reported as not found.
- An approval is checked against the drafts on offer before anything is sent to Prefect: the option must exist, a GIF must be a candidate number, a URL, or "none".
- Errors say what to do next, for example "Run X is not waiting for approval (state: Completed). Call get_drafts to see what is waiting".
- Problems with the server's Prefect settings are described without revealing the values.

## Secrets

Set in the FastMCP Gateway server's environment settings, never in the repo: `PREFECT_API_URL` and `PREFECT_API_KEY`. Changing them requires a redeploy.

## Results (2026-11-15)

- 77 tests pass, including 34 for the tools with Prefect replaced by a fake.
- Live run against the real deployment through the MCP protocol: `generate_post` returned in 1 second, drafts were ready at 84 seconds, `regenerate` produced a second round on the next story, an invalid choice was rejected with a clear message, and `approve_post` returned the final post at once. `get_drafts` then showed the run as approved.
- The same tools were then confirmed working from the regular Claude app through the deployed connector.
