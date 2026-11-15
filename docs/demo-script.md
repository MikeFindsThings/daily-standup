# Demo script: five minutes, screen share

The story in one line: a scheduled Prefect flow does the work and waits for a human, and the same approval can be given from the Prefect UI or from Claude through a governed MCP server.

## Before the call

Do these in order. The whole list takes about ten minutes.

1. **Start run A about 10 minutes before**, so drafts are waiting when you begin:
   ```bash
   uv run prefect deployment run daily-standup/daily
   ```
   Confirm in the Prefect UI that it shows as **Suspended**.
2. **Make sure a finished run exists** with an approved post, to show the final artifact without waiting. Any earlier approved run works.
3. **Warm the local cache** for the caching beat. This runs only the news fetch, with no model calls and no approval step:
   ```bash
   uv run python flows/daily_standup.py --fetch-only
   ```
   The cache lasts 3 hours.
4. **Open these tabs**, in this order:
   - Prefect UI: the `daily-standup/daily` deployment page
   - Prefect UI: run A (suspended)
   - Prefect UI: the finished run's Artifacts tab
   - Prefect UI: Automations
   - GitHub: `flows/daily_standup.py`
   - Claude: a new chat with the Daily Standup connector turned on
   - A terminal in the repo folder
5. **Check the connector** by asking Claude to call `ping`. If it shows stale tools, disconnect and reconnect it.
6. **Close** anything with notifications, and any tab showing API keys or account settings.

## The walkthrough

### 0:00 What it is (30 seconds)

Say: "This is a bot that reads the morning's tech news and drafts one-line jokes for LinkedIn in my voice. It never posts. It leaves drafts waiting, and I approve one. I built it to show a Prefect flow, a FastMCP server, and Claude working as one system."

Show: the README's architecture diagram, briefly.

### 0:30 The deployment and the trigger (45 seconds)

Show: the deployment page.

Point at:
- The **schedule**, 9:30 AM Pacific every day.
- The **work pool**, Prefect Managed. "There is no infrastructure of mine here. Each run is a fresh container that clones the repo."
- Click **Run** to start run B. "I'll come back to this one. It takes about ninety seconds to start, which matters for a design decision later."

### 1:15 Retries and caching (60 seconds)

Show: `flows/daily_standup.py` on GitHub, the `fetch_news` task decorator.

Say: "Each news source is its own task with three retries, exponential backoff, and jitter. If one feed is down it retries alone, and if it stays down the run carries on without it."

Show: the terminal. Run only the news fetch:
```bash
uv run python flows/daily_standup.py --fetch-only
```
Point at the four `fetch-...` lines finishing in state **Cached**. "The fetch is cached for three hours on its inputs, so iterating on the prompt doesn't re-hit the feeds."

Be straight about the limit if asked: "On managed compute the cache is on the container's disk, so it doesn't carry between cloud runs. Sharing it needs a results bucket, which I left out."

### 2:15 The pause (75 seconds)

Show: run A, suspended.

Point at:
- The **task runs**: fetch, pick, write, voice check. Open the voice check's logs if a draft was dropped; the reason is there.
- The **state**: Suspended. "This is the decision I'd want to talk about. Pause keeps the process alive, so on serverless I'd pay for a container to wait for me all morning. Suspend exits and releases it."
- "The catch is that a resumed run starts from the top in a new container. So the flow saves its picks and drafts as artifacts and reuses them, otherwise I'd approve one joke and get a different one."

Click **Resume**. Show the form: the drafts at the top, then action, choice, edit, and GIF. "This is a typed input, so the UI builds the form from the model." Do not submit it. Close the form.

### 3:30 The same approval, from Claude (60 seconds)

Show: the Claude chat.

Type: `Check Daily Standup for drafts waiting on me.`

Claude calls `get_drafts` and shows the same three options from run A.

Type the number of the one you like.

Claude calls `approve_post` and returns the final text and link. Say: "That went through a FastMCP server hosted on FastMCP Gateway. Sign-in is handled by the gateway, so only I can call it. The server is thin: it reads the artifacts and resumes the run. And the tools return immediately instead of waiting, because of that ninety-second start-up."

### 4:30 The artifact and the automations (30 seconds)

Show: run A in the Prefect UI, now running or completed, then the finished run's Artifacts tab with the copy-ready post.

Show: the Automations page. "Two automations. One notifies me if a run fails. The other marks a run as skipped if I ignore it for twelve hours, because a timed-out suspension would otherwise be reported as a failure."

Close: "Nothing was posted. That's the point of the design: the model drafts, Prefect holds the state, and a human decides."

## If something goes wrong

| Problem | What to do |
| --- | --- |
| Claude Duet can't see the connector | Use the regular Claude app. The script above already does; Duet is only worth showing if it was confirmed working beforehand. |
| The Claude connector fails or is slow | Do the approval in the Prefect UI: on run A, click Resume, pick an option, submit. Say: "The MCP server is a convenience. The approval is a Prefect feature and works without it." |
| Run A is not suspended yet | Use run B's deployment page to talk about cold start while you wait, or switch to the finished run and show its artifacts first. |
| Run A failed | Show it. Open the failed task's logs and the retries, then the failure automation. A failure with good visibility is a fair demo of an orchestrator. |
| No story passed the voice check | The run ends as Skipped with the reason in the logs. Show the dropped drafts and their reasons, then use the finished run for the artifact. |
| Prefect Cloud is unreachable | Walk through the code and run the tests (`uv run pytest`), which need no network. |

## Questions to expect

- **Why not let it post automatically?** A joke that misses is public and has my name on it. The approval step is cheap and the downside of skipping it is not.
- **Why artifacts for state, not a database or a Variable?** The drafts are already something I want to see in the UI, tied to the run. Using them as the state means one copy, no extra service, and the MCP server reads the same thing I do.
- **Why does regenerate switch stories?** If three jokes on a story didn't land, a fourth usually won't. The flow already ranked three stories, so it moves to the next.
- **What does it cost?** About five cents a run in model usage, measured, and a few minutes of serverless compute.
- **What would you change at scale?** Results storage in a bucket so caching works across runs, a service account in place of a personal API key for the server, and evaluation of the voice check against a labelled set of good and bad drafts.
