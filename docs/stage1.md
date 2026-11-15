# Stage 1: the Prefect flow

The flow in `flows/daily_standup.py` drafts a post and waits for a human. It is deployed as `daily-standup/daily` on the Prefect Managed pool and runs every day at 9:30 AM Pacific.

## Steps

| Step | What it does | Prefect feature |
| --- | --- | --- |
| `fetch_news` | Headlines from Hacker News, The Verge, Ars Technica, and TechCrunch, one task per source, run concurrently. An optional `topic` steers the search. | Retries with exponential backoff and jitter; 3-hour cache keyed on inputs |
| `pick_story` | Claude Haiku 5.5 picks the 3 stories with the best comedic angle. | Task retries |
| `write_captions` | Claude Opus 5.5 writes 3 options from `style/voice.md`. | Secret block for the API key |
| `voice_check` | Rule checks (length, banned phrases, hashtags, em dashes), then a Claude Haiku 5.5 judge. Failures are dropped and one rewrite is attempted. | Task logs show why each draft was dropped |
| `find_gif` | Giphy search, only for drafts that asked for a GIF. | Retries; Secret block |
| `publish_drafts` | Drafts written as a table artifact tied to the run. | Artifacts |
| approval | The run suspends and waits for a typed reply: approve, regenerate, or skip. | `suspend_flow_run` with a `RunInput` |
| `publish_artifact` | The final post as a copy-ready markdown artifact. | Artifacts, versioned by key |

## Why suspend, not pause

`pause_flow_run` keeps the process alive, so a managed container would bill minutes for the whole wait. `suspend_flow_run` exits the process and releases the container. When the run is resumed, Prefect starts the flow again from the top in a new container, so the flow saves its picks and drafts as artifacts and reuses them on every start. A local run has no deployment to restart from, so it pauses in place instead.

## Automations

Created by `scripts/setup_automations.py`:

- **Skip when unanswered:** a run suspended for 12 hours is cancelled and labelled "Skipped". Without this, Prefect marks a timed-out suspension as Failed.
- **Notify on failure:** sends a notification through the `standup-alerts` Email block when a run fails or crashes.

## Running it

```bash
uv run prefect deployment run daily-standup/daily              # on Prefect Managed
uv run prefect deployment run daily-standup/daily -p topic=aws # steer the news search
uv run python flows/daily_standup.py                           # locally (pauses instead of suspending)
uv run python flows/daily_standup.py --fetch-only              # only the news fetch: retries and caching
uv run pytest
```

Approve from the Prefect UI: open the run, click Resume, fill in the form.

## Results (2026-11-14)

- A managed run reaches the approval step in about 100 seconds; most of that is container start-up and package install. Each resume takes about a minute.
- Regenerate, approve, skip, and an invalid choice were all exercised. On resume, the news fetch and story pick were not repeated.
- One measured run cost about $0.05 in model usage. Each Claude call logs its tokens and cost.
- The 3-hour news cache works on a local machine. On Prefect Managed it does not carry over between runs, because the cache is stored on the container's disk.
- The 12-hour skip automation has been created but not yet observed firing.
