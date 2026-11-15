# Connecting Claude and Claude Duet

The Daily Standup server is a custom connector in Claude. Once connected, the regular Claude app can use it in any chat, and Claude Duet can be told to check it once a day.

## 1. Add the connector

1. Go to https://claude.ai/customize/connectors.
2. Click **+ Add**, then **Add custom connector**.
3. Name it `Daily Standup` and paste the server URL from FastMCP Gateway, including the `/mcp` at the end. Click **Continue**.
4. Accept the detected auth settings and click **Continue**.
5. Authentication: **Sign in now**. OAuth client: **Use Claude's published identity**. Click **Add**.
6. Complete the FastMCP Gateway sign-in when it appears.

If the sign-in fails, remove the connector and add it again with **Register automatically** as the OAuth client.

When the server's tools change, Claude can keep showing the old list. Disconnect and reconnect the connector to refresh it.

## 2. Check it in a regular chat

Open a new chat, turn the connector on (the **+** button, then **Connectors**), and ask:

> Check Daily Standup for drafts waiting on me.

Claude should call `get_drafts` and show up to three numbered options. Reply with a pick, such as "2", and it should call `approve_post` and give you the final text and link. This has been tested and works.

## 3. Set up Claude Duet

Claude Duet is in research preview, and whether it can use custom connectors had not been confirmed when this was written. Try step 2's request in Duet first. If Duet cannot see the connector, use the regular Claude app as in step 2; everything else works the same.

If it can, give Duet this standing instruction:

> Once a day at 9:45 AM Pacific, call get_drafts on the Daily Standup connector with no run ID.
>
> If a run is waiting for approval, show me the story and the numbered options in my brief, exactly as written, with any GIF candidates. Do not rewrite, rank, or comment on them.
>
> When I reply with a number, call approve_post with that run and choice, then show me the final text and the link or GIF to copy. If I reply with my own wording, pass it as the edit. If I say "regenerate", call regenerate, then check get_drafts once more about three minutes later and show me the new options. If I say "skip", call skip.
>
> Never call approve_post unless I have told you which option I want. Never post anything anywhere.
>
> If nothing is waiting, say so in one line and do not start a new run.

Why it is written this way:

- **9:45 AM, once a day.** The scheduled run starts at 9:30 AM Pacific and needs about two minutes to reach the approval step. One check a day keeps Duet's usage low.
- **"Exactly as written."** The drafts have already passed a voice check. A client that paraphrases them undoes that.
- **"Do not start a new run."** `generate_post` is for on-demand posts. The scheduled run already covers the morning.
- **No reply needed.** If you ignore the brief, the run is marked as skipped after 12 hours and nothing is posted.

## 4. On-demand posts

In any chat with the connector on:

> Generate a Daily Standup post about the AWS outage.

Claude calls `generate_post`, which returns straight away. Ask it to check the drafts about two minutes later.

## Troubleshooting

| What you see | What it means |
| --- | --- |
| "The server's environment settings are missing" | `PREFECT_API_URL` or `PREFECT_API_KEY` is not set on the FastMCP Gateway server. Add it and redeploy. |
| "PREFECT_API_URL is set but malformed" | The message says how. The value must be the `https://api.prefect.cloud/api/accounts/.../workspaces/...` address with no quotes or prefix. Redeploy after fixing it. |
| "is not waiting for approval" | The run was already approved, skipped, or timed out. Ask Claude to call `get_drafts` with no run ID. |
| Status stays `in_progress` | The run is starting up. It takes about two minutes. Check the run in the Prefect UI if it lasts longer than five. |
| Claude shows tools that no longer exist | Disconnect and reconnect the connector. |
