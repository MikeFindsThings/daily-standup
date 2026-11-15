# daily-standup

A bot that reads the morning's tech news and drafts one-line LinkedIn jokes about it, in my voice. It never posts anything. I pick a draft, and it hands me the final text to post myself.

**Status: in progress.** The flow runs daily on Prefect Cloud and waits for approval in the Prefect UI. The Claude-facing tools are next.

## How it fits together

- **Prefect Cloud** runs the flow on a daily schedule: fetch news, pick a story, write captions, check them against my voice, then pause for approval.
- **A FastMCP server on FastMCP Gateway** gives an AI client controlled access to that flow: start a run, read the drafts, approve or skip.
- **Claude** calls the server, shows me the drafts, and sends back my pick.

A full write-up, architecture notes, and a demo script will land here when the build is finished.
