"""The Daily Standup MCP server: controlled access to the daily-standup Prefect flow.

Deployed to FastMCP Gateway with entrypoint `mcp_server/server.py:mcp`. Expects
PREFECT_API_URL and PREFECT_API_KEY in the host's environment settings.

The server is a thin layer. It starts runs, reads what the flow published, and sends the
human's reply back to a suspended run. All real work happens in the flow. Every tool
returns quickly: a run takes a minute or two to reach the approval step, so tools hand
back a run ID and the caller checks in again, instead of holding a connection open.
"""

import os
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Annotated
from uuid import UUID

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def _clean(value: str | None) -> str:
    """Strip whitespace and wrapping quotes that often sneak in when pasting env values."""
    return (value or "").strip().strip("'\"").strip()


def _normalize_prefect_env() -> None:
    """Tidy the two Prefect settings in place, before Prefect reads them."""
    url = _clean(os.environ.get("PREFECT_API_URL"))
    if url.upper().startswith("PREFECT_API_URL="):
        url = _clean(url.split("=", 1)[1])
    if url:
        os.environ["PREFECT_API_URL"] = url.rstrip("/")
    key = _clean(os.environ.get("PREFECT_API_KEY"))
    if key:
        os.environ["PREFECT_API_KEY"] = key


_normalize_prefect_env()

from fastmcp import FastMCP  # noqa: E402
from fastmcp.exceptions import ToolError  # noqa: E402
from pydantic import Field, ValidationError  # noqa: E402

from mcp_server import prefect_api as api  # noqa: E402
from standup import store  # noqa: E402
from standup.approval import resolve_final  # noqa: E402
from standup.models import Approval, Draft  # noqa: E402

MAX_TOPIC_CHARS = 80
MAX_EDIT_CHARS = 600
MAX_RECENT = 20

mcp = FastMCP(
    "Daily Standup",
    instructions=(
        "Controls a Prefect flow that drafts short, deadpan tech-news LinkedIn posts in one person's voice. "
        "Nothing is ever posted automatically: the flow waits for the human to approve a draft, and the "
        "result is text for them to post themselves. Typical use: call get_drafts to see the options "
        "waiting for approval, show them to the human, then call approve_post, regenerate, or skip with "
        "their decision. Never approve on the human's behalf without being told which option they want."
    ),
)


# --- validation helpers (pure; covered by tests) -----------------------------


def check_prefect_settings() -> None:
    """Raise a ToolError that says what is wrong with the settings, without revealing them."""
    url, key = os.environ.get("PREFECT_API_URL", ""), os.environ.get("PREFECT_API_KEY", "")
    missing = [name for name, value in (("PREFECT_API_URL", url), ("PREFECT_API_KEY", key)) if not value]
    if missing:
        raise ToolError(f"The server's environment settings are missing: {', '.join(missing)}.")
    if not url.startswith("https://api.prefect.cloud/api/accounts/"):
        if url.startswith(("pnu_", "pnb_")):
            hint = "it looks like an API key; the URL and key may be swapped"
        elif "app.prefect.cloud" in url:
            hint = "it is the browser address (app.prefect.cloud); use the api.prefect.cloud/api/accounts/... form"
        elif "://" not in url:
            hint = "it has no https:// at the start"
        else:
            hint = "it is not a Prefect Cloud API address"
        raise ToolError(
            f"PREFECT_API_URL is set but malformed ({len(url)} characters; {hint}). "
            "Expected https://api.prefect.cloud/api/accounts/<account-id>/workspaces/<workspace-id>."
        )


def parse_run_id(run_id: str) -> UUID:
    try:
        return UUID(str(run_id).strip())
    except (ValueError, AttributeError):
        raise ToolError(
            f"{run_id!r} is not a valid run ID. Use the run_id returned by generate_post or get_drafts."
        ) from None


def clean_topic(topic: str | None) -> str | None:
    topic = " ".join((topic or "").split())
    if not topic:
        return None
    if len(topic) > MAX_TOPIC_CHARS:
        raise ToolError(f"Topic is too long ({len(topic)} characters; limit {MAX_TOPIC_CHARS}). Use a few keywords.")
    return topic


def build_approval(drafts: list[Draft], choice: int, edit: str | None, gif: str | None) -> tuple[dict, dict]:
    """Check an approval against the drafts on offer.

    Returns (the reply to send to the flow, the final post it will produce). Raises a
    ToolError the caller can act on, before anything is sent to Prefect.
    """
    edit = (edit or "").strip() or None
    if edit and len(edit) > MAX_EDIT_CHARS:
        raise ToolError(f"Edit is too long ({len(edit)} characters; limit {MAX_EDIT_CHARS}).")
    try:
        approval = Approval(action="approve", choice=choice, edit=edit, gif=(gif or "").strip() or None)
        final = resolve_final(drafts, approval)
    except ValidationError:
        options = ", ".join(str(d.option) for d in drafts)
        raise ToolError(f"Choice must be one of the option numbers on offer: {options}.") from None
    except ValueError as problem:
        raise ToolError(str(problem)) from None
    reply = {"action": "approve", "choice": approval.choice, "edit": approval.edit, "gif": approval.gif}
    return reply, final_post_dict(final.text, final.story_url, final.gif_url, final.format)


def final_post_dict(text: str, link: str | None, gif: str | None, post_format: str) -> dict:
    return {"text": text, "format": post_format, "link_to_share": link, "gif": gif}


def draft_dict(d: Draft) -> dict:
    return {"option": d.option, "text": d.text, "format": d.format, "gif_candidates": d.gif_urls}


def ui_url(run_id: UUID) -> str | None:
    base = os.environ.get("PREFECT_API_URL", "")
    if not base.startswith("https://api.prefect.cloud/api/accounts/"):
        return None
    base = base.replace("https://api.prefect.cloud/api/accounts/", "https://app.prefect.cloud/account/")
    return f"{base.replace('/workspaces/', '/workspace/')}/runs/flow-run/{run_id}"


# --- shared steps -------------------------------------------------------------


async def _read_run(run_id: str):
    check_prefect_settings()
    rid = parse_run_id(run_id)
    try:
        return await api.read_run(rid)
    except api.NotOurRun:
        raise ToolError(f"No daily-standup run with ID {rid}. Call get_drafts with no run_id to find the current one.") from None


async def _waiting_drafts(run_id: str) -> tuple[UUID, list[Draft]]:
    """The drafts of a run that is waiting for approval, or a ToolError saying why not."""
    run = await _read_run(run_id)
    if not (run.state and run.state.is_paused()):
        state = run.state.name if run.state else "unknown"
        raise ToolError(
            f"Run {run.name} is not waiting for approval (state: {state}). "
            "Call get_drafts to see what is waiting, or generate_post to start a new run."
        )
    rounds = store.state_from_artifacts(await api.run_artifacts(run.id)).rounds
    if not rounds:
        raise ToolError(f"Run {run.name} is waiting but has no drafts published. Check it in the Prefect UI.")
    return run.id, rounds[max(rounds)]


async def _send(run_id: UUID, reply: dict) -> None:
    try:
        await api.send_reply(run_id, reply)
    except api.ResumeRejected as rejected:
        raise ToolError(f"Prefect did not accept the reply: {rejected}. Call get_drafts to see the run's state.") from None


async def _describe(run) -> dict:
    """What a caller needs to know about one run, whatever state it is in."""
    state = run.state
    base = {"run_id": str(run.id), "run_name": run.name, "prefect_url": ui_url(run.id)}
    if state is None or state.is_scheduled() or state.is_pending() or state.is_running():
        return {**base, "status": "in_progress",
                "message": "The run is still working. Call get_drafts again in about a minute."}

    artifacts = await api.run_artifacts(run.id)
    if state.is_paused():
        rounds = store.state_from_artifacts(artifacts).rounds
        if not rounds:
            return {**base, "status": "in_progress", "message": "Drafts are not published yet. Try again shortly."}
        drafts = rounds[max(rounds)]
        return {
            **base, "status": "waiting_for_approval", "round": max(rounds),
            "story": {"title": drafts[0].story_title, "url": drafts[0].story_url},
            "drafts": [draft_dict(d) for d in drafts],
            "message": "Show these to the human. Then call approve_post, regenerate, or skip with their decision.",
        }
    if state.name == "Skipped":
        return {**base, "status": "skipped", "message": state.message or "No post for this run."}
    if state.is_completed():
        final = store.final_from_artifacts(artifacts)
        if final:
            return {**base, "status": "approved", "final_post_text": final["text"],
                    "final_post_markdown": final["markdown"],
                    "message": "Approved. This is text for the human to post themselves; nothing was posted."}
        return {**base, "status": "finished", "message": "The run finished without a post."}
    return {**base, "status": "failed", "state": state.name, "message": state.message or "The run did not finish."}


# --- tools --------------------------------------------------------------------


@mcp.tool
def ping() -> dict:
    """Health check. Returns the server's current UTC time. Use this to confirm the connector works."""
    return {"ok": True, "utc_time": datetime.now(timezone.utc).isoformat()}


@mcp.tool
async def generate_post(
    topic: Annotated[str | None, Field(description="Optional keywords to steer the news search, e.g. 'aws outage'. Omit for general tech news.")] = None,
) -> dict:
    """Start a new run that finds a tech story and drafts post options. Returns immediately.

    The drafts are NOT in this response. The run takes about two minutes to reach the
    approval step: wait, then call get_drafts with the returned run_id. Use this for an
    on-demand post; the scheduled morning run does not need it.
    """
    check_prefect_settings()
    run = await api.start_run(clean_topic(topic))
    return {
        "run_id": str(run.id), "run_name": run.name, "status": "in_progress", "prefect_url": ui_url(run.id),
        "message": "Run started. Drafts take about two minutes. Then call get_drafts with this run_id.",
    }


@mcp.tool
async def get_drafts(
    run_id: Annotated[str | None, Field(description="A run ID from generate_post. Omit to find the run currently waiting for approval, such as the scheduled morning run.")] = None,
) -> dict:
    """Get the draft post options for a run, or its current status.

    With no run_id, returns the most recent run waiting for approval. The status field is
    one of: waiting_for_approval (drafts included), in_progress (check again in a minute),
    approved (final post included), skipped, failed, or none_waiting.
    """
    check_prefect_settings()
    if run_id:
        return await _describe(await _read_run(run_id))

    waiting = await api.waiting_runs()
    if waiting:
        result = await _describe(waiting[0])
        if len(waiting) > 1:
            result["other_waiting_run_ids"] = [str(r.id) for r in waiting[1:]]
        return result
    latest = await api.latest_run()
    if latest is None:
        return {"status": "none_waiting", "message": "No runs yet. Call generate_post to start one."}
    return {
        "status": "none_waiting", "latest_run": await _describe(latest),
        "message": "No run is waiting for approval. latest_run shows the most recent one. Call generate_post to start a new run.",
    }


@mcp.tool
async def approve_post(
    run_id: Annotated[str, Field(description="The run waiting for approval, from get_drafts.")],
    choice: Annotated[int, Field(description="The option number the human picked (1, 2, or 3).")],
    edit: Annotated[str | None, Field(description="Optional. The human's own wording, used instead of the option's text.")] = None,
    gif: Annotated[str | None, Field(description="Optional. A GIF candidate number, a GIF URL, or 'none' to drop the GIF.")] = None,
) -> dict:
    """Approve one draft, as chosen by the human. Returns the final post immediately.

    Only call this with the human's explicit pick. The response contains the final text
    (plus the story link or GIF when there is one) for the human to copy and post
    themselves. Nothing is posted automatically. The flow finishes recording the approval
    in the background within about a minute.
    """
    rid, drafts = await _waiting_drafts(run_id)
    reply, final = build_approval(drafts, choice, edit, gif)
    await _send(rid, reply)
    return {
        "run_id": str(rid), "status": "approved", "final_post": final,
        "message": "Approved. Give the human this text to post themselves. Nothing was posted automatically.",
    }


@mcp.tool
async def regenerate(
    run_id: Annotated[str, Field(description="The run waiting for approval, from get_drafts.")],
) -> dict:
    """Reject the current drafts and ask for new ones on the next-best story. Returns immediately.

    New drafts take about two minutes: wait, then call get_drafts with the same run_id.
    There are at most 3 stories per run; after that the run ends as skipped.
    """
    rid, _ = await _waiting_drafts(run_id)
    await _send(rid, {"action": "regenerate", "choice": 1})
    return {"run_id": str(rid), "status": "in_progress",
            "message": "New drafts requested. Call get_drafts with this run_id in about two minutes."}


@mcp.tool
async def skip(
    run_id: Annotated[str, Field(description="The run waiting for approval, from get_drafts.")],
) -> dict:
    """End a waiting run with no post today. Use when the human doesn't want any of the options."""
    rid, _ = await _waiting_drafts(run_id)
    await _send(rid, {"action": "skip", "choice": 1})
    return {"run_id": str(rid), "status": "skipped", "message": "Skipped. No post for this run."}


@mcp.tool
async def recent_posts(
    n: Annotated[int, Field(description="How many posts to return, newest first.", ge=1, le=MAX_RECENT)] = 5,
) -> dict:
    """List recently approved posts, newest first. Use it to avoid repeating a joke or topic."""
    check_prefect_settings()
    posts = await api.approved_posts(n)
    return {"count": len(posts), "posts": posts}


if __name__ == "__main__":
    mcp.run(transport="http", port=8000)
