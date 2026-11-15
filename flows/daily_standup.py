"""The daily-standup flow: find a funny tech story, draft one-liners, wait for a human.

Nothing here posts anywhere. The flow ends by publishing a copy-ready artifact.
"""

import sys
from datetime import timedelta
from pathlib import Path
from uuid import UUID

# Prefect runs this file from a fresh clone; make the repo root importable.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import anthropic  # noqa: E402
from prefect import flow, get_run_logger, task  # noqa: E402
from prefect.blocks.system import Secret  # noqa: E402
from prefect.cache_policies import INPUTS  # noqa: E402
from prefect.exceptions import FlowPauseTimeout  # noqa: E402
from prefect.flow_runs import pause_flow_run, suspend_flow_run  # noqa: E402
from prefect.runtime import deployment, flow_run  # noqa: E402
from prefect.states import Completed  # noqa: E402
from prefect.tasks import exponential_backoff  # noqa: E402

from standup import llm, news, store, voice  # noqa: E402
from standup.approval import drafts_markdown, resolve_final  # noqa: E402
from standup.gifs import search_gifs  # noqa: E402
from standup.models import Approval, Draft, FinalPost, Pick, Story  # noqa: E402

ANTHROPIC_SECRET = "anthropic-api-key"
GIPHY_SECRET = "giphy-api-key"

MAX_CANDIDATES = 60
# A separate automation skips the run after 12 hours with no reply (scripts/setup_automations.py).
# This longer timeout is only a backstop in case that automation is missing.
APPROVAL_TIMEOUT_SECONDS = 13 * 60 * 60


def _claude() -> anthropic.Anthropic:
    return anthropic.Anthropic(api_key=Secret.load(ANTHROPIC_SECRET).get())


# --- tasks ------------------------------------------------------------------


@task(
    retries=3,
    retry_delay_seconds=exponential_backoff(backoff_factor=5),
    retry_jitter_factor=0.5,
    cache_policy=INPUTS,
    cache_expiration=timedelta(hours=3),
    persist_result=True,
    task_run_name="fetch-{source}",
)
def fetch_news(source: str, topic: str | None = None) -> list[Story]:
    """Headlines from one source. Retried with backoff; cached for 3 hours per (source, topic)."""
    stories = news.fetch_source(source, topic)
    get_run_logger().info("%s: %d stories", source, len(stories))
    return stories


@task(retries=1, retry_delay_seconds=10)
def pick_story(stories: list[Story], recent: list[str]) -> list[Pick]:
    """Claude Haiku picks the 3 stories with the best comedic angle."""
    picks = llm.pick_stories(_claude(), stories, recent)
    logger = get_run_logger()
    for p in picks:
        logger.info("Pick %d: %s (%s)", p.rank, p.title, p.reason)
    return picks


@task(retries=1, retry_delay_seconds=10)
def write_captions(pick: Pick, recent: list[str], feedback: list[str] | None = None) -> list[llm.Caption]:
    """Claude writes 3 options for one story, using style/voice.md."""
    return llm.write_captions(_claude(), pick, voice.load_voice(), recent, feedback)


@task(retries=1, retry_delay_seconds=10)
def voice_check(pick: Pick, captions: list[llm.Caption]) -> tuple[list[llm.Caption], list[str]]:
    """Drop drafts that fail the voice. Rules first (free), then an LLM judge on the survivors.

    Returns (passing captions, a line per rejected draft explaining why).
    """
    logger = get_run_logger()
    survivors: list[llm.Caption] = []
    rejected: list[str] = []
    for caption in captions:
        problems = voice.check_draft(caption.text)
        if problems:
            rejected.append(f"{caption.text!r} ({'; '.join(problems)})")
        else:
            survivors.append(caption)

    passing: list[llm.Caption] = []
    if survivors:
        verdicts = {
            v.index: v
            for v in llm.judge_captions(_claude(), pick, [c.text for c in survivors], voice.load_voice())
        }
        for i, caption in enumerate(survivors, 1):
            verdict = verdicts.get(i)
            if verdict and verdict.passes:
                passing.append(caption)
            else:
                reason = verdict.reason if verdict else "no verdict returned"
                rejected.append(f"{caption.text!r} ({reason})")

    for line in rejected:
        logger.info("Voice check dropped: %s", line)
    logger.info("Voice check: %d of %d drafts passed", len(passing), len(captions))
    return passing, rejected


@task(retries=2, retry_delay_seconds=exponential_backoff(backoff_factor=3))
def find_gif(query: str) -> list[str]:
    """Up to 3 GIF candidates from Giphy. Only called for drafts that asked for a GIF."""
    urls = search_gifs(Secret.load(GIPHY_SECRET).get(), query)
    get_run_logger().info("Giphy %r: %d candidates", query, len(urls))
    return urls


@task
def publish_picks(picks: list[Pick]) -> None:
    store.save_picks(picks)


@task
def publish_drafts(drafts: list[Draft]) -> None:
    """Write the drafts as an artifact tied to this run, for the UI and the MCP server."""
    store.save_drafts(drafts)


@task
def publish_artifact(final: FinalPost, story_title: str, story_url: str) -> None:
    """The copy-ready final post."""
    store.save_final(final, story_title, story_url)


# --- flow helpers -----------------------------------------------------------


def gather_news(topic: str | None) -> list[Story]:
    """Fetch every source concurrently. One source failing is fine; all failing is not."""
    logger = get_run_logger()
    futures = {source: fetch_news.submit(source, topic) for source in news.SOURCES}
    groups: list[list[Story]] = []
    for source, future in futures.items():
        try:
            groups.append(future.result())
        except Exception as exc:  # noqa: BLE001 - a dead feed should not sink the run
            logger.warning("Skipping %s after retries: %s", source, exc)
    if not groups:
        raise RuntimeError("Every news source failed.")
    stories = news.dedupe(news.interleave(groups))[:MAX_CANDIDATES]
    logger.info("%d candidate stories after dedupe", len(stories))
    return stories


def drafts_for_story(pick: Pick, round_no: int, recent: list[str]) -> list[Draft]:
    """Up to 3 drafts that pass the voice check. One rewrite attempt if the first batch falls short."""
    passing: list[llm.Caption] = []
    feedback: list[str] | None = None
    for _attempt in range(2):
        captions = write_captions(pick, recent, feedback)
        ok, rejected = voice_check(pick, captions)
        seen = {c.text for c in passing}
        passing += [c for c in ok if c.text not in seen]
        if len(passing) >= 3 or not rejected:
            break
        feedback = rejected + [f"{c.text!r} (already accepted; write something different)" for c in passing]

    drafts = []
    for option, caption in enumerate(passing[:3], 1):
        gif_urls: list[str] = []
        if caption.wants_gif and caption.gif_query.strip():
            try:
                gif_urls = find_gif(caption.gif_query.strip())
            except Exception as exc:  # noqa: BLE001 - a GIF is optional
                get_run_logger().warning("No GIF for option %d: %s", option, exc)
        fmt = caption.format if (caption.format != "gif" or gif_urls) else "repost"
        drafts.append(Draft(
            round=round_no, option=option, text=caption.text.strip(), format=fmt,
            story_title=pick.title, story_url=pick.url,
            gif_query=caption.gif_query if gif_urls else "", gif_urls=gif_urls,
        ))
    return drafts


def next_round(picks: list[Pick], used_urls: set[str], round_no: int, recent: list[str]) -> list[Draft]:
    """Drafts for the best story not used yet. Moves on if nothing passes for a story."""
    # Start after the last story that produced drafts, so a resumed run never retries
    # a story that was already used or already came up empty.
    last_used = max((p.rank for p in picks if p.url in used_urls), default=0)
    for pick in picks:
        if pick.rank <= last_used:
            continue
        used_urls.add(pick.url)
        drafts = drafts_for_story(pick, round_no, recent)
        if drafts:
            return drafts
        get_run_logger().info("No drafts passed for %r; trying the next story", pick.title)
    return []


def wait_for_approval(drafts: list[Draft], key: str, note: str) -> Approval | None:
    """Stop and wait for a human. Returns None if nobody answers in time.

    On a deployment this suspends: the process exits and the infrastructure is released,
    so waiting costs nothing. When resumed, Prefect starts the flow again from the top and
    this call returns the stored reply. A local run has no deployment to restart from, so
    it pauses in place instead.
    """
    request = Approval.with_initial_data(description=drafts_markdown(drafts, note))
    if deployment.id:
        return suspend_flow_run(wait_for_input=request, timeout=APPROVAL_TIMEOUT_SECONDS, key=key)
    try:
        return pause_flow_run(wait_for_input=request, timeout=APPROVAL_TIMEOUT_SECONDS, key=key)
    except FlowPauseTimeout:
        return None


def skipped(reason: str):
    get_run_logger().info("Skipped: %s", reason)
    return Completed(name="Skipped", message=reason)


# --- flow -------------------------------------------------------------------


@flow(name="daily-standup", log_prints=True)
def daily_standup(topic: str | None = None):
    """Draft today's post and wait for approval.

    Args:
        topic: optional words to steer the news search, e.g. "openai" or "aws outage".
    """
    logger = get_run_logger()
    state = store.load_run_state(UUID(str(flow_run.id)))
    recent = store.recent_posts(5)

    picks = state.picks
    if picks:
        logger.info("Resumed: reusing %d picks and %d round(s) of drafts", len(picks), len(state.rounds))
    else:
        stories = gather_news(topic)
        if not stories:
            return skipped("No stories found" + (f" for topic {topic!r}." if topic else "."))
        picks = pick_story(stories, recent)
        if not picks:
            return skipped("No story had a usable angle.")
        publish_picks(picks)

    used_urls = {d.story_url for drafts in state.rounds.values() for d in drafts}
    round_no = 0
    ask = 0
    while True:
        round_no += 1
        drafts = state.rounds.get(round_no)
        if drafts is None:
            drafts = next_round(picks, used_urls, round_no, recent)
            if not drafts:
                return skipped("No more stories with drafts that pass the voice check.")
            publish_drafts(drafts)
            state.rounds[round_no] = drafts

        note = ""
        while True:
            ask += 1
            approval = wait_for_approval(drafts, key=f"approval-{ask}", note=note)
            if approval is None:
                return skipped("No reply before the approval timeout.")
            if approval.action == "skip":
                return skipped("Skipped by request.")
            if approval.action == "regenerate":
                break
            try:
                final = resolve_final(drafts, approval)
            except ValueError as problem:
                note = str(problem)  # ask again, with the problem shown above the form
                continue
            publish_artifact(final, drafts[0].story_title, drafts[0].story_url)
            logger.info("Approved: %s", final.text)
            return final.text


@flow(name="daily-standup-fetch", log_prints=True)
def fetch_only(topic: str | None = None) -> int:
    """Only the news fetch. Shows retries and caching without any model calls or approval step."""
    return len(gather_news(topic))


if __name__ == "__main__":
    if "--fetch-only" in sys.argv:
        fetch_only()
    else:
        daily_standup()
