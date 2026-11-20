"""PostHog product analytics and LLM analytics for the daily-standup flow.

Ground rules (docs/Daily Standup PostHog tracking plan.md):

- Analytics never breaks a run. Nothing here raises. A missing token, a PostHog outage
  or a bad call logs one warning, switches analytics off for the rest of the process,
  and the flow carries on.
- No content leaves the flow. Callers pass counts, ranks, categories and timings only;
  never post text, captions, headlines, prompts or GIF links.
- Everything is a no-op until `configure` is called, so the MCP server and the tests
  send nothing.

Each flow run is its own anonymous distinct ID (`run-<flow run id>`) with person
profiles off: these are backend events with no human user behind them.
"""

import logging
from typing import Any

POSTHOG_HOST = "https://us.i.posthog.com"
TIMEOUT_SECONDS = 5  # sync mode blocks on each send, so keep a dead endpoint cheap

log = logging.getLogger(__name__)

_client: Any = None
_distinct_id: str | None = None
_base: dict[str, Any] = {}
_warned = False


def configure(token: str | None, flow_run_id: str, deployment: str, client: Any = None) -> bool:
    """Turn analytics on for this process. Returns True when events will be sent.

    `client` is for tests; normally a PostHog client is built from `token`.
    """
    global _client, _distinct_id, _base, _warned
    _client, _distinct_id, _base, _warned = None, None, {}, False
    if client is None:
        token = (token or "").strip()
        if not token:
            _warn("no PostHog project token; analytics is off for this run")
            return False
        try:
            from posthog import Posthog

            client = Posthog(
                token, host=POSTHOG_HOST, sync_mode=True, timeout=TIMEOUT_SECONDS, max_retries=0
            )
        except Exception as exc:  # noqa: BLE001
            _warn(f"PostHog client could not start ({type(exc).__name__}); analytics is off")
            return False
    _client = client
    _distinct_id = f"run-{flow_run_id}"
    _base = {"flow_run_id": str(flow_run_id), "deployment": deployment}
    return True


def reset() -> None:
    """Turn analytics off. Used by tests."""
    global _client, _distinct_id, _base, _warned
    _client, _distinct_id, _base, _warned = None, None, {}, False


def is_configured() -> bool:
    return _client is not None


def trace_id() -> str | None:
    return _base.get("flow_run_id")


def capture(event: str, **props: Any) -> None:
    """Send one event. A no-op before `configure`; never raises."""
    global _client
    if _client is None:
        return
    try:
        _client.capture(
            event,
            distinct_id=_distinct_id,
            properties={**_base, **props},
            options={"process_person_profile": False},
        )
    except Exception as exc:  # noqa: BLE001 - analytics must never break a run
        _client = None
        _warn(f"PostHog capture failed ({type(exc).__name__}); analytics is off for the rest of this run")


def ai_generation(
    *,
    span_name: str,
    model: str,
    input_tokens: int = 0,
    output_tokens: int = 0,
    latency: float,
    stop_reason: str | None = None,
    input_cost: float = 0.0,
    output_cost: float = 0.0,
    error: str | None = None,
) -> None:
    """One `$ai_generation` event for a Claude call. Prompts and replies are never sent."""
    props: dict[str, Any] = {
        "$ai_trace_id": trace_id(),
        "$ai_span_name": span_name,
        "$ai_provider": "anthropic",
        "$ai_model": model,
        "$ai_input_tokens": input_tokens,
        "$ai_output_tokens": output_tokens,
        "$ai_latency": round(latency, 3),
        "$ai_stop_reason": stop_reason,
        "$ai_input_cost_usd": round(input_cost, 6),
        "$ai_output_cost_usd": round(output_cost, 6),
        "$ai_total_cost_usd": round(input_cost + output_cost, 6),
        "$ai_is_error": error is not None,
    }
    if error is not None:
        props["$ai_error"] = error
    capture("$ai_generation", **props)


def _warn(message: str) -> None:
    """Log at most one analytics warning per run."""
    global _warned
    if _warned:
        return
    _warned = True
    try:
        from prefect import get_run_logger

        get_run_logger().warning(message)
    except Exception:  # noqa: BLE001 - no run context (tests, MCP server)
        log.warning(message)


# --- pure helpers (tested) -----------------------------------------------------


def trigger_from_tags(tags: list[str] | None) -> str:
    """How a run was started: Prefect's scheduler tags its runs `auto-scheduled`, the MCP
    server tags the runs it starts `mcp`, and anything else was started by hand."""
    tags = set(tags or [])
    if "auto-scheduled" in tags:
        return "schedule"
    if "mcp" in tags:
        return "mcp"
    return "manual"


DROP_REASONS = {
    "empty": "empty",
    "too long": "too_long",
    "too many sentences": "too_many_sentences",
    "banned phrase": "banned_phrase",
    "hashtag": "hashtag",
    "em dash": "em_dash",
    "more than one emoji": "emoji",
    "line breaks": "line_breaks",
    "no capitalization": "no_capitalization",
}


def drop_reason(problem: str) -> str:
    """A fixed id for one `voice.check_draft` message. The messages carry numbers and
    phrases ("too long (41 words; limit 35)"), which would split a breakdown into one
    row per draft."""
    for prefix, reason in DROP_REASONS.items():
        if problem.startswith(prefix):
            return reason
    return "other_rule"


def drop_reasons(problems: list[str]) -> list[str]:
    """Every rule a draft broke, as fixed ids in check order, without repeats."""
    return list(dict.fromkeys(drop_reason(p) for p in problems))


def is_replay(ask: int, answered_at_start: int) -> bool:
    """Whether approval request number `ask` (1-based) is a reply this run already acted
    on before it was suspended.

    A resumed run restarts from the top and gets every earlier reply back. The reply
    numbered `answered_at_start` is the one that woke this run, so it counts; the ones
    before it are replays. A local run never restarts, so `answered_at_start` is 0 and
    every reply counts.
    """
    return ask < answered_at_start


def count_answered(input_keys: list[str]) -> int:
    """How many approval replies are stored for a run, from its run-input keys."""
    return sum(1 for key in input_keys if key.endswith("-response"))


def minutes_since(start, now) -> float:
    """Minutes from `start` to `now`, to one decimal place, never negative."""
    return max(0.0, round((now - start).total_seconds() / 60, 1))
