# Analytics

The flow sends product events and LLM analytics to PostHog, to answer three questions: where drafts die, why, and what each run costs in Claude calls. The code is in `standup/analytics.py`; the events are sent from `flows/daily_standup.py` and `standup/llm.py`.

## Ground rules

- **Analytics never breaks a run.** Nothing in `standup/analytics.py` raises. A missing token, a PostHog outage or a bad call logs one warning, analytics switches off for the rest of that process, and the flow carries on. A resumed run is a new process, so it tries PostHog again.
- **No content leaves the flow.** No post text, captions, headlines, prompts, judge feedback or GIF links in any property. Counts, ranks, fixed category ids and timings only. The tests check this against sample captions and headlines.
- **The MCP server sends nothing.** Its only change is tagging the runs it starts, so the flow can report how a run was triggered.

## Setup

- **Project:** PostHog Cloud, US region (`https://us.i.posthog.com`). The EU region only changes `POSTHOG_HOST`.
- **Token:** a Prefect Secret block named `posthog-project-token`. Without it the flow logs one warning and runs normally.
- **Package:** `posthog>=8,<9`, in both `pyproject.toml` and the `pip_packages` list in `prefect.yaml`, because the managed pool installs only what that list names.
- **Client:** `Posthog(token, host=..., sync_mode=True, timeout=5, max_retries=0)`. Sync mode sends each event before the call returns, so nothing is lost when a suspend exits the process. A run sends about 15 events. The short timeout and no retries keep a PostHog outage from stalling a run.
- **Identity:** every event uses `distinct_id = "run-<flow run id>"` with `process_person_profile` off, plus `flow_run_id` and `deployment` (`daily` or `local`). These are backend events with no human user behind them, so each run is its own anonymous ID and funnels count runs.

## Events

Each event fires only on the code path that does the work for the first time, never on the path that reuses stored artifacts after a resume.

| Event | Fires | Properties |
| --- | --- | --- |
| `run_started` | At the top of the flow, only on a fresh start (no stored picks) | `trigger` (schedule, mcp or manual), `topic_set` |
| `stories_picked` | After the picks are published | `candidates`, `sources_ok`, `sources_failed`, `picks` |
| `draft_dropped` | In the voice check, once per dropped draft, after the judge call returns, so a task retry can't send rule drops twice | `story_rank`, `stage` (rules or judge), `reason` (the first rule broken, or `judge`), `rules_failed` (every rule broken, as fixed ids) |
| `drafts_generated` | After a round's drafts are published | `round`, `story_rank`, `story_source`, `drafts`, `rewrite_used`, `gif_drafts` |
| `drafts_regenerated` | On a regenerate reply, behind the resume guard below | `round` (the round rejected), `wait_minutes` |
| `draft_approved` | After the final post is published | `round`, `option`, `format`, `edited`, `gif`, `wait_minutes` |
| `run_skipped` | In `skipped()`, which every early exit goes through | `reason` (by_request, no_stories, no_angle, no_drafts_pass, timeout_backstop), `round` |
| `$ai_generation` | Every Claude call | See LLM analytics |

Notes:

- **`trigger`:** Prefect's scheduler tags its runs `auto-scheduled`, and the MCP server tags its runs `mcp`. Anything else is `manual`.
- **`reason` and `rules_failed`:** the voice check's messages carry numbers ("too long (41 words; limit 35)"), which would split a breakdown into one row per draft, so each is mapped to a fixed id such as `too_long`, `em_dash` or `banned_phrase`.
- **`wait_minutes`:** time from when that round's drafts were published to the reply.
- **`timeout_backstop`** only appears on local runs. On a deployment, the "skip when unanswered" automation cancels an ignored run first, and a cancelled run can't report itself.

## Resume guard

A suspended run restarts from the top when resumed, and every reply it received before comes back to it. Picks and drafts are read back from artifacts, so their events sit on code paths that don't rerun. Approve and skip end the run, so they are never replayed. An invalid approval asks again and sends nothing.

That leaves one event that could double-count: `drafts_regenerated`. A run regenerated twice would otherwise report 1 + 2 = 3 regenerations.

The rule:

1. At the start of a run, count the replies already stored for it. Prefect stores each one as run input under `suspended-approval-<n>-response`.
2. A reply is a replay when its number is lower than that count. The reply numbered exactly the count is the one that woke this run, so it is reported.
3. A local run pauses in place and never restarts, so its count is 0 and every reply is reported.
4. If the count can't be read, regenerations aren't reported for that run. A missing number is better than a wrong one.

The comparison is a pure function, `analytics.is_replay`, with tests for each case, including a run regenerated twice that must report 2.

## LLM analytics

`$ai_generation` is captured by hand, not through PostHog's Anthropic wrapper. In posthog 8.1.0 the wrapper instruments only `messages.create` and `messages.stream`, while the flow calls `messages.parse` and `beta.messages.parse` (the second for the server-side refusal fallback).

Each call is wrapped in `_timed()` in `standup/llm.py`, which measures latency and records a failed generation if the call raises. `_parsed()` sends the generation for every response.

| Property | Value |
| --- | --- |
| `$ai_trace_id` | The flow run ID, so each run is one trace |
| `$ai_span_name` | `pick_stories`, `write_captions` or `judge_captions` |
| `$ai_provider` / `$ai_model` | `anthropic` / the model that actually answered, which shows when the fallback ran |
| `$ai_input_tokens` / `$ai_output_tokens` | From the response's usage |
| `$ai_latency` | Seconds |
| `$ai_stop_reason` | The response's stop reason |
| `$ai_input_cost_usd`, `$ai_output_cost_usd`, `$ai_total_cost_usd` | From the `PRICES` table in `standup/llm.py`, sent explicitly rather than left to PostHog's own pricing |
| `$ai_is_error` / `$ai_error` | True and the exception's class name when a call raises, or `ModelRefused` on a refusal |

The prompt and the reply (`$ai_input`, `$ai_output_choices`) are never sent.
