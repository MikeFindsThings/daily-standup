"""PostHog analytics: the module's safety rules, the pure helpers, and what the flow sends.

A fake client stands in for PostHog. Nothing here touches the network or a real token.
"""

import logging
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from uuid import uuid4

import pytest

from standup import analytics, llm, store
from standup.models import Pick
from standup.voice import check_draft

RUN_ID = "11111111-2222-3333-4444-555555555555"
HEADLINE = "Airline grounds every flight after a routine software upgrade"
CAPTION = "Every outage I've ever caused also started as a routine upgrade."
JUDGE_WORDS = "explains its own joke"
PICK = Pick(rank=2, title=HEADLINE, url="https://example.com/story", source="The Verge",
            summary="The upgrade was routine, officials said.", detail="routine upgrade", reason="irony")


class FakePostHog:
    def __init__(self, fail=False):
        self.events: list[dict] = []
        self.fail = fail

    def capture(self, event, **kwargs):
        if self.fail:
            raise ConnectionError("posthog is down")
        self.events.append({"event": event, **kwargs})

    def named(self, name):
        return [e for e in self.events if e["event"] == name]


@pytest.fixture(autouse=True)
def analytics_off():
    analytics.reset()
    yield
    analytics.reset()


@pytest.fixture
def posthog():
    fake = FakePostHog()
    assert analytics.configure(None, RUN_ID, "daily", client=fake)
    return fake


def all_values(events):
    """Every property value sent, as strings, for content-leak checks."""
    for e in events:
        yield str(e.get("distinct_id"))
        for value in (e.get("properties") or {}).values():
            yield str(value)


# --- the module's safety rules -------------------------------------------------


def test_capture_is_a_no_op_before_configure():
    assert not analytics.is_configured()
    analytics.capture("run_started", trigger="manual")  # nothing to send to, nothing raised
    analytics.ai_generation(span_name="pick_stories", model="claude-haiku-5-5", latency=1.0)


def test_no_token_means_off_with_one_warning(caplog):
    with caplog.at_level(logging.WARNING):
        assert analytics.configure("   ", RUN_ID, "daily") is False
        analytics.capture("run_started")
    assert not analytics.is_configured()
    assert len([r for r in caplog.records if "analytics" in r.message]) == 1


def test_client_exception_is_logged_once_not_raised(caplog):
    fake = FakePostHog(fail=True)
    analytics.configure(None, RUN_ID, "daily", client=fake)
    with caplog.at_level(logging.WARNING):
        analytics.capture("run_started")
        analytics.capture("stories_picked")  # analytics is off after the first failure
    warnings = [r for r in caplog.records if "PostHog capture failed" in r.message]
    assert len(warnings) == 1 and "ConnectionError" in warnings[0].message
    assert not analytics.is_configured()


def test_every_event_is_an_anonymous_run_with_profiles_off(posthog):
    analytics.capture("run_started", trigger="schedule", topic_set=False)
    (event,) = posthog.events
    assert event["distinct_id"] == f"run-{RUN_ID}"
    assert event["options"] == {"process_person_profile": False}
    assert event["properties"] == {"flow_run_id": RUN_ID, "deployment": "daily",
                                   "trigger": "schedule", "topic_set": False}


# --- pure helpers ----------------------------------------------------------------


@pytest.mark.parametrize(
    "tags, trigger",
    [
        (["auto-scheduled"], "schedule"),
        (["mcp"], "mcp"),
        ([], "manual"),
        (None, "manual"),
        (["auto-scheduled", "something-else"], "schedule"),
    ],
)
def test_trigger_from_tags(tags, trigger):
    assert analytics.trigger_from_tags(tags) == trigger


@pytest.mark.parametrize(
    "text",
    [
        "   ",
        " ".join(["word"] * 40) + ".",
        "One. Two. Three.",
        "Here's the thing about prod.",
        "Never push to prod. #devops",
        "The upgrade went fine — for the vendor.",
        "Shipping it 🚀🔥",
        "I got stuck.\nAt the airport.",
        "never push directly to prod.",
    ],
)
def test_every_check_draft_message_maps_to_a_fixed_reason_id(text):
    problems = check_draft(text)
    assert problems
    for problem in problems:
        reason = analytics.drop_reason(problem)
        assert reason in analytics.DROP_REASONS.values(), problem
        assert not any(ch.isdigit() for ch in reason)


def test_reason_ids_do_not_split_on_numbers():
    assert analytics.drop_reason("too long (41 words; limit 35)") == analytics.drop_reason(
        "too long (52 words; limit 35)") == "too_long"
    assert analytics.drop_reason("banned phrase: 'delve'") == "banned_phrase"
    assert analytics.drop_reason("something new") == "other_rule"


def test_replay_rule_for_a_resumed_run():
    # Woken by the 3rd reply: replies 1 and 2 were acted on before, reply 3 is new.
    answered_at_start = 3
    assert analytics.is_replay(1, answered_at_start)
    assert analytics.is_replay(2, answered_at_start)
    assert not analytics.is_replay(3, answered_at_start)  # the reply that woke the run counts
    assert not analytics.is_replay(4, answered_at_start)


def test_replay_rule_for_a_local_run_counts_every_reply():
    assert not any(analytics.is_replay(ask, 0) for ask in range(1, 6))


def test_regenerated_twice_reports_two_not_three():
    """The double-count the guard exists for: regenerate, regenerate, then approve."""
    reported = 0
    for answered_at_start, replies in [(1, ["regenerate"]),
                                       (2, ["regenerate", "regenerate"]),
                                       (3, ["regenerate", "regenerate", "approve"])]:
        for ask, action in enumerate(replies, 1):
            if action == "regenerate" and not analytics.is_replay(ask, answered_at_start):
                reported += 1
    assert reported == 2


def test_count_answered_counts_only_responses():
    keys = ["suspended-approval-1-description", "suspended-approval-1-response",
            "suspended-approval-1-schema", "suspended-approval-2-response"]
    assert analytics.count_answered(keys) == 2
    assert analytics.count_answered([]) == 0


def test_minutes_since():
    start = datetime(2026, 11, 20, 17, 30, tzinfo=timezone.utc)
    assert analytics.minutes_since(start, start + timedelta(minutes=95, seconds=30)) == 95.5
    assert analytics.minutes_since(start, start - timedelta(minutes=1)) == 0.0


def test_store_keeps_each_rounds_publish_time():
    import json
    from standup.models import Draft
    from standup.store import KEY_DRAFTS, draft_to_row, state_from_artifacts

    def artifact(round_no, created):
        draft = Draft(round=round_no, option=1, text="x", format="repost", story_title="t", story_url="u")
        return SimpleNamespace(key=KEY_DRAFTS, data=json.dumps([draft_to_row(draft)]), created=created)

    t1 = datetime(2026, 11, 20, 17, 32, tzinfo=timezone.utc)
    t2 = t1 + timedelta(hours=2)
    state = state_from_artifacts([artifact(1, t1), artifact(2, t2)])
    assert state.round_created == {1: t1, 2: t2}


# --- $ai_generation from the Claude calls -------------------------------------------


def fake_response(stop_reason="end_turn", parsed=object(), model="claude-opus-5-5"):
    return SimpleNamespace(
        model=model, stop_reason=stop_reason, parsed_output=parsed,
        usage=SimpleNamespace(input_tokens=2000, output_tokens=1000),
    )


def test_parsed_sends_one_ai_generation_with_the_documented_properties(posthog):
    llm._parsed(fake_response(), "write_captions", latency=4.25)
    (event,) = posthog.named("$ai_generation")
    props = event["properties"]
    assert props["$ai_trace_id"] == RUN_ID
    assert props["$ai_span_name"] == "write_captions"
    assert props["$ai_provider"] == "anthropic" and props["$ai_model"] == "claude-opus-5-5"
    assert (props["$ai_input_tokens"], props["$ai_output_tokens"]) == (2000, 1000)
    assert props["$ai_latency"] == 4.25 and props["$ai_stop_reason"] == "end_turn"
    assert props["$ai_input_cost_usd"] == pytest.approx(0.008)
    assert props["$ai_output_cost_usd"] == pytest.approx(0.02)
    assert props["$ai_total_cost_usd"] == pytest.approx(0.028)
    assert props["$ai_is_error"] is False and "$ai_error" not in props
    assert "$ai_input" not in props and "$ai_output_choices" not in props


def test_parsed_records_the_model_that_answered(posthog):
    llm._parsed(fake_response(model="claude-opus-4-8"), "write_captions", latency=1.0)
    assert posthog.named("$ai_generation")[0]["properties"]["$ai_model"] == "claude-opus-4-8"


@pytest.mark.parametrize("response", [fake_response(stop_reason="refusal"), fake_response(parsed=None)])
def test_a_refusal_sends_an_error_generation(posthog, response):
    with pytest.raises(llm.ModelRefused):
        llm._parsed(response, "judge_captions", latency=1.0)
    (event,) = posthog.named("$ai_generation")
    assert event["properties"]["$ai_is_error"] is True
    assert event["properties"]["$ai_error"] == "ModelRefused"


def test_a_call_that_raises_sends_an_error_generation(posthog):
    def boom():
        raise TimeoutError("no answer")

    with pytest.raises(TimeoutError):
        llm._timed("pick_stories", llm.PICK_MODEL, boom)
    (event,) = posthog.named("$ai_generation")
    props = event["properties"]
    assert props["$ai_is_error"] is True and props["$ai_error"] == "TimeoutError"
    assert props["$ai_span_name"] == "pick_stories" and props["$ai_model"] == llm.PICK_MODEL


def test_each_claude_call_is_timed_and_captured(posthog):
    """pick_stories end to end with a fake Anthropic client: one generation, no content."""
    from standup.models import Story

    class FakeMessages:
        def parse(self, **kwargs):
            parsed = llm._PickList(picks=[llm._PickItem(index=1, detail="routine upgrade", reason="irony")])
            return fake_response(parsed=parsed, model=llm.PICK_MODEL)

    client = SimpleNamespace(messages=FakeMessages())
    stories = [Story(title=HEADLINE, url="https://example.com/story", source="The Verge")]
    picks = llm.pick_stories(client, stories, recent_posts=[CAPTION])
    assert picks[0].title == HEADLINE
    (event,) = posthog.named("$ai_generation")
    assert event["properties"]["$ai_span_name"] == "pick_stories"
    assert event["properties"]["$ai_latency"] >= 0
    for value in all_values(posthog.events):
        assert HEADLINE not in value and CAPTION not in value


# --- the flow's voice check ----------------------------------------------------------


@pytest.fixture
def flow_module(monkeypatch):
    import flows.daily_standup as module

    monkeypatch.setattr(module, "get_run_logger", lambda: logging.getLogger("test-flow"))
    monkeypatch.setattr(module, "_claude", lambda: None)
    monkeypatch.setattr(module.voice, "load_voice", lambda: "voice guide")
    return module


def test_voice_check_sends_one_drop_per_dropped_draft_with_fixed_reasons(posthog, flow_module, monkeypatch):
    captions = [
        llm.Caption(text=CAPTION, format="repost", wants_gif=False),                      # judge fails it
        llm.Caption(text="The upgrade went fine — for the vendor.", format="repost", wants_gif=False),
        llm.Caption(text=" ".join(["routine"] * 40) + ".", format="repost", wants_gif=False),
        llm.Caption(text="Rare to see a ground stop called routine.", format="repost", wants_gif=False),
    ]
    monkeypatch.setattr(flow_module.llm, "judge_captions", lambda client, pick, texts, voice: [
        llm.Verdict(index=1, passes=False, reason=JUDGE_WORDS),
        llm.Verdict(index=2, passes=True, reason="fine"),
    ])
    passing, rejected = flow_module.voice_check.fn(PICK, captions)

    assert [c.text for c in passing] == ["Rare to see a ground stop called routine."]
    drops = [e["properties"] for e in posthog.named("draft_dropped")]
    assert sorted((d["stage"], d["reason"], tuple(d["rules_failed"])) for d in drops) == [
        ("judge", "judge", ()),
        ("rules", "em_dash", ("em_dash",)),
        ("rules", "too_long", ("too_long", "no_capitalization")),  # broke two rules
    ]
    assert all(d["story_rank"] == 2 for d in drops)


def test_drop_reasons_lists_every_rule_once_in_check_order():
    problems = check_draft("here's the thing — " + " ".join(["delve"] * 40) + " #devops")
    reasons = analytics.drop_reasons(problems)
    assert reasons[0] == "too_long"
    assert set(reasons) == {"too_long", "banned_phrase", "hashtag", "em_dash", "no_capitalization"}
    assert len(reasons) == len(set(reasons))  # two banned phrases, one id
    assert analytics.drop_reasons([]) == []


def test_rule_drops_are_not_sent_when_the_judge_call_fails(posthog, flow_module, monkeypatch):
    """A task retry reruns voice_check; drops are only sent once the judge has answered."""
    def judge_down(*args):
        raise ConnectionError("judge unavailable")

    monkeypatch.setattr(flow_module.llm, "judge_captions", judge_down)
    captions = [llm.Caption(text="Shipping it 🚀🔥", format="repost", wants_gif=False),
                llm.Caption(text="A fine caption.", format="repost", wants_gif=False)]
    with pytest.raises(ConnectionError):
        flow_module.voice_check.fn(PICK, captions)
    assert posthog.named("draft_dropped") == []


def test_no_event_carries_caption_headline_or_judge_text(posthog, flow_module, monkeypatch):
    monkeypatch.setattr(flow_module.llm, "judge_captions", lambda client, pick, texts, voice: [
        llm.Verdict(index=1, passes=False, reason=JUDGE_WORDS)])
    flow_module.voice_check.fn(PICK, [llm.Caption(text=CAPTION, format="repost", wants_gif=False),
                                      llm.Caption(text=CAPTION + " #devops", format="repost", wants_gif=False)])
    llm._parsed(fake_response(), "write_captions", latency=1.0)
    flow_module.skipped("No stories found for topic 'routine upgrade'.", "no_stories", 0)

    assert posthog.events
    for value in all_values(posthog.events):
        for text in (CAPTION, HEADLINE, JUDGE_WORDS, "routine upgrade", PICK.url):
            assert text not in value


def test_skipped_sends_the_reason_id_not_the_message(posthog, flow_module):
    flow_module.skipped("Skipped by request.", "by_request", 2)
    (event,) = posthog.named("run_skipped")
    assert event["properties"]["reason"] == "by_request" and event["properties"]["round"] == 2


# --- the MCP server tags its runs -----------------------------------------------------


async def test_mcp_start_run_tags_the_run(monkeypatch):
    from mcp_server import prefect_api

    calls = {}

    class FakeClient:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *exc):
            return False

        async def read_deployment_by_name(self, name):
            return SimpleNamespace(id=uuid4())

        async def create_flow_run_from_deployment(self, deployment_id, **kwargs):
            calls.update(kwargs)
            return SimpleNamespace(id=uuid4())

    monkeypatch.setattr(prefect_api, "get_client", lambda: FakeClient())
    await prefect_api.start_run("aws")
    assert calls["tags"] == ["mcp"] and calls["parameters"] == {"topic": "aws"}
    assert analytics.trigger_from_tags(calls["tags"]) == "mcp"


def test_the_mcp_server_sends_nothing():
    import mcp_server.server  # noqa: F401 - importing must not turn analytics on

    assert not analytics.is_configured()


def test_answered_replies_reads_the_suspended_prefix(monkeypatch):
    seen = {}

    class FakeSyncClient:
        def __enter__(self):
            return self

        def __exit__(self, *exc):
            return False

        def filter_flow_run_input(self, flow_run_id, key_prefix, limit, exclude_keys):
            seen.update(prefix=key_prefix, limit=limit, exclude=exclude_keys)
            return [SimpleNamespace(key=k) for k in (
                "suspended-approval-1-response", "suspended-approval-1-schema",
                "suspended-approval-2-response", "suspended-approval-2-description")]

    monkeypatch.setattr(store, "get_client", lambda sync_client: FakeSyncClient())
    assert store.answered_replies(uuid4()) == 2
    assert seen == {"prefix": "suspended-approval-", "limit": 50, "exclude": set()}
