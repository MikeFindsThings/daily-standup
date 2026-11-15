"""MCP tool behaviour and input validation, with Prefect replaced by a fake."""

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest
from fastmcp import Client
from fastmcp.exceptions import ToolError
from prefect.states import Completed, Failed, Running, Suspended

from mcp_server import server
from mcp_server import prefect_api as api
from standup.models import Draft
from standup.store import KEY_DRAFTS, KEY_FINAL, draft_to_row

STORY = dict(story_title="Airline grounds flights after software upgrade", story_url="https://example.com/s")
DRAFTS = [
    Draft(round=1, option=1, text="Repost text.", format="repost", **STORY),
    Draft(round=1, option=2, text="GIF text.", format="gif", gif_urls=["https://giphy.com/a"], **STORY),
]
NOW = datetime(2026, 11, 14, tzinfo=timezone.utc)


def drafts_artifact(drafts=DRAFTS):
    return SimpleNamespace(key=KEY_DRAFTS, data=json.dumps([draft_to_row(d) for d in drafts]), created=NOW, description="")


def final_artifact(text="Approved text."):
    return SimpleNamespace(key=KEY_FINAL, data=f"## Today's post\n{text}", created=NOW, description=text)


class FakePrefect:
    """Stands in for mcp_server.prefect_api."""

    def __init__(self):
        self.runs: dict[UUID, SimpleNamespace] = {}
        self.artifacts: dict[UUID, list] = {}
        self.replies: list[tuple[UUID, dict]] = []
        self.started: list[str | None] = []
        self.reject_replies = False

    def add_run(self, state, artifacts=(), name="test-run"):
        run = SimpleNamespace(id=uuid4(), name=name, state=state)
        self.runs[run.id] = run
        self.artifacts[run.id] = list(artifacts)
        return run

    async def start_run(self, topic):
        self.started.append(topic)
        return self.add_run(Running(), name="new-run")

    async def read_run(self, run_id):
        if run_id not in self.runs:
            raise api.NotOurRun(str(run_id))
        return self.runs[run_id]

    async def waiting_runs(self, limit=5):
        return [r for r in self.runs.values() if r.state.is_paused()][:limit]

    async def latest_run(self):
        return next(iter(self.runs.values()), None)

    async def run_artifacts(self, run_id):
        return self.artifacts[run_id]

    async def send_reply(self, run_id, reply):
        if self.reject_replies:
            raise api.ResumeRejected("the run is no longer waiting")
        self.replies.append((run_id, reply))

    async def approved_posts(self, n):
        return [{"text": f"post {i}", "approved_at": NOW.isoformat()} for i in range(1, n + 1)]


@pytest.fixture
def prefect(monkeypatch):
    fake = FakePrefect()
    for name in ("start_run", "read_run", "waiting_runs", "latest_run", "run_artifacts", "send_reply", "approved_posts"):
        monkeypatch.setattr(api, name, getattr(fake, name))
    monkeypatch.setenv("PREFECT_API_URL", "https://api.prefect.cloud/api/accounts/acct/workspaces/ws")
    monkeypatch.setenv("PREFECT_API_KEY", "test-key")
    return fake


async def call(tool, **args):
    async with Client(server.mcp) as client:
        return (await client.call_tool(tool, args)).data


# --- the tool list ------------------------------------------------------------


async def test_server_exposes_exactly_the_planned_tools():
    async with Client(server.mcp) as client:
        names = {t.name for t in await client.list_tools()}
    assert names == {"ping", "generate_post", "get_drafts", "approve_post", "regenerate", "skip", "recent_posts"}


# --- generate_post ------------------------------------------------------------


async def test_generate_post_returns_a_run_id_without_waiting(prefect):
    result = await call("generate_post", topic="  aws   outage ")
    assert result["status"] == "in_progress" and UUID(result["run_id"])
    assert prefect.started == ["aws outage"]


async def test_generate_post_treats_blank_topic_as_none(prefect):
    await call("generate_post", topic="   ")
    assert prefect.started == [None]


async def test_generate_post_rejects_a_very_long_topic(prefect):
    with pytest.raises(ToolError, match="too long"):
        await call("generate_post", topic="x" * 81)
    assert prefect.started == []


# --- get_drafts ---------------------------------------------------------------


async def test_get_drafts_finds_the_waiting_run_when_no_id_is_given(prefect):
    run = prefect.add_run(Suspended(), [drafts_artifact()])
    result = await call("get_drafts")
    assert result["status"] == "waiting_for_approval" and result["run_id"] == str(run.id)
    assert [d["option"] for d in result["drafts"]] == [1, 2]
    assert result["drafts"][1]["gif_candidates"] == ["https://giphy.com/a"]
    assert result["story"]["url"] == "https://example.com/s"


async def test_get_drafts_shows_only_the_latest_round(prefect):
    round2 = [Draft(round=2, option=1, text="Second story.", format="repost",
                    story_title="Other story", story_url="https://example.com/t")]
    prefect.add_run(Suspended(), [drafts_artifact(), drafts_artifact(round2)])
    result = await call("get_drafts")
    assert result["round"] == 2 and [d["text"] for d in result["drafts"]] == ["Second story."]


async def test_get_drafts_says_when_nothing_is_waiting(prefect):
    result = await call("get_drafts")
    assert result["status"] == "none_waiting"
    prefect.add_run(Completed(), [final_artifact()])
    result = await call("get_drafts")
    assert result["status"] == "none_waiting" and result["latest_run"]["status"] == "approved"


@pytest.mark.parametrize(
    "state, artifacts, status",
    [
        (Running(), [], "in_progress"),
        (Completed(), [final_artifact()], "approved"),
        (Completed(name="Skipped", message="Skipped by request."), [], "skipped"),
        (Failed(message="boom"), [], "failed"),
    ],
)
async def test_get_drafts_reports_each_run_state(prefect, state, artifacts, status):
    run = prefect.add_run(state, artifacts)
    assert (await call("get_drafts", run_id=str(run.id)))["status"] == status


@pytest.mark.parametrize("tool", ["get_drafts", "approve_post", "regenerate", "skip"])
async def test_malformed_run_id_is_a_clear_error(prefect, tool):
    args = {"run_id": "not-a-uuid"} | ({"choice": 1} if tool == "approve_post" else {})
    with pytest.raises(ToolError, match="not a valid run ID"):
        await call(tool, **args)


async def test_unknown_run_id_is_a_clear_error(prefect):
    with pytest.raises(ToolError, match="No daily-standup run"):
        await call("get_drafts", run_id=str(uuid4()))


# --- approve_post -------------------------------------------------------------


async def test_approve_post_sends_the_reply_and_returns_the_final_post(prefect):
    run = prefect.add_run(Suspended(), [drafts_artifact()])
    result = await call("approve_post", run_id=str(run.id), choice=1)
    assert result["final_post"] == {"text": "Repost text.", "format": "repost",
                                    "link_to_share": "https://example.com/s", "gif": None}
    assert prefect.replies == [(run.id, {"action": "approve", "choice": 1, "edit": None, "gif": None})]


async def test_approve_post_with_edit_and_no_gif(prefect):
    run = prefect.add_run(Suspended(), [drafts_artifact()])
    result = await call("approve_post", run_id=str(run.id), choice=2, edit=" My words. ", gif="none")
    assert result["final_post"]["text"] == "My words." and result["final_post"]["gif"] is None
    assert prefect.replies[0][1] == {"action": "approve", "choice": 2, "edit": "My words.", "gif": "none"}


@pytest.mark.parametrize(
    "args, message",
    [
        ({"choice": 3}, "no option 3"),
        ({"choice": 0}, "option numbers on offer: 1, 2"),
        ({"choice": 2, "gif": "4"}, "no GIF 4"),
        ({"choice": 1, "gif": "banana"}, "GIF must be"),
        ({"choice": 1, "edit": "x" * 601}, "too long"),
    ],
)
async def test_approve_post_rejects_bad_input_before_sending_anything(prefect, args, message):
    run = prefect.add_run(Suspended(), [drafts_artifact()])
    with pytest.raises(ToolError, match=message):
        await call("approve_post", run_id=str(run.id), **args)
    assert prefect.replies == []


@pytest.mark.parametrize("tool", ["approve_post", "regenerate", "skip"])
async def test_acting_on_a_run_that_is_not_waiting_is_a_clear_error(prefect, tool):
    run = prefect.add_run(Completed(), [final_artifact()])
    args = {"run_id": str(run.id)} | ({"choice": 1} if tool == "approve_post" else {})
    with pytest.raises(ToolError, match="not waiting for approval"):
        await call(tool, **args)
    assert prefect.replies == []


async def test_a_reply_prefect_rejects_is_a_clear_error(prefect):
    run = prefect.add_run(Suspended(), [drafts_artifact()])
    prefect.reject_replies = True
    with pytest.raises(ToolError, match="did not accept the reply"):
        await call("skip", run_id=str(run.id))


# --- regenerate, skip, recent_posts -------------------------------------------


async def test_regenerate_and_skip_send_the_matching_action(prefect):
    run = prefect.add_run(Suspended(), [drafts_artifact()])
    assert (await call("regenerate", run_id=str(run.id)))["status"] == "in_progress"
    assert (await call("skip", run_id=str(run.id)))["status"] == "skipped"
    assert [reply["action"] for _, reply in prefect.replies] == ["regenerate", "skip"]


async def test_recent_posts_returns_n_and_validates_range(prefect):
    assert (await call("recent_posts", n=3))["count"] == 3
    assert (await call("recent_posts"))["count"] == 5
    for bad in (0, 21):
        with pytest.raises(ToolError):
            await call("recent_posts", n=bad)


# --- settings -----------------------------------------------------------------


@pytest.mark.parametrize(
    "url, hint",
    [
        ("", "missing: PREFECT_API_URL"),
        ("pnu_fake", "looks like an API key"),
        ("https://app.prefect.cloud/account/a/workspace/b", "browser address"),
        ("api.prefect.cloud/api/accounts/a", "no https://"),
    ],
)
async def test_bad_prefect_settings_are_described_without_revealing_them(prefect, monkeypatch, url, hint):
    monkeypatch.setenv("PREFECT_API_URL", url)
    with pytest.raises(ToolError, match=hint) as error:
        await call("get_drafts")
    assert "test-key" not in str(error.value)


def test_env_values_are_tidied_before_prefect_reads_them(monkeypatch):
    good = "https://api.prefect.cloud/api/accounts/acct/workspaces/ws"
    monkeypatch.setenv("PREFECT_API_URL", f"PREFECT_API_URL='{good}/'\n")
    monkeypatch.setenv("PREFECT_API_KEY", ' "test-key" ')
    server._normalize_prefect_env()
    import os
    assert os.environ["PREFECT_API_URL"] == good and os.environ["PREFECT_API_KEY"] == "test-key"
