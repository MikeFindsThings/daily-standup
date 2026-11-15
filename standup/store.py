"""Run state kept in Prefect artifacts.

A suspended flow run restarts from the top in a fresh container when it is resumed, so
nothing in memory or on disk survives the approval wait. The flow writes its picks and
drafts as artifacts tied to the flow run, and reads them back on every start. The same
artifacts are what the MCP server reads to show drafts.
"""

import json
from dataclasses import dataclass, field
from uuid import UUID

from prefect import get_client
from prefect.artifacts import create_markdown_artifact, create_table_artifact
from prefect.client.schemas.filters import ArtifactFilter, ArtifactFilterFlowRunId, ArtifactFilterKey
from prefect.client.schemas.sorting import ArtifactSort

from standup.approval import final_markdown
from standup.models import Draft, FinalPost, Pick

KEY_PICKS = "standup-picks"
KEY_DRAFTS = "standup-drafts"
KEY_FINAL = "standup-final"


@dataclass
class RunState:
    picks: list[Pick] = field(default_factory=list)
    rounds: dict[int, list[Draft]] = field(default_factory=dict)


def _rows(data) -> list[dict]:
    if isinstance(data, str):
        data = json.loads(data)
    return data if isinstance(data, list) else []


def draft_to_row(d: Draft) -> dict:
    row = d.model_dump()
    row["gif_urls"] = " ".join(d.gif_urls)
    return row


def row_to_draft(row: dict) -> Draft:
    return Draft(**{**row, "gif_urls": (row.get("gif_urls") or "").split()})


def save_picks(picks: list[Pick]) -> None:
    create_table_artifact(
        key=KEY_PICKS,
        table=[p.model_dump() for p in picks],
        description="Stories with the best comedic angle, best first.",
    )


def save_drafts(drafts: list[Draft]) -> None:
    create_table_artifact(
        key=KEY_DRAFTS,
        table=[draft_to_row(d) for d in drafts],
        description=f"Round {drafts[0].round} drafts for: {drafts[0].story_title}",
    )


def save_final(final: FinalPost, story_title: str, story_url: str) -> None:
    create_markdown_artifact(
        key=KEY_FINAL,
        markdown=final_markdown(final, story_title, story_url),
        description=final.text,  # plain text, read back by recent_posts
    )


def load_run_state(flow_run_id: UUID) -> RunState:
    """Everything this flow run has already published."""
    state = RunState()
    with get_client(sync_client=True) as client:
        artifacts = client.read_artifacts(
            artifact_filter=ArtifactFilter(flow_run_id=ArtifactFilterFlowRunId(any_=[flow_run_id])),
            sort=ArtifactSort.CREATED_DESC,
            limit=50,
        )
    for artifact in artifacts:
        if artifact.key == KEY_PICKS and not state.picks:
            state.picks = [Pick(**row) for row in _rows(artifact.data)]
        elif artifact.key == KEY_DRAFTS:
            drafts = [row_to_draft(row) for row in _rows(artifact.data)]
            if drafts:
                state.rounds.setdefault(drafts[0].round, drafts)
    return state


def recent_posts(n: int = 5) -> list[str]:
    """Text of the last `n` approved posts, newest first."""
    with get_client(sync_client=True) as client:
        artifacts = client.read_artifacts(
            artifact_filter=ArtifactFilter(key=ArtifactFilterKey(any_=[KEY_FINAL])),
            sort=ArtifactSort.CREATED_DESC,
            limit=n,
        )
    return [a.description for a in artifacts if a.description]
