"""Everything the MCP server asks of Prefect Cloud, in one place.

The tools in server.py call these functions and nothing else, which keeps the server a
thin layer and lets the tests replace this module with a fake.
"""

from uuid import UUID

from prefect import get_client
from prefect.client.schemas.filters import (
    FlowFilter, FlowFilterName, FlowRunFilter, FlowRunFilterState, FlowRunFilterStateType,
)
from prefect.client.schemas.objects import FlowRun, StateType
from prefect.client.schemas.responses import SetStateStatus
from prefect.client.schemas.sorting import ArtifactSort, FlowRunSort
from prefect.exceptions import ObjectNotFound

from standup import store

FLOW_NAME = "daily-standup"
DEPLOYMENT = f"{FLOW_NAME}/daily"
_THIS_FLOW = FlowFilter(name=FlowFilterName(any_=[FLOW_NAME]))


class NotOurRun(LookupError):
    """The run ID doesn't exist, or belongs to a different flow."""


class ResumeRejected(RuntimeError):
    """Prefect refused the reply, usually because the run is no longer waiting."""


async def start_run(topic: str | None) -> FlowRun:
    async with get_client() as client:
        deployment = await client.read_deployment_by_name(DEPLOYMENT)
        return await client.create_flow_run_from_deployment(
            deployment.id,
            parameters={"topic": topic} if topic else {},
            tags=["mcp"],  # read by the flow as analytics `trigger`
        )


async def read_run(run_id: UUID) -> FlowRun:
    """A daily-standup flow run. Raises NotOurRun for anything else, so the tools can
    never read or resume a run that belongs to another flow."""
    async with get_client() as client:
        try:
            run = await client.read_flow_run(run_id)
            flow = await client.read_flow(run.flow_id)
        except ObjectNotFound:
            raise NotOurRun(str(run_id)) from None
    if flow.name != FLOW_NAME:
        raise NotOurRun(str(run_id))
    return run


async def waiting_runs(limit: int = 5) -> list[FlowRun]:
    """Runs suspended for approval, newest first."""
    async with get_client() as client:
        return await client.read_flow_runs(
            flow_filter=_THIS_FLOW,
            flow_run_filter=FlowRunFilter(
                state=FlowRunFilterState(type=FlowRunFilterStateType(any_=[StateType.PAUSED]))
            ),
            sort=FlowRunSort.EXPECTED_START_TIME_DESC,
            limit=limit,
        )


async def latest_run() -> FlowRun | None:
    """The most recent run that has actually started (ignores future scheduled runs)."""
    async with get_client() as client:
        runs = await client.read_flow_runs(
            flow_filter=_THIS_FLOW,
            flow_run_filter=FlowRunFilter(
                state=FlowRunFilterState(type=FlowRunFilterStateType(not_any_=[StateType.SCHEDULED]))
            ),
            sort=FlowRunSort.EXPECTED_START_TIME_DESC,
            limit=1,
        )
    return runs[0] if runs else None


async def run_artifacts(run_id: UUID) -> list:
    async with get_client() as client:
        return await client.read_artifacts(
            artifact_filter=store.run_artifact_filter(run_id), sort=ArtifactSort.CREATED_DESC, limit=50
        )


async def send_reply(run_id: UUID, reply: dict) -> None:
    """Resume a suspended run with the human's reply."""
    async with get_client() as client:
        result = await client.resume_flow_run(run_id, run_input=reply)
    if result.status != SetStateStatus.ACCEPT:
        reason = getattr(result.details, "reason", None) or "the run is no longer waiting"
        raise ResumeRejected(reason)


async def approved_posts(n: int) -> list[dict]:
    async with get_client() as client:
        artifacts = await client.read_artifacts(
            artifact_filter=store.final_posts_filter(), sort=ArtifactSort.CREATED_DESC, limit=n
        )
    return [
        {"text": a.description, "approved_at": a.created.isoformat() if a.created else None}
        for a in artifacts if a.description
    ]
