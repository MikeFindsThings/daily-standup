"""Stage 0 spike: FastMCP server with a ping tool and a Prefect trigger tool.

Deployed to FastMCP Gateway with entrypoint `mcp_server/server.py:mcp`.
Expects PREFECT_API_URL and PREFECT_API_KEY in the host's environment settings;
the Prefect client reads them itself, so this file never touches their values.
"""

import os
from datetime import datetime, timezone

from fastmcp import FastMCP
from fastmcp.exceptions import ToolError

HELLO_DEPLOYMENT = os.environ.get("HELLO_DEPLOYMENT", "hello/stage0")

mcp = FastMCP(
    "Daily Standup",
    instructions=(
        "Controls a Prefect flow that drafts short tech-news LinkedIn posts. "
        "Nothing is ever posted automatically; a human approves every draft."
    ),
)


@mcp.tool
def ping() -> dict:
    """Health check. Returns the server's current UTC time. Use this to confirm the connector works."""
    return {"ok": True, "utc_time": datetime.now(timezone.utc).isoformat()}


@mcp.tool
async def trigger_hello_flow(name: str = "world") -> dict:
    """Start the Stage 0 test flow on Prefect Cloud and return right away.

    Returns the flow run ID, its current state (usually SCHEDULED or PENDING, since
    the run starts in the background), and a link to the run in the Prefect UI.
    """
    if not os.environ.get("PREFECT_API_URL") or not os.environ.get("PREFECT_API_KEY"):
        raise ToolError(
            "The server is missing PREFECT_API_URL or PREFECT_API_KEY in its environment settings."
        )

    from prefect import get_client
    from prefect.exceptions import ObjectNotFound

    async with get_client() as client:
        try:
            deployment = await client.read_deployment_by_name(HELLO_DEPLOYMENT)
        except ObjectNotFound:
            raise ToolError(
                f"No Prefect deployment named '{HELLO_DEPLOYMENT}'. Run `prefect deploy --all` first."
            )
        run = await client.create_flow_run_from_deployment(
            deployment.id, parameters={"name": name}
        )

    ui_url = os.environ["PREFECT_API_URL"].replace(
        "https://api.prefect.cloud/api/accounts/", "https://app.prefect.cloud/account/"
    ).replace("/workspaces/", "/workspace/")
    return {
        "flow_run_id": str(run.id),
        "flow_run_name": run.name,
        "state": run.state.type.value if run.state else "UNKNOWN",
        "ui_url": f"{ui_url}/runs/flow-run/{run.id}",
    }


if __name__ == "__main__":
    mcp.run(transport="http", port=8000)
