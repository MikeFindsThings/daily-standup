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


def _clean(value: str | None) -> str:
    """Strip whitespace and wrapping quotes that often sneak in when pasting env values."""
    return (value or "").strip().strip("'\"").strip()


def _prefect_settings() -> tuple[str, str]:
    """Return (api_url, api_key) from the environment, or raise a ToolError that says
    what is wrong without revealing either value."""
    raw_url = os.environ.get("PREFECT_API_URL")
    api_url, api_key = _clean(raw_url), _clean(os.environ.get("PREFECT_API_KEY"))
    if api_url.upper().startswith("PREFECT_API_URL="):
        api_url = _clean(api_url.split("=", 1)[1])
    if not api_url or not api_key:
        missing = [n for n, v in (("PREFECT_API_URL", api_url), ("PREFECT_API_KEY", api_key)) if not v]
        raise ToolError(f"The server's environment settings are missing: {', '.join(missing)}.")
    if not api_url.startswith("https://api.prefect.cloud/api/accounts/"):
        if api_url.startswith("pnu_") or api_url.startswith("pnb_"):
            hint = "it looks like an API key; the URL and key may be swapped"
        elif "app.prefect.cloud" in api_url:
            hint = "it is the browser address (app.prefect.cloud); use the api.prefect.cloud/api/accounts/... form"
        elif "://" not in api_url:
            hint = "it has no https:// at the start"
        else:
            hint = "it is not a Prefect Cloud API address"
        raise ToolError(
            f"PREFECT_API_URL is set but malformed ({len(raw_url or '')} characters; {hint}). "
            "Expected https://api.prefect.cloud/api/accounts/<account-id>/workspaces/<workspace-id>."
        )
    return api_url.rstrip("/"), api_key


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
    api_url, api_key = _prefect_settings()

    from prefect.client.orchestration import PrefectClient
    from prefect.exceptions import ObjectNotFound

    async with PrefectClient(api=api_url, api_key=api_key) as client:
        try:
            deployment = await client.read_deployment_by_name(HELLO_DEPLOYMENT)
        except ObjectNotFound:
            raise ToolError(
                f"No Prefect deployment named '{HELLO_DEPLOYMENT}'. Run `prefect deploy --all` first."
            )
        run = await client.create_flow_run_from_deployment(
            deployment.id, parameters={"name": name}
        )

    ui_url = api_url.replace(
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
