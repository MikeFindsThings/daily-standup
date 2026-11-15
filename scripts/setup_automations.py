"""Create (or update) the two Prefect automations for daily-standup. Safe to re-run.

    uv run python scripts/setup_automations.py

1. Skip when nobody answers: a run that stays suspended for 12 hours is cancelled and
   labelled "Skipped", so an ignored draft never shows up as a failure.
2. Tell me when a run fails: sends a notification through a Prefect notification block.
   Create that block yourself first (it holds your address, which stays out of this repo):
   Prefect Cloud UI -> Blocks -> + -> Email -> name it `standup-alerts`.
"""

from datetime import timedelta

from prefect import get_client
from prefect.automations import Automation
from prefect.client.schemas.objects import StateType
from prefect.events.actions import ChangeFlowRunState, SendNotification
from prefect.events.schemas.automations import EventTrigger, Posture
from prefect.exceptions import ObjectNotFound

FLOW_NAME = "daily-standup"
ALERT_BLOCK_TYPE = "email"
ALERT_BLOCK_NAME = "standup-alerts"
SKIP_AFTER = timedelta(hours=12)

ONLY_THIS_FLOW = {"prefect.resource.role": "flow", "prefect.resource.name": FLOW_NAME}
ANY_FLOW_RUN = {"prefect.resource.id": "prefect.flow-run.*"}


def upsert(automation: Automation) -> None:
    try:
        existing = Automation.read(name=automation.name)
    except (ValueError, ObjectNotFound):
        existing = None
    if existing:
        existing.delete()
    automation.create()
    print(f"{'Updated' if existing else 'Created'}: {automation.name}")


def skip_when_unanswered() -> Automation:
    return Automation(
        name="daily-standup: skip when unanswered",
        description="Cancel a run that has waited 12 hours for approval, labelled Skipped.",
        trigger=EventTrigger(
            posture=Posture.Proactive,
            match=ANY_FLOW_RUN,
            match_related=ONLY_THIS_FLOW,
            after={"prefect.flow-run.Suspended"},
            expect={
                "prefect.flow-run.Resuming", "prefect.flow-run.Running", "prefect.flow-run.Completed",
                "prefect.flow-run.Skipped", "prefect.flow-run.Failed", "prefect.flow-run.Crashed",
                "prefect.flow-run.Cancelled", "prefect.flow-run.Cancelling",
            },
            for_each={"prefect.resource.id"},
            threshold=1,
            within=SKIP_AFTER,
        ),
        actions=[ChangeFlowRunState(
            state=StateType.CANCELLED, name="Skipped",
            message="No reply within 12 hours. Nothing was posted.",
        )],
    )


def notify_on_failure(block_document_id) -> Automation:
    return Automation(
        name="daily-standup: notify on failure",
        description="Send a notification when a daily-standup run fails or crashes.",
        trigger=EventTrigger(
            posture=Posture.Reactive,
            match=ANY_FLOW_RUN,
            match_related=ONLY_THIS_FLOW,
            expect={"prefect.flow-run.Failed", "prefect.flow-run.Crashed", "prefect.flow-run.TimedOut"},
            for_each={"prefect.resource.id"},
            threshold=1,
            within=timedelta(0),
        ),
        actions=[SendNotification(
            block_document_id=block_document_id,
            subject="daily-standup run {{ flow_run.name }} {{ flow_run.state.name }}",
            body=(
                "Flow run {{ flow_run.name }} entered state {{ flow_run.state.name }}.\n"
                "Message: {{ flow_run.state.message }}\n"
                "Open it: {{ flow_run|ui_url }}"
            ),
        )],
    )


def main() -> None:
    upsert(skip_when_unanswered())
    with get_client(sync_client=True) as client:
        try:
            block = client.read_block_document_by_name(ALERT_BLOCK_NAME, ALERT_BLOCK_TYPE)
        except ObjectNotFound:
            block = None
    if block is None:
        print(
            f"Not created: failure notification. No `{ALERT_BLOCK_TYPE}` block named "
            f"`{ALERT_BLOCK_NAME}` exists yet. Create it in the Prefect Cloud UI, then re-run this script."
        )
        return
    upsert(notify_on_failure(block.id))


if __name__ == "__main__":
    main()
