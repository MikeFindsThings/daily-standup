"""Stage 0 spike: a trivial flow to prove Prefect Managed compute works end to end."""

from datetime import datetime, timezone

from prefect import flow, get_run_logger, task


@task(retries=2, retry_delay_seconds=5)
def say_hello(name: str) -> str:
    message = f"Hello, {name}. It is {datetime.now(timezone.utc).isoformat()} UTC."
    get_run_logger().info(message)
    return message


@flow(name="hello", log_prints=True)
def hello(name: str = "world") -> str:
    return say_hello(name)


if __name__ == "__main__":
    hello()
