"""Run identity and the clock."""

import os
import secrets
from datetime import UTC, datetime


def utc_now() -> datetime:
    return datetime.now(UTC)


def new_run_id(now: datetime | None = None) -> str:
    """`<yyyymmddThhmmssZ>-<6 hex>`."""
    return f"{(now or utc_now()):%Y%m%dT%H%M%SZ}-{secrets.token_hex(3)}"


def get_run_id() -> str:
    """The `RUN_ID` env var, or a fresh id. Call once per process and pass the result around."""
    return os.environ.get("RUN_ID") or new_run_id()
