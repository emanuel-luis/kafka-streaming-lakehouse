import json
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any

import pytest
from hypothesis import given
from hypothesis import strategies as st

from lakestream.ops.expectations import build_expected, parse_minute
from lakestream.producer.generator import GenParams, InjectParams, generate, plan_sends
from lakestream.producer.manifest import build_manifest
from lakestream.runmeta import utc_now


def _manifest(seed: int, sessions: int = 30, base: str = "2026-01-01T00:00:00Z") -> dict[str, Any]:
    p = GenParams(sessions=sessions, users=20, seed=seed, base_time=parse_minute(base))
    plan = plan_sends(generate(p), p, InjectParams())
    now = utc_now()
    m = build_manifest(
        plan=plan,
        run_id="r",
        seed=seed,
        schema_version=1,
        base_time=p.base_time,
        span_minutes=p.span_minutes,
        sessions=p.sessions,
        started_at=now,
        finished_at=now,
        delivered=len(plan.items),
        failed=0,
    )
    loaded: dict[str, Any] = json.loads(json.dumps(m))  # exactly what the check reads
    return loaded


@given(st.integers(0, 10**6))
def test_one_manifest(seed: int) -> None:
    m = _manifest(seed)
    e = build_expected([m])
    assert e.bronze_rows == m["produced"]["avro"]
    assert e.dlq_records == m["produced"]["malformed"]
    assert e.rejects == sum(e.rejects_by_reason.values()) == m["invalid"]["total"]
    assert e.silver_rows == m["clean_events"] == sum(e.events_by_type.values())
    assert sum(e.minute_counts.values()) == e.silver_rows  # every clean event in one minute row
    assert all(t.tzinfo is not None and t.second == 0 for t, _ in e.minute_counts)
    assert e.late_event_ids == set(m["late_event_ids"])
    assert e.approved_amount == Decimal(m["approved_amount"])
    assert (e.sessions, e.orders_placed, e.payments_approved) == (
        m["sessions"],
        m["orders_placed"],
        m["payments_approved"],
    )


@given(st.integers(0, 10**6), st.integers(0, 10**6))
def test_manifests_are_summed(seed1: int, seed2: int) -> None:
    m1, m2 = _manifest(seed1), _manifest(seed2, base="2026-01-02T00:00:00Z")
    e1, e2, both = build_expected([m1]), build_expected([m2]), build_expected([m1, m2])
    assert both.bronze_rows == e1.bronze_rows + e2.bronze_rows
    assert both.silver_rows == e1.silver_rows + e2.silver_rows
    assert both.events_by_type == e1.events_by_type + e2.events_by_type
    assert both.rejects_by_reason == e1.rejects_by_reason + e2.rejects_by_reason
    assert both.minute_counts == e1.minute_counts + e2.minute_counts
    assert both.approved_amount == e1.approved_amount + e2.approved_amount
    assert both.late_event_ids == e1.late_event_ids | e2.late_event_ids


def test_runs_sharing_a_minute_add_up_in_one_gold_row() -> None:
    m = _manifest(42)
    e = build_expected([m, m])
    assert e.minute_counts == {k: 2 * v for k, v in build_expected([m]).minute_counts.items()}
    assert len(e.minute_counts) == len(m["minute_counts"])


def test_empty_and_minute_parsing() -> None:
    assert build_expected([]).approved_amount == Decimal("0.00")
    assert parse_minute("2026-01-01T00:05:00Z") == datetime(2026, 1, 1, 0, 5, tzinfo=UTC)
    with pytest.raises(ValueError, match="time zone"):
        parse_minute("2026-01-01T00:05:00")
