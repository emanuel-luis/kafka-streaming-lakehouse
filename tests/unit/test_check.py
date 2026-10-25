import os
import uuid
from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

import pytest

from lakestream.ops.check import Query, gold_time_travel, render, run_checks, time_travel
from lakestream.ops.expectations import Expected, build_expected
from lakestream.settings import TrinoSettings

LATE = str(uuid.UUID(int=7, version=4))
MANIFEST: dict[str, Any] = {
    "produced": {"total": 13, "avro": 12, "malformed": 1},
    "clean_events": 10,
    "late_event_ids": [LATE],
    "invalid": {"total": 2, "by_reason": {"invalid_quantity": 1, "invalid_currency": 1}},
    "events_by_type": {"page_view": 7, "payment": 3},
    "sessions": 4,
    "orders_placed": 0,
    "payments_approved": 2,
    "approved_amount": "12.50",
    "minute_counts": [
        ["2026-01-01T00:00:00Z", "page_view", 7],
        ["2026-01-01T00:01:00Z", "payment", 3],
    ],
}


def _lake(exp: Expected, minutes: list[tuple[datetime, str, int]] | None = None) -> Query:
    """A fake Trino that holds exactly what `exp` expects, unless `minutes` is given."""
    brt = timezone(timedelta(hours=-3))  # the client may return another zone: same instant
    rows = minutes or [(m.astimezone(brt), t, n) for (m, t), n in exp.minute_counts.items()]
    answers: dict[str, list[Any]] = {
        "SELECT DISTINCT kafka_topic": [(exp.bronze_rows, exp.bronze_rows)],
        "ingest.records": [(exp.bronze_rows,)],
        "silver.rejects": list(exp.rejects_by_reason.items()),
        "count(DISTINCT event_id) FROM silver.events\n": [(exp.silver_rows, exp.silver_rows)],
        "event_type, count(*)": list(exp.events_by_type.items()),
        "count(device_type)": [(exp.device_type_events,)],
        "WHERE event_id IN": [(len(exp.late_event_ids),)],
        "ops.job_runs": [(0,)],
        "gold.events_per_minute": rows,
        "gold.revenue_5min": [(exp.orders_placed, exp.payments_approved, exp.approved_amount)],
        "gold.user_sessions": [(exp.sessions, exp.silver_rows)],
        "operation = 'append' ORDER BY": [(101,)],
        "FOR VERSION AS OF 101": [(5,)],
        "SELECT count(*) FROM bronze.events\n": [(exp.bronze_rows,)],
    }

    def q(sql: str) -> list[Any]:
        hits = [v for k, v in answers.items() if k in sql + "\n"]
        assert len(hits) == 1, sql
        return hits[0]

    return q


def test_a_consistent_lake_passes() -> None:
    exp = build_expected([MANIFEST])
    checks = run_checks(exp, _lake(exp), {(0, 4)})
    assert [c.name for c in checks if not c.ok] == []
    assert exp.approved_amount == Decimal("12.50")
    tt = time_travel(_lake(exp))
    assert tt.ok and tt.actual == 5
    assert render([*checks, tt]).endswith(f"{len(checks) + 1}/{len(checks) + 1} checks passed")


def test_mismatches_fail() -> None:
    exp = build_expected([MANIFEST])
    start = datetime(2026, 1, 1, tzinfo=UTC)
    wrong = [(start, "page_view", 7), (start + timedelta(minutes=1), "payment", 2)]
    checks = {c.name: c for c in run_checks(exp, _lake(exp, wrong), set())}
    assert not checks["gold events_per_minute rows"].ok
    assert "1 missing, 1 unexpected" in str(checks["gold events_per_minute rows"].actual)
    assert not checks["dlq distinct original coordinates"].ok
    failed = [c for c in checks.values() if not c.ok]
    assert len(failed) == 2
    assert "FAIL gold events_per_minute rows" in render(failed)


def test_trino_settings_defaults(monkeypatch: pytest.MonkeyPatch) -> None:
    for name in [n for n in os.environ if n.startswith("TRINO_")]:
        monkeypatch.delenv(name)
    t = TrinoSettings(_env_file=None)
    assert (t.host, t.port, t.user, t.catalog) == ("127.0.0.1", 18080, "lakestream", "lake")


def test_device_type_expectation_sums_v2_manifests() -> None:
    v2 = {**MANIFEST, "schema_version": 2, "device_type_events": 10}
    exp = build_expected([MANIFEST, v2])
    assert (exp.manifests, exp.device_type_events) == (2, 10)
    checks = {c.name: c for c in run_checks(exp, _lake(exp), {(0, 4), (1, 4)})}
    assert checks["silver rows with device_type (v2 runs)"].ok
    wrong = Expected(device_type_events=3)
    lake = _lake(exp)
    bad = {c.name: c for c in run_checks(wrong, lake, set())}
    assert not bad["silver rows with device_type (v2 runs)"].ok


def test_gold_time_travel() -> None:
    answers = {
        "SELECT sum(event_count) FROM gold.events_per_minute\n": [(20,)],
        "events_per_minute$snapshots": [(202,), (201,)],
        "FOR VERSION AS OF 201": [(10,)],
    }

    def q(sql: str) -> list[Any]:
        [hit] = [v for k, v in answers.items() if k in sql + "\n"]
        return hit

    check = gold_time_travel(q)
    assert check.ok and check.actual == 10 and "201" in check.name
    answers["events_per_minute$snapshots"] = [(202,)]  # a single gold run: nothing to compare
    assert not gold_time_travel(q).ok
