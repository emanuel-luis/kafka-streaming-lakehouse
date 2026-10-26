"""Run manifest: what the producer sent, so the end-to-end check can compute expectations."""

import json
import os
from collections import Counter
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from lakestream.producer.model import SendPlan


def _iso(ts: datetime) -> str:
    return ts.astimezone(UTC).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def minute_counts(plan: SendPlan) -> list[list[Any]]:
    """`[minute_utc, event_type, count]` over the distinct valid events, sorted."""
    counts = Counter(
        (e.event_time.astimezone(UTC).replace(second=0, microsecond=0), e.event_type)
        for e in plan.clean
    )
    return [[_iso(m).replace(":00.000Z", ":00Z"), t, c] for (m, t), c in sorted(counts.items())]


def build_manifest(
    *,
    plan: SendPlan,
    run_id: str,
    seed: int,
    schema_version: int,
    base_time: datetime,
    span_minutes: int,
    sessions: int,
    started_at: datetime,
    finished_at: datetime,
    delivered: int,
    failed: int,
) -> dict[str, Any]:
    malformed = sum(plan.malformed_by_kind.values())
    invalid = sum(plan.invalid_by_reason.values())
    total = len(plan.clean) + len(plan.duplicate_ids) + invalid + malformed
    payments = [e for e in plan.clean if e.event_type == "payment"]
    approved = [e for e in payments if e.payment_status == "approved"]
    approved_amount = sum((e.amount for e in approved if e.amount is not None), Decimal("0.00"))
    return {
        "run_id": run_id,
        "seed": seed,
        "schema_version": schema_version,
        "base_time": _iso(base_time),
        "span_minutes": span_minutes,
        "started_at": _iso(started_at),
        "finished_at": _iso(finished_at),
        "produced": {"total": total, "avro": total - malformed, "malformed": malformed},
        "clean_events": len(plan.clean),
        "duplicates": len(plan.duplicate_ids),
        "late": len(plan.late_ids),
        "late_event_ids": list(plan.late_ids),
        "invalid": {"total": invalid, "by_reason": dict(plan.invalid_by_reason)},
        "malformed_by_kind": dict(plan.malformed_by_kind),
        "events_by_type": dict(sorted(Counter(e.event_type for e in plan.clean).items())),
        "sessions": sessions,
        "orders_placed": sum(e.event_type == "order_placed" for e in plan.clean),
        "payments_approved": len(approved),
        "approved_amount": f"{approved_amount:.2f}",
        "minute_counts": minute_counts(plan),
        "delivery": {"delivered": delivered, "failed": failed},
    }


def write_manifest(path: Path, manifest: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    os.replace(tmp, path)
