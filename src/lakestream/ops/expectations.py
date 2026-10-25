"""What every layer must hold after the runs in one or more producer manifests. Pure, no I/O.

Every expectation is summed over the manifests. Gold has one row per (minute, event type), so the
minute counts of runs that share a minute add up in the same row.
"""

from collections import Counter
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class Expected:
    bronze_rows: int = 0
    dlq_records: int = 0
    rejects: int = 0
    rejects_by_reason: Counter[str] = field(default_factory=Counter)
    silver_rows: int = 0
    events_by_type: Counter[str] = field(default_factory=Counter)
    late_event_ids: frozenset[str] = frozenset()
    minute_counts: Counter[tuple[datetime, str]] = field(default_factory=Counter)
    sessions: int = 0
    orders_placed: int = 0
    payments_approved: int = 0
    approved_amount: Decimal = Decimal("0.00")


def parse_minute(value: str) -> datetime:
    """`2026-01-01T00:01:00Z` -> an aware UTC datetime."""
    ts = datetime.fromisoformat(value)
    if ts.tzinfo is None:
        raise ValueError(f"minute without a time zone: {value!r}")
    return ts


def build_expected(manifests: Iterable[Mapping[str, Any]]) -> Expected:
    """Sum the expectations of every manifest. Raises KeyError on a manifest missing a field."""
    bronze = dlq = rejects = silver = sessions = orders = approved = 0
    by_reason: Counter[str] = Counter()
    by_type: Counter[str] = Counter()
    minutes: Counter[tuple[datetime, str]] = Counter()
    late: set[str] = set()
    amount = Decimal("0.00")
    for m in manifests:
        bronze += m["produced"]["avro"]
        dlq += m["produced"]["malformed"]
        rejects += m["invalid"]["total"]
        by_reason.update(m["invalid"]["by_reason"])
        silver += m["clean_events"]
        by_type.update(m["events_by_type"])
        late.update(m["late_event_ids"])
        for minute, event_type, count in m["minute_counts"]:
            minutes[(parse_minute(minute), event_type)] += count
        sessions += m["sessions"]
        orders += m["orders_placed"]
        approved += m["payments_approved"]
        amount += Decimal(m["approved_amount"])
    return Expected(
        bronze_rows=bronze,
        dlq_records=dlq,
        rejects=rejects,
        rejects_by_reason=+by_reason,  # unary + drops zero counts
        silver_rows=silver,
        events_by_type=+by_type,
        late_event_ids=frozenset(late),
        minute_counts=+minutes,
        sessions=sessions,
        orders_placed=orders,
        payments_approved=approved,
        approved_amount=amount,
    )
