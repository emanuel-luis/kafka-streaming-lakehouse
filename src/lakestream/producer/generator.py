"""Pure, seeded event generator and injections. No I/O, no wall clock."""

import random
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from typing import Any

from lakestream.contracts import MAX_SPAN_MINUTES, PAYMENT_METHODS, REJECT_REASONS
from lakestream.producer.model import Event, SendItem, SendPlan

DEFAULT_BASE_TIME = datetime(2026, 1, 1, tzinfo=UTC)
MIN_USER_GAP_MS = 45 * 60_000  # between two sessions of one user
CURRENCIES = ("BRL", "USD", "EUR")
MALFORMED_KINDS = ("bad_magic", "truncated")
_PLACEMENT_ATTEMPTS = 20


@dataclass(frozen=True)
class GenParams:
    sessions: int = 400
    users: int = 250
    seed: int = 42
    base_time: datetime = DEFAULT_BASE_TIME
    span_minutes: int = 90

    def __post_init__(self) -> None:
        if not 1 <= self.span_minutes <= MAX_SPAN_MINUTES:
            raise ValueError(f"span-minutes must be between 1 and {MAX_SPAN_MINUTES}")
        if self.sessions < 0 or self.users < 1:
            raise ValueError("sessions must be >= 0 and users >= 1")
        if self.base_time.tzinfo is None:
            raise ValueError("base-time must carry a time zone")


@dataclass(frozen=True)
class InjectParams:
    duplicate_rate: float = 0.02
    late_rate: float = 0.03
    late_max_seconds: int = 180
    malformed: int = 10
    invalid: int = 14

    def __post_init__(self) -> None:
        if not (0 <= self.duplicate_rate <= 1 and 0 <= self.late_rate <= 1):
            raise ValueError("duplicate-rate and late-rate must be between 0 and 1")
        if self.duplicate_rate + self.late_rate > 1:
            raise ValueError("duplicate-rate plus late-rate must not exceed 1")
        if min(self.late_max_seconds, self.malformed, self.invalid) < 0:
            raise ValueError("late-max-seconds, malformed and invalid must be >= 0")


def _uuid(rng: random.Random) -> str:
    return str(uuid.UUID(int=rng.getrandbits(128), version=4))


def _money(cents: int) -> Decimal:
    return Decimal(cents).scaleb(-2)


def _funnel(rng: random.Random, user_id: str) -> list[dict[str, Any]]:
    """Event fields (no time) of one session, in order."""
    base = {"user_id": user_id, "session_id": _uuid(rng)}
    out: list[dict[str, Any]] = []
    for _ in range(rng.randint(1, 8)):
        product = f"p-{rng.randint(1, 200):04d}" if rng.random() < 0.5 else None
        out.append(
            {
                **base,
                "event_id": _uuid(rng),
                "event_type": "page_view",
                "page_url": f"/shop/{rng.choice(('home', 'search', 'product', 'cart'))}",
                "product_id": product,
            }
        )
    if rng.random() < 0.35:
        total = Decimal("0.00")
        for _ in range(rng.randint(1, 3)):
            quantity, price = rng.randint(1, 5), _money(rng.randint(199, 19999))
            total += quantity * price
            out.append(
                {
                    **base,
                    "event_id": _uuid(rng),
                    "event_type": "add_to_cart",
                    "product_id": f"p-{rng.randint(1, 200):04d}",
                    "quantity": quantity,
                    "unit_price": price,
                }
            )
        if rng.random() < 0.5:
            order = {
                "order_id": f"o-{rng.getrandbits(48):012x}",
                "amount": total,
                "currency": rng.choice(CURRENCIES),
            }
            out.append({**base, "event_id": _uuid(rng), "event_type": "order_placed", **order})
            out.append(
                {
                    **base,
                    **order,
                    "event_id": _uuid(rng),
                    "event_type": "payment",
                    "payment_method": rng.choice(PAYMENT_METHODS),
                    "payment_status": "approved" if rng.random() < 0.9 else "declined",
                }
            )
    return out


def generate(p: GenParams) -> list[Event]:
    """Every clean event of the run, sorted by event time."""
    rng = random.Random(p.seed)
    span_ms = p.span_minutes * 60_000 - 1  # last usable offset
    user_ids = [f"user-{i:05d}" for i in range(p.users)]
    busy: dict[str, list[tuple[int, int]]] = {}
    keyed: list[tuple[datetime, int, int, Event]] = []
    for s in range(p.sessions):
        user = user_ids[rng.randrange(len(user_ids))]
        fields = _funnel(rng, user)
        gaps = [rng.randint(1_000, 120_000) for _ in fields[1:]]
        if sum(gaps) > span_ms:  # tiny spans: squeeze the session into the span
            total = sum(gaps)
            gaps = [g * span_ms // total for g in gaps]
        duration = sum(gaps)
        start = 0
        for _ in range(_PLACEMENT_ATTEMPTS):
            start = rng.randint(0, span_ms - duration)
            if all(
                start >= end + MIN_USER_GAP_MS or start + duration + MIN_USER_GAP_MS <= begin
                for begin, end in busy.get(user, ())
            ):
                break
        else:  # the user is full: mint a new one rather than break the 45-minute rule
            user = f"user-{len(user_ids):05d}"
            user_ids.append(user)
            fields = [{**f, "user_id": user} for f in fields]
            start = rng.randint(0, span_ms - duration)
        busy.setdefault(user, []).append((start, start + duration))
        offset = start
        for seq, f in enumerate(fields):
            if seq:
                offset += gaps[seq - 1]
            event = Event(event_time=p.base_time + timedelta(milliseconds=offset), **f)
            keyed.append((event.event_time, s, seq, event))
    keyed.sort(key=lambda t: t[:3])
    return [t[3] for t in keyed]


def _extra_event(rng: random.Random, p: GenParams, kind: str, **over: Any) -> Event:
    """A fresh event for the invalid and malformed injections."""
    offset_ms = rng.randint(0, p.span_minutes * 60_000 - 1)
    fields: dict[str, Any] = {
        "event_id": _uuid(rng),
        "event_type": kind,
        "event_time": p.base_time + timedelta(milliseconds=offset_ms),
        "user_id": f"user-{rng.randrange(p.users):05d}",
        "session_id": _uuid(rng),
    }
    order_id = f"o-{rng.getrandbits(48):012x}"
    if kind == "page_view":
        fields["page_url"] = "/shop/home"
    elif kind == "add_to_cart":
        fields.update(product_id="p-0001", quantity=1, unit_price=_money(999))
    elif kind == "order_placed":
        fields.update(order_id=order_id, amount=_money(4999), currency="BRL")
    elif kind == "payment":
        fields.update(
            order_id=order_id,
            amount=_money(4999),
            currency="BRL",
            payment_method="card",
            payment_status="approved",
        )
    fields.update(over)
    return Event(**fields)


def make_invalid(rng: random.Random, p: GenParams, reason: str) -> Event:
    """An Avro-valid event that breaks exactly the rule `reason` and none before it."""
    match reason:
        case "invalid_event_id":
            bad_id = f"not-a-uuid-{rng.getrandbits(32):08x}"
            return _extra_event(rng, p, "page_view", event_id=bad_id)
        case "unknown_event_type":
            return _extra_event(rng, p, "page_view", event_type="refund")
        case "missing_required_field":
            return _extra_event(rng, p, "page_view", page_url=None)
        case "invalid_quantity":
            return _extra_event(rng, p, "add_to_cart", quantity=rng.choice((0, 101)))
        case "invalid_amount":
            return _extra_event(rng, p, "order_placed", amount=Decimal("0.00"))
        case "invalid_currency":
            return _extra_event(rng, p, "order_placed", currency="usd")
        case "invalid_payment_status":
            return _extra_event(rng, p, "payment", payment_status="refunded")
    raise ValueError(f"unknown reason {reason}")


def bad_magic_frame(event: Event) -> bytes:
    """Bytes that are not Confluent-framed: the first byte is never 0."""
    return b"not-avro:" + event.event_id.encode()


def truncate_frame(frame: bytes) -> bytes:
    """Keep the 5-byte header and cut the body in half (never the whole frame)."""
    return frame[: min(len(frame) - 1, 5 + max(1, (len(frame) - 5) // 2))]


def plan_sends(events: list[Event], p: GenParams, inj: InjectParams) -> SendPlan:
    """Order the sends and apply the four injections, on disjoint records."""
    rng = random.Random(f"{p.seed}:inject")
    n = len(events)
    k_late = min(round(inj.late_rate * n), n) if inj.late_max_seconds else 0
    k_dup = min(round(inj.duplicate_rate * n), n - k_late)
    perm = rng.sample(range(n), n)
    late = set(perm[:k_late])
    dup = set(perm[k_late : k_late + k_dup])

    def send_ms(i: int) -> int:
        ms = int(events[i].event_time.timestamp() * 1000)
        return ms + rng.randint(1, inj.late_max_seconds * 1000) if i in late else ms

    order = sorted(range(n), key=lambda i: (send_ms(i), i))
    # sort key = (position, tie-break, serial); a float position puts an extra between two records
    entries: list[tuple[float, int, int, SendItem]] = []

    def add(pos: float, tie: int, item: SendItem) -> None:
        entries.append((pos, tie, len(entries), item))

    for pos, i in enumerate(order):
        ev = events[i]
        add(pos, 0, SendItem("late" if i in late else "clean", ev.user_id, ev))
        if i in dup:
            add(pos + rng.randint(1, 5), 1, SendItem("duplicate", ev.user_id, ev))
    by_reason = dict.fromkeys(REJECT_REASONS, 0)
    for j in range(inj.invalid):
        reason = REJECT_REASONS[j % len(REJECT_REASONS)]
        by_reason[reason] += 1
        ev = make_invalid(rng, p, reason)
        add(rng.uniform(0, n), 2, SendItem("invalid", ev.user_id, ev))
    by_kind = dict.fromkeys(MALFORMED_KINDS, 0)
    for j in range(inj.malformed):
        kind = MALFORMED_KINDS[j % len(MALFORMED_KINDS)]
        by_kind[kind] += 1
        ev = _extra_event(rng, p, "page_view")
        add(rng.uniform(0, n), 2, SendItem(kind, ev.user_id, ev))
    entries.sort(key=lambda t: t[:3])
    return SendPlan(
        items=tuple(t[3] for t in entries),
        clean=tuple(events),
        duplicate_ids=tuple(events[i].event_id for i in sorted(dup)),
        late_ids=tuple(events[i].event_id for i in order if i in late),
        invalid_by_reason=by_reason,
        malformed_by_kind=by_kind,
    )
