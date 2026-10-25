"""Plain data types of the producer."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any


@dataclass(frozen=True)
class Event:
    event_id: str
    event_type: str
    event_time: datetime
    user_id: str
    session_id: str
    page_url: str | None = None
    product_id: str | None = None
    quantity: int | None = None
    unit_price: Decimal | None = None
    order_id: str | None = None
    amount: Decimal | None = None
    currency: str | None = None
    payment_method: str | None = None
    payment_status: str | None = None
    device_type: str | None = None  # schema v2

    def as_record(self) -> dict[str, Any]:
        """The Avro record, with the Python types the serializer expects (v1 has no device_type)."""
        record = dict(self.__dict__)
        if record["device_type"] is None:
            del record["device_type"]
        return record


@dataclass(frozen=True)
class SendItem:
    """One record to send. kind: clean, late, duplicate, invalid, bad_magic or truncated."""

    kind: str
    key: str
    event: Event


@dataclass(frozen=True)
class SendPlan:
    items: tuple[SendItem, ...]
    clean: tuple[Event, ...]  # every distinct valid event, in event-time order
    duplicate_ids: tuple[str, ...]
    late_ids: tuple[str, ...]
    invalid_by_reason: dict[str, int]
    malformed_by_kind: dict[str, int]
