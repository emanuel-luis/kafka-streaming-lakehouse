"""Event contract shared by the producer, the consumer and the Spark jobs."""

import re
from collections.abc import Mapping
from typing import Any

SUBJECT = "shop.events-value"
EVENT_TYPES = ("page_view", "add_to_cart", "order_placed", "payment")
PAYMENT_STATUSES = ("approved", "declined")
PAYMENT_METHODS = ("card", "wallet", "bank_transfer")
WATERMARK_DELAY = "3 hours"
MAX_SPAN_MINUTES = 150

# Validation reports the first rule that fails, in this order.
REJECT_REASONS = (
    "invalid_event_id",
    "unknown_event_type",
    "missing_required_field",
    "invalid_quantity",
    "invalid_amount",
    "invalid_currency",
    "invalid_payment_status",
)

# Required next to event_id, event_type, event_time, user_id and session_id.
REQUIRED_FIELDS: dict[str, tuple[str, ...]] = {
    "page_view": ("page_url",),
    "add_to_cart": ("product_id", "quantity", "unit_price"),
    "order_placed": ("order_id", "amount", "currency"),
    "payment": ("order_id", "amount", "currency", "payment_method", "payment_status"),
}

_UUID = re.compile(r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}", re.IGNORECASE)
_CURRENCY = re.compile(r"[A-Z]{3}")


def validate(record: Mapping[str, Any]) -> str | None:
    """Reference validator: the first broken rule, or None. The Spark validator mirrors it."""
    event_id = record.get("event_id")
    if not isinstance(event_id, str) or not _UUID.fullmatch(event_id):
        return REJECT_REASONS[0]
    event_type = record.get("event_type")
    if event_type not in EVENT_TYPES:
        return REJECT_REASONS[1]
    if (
        not record.get("user_id")
        or not record.get("session_id")
        or any(record.get(f) is None for f in REQUIRED_FIELDS[event_type])
    ):
        return REJECT_REASONS[2]
    quantity = record.get("quantity")
    if quantity is not None and not 1 <= quantity <= 100:
        return REJECT_REASONS[3]
    if any(record.get(f) is not None and not record[f] > 0 for f in ("unit_price", "amount")):
        return REJECT_REASONS[4]
    currency = record.get("currency")
    if currency is not None and not _CURRENCY.fullmatch(currency):
        return REJECT_REASONS[5]
    status = record.get("payment_status")
    if status is not None and status not in PAYMENT_STATUSES:
        return REJECT_REASONS[6]
    return None
