"""Dead-letter topic: original key and value bytes, the error and the coordinates in headers."""

import logging
from datetime import UTC, datetime

from confluent_kafka import KafkaError, Message, Producer

from lakestream.consumer.batch import Rec
from lakestream.consumer.decode import DecodeFailure

log = logging.getLogger("lakestream.dlq")

Headers = list[tuple[str, str | bytes | None]]


class DlqDeliveryError(Exception):
    """At least one DLQ record was not delivered: the batch must be retried."""


def build_headers(
    rec: Rec, failure: DecodeFailure, *, group: str, run_id: str, failed_at: datetime
) -> Headers:
    values = {
        "dlq.error.class": failure.error_class,
        "dlq.error.message": failure.message,
        "dlq.original.topic": rec.topic,
        "dlq.original.partition": str(rec.partition),
        "dlq.original.offset": str(rec.offset),
        "dlq.original.timestamp": str(rec.timestamp_ms),
        "dlq.consumer.group": group,
        "dlq.run_id": run_id,
        "dlq.failed_at": failed_at.astimezone(UTC)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z"),
    }
    return [(k, v.encode("utf-8")) for k, v in values.items()]


class DlqWriter:
    def __init__(self, bootstrap_servers: str, topic: str, group: str, run_id: str) -> None:
        self.topic, self.group, self.run_id = topic, group, run_id
        self._producer = Producer(
            {
                "bootstrap.servers": bootstrap_servers,
                "enable.idempotence": True,
                "acks": "all",
                "linger.ms": 20,
                "message.timeout.ms": 30_000,
            }
        )
        self._failed = 0

    def _on_delivery(self, err: KafkaError | None, _msg: Message) -> None:
        if err is not None:
            self._failed += 1
            log.error("dlq delivery failed", extra={"error": str(err)})

    def send(self, items: list[tuple[Rec, DecodeFailure]]) -> None:
        """Produce every item and flush; raise DlqDeliveryError if any delivery failed."""
        self._failed = 0
        now = datetime.now(UTC)
        for rec, failure in items:
            headers = build_headers(
                rec, failure, group=self.group, run_id=self.run_id, failed_at=now
            )
            while True:
                try:
                    self._producer.produce(
                        self.topic,
                        key=rec.key,
                        value=rec.value,
                        headers=headers,
                        on_delivery=self._on_delivery,
                    )
                    break
                except BufferError:
                    self._producer.poll(0.5)
        remaining = self._producer.flush(60)
        if remaining or self._failed:
            raise DlqDeliveryError(f"{self._failed} failed, {remaining} undelivered")
