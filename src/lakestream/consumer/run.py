"""Kafka I/O of `lakestream consume`: poll, decode, DLQ, bronze append, then commit offsets."""

import json
import logging
import os
import signal
import socket
import time
from functools import partial
from types import FrameType
from typing import Any

from confluent_kafka import Consumer, KafkaException, Message, TopicPartition
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from pyiceberg.catalog import Catalog
from pyiceberg.table import Table
from pyiceberg.types import NestedField

from lakestream.consumer.batch import (
    BatchBuffer,
    Rec,
    catch_up_commits,
    first_offsets,
    resume_offset,
    with_backoff,
)
from lakestream.consumer.bronze import (
    OffsetFenceError,
    append_batch,
    avro_fields,
    bronze_schema,
    build_row,
    build_summary,
    ensure_table,
    last_written_offsets,
)
from lakestream.consumer.decode import DecodeFailure, decode
from lakestream.consumer.dlq import DlqWriter
from lakestream.contracts import SUBJECT
from lakestream.lake import load_lake_catalog
from lakestream.registry import load_schema
from lakestream.runmeta import utc_now
from lakestream.settings import (
    ConsumerSettings,
    IcebergSettings,
    KafkaSettings,
    S3Settings,
)

log = logging.getLogger("lakestream.consume")

POLL_MAX_MESSAGES = 500


def consumer_config(kafka: KafkaSettings, group: str, client_id: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": kafka.bootstrap_servers,
        "group.id": group,
        "client.id": client_id,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
        "partition.assignment.strategy": "cooperative-sticky",
        "group.protocol": "classic",
    }


class BronzeConsumer:
    def __init__(
        self,
        *,
        kafka: KafkaSettings,
        registry_url: str,
        catalog: Catalog,
        cfg: ConsumerSettings,
        run_id: str,
        crash_after_write_batch: int | None = None,
    ) -> None:
        self.kafka, self.cfg, self.run_id = kafka, cfg, run_id
        self.catalog = catalog
        self.table: Table = ensure_table(catalog, cfg.bronze_table, bronze_schema(load_schema(1)))
        self.buffer = BatchBuffer(cfg.batch_max_messages, cfg.batch_max_seconds)
        self.consumer = Consumer(
            consumer_config(kafka, cfg.group_id, f"{socket.gethostname()}-{run_id}")
        )
        self.dlq = DlqWriter(kafka.bootstrap_servers, kafka.dlq_topic, cfg.group_id, run_id)
        self.registry = SchemaRegistryClient({"url": registry_url})
        # No reader schema: every record decodes with its writer schema.
        self.deserializer = AvroDeserializer(
            self.registry, conf={"subject.name.strategy": lambda _ctx, _name: SUBJECT}
        )
        self.writer_fields: dict[int, list[NestedField]] = {}  # per schema id
        self.crash_after = crash_after_write_batch
        self.writes = 0
        self.batch_id = 0
        self.stop = False
        self.last_activity = time.monotonic()
        self.assigned = False
        self.pending_commits: dict[int, int] = {}

    # --- rebalance callbacks (run inside poll/consume) ----------------------------------------

    def on_assign(self, consumer: Consumer, partitions: list[TopicPartition]) -> None:
        committed = consumer.committed(partitions, timeout=30)
        written = last_written_offsets(self.table, self.cfg.group_id, self.kafka.topic)
        before = {tp.partition: tp.offset for tp in committed}
        for tp in committed:
            tp.offset = resume_offset(tp.offset, written.get(tp.partition))
        # Committed from the main loop, not inside this callback.
        self.pending_commits.update(
            catch_up_commits(before, {tp.partition: tp.offset for tp in committed})
        )
        consumer.incremental_assign(committed)
        self.assigned = True
        self.last_activity = time.monotonic()
        log.info(
            "partitions assigned",
            extra={"offsets": {tp.partition: tp.offset for tp in committed}},
        )

    def on_revoke(self, _consumer: Consumer, partitions: list[TopicPartition]) -> None:
        dropped = self.buffer.drop_partitions({tp.partition for tp in partitions})
        for tp in partitions:
            self.pending_commits.pop(tp.partition, None)
        log.info(
            "partitions revoked",
            extra={"partitions": [tp.partition for tp in partitions], "dropped": dropped},
        )

    # --- batch --------------------------------------------------------------------------------

    def _on_retry(self, exc: Exception, attempt: int, delay: float) -> None:
        log.warning(
            "batch step failed, retrying",
            extra={
                "batch_id": self.batch_id,
                "attempt": attempt,
                "delay": delay,
                "error": f"{type(exc).__name__}: {exc}",
            },
        )

    def _split(self, recs: list[Rec]) -> tuple[list[tuple[Rec, int, dict[str, Any]]], list[Any]]:
        good: list[tuple[Rec, int, dict[str, Any]]] = []
        bad: list[tuple[Rec, DecodeFailure]] = []
        for rec in recs:
            try:
                schema_id, record = decode(self.deserializer, rec)
            except DecodeFailure as failure:
                bad.append((rec, failure))
            else:
                good.append((rec, schema_id, record))
        return good, bad

    def _fields_of(self, schema_ids: set[int]) -> list[NestedField]:
        """Iceberg columns of the writer schemas; a new schema id is looked up once."""
        for sid in sorted(schema_ids - self.writer_fields.keys()):
            schema = with_backoff(
                partial(self.registry.get_schema, sid), self.cfg.write_retries, self._on_retry
            )
            if not schema.schema_str:
                raise ValueError(f"the registry returned no schema text for id {sid}")
            self.writer_fields[sid] = avro_fields(json.loads(schema.schema_str))
            log.info("new writer schema", extra={"schema_id": sid})
        return [f for sid in sorted(schema_ids) for f in self.writer_fields[sid]]

    def flush_batch(self) -> None:
        offsets = self.buffer.last_offsets()
        recs = self.buffer.take()
        if not recs:
            return
        self.batch_id += 1
        started = time.monotonic()
        retries = self.cfg.write_retries
        good, bad = with_backoff(lambda: self._split(recs), retries, self._on_retry)
        if bad:
            with_backoff(lambda: self.dlq.send(bad), retries, self._on_retry)
        if good:
            ingested_at = utc_now()
            rows = [
                build_row(
                    r,
                    sid,
                    record,
                    run_id=self.run_id,
                    batch_id=self.batch_id,
                    ingested_at=ingested_at,
                )
                for r, sid, record in good
            ]
            summary = build_summary(
                run_id=self.run_id,
                batch_id=self.batch_id,
                group=self.cfg.group_id,
                topic=self.kafka.topic,
                offsets=offsets,
                records=len(rows),
                dlq_records=len(bad),
            )
            append_batch(
                self.table,
                rows,
                summary,
                first_offsets=first_offsets(recs),
                retries=retries,
                writer_fields=self._fields_of({sid for _, sid, _ in good}),
            )
            self.writes += 1
            if self.crash_after is not None and self.writes == self.crash_after:
                os._exit(1)  # test hook: written to Iceberg, not committed to Kafka
        to_commit = [TopicPartition(self.kafka.topic, p, o + 1) for p, o in sorted(offsets.items())]
        # A pending catch-up commit is older than this batch: drop it so it never moves back.
        for p in offsets:
            self.pending_commits.pop(p, None)
        with_backoff(
            lambda: self.consumer.commit(offsets=to_commit, asynchronous=False),
            retries,
            self._on_retry,
        )
        log.info(
            "batch committed",
            extra={
                "batch_id": self.batch_id,
                "records": len(good),
                "dlq_records": len(bad),
                "offsets": offsets,
                "seconds": round(time.monotonic() - started, 3),
            },
        )

    # --- loop ---------------------------------------------------------------------------------

    def _add(self, msg: Message) -> None:
        err = msg.error()
        if err is not None:
            if err.fatal():
                raise KafkaException(err)
            log.warning("consumer error", extra={"error": str(err)})
            return
        self.buffer.add(
            Rec(
                msg.topic() or self.kafka.topic,
                msg.partition() or 0,
                msg.offset() or 0,
                msg.timestamp()[1],
                msg.key(),
                msg.value(),
            )
        )

    def _commit_catch_up(self) -> None:
        """Commit resume offsets that bronze snapshots put ahead of the group's Kafka commit."""
        if not self.pending_commits:
            return
        offsets = [TopicPartition(self.kafka.topic, p, o) for p, o in self.pending_commits.items()]
        try:
            self.consumer.commit(offsets=offsets, asynchronous=False)
        except KafkaException as exc:  # e.g. a rebalance in progress; retried on the next poll
            log.warning("catch-up commit failed, retrying", extra={"error": str(exc)})
            return
        log.info(
            "committed offsets already written to bronze",
            extra={"offsets": dict(sorted(self.pending_commits.items()))},
        )
        self.pending_commits.clear()

    def _beat(self) -> None:
        self.cfg.heartbeat_file.touch()

    def run(self, exit_when_idle: float | None) -> int:
        def request_stop(_signum: int, _frame: FrameType | None) -> None:
            self.stop = True

        signal.signal(signal.SIGTERM, request_stop)
        signal.signal(signal.SIGINT, request_stop)
        self.consumer.subscribe(
            [self.kafka.topic],
            on_assign=self.on_assign,
            on_revoke=self.on_revoke,
            on_lost=self.on_revoke,
        )
        log.info("consumer started", extra={"topic": self.kafka.topic, "group": self.cfg.group_id})
        try:
            while not self.stop:
                self._beat()
                msgs = self.consumer.consume(
                    num_messages=min(POLL_MAX_MESSAGES, self.buffer.room()), timeout=1.0
                )
                self._commit_catch_up()
                for msg in msgs:
                    self._add(msg)
                if msgs:
                    self.last_activity = time.monotonic()
                if self.buffer.due():
                    self.flush_batch()
                    self.last_activity = time.monotonic()
                elif (
                    exit_when_idle is not None
                    and self.assigned
                    and time.monotonic() - self.last_activity >= exit_when_idle
                ):
                    log.info("idle, exiting")
                    break
            self.flush_batch()  # a stop request or idle exit finishes the buffered batch
        except OffsetFenceError:
            log.exception(
                "batch fenced by offsets another writer wrote, exiting without committing"
            )
            self.consumer.close()
            return 1
        except Exception:
            log.exception("consumer failed, exiting without committing the open batch")
            self.consumer.close()
            return 1
        self.consumer.close()
        return 0


def run_consume(
    *,
    kafka: KafkaSettings,
    registry_url: str,
    iceberg: IcebergSettings,
    s3: S3Settings,
    cfg: ConsumerSettings,
    run_id: str,
    exit_when_idle: float | None = None,
    crash_after_write_batch: int | None = None,
) -> int:
    """Run until stopped. Returns the exit code: 0 after a clean stop, 1 on a failure."""
    consumer = BronzeConsumer(
        kafka=kafka,
        registry_url=registry_url,
        catalog=load_lake_catalog(iceberg, s3),
        cfg=cfg,
        run_id=run_id,
        crash_after_write_batch=crash_after_write_batch,
    )
    return consumer.run(exit_when_idle)
