#!/bin/sh
# Creates the Kafka topics of the contract in docs/PLAN.md. Idempotent; runs in the cp-kafka image.
set -eu

BOOTSTRAP="${KAFKA_BOOTSTRAP_SERVERS:-kafka:29092}"
TOPIC="${KAFKA_TOPIC:-shop.events}"
DLQ_TOPIC="${KAFKA_DLQ_TOPIC:-shop.events.dlq}"

# Each tool call is a JVM of its own; keep the heap small.
export KAFKA_HEAP_OPTS="-Xmx64m"

kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic "$TOPIC" --partitions 6 --replication-factor 1 \
  --config retention.ms=604800000 --config cleanup.policy=delete

kafka-topics --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
  --topic "$DLQ_TOPIC" --partitions 1 --replication-factor 1 \
  --config retention.ms=2592000000

kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$TOPIC"
kafka-topics --bootstrap-server "$BOOTSTRAP" --describe --topic "$DLQ_TOPIC"
