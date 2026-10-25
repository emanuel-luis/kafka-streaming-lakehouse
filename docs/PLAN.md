# Implementation plan

This is the plan the repository is built from. Decisions and their reasons are in
[DECISIONS.md](DECISIONS.md), and every name in this file is a contract: when one changes, the
Python side and the compose side change together.

## Data flow

| Layer | Where | What | Written by |
| --- | --- | --- | --- |
| Transport | topic `shop.events` (6 partitions, key `user_id`) | Avro `ShopEvent`, Confluent wire format | `lakestream produce` |
| Dead letters | topic `shop.events.dlq` (1 partition) | original key and value bytes, error in headers | `lakestream consume` |
| Bronze | `lake.bronze.events` | one row per Avro-decodable record, with Kafka coordinates; append-only | `lakestream consume` (PyIceberg) |
| Silver | `lake.silver.events` | valid events, deduplicated by `event_id` | `lakestream silver` (streaming) |
| Silver rejects | `lake.silver.rejects` | Avro-valid records that break the contract, with the reason | `lakestream silver` (streaming) |
| Gold | `lake.gold.events_per_minute`, `lake.gold.revenue_5min`, `lake.gold.user_sessions` | aggregates over event time | `lakestream gold` (batch) |
| Ops | `lake.ops.job_runs` | one row per Spark job run | Spark jobs |
| Serving | Trino catalog `lake` | SQL, time travel | `lakestream check`, README |

The catalog is an Iceberg REST catalog with warehouse `s3://lakehouse/warehouse`. Spark
checkpoints live on the named volume `spark_checkpoints`, mounted at `/checkpoints/<query>`.
Iceberg data files are read and written through S3FileIO, so nothing in the stack needs Hadoop's
s3a; putting checkpoints on S3 would bring in `hadoop-aws` and the full AWS SDK bundle for that one
purpose.

## Repository layout

```
compose.yaml  .env.example  Makefile  .dockerignore  CHANGELOG.md  CLAUDE.md  README.md
docker/app.Dockerfile          # targets: app (no JVM) and spark (JRE + pyspark + baked Ivy jars)
scripts/create-topics.sh       # kafka-init (cp-kafka image)
scripts/create-bucket.sh       # s3-init (curl image, SigV4 PUT)
scripts/demo.sh                # make demo
trino/catalog/lake.properties
src/lakestream/
  __init__.py py.typed cli.py settings.py logging.py runmeta.py contracts.py registry.py
  schemas/shop_event.v1.avsc shop_event.v2.avsc shop_event.v2-breaking.avsc
  producer/  model.py generator.py manifest.py run.py
  consumer/  decode.py batch.py bronze.py dlq.py run.py
  ops/       lag.py dlq_show.py expectations.py check.py
  spark/     session.py tables.py silver.py silver_job.py gold.py gold_job.py
             maintain.py checkpoint.py runlog.py
tests/unit/ tests/spark/ tests/integration/ conftest.py
.github/workflows/ci.yml security.yml  .github/dependabot.yml
```

The Avro schemas ship inside the package (`importlib.resources`) so that the wheel and the image
carry them. `cli.py`, `settings.py`, `logging.py`, `runmeta.py`, `contracts.py`, `registry.py`,
`producer/`, `consumer/` and `ops/` never import pyspark. `cli.py` imports each command's module
inside the command function, and nothing does work at import time.

## Pinned versions

Checked on 2026-10-25. App base images: `python:3.12.15-slim-bookworm`, the JRE copied from
`eclipse-temurin:17-jre` (pin the exact tag at build time), and `ghcr.io/astral-sh/uv` (pinned).

| Piece | Pin |
| --- | --- |
| Kafka / Schema Registry | `confluentinc/cp-kafka:8.3.2`, `confluentinc/cp-schema-registry:8.3.2` |
| Object storage | `rustfs/rustfs:1.0.3` |
| Iceberg REST | `apache/iceberg-rest-fixture@sha256:db8de90b5b7693d4ac334c336f91d9bbe320d7b19f4f514d26de84cdfbcbfe8d` |
| Trino | `trinodb/trino:484` |
| Kafka UI | `ghcr.io/kafbat/kafka-ui:v1.5.0` |
| Init | `curlimages/curl:8.23.0` |
| Python deps | `confluent-kafka[avro,schemaregistry]==2.16.0`, `pyiceberg[pyarrow]==0.12.0`, `trino==0.340.0`, `typer`, `pydantic-settings`; extra `spark`: `pyspark==4.1.3` |
| Spark packages | `org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0`, `org.apache.iceberg:iceberg-aws-bundle:1.12.0` |

## Kafka contract

| Topic | Partitions | Key | Config |
| --- | --- | --- | --- |
| `shop.events` | 6 | `user_id`, UTF-8 string | `retention.ms=604800000`, `cleanup.policy=delete` |
| `shop.events.dlq` | 1 | original key bytes | `retention.ms=2592000000` |

- Broker auto-create is off. `kafka-init` runs `kafka-topics --create --if-not-exists` for both
  topics.
- One topic with an envelope record, not one topic per event type. The funnel and the sessions
  need per-user ordering across all four types, and only a single topic keyed by `user_id` gives
  that. It also means one bronze table and one atomic append per batch.
- Six partitions let two consumer instances split them 3/3 and rebalance visibly when a third
  joins.
- The producer sets `partitioner=murmur2_random`. librdkafka's default (`consistent_random`,
  CRC32) places keys differently from Java clients; the README shows the difference.
- DLQ format: key and value are the original bytes, unchanged; the error goes in headers:
  `dlq.error.class`, `dlq.error.message`, `dlq.original.topic`, `dlq.original.partition`,
  `dlq.original.offset`, `dlq.original.timestamp` (epoch ms), `dlq.consumer.group`, `dlq.run_id`,
  `dlq.failed_at` (UTC ISO-8601). This follows Kafka Connect's DLQ convention, needs no schema and
  keeps the poison bytes exactly as they arrived. Delivery to the DLQ is at-least-once, so readers
  deduplicate on (`original.partition`, `original.offset`).
- No retry topic. A record that fails to decode is permanent and goes to the DLQ at once. Sink
  errors are transient: the whole batch is retried in process with bounded backoff (1, 2, 4, 8,
  16 s). When the retries run out, the consumer exits non-zero without committing and
  `restart: on-failure` resumes it. A retry topic pays off for per-record calls to an external
  service; here the sink is a batch commit.

## Avro contract

Record `lakehouse.shop.ShopEvent`. Subjects use TopicNameStrategy (`shop.events-value`); the
subject's compatibility is set explicitly to `BACKWARD`.

| Field | Avro type | Required for |
| --- | --- | --- |
| event_id | string (UUID text, no logical type, so an invalid id is still valid Avro) | all |
| event_type | string (not an enum, so unknown types reach silver rejects) | all |
| event_time | long, `timestamp-millis` | all |
| user_id, session_id | string | all |
| page_url | ["null","string"], default null | page_view |
| product_id | ["null","string"], default null | add_to_cart (optional on page_view) |
| quantity | ["null","int"], default null | add_to_cart |
| unit_price | ["null", bytes decimal(10,2)], default null | add_to_cart |
| order_id | ["null","string"], default null | order_placed, payment |
| amount | ["null", bytes decimal(12,2)], default null | order_placed (order total), payment |
| currency | ["null","string"], default null | order_placed, payment |
| payment_method | ["null","string"], default null | payment (`card`, `wallet`, `bank_transfer`) |
| payment_status | ["null","string"], default null | payment (`approved`, `declined`) |

v2 adds `device_type: ["null","string"], default null` (`mobile`, `desktop`, `tablet`), a
BACKWARD-compatible change. `shop_event.v2-breaking.avsc` adds a required field with no default
and exists only to show the registry rejecting it.

How each component handles v2:

- `register-schemas --version 2` first calls the compatibility endpoint, then registers.
- The producer sends v2 with `auto.register.schemas=false` and looks up the schema id.
- The consumer decodes every record with its writer schema (`AvroDeserializer` without a reader
  schema). When it sees a new schema id, it maps the writer schema to Iceberg types and calls
  `update_schema().union_by_name(...)` before appending.
- Silver's `ensure_tables` adds missing nullable contract columns with `ALTER TABLE ... ADD
  COLUMN`; the silver projection uses `lit(null)` for a column bronze lacks.

## Bronze contract (`lake.bronze.events`)

Iceberg format v2, `write.parquet.compression-codec=zstd`, partitioned by `days(ingested_at)`.
Every Avro field above in its Iceberg type (timestamps as `timestamptz` in microseconds, decimals
as decimal(p,s)), plus:

| Column | Type |
| --- | --- |
| kafka_topic | string |
| kafka_partition | int |
| kafka_offset | long |
| kafka_timestamp | timestamptz |
| kafka_key | string |
| schema_id | int |
| ingested_at | timestamptz |
| run_id | string |
| batch_id | long |

Arrow batches are built from `table.schema().as_arrow()` so the types match exactly.
(`kafka_topic`, `kafka_partition`, `kafka_offset`) is unique; the e2e check asserts it.

Every append carries these snapshot summary properties:

| Property | Value |
| --- | --- |
| `ingest.run_id` | the consumer's run id |
| `ingest.batch_id` | the batch number |
| `ingest.consumer_group` | the group id |
| `ingest.topic` | `shop.events` |
| `ingest.offsets` | JSON `{"<partition>": <last offset consumed in this batch, inclusive>}` |
| `ingest.records` | rows appended |
| `ingest.dlq_records` | records sent to the DLQ in this batch |

## Consumer design (`lakestream consume`)

1. Config: `enable.auto.commit=false`, `enable.auto.offset.store=false`,
   `auto.offset.reset=earliest`, `partition.assignment.strategy=cooperative-sticky`,
   `group.protocol=classic` (KIP-848 is a possible follow-up).
2. Loop: `consume(num_messages, timeout)` accumulates records until
   `CONSUMER_BATCH_MAX_MESSAGES` (2000) or `CONSUMER_BATCH_MAX_SECONDS` (5). The consumer touches
   its heartbeat file on every iteration.
3. Decode: parse the 5-byte header (magic byte 0 plus schema id) before deserializing. A frame
   that is not Avro, a schema id the registry answers 404 for, a fastavro decode error or a null
   value goes to the DLQ. A registry connection error or 5xx is transient and triggers a batch
   retry; such records never go to the DLQ.
4. Per batch: produce the DLQ records and `flush()`, failing the batch if any delivery failed.
   Then the bronze append with summary properties, wrapped in retry. On `CommitFailedException`
   refresh the table and append again. When the outcome is unknown, first look for a snapshot
   carrying this `run_id` and `batch_id` and treat one as success. Last,
   `commit(offsets=[last+1 per partition], asynchronous=False)`. A batch of only DLQ records skips
   the append and still commits.
5. Idempotency across a crash between append and commit: in `on_assign`, for each partition,
   `resume = max(committed, last_written + 1)`. `last_written` comes from walking `ingest.offsets`
   in bronze snapshots, newest first, filtered to this group and topic, skipping non-append
   snapshots. Set `tp.offset` and call `incremental_assign`. Kafka offsets alone cannot make this
   safe because the Kafka commit is a separate step from the Iceberg commit; the snapshot summary
   commits atomically with the data, so the table itself says what it holds.
6. `on_revoke` drops the buffered records of revoked partitions (never written or committed, so
   the new owner reads them again). Appends and commits happen between polls and rebalance
   callbacks run only inside polls, so a batch's append and commit never straddle a rebalance.
7. Two instances (`deploy.replicas: 2`) append to the same table under Iceberg's optimistic
   concurrency. Appends never conflict on data; a stale-metadata 409 is retried as above.
8. Schema evolution runs under the same retry. `union_by_name` is a no-op when the other instance
   already added the column.
9. SIGTERM or SIGINT sets a flag. The consumer finishes the current batch (append and commit),
   calls `close()` and exits 0. Compose sets `stop_grace_period: 30s`.
10. Test hook: hidden option `--crash-after-write-batch N` calls `os._exit(1)` after the Nth
    append and before its commit.
11. Consumer run metadata lives in the snapshot summary and the JSON logs (`run_id`, `batch_id`,
    counts, offsets, durations). No ops table, which would cost a second, non-atomic commit per
    batch.

## Silver (`lakestream silver --trigger available-now|processing-time [--interval "30 seconds"]`)

Two streaming queries read `lake.bronze.events` with `streaming-skip-overwrite-snapshots=true` and
`streaming-skip-delete-snapshots=true`. Both write through the native Iceberg streaming sink,
which records query id and epoch id in the snapshot summary and skips epochs it already
committed, so the sink is exactly-once with no custom code. `ensure_tables` creates both tables
first.

- `silver_events` (checkpoint `/checkpoints/silver_events`): `validate`, keep rows where
  `reject_reason` is null, `withWatermark("event_time", WATERMARK_DELAY)`,
  `dropDuplicatesWithinWatermark(["event_id"])`, `project_silver`.
- `silver_rejects` (checkpoint `/checkpoints/silver_rejects`): `validate`, keep rows where
  `reject_reason` is not null, `project_rejects`. Stateless.

Validation reports the first rule that fails, in this order (constants in `contracts.py`):

1. `invalid_event_id`: not a canonical UUID.
2. `unknown_event_type`: not one of the four types.
3. `missing_required_field`: empty `user_id` or `session_id`, or a field required by the table
   above is null.
4. `invalid_quantity`: not between 1 and 100.
5. `invalid_amount`: `unit_price` or `amount` is not greater than 0.
6. `invalid_currency`: does not match `^[A-Z]{3}$`.
7. `invalid_payment_status`: not `approved` or `declined`.

`WATERMARK_DELAY = "3 hours"`. The producer refuses `--span-minutes` above 150, so the event-time
spread inside one run is always smaller than the delay: no record can fall behind the watermark,
whatever the micro-batch split or the skew between the two consumers. Dedupe is exact and silver
is deterministic in both trigger modes. Each job run appends one row to `lake.ops.job_runs`, with
`rows_dropped_late` summed from `numRowsDroppedByWatermark` in `recentProgress`.

| `lake.silver.events` (partition `days(event_time)`) | `lake.silver.rejects` (partition `days(processed_at)`) |
| --- | --- |
| event_id, event_type, event_time, user_id, session_id, page_url, product_id, quantity, unit_price, order_id, amount, currency, payment_method, payment_status, device_type (from Phase 5), kafka_partition, kafka_offset, ingested_at, processed_at | event_id, event_type, event_time, user_id, reject_reason, record_json (`to_json` of the bronze row), kafka_topic, kafka_partition, kafka_offset, ingested_at, processed_at |

`lake.ops.job_runs`: run_id, job, started_at, finished_at, status, rows_in, rows_out,
rows_rejected, rows_dropped_late, details (JSON string).

## Gold (`lakestream gold`)

Gold is a batch recompute from `lake.silver.events`. Each table is replaced in one snapshot with
`writeTo(t).overwrite(lit(True))`, with `snapshot-property.run_id` set. Append-mode watermark
aggregations never emit the last windows when data stops, so an `availableNow` demo would come up
short; update mode with MERGE through foreachBatch converges but needs its own idempotency
bookkeeping, and update mode with `session_window` is doubtful. A recompute is deterministic and
re-runnable; at demo scale a full scan of silver costs seconds. A live deployment would recompute
only the partitions it touched.

| Table | Grain | Columns |
| --- | --- | --- |
| `gold.events_per_minute` (partition `days(window_start)`) | 1-minute tumbling window by event type | window_start, window_end, event_type, event_count, unique_users |
| `gold.revenue_5min` (partition `days(window_start)`) | 5-minute window by currency | window_start, window_end, currency, orders_placed, order_amount, payments_approved, payments_declined, approved_amount |
| `gold.user_sessions` (partition `days(session_start)`) | `session_window(event_time, "30 minutes")` per user | user_id, session_start, last_event_at, session_window_end, duration_seconds, event_count, page_views, add_to_carts, orders_placed, payments_approved, revenue, reached_cart, reached_order, reached_payment |

How late data is shown:

- Late events keep their event time and are sent after events with a later event time (by up to
  `--late-max-seconds`). They sit at higher offsets than newer events in the same partition, yet
  land in the window of their own event time. The manifest's per-minute expectation proves it,
  and a README query compares `event_time` order with `kafka_offset` order.
- Too-late scenario, optional and gated by a smoke test: after the run-2 silver pass,
  `produce --too-late N` sends events dated on day 1. If the watermark survives an `availableNow`
  restart, silver drops them, and the check asserts they are missing and
  `rows_dropped_late >= N`.

## Producer (`lakestream produce`)

The generator (`generator.py`) is pure and deterministic given a seed. Event ids are
`uuid.UUID(int=rng.getrandbits(128), version=4)`.

Event time uses a fixed synthetic base (`--base-time`, default `2026-01-01T00:00:00Z`) plus a
simulated clock spread over `--span-minutes` (default 90, max 150). It is never wall-clock time,
so the same seed gives identical gold. `--rate` paces only the sending.

Funnel per session: 1 to 8 `page_view`; with probability 0.35, 1 to 3 `add_to_cart`; from a cart,
`order_placed` with probability 0.5, amount = cart total; then one `payment`, approved with
probability 0.9.

Invariants (Hypothesis tests): gaps inside a session are at most 5 minutes; a user's sessions are
at least 45 minutes apart; every event falls inside the span.

Send order is by event time, then the injections, applied to disjoint records:

| Injection | Default | Effect |
| --- | --- | --- |
| duplicates | `--duplicate-rate 0.02` | a valid event re-sent with identical bytes, a few records later |
| late | `--late-rate 0.03`, `--late-max-seconds 180` | the event's send position moves later; its event time is unchanged |
| malformed | `--malformed 10` | alternates `bad_magic` (not Avro-framed) and `truncated` (real header, cut body) |
| invalid | `--invalid 14` | Avro-valid extra records, cycling through the 7 reasons |

Kafka config: `enable.idempotence=true`, `acks=all`, `linger.ms=20`, `compression.type=lz4`,
`partitioner=murmur2_random`. Valid records are serialized with `AvroSerializer` and sent with a
plain `Producer`, so raw bytes can be sent the same way. A delivery callback counts delivered and
failed records. After `flush()`, the producer exits 1 if any delivery failed, and writes the
manifest only after a clean flush.

The manifest is JSON written to `--manifest` (shared volume `/runs`): run_id, seed,
schema_version, base_time, span_minutes, started_at, finished_at; `produced{total, avro,
malformed}`, clean_events, duplicates, late, late_event_ids; `invalid{total, by_reason}`,
`malformed_by_kind`, `events_by_type`; sessions, orders_placed, payments_approved,
approved_amount (decimal string); `minute_counts` (`[minute_utc, event_type, count]`),
`delivery{delivered, failed}`. Invariants: `total = clean_events + duplicates + invalid.total +
malformed` and `avro = total - malformed`.

## CLI (`lakestream`, Typer; the entrypoint of both images)

| Command | Options |
| --- | --- |
| `register-schemas` | `--version 1\|2`, `--check-only PATH` |
| `produce` | `--sessions 400 --users 250 --seed 42 --base-time --span-minutes 90 --rate 0 --duplicate-rate --late-rate --late-max-seconds --malformed --invalid --too-late 0 --schema-version 1 --manifest PATH` |
| `consume` | `--group bronze-ingest`, `--exit-when-idle SECONDS`, hidden `--crash-after-write-batch N` |
| `lag` | `--group`, `--wait`, `--timeout 300`, `--json` (per partition: committed, end, lag, owner) |
| `dlq` | `--limit 10` (prints the headers and a hex preview) |
| `silver` | `--trigger available-now\|processing-time`, `--interval` |
| `gold` | none |
| `maintain` | `--older-than-minutes 60`, `--retain-last 10`, `--table` (repeatable; default all) |
| `check` | `--manifest PATH` (repeatable, summed), `--timeout 120`, `--time-travel/--no-time-travel` |
| `spark-warmup` | none (resolves the Ivy packages at image build time) |

## Settings and environment

pydantic-settings, one class per prefix, reading `.env` with `extra="ignore"`. Secrets are
`SecretStr` and never logged. Host-side defaults below; inside compose an `x-app-env` anchor sets
service-name endpoints.

| Variable | Default (host) | In compose |
| --- | --- | --- |
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:19092` | `kafka:29092` |
| `KAFKA_TOPIC`, `KAFKA_DLQ_TOPIC` | `shop.events`, `shop.events.dlq` | same |
| `SCHEMA_REGISTRY_URL` | `http://localhost:18081` | `http://schema-registry:8081` |
| `S3_ENDPOINT`, `S3_ACCESS_KEY`, `S3_SECRET_KEY`, `S3_REGION`, `S3_BUCKET` | `http://localhost:19000`, `lake_admin`, `lake_secret_pw`, `us-east-1`, `lakehouse` | `http://s3:9000` |
| `ICEBERG_REST_URI`, `ICEBERG_WAREHOUSE`, `ICEBERG_CATALOG` | `http://localhost:18181`, `s3://lakehouse/warehouse`, `lake` | `http://iceberg-rest:8181` |
| `TRINO_HOST`, `TRINO_PORT`, `TRINO_USER`, `TRINO_CATALOG` | `localhost`, `18080`, `lakestream`, `lake` | `trino`, `8080` |
| `CONSUMER_GROUP_ID`, `CONSUMER_BATCH_MAX_MESSAGES`, `CONSUMER_BATCH_MAX_SECONDS`, `CONSUMER_WRITE_RETRIES`, `CONSUMER_BRONZE_TABLE`, `CONSUMER_HEARTBEAT_FILE` | `bronze-ingest`, `2000`, `5`, `5`, `bronze.events`, `/tmp/consumer-heartbeat` | same |
| `PRODUCER_*` | the defaults of every `produce` option | same |
| `SPARK_MASTER`, `SPARK_DRIVER_MEMORY`, `SPARK_IVY_DIR`, `SPARK_CHECKPOINT_ROOT` | `local[2]`, `1g`, unset, `/checkpoints` | `/opt/spark-ivy` |
| `RUN_ID` | generated: `<yyyymmddThhmmssZ>-<6 hex>` | |
| `AWS_REQUEST_CHECKSUM_CALCULATION`, `AWS_RESPONSE_CHECKSUM_VALIDATION` | `when_required` (precaution, set on iceberg-rest, trino and spark-job) | |
| Host ports: `KAFKA_PORT`, `SCHEMA_REGISTRY_PORT`, `S3_API_PORT`, `S3_CONSOLE_PORT`, `ICEBERG_REST_PORT`, `TRINO_PORT`, `KAFKA_UI_PORT`, `SPARK_UI_PORT` | 19092, 18081, 19000, 19001, 18181, 18080, 18090, 14040 | bound to 127.0.0.1 |

Spark uses catalog `lake` (`SparkCatalog`, `type=rest`, `io-impl=S3FileIO`, `s3.endpoint`,
`s3.path-style-access=true`, `s3.access-key-id`, `s3.secret-access-key`, `client.region`), with
`spark.sql.defaultCatalog=lake`, the Iceberg SQL extensions, session time zone UTC and
`spark.sql.shuffle.partitions=4`. Spark tests use a plain local session with no packages.

Trino `lake.properties`: `connector.name=iceberg`, `iceberg.catalog.type=rest`,
`iceberg.rest-catalog.uri=http://iceberg-rest:8181`,
`iceberg.rest-catalog.warehouse=s3://lakehouse/warehouse`, `fs.s3.enabled=true` (or
`fs.native-s3.enabled=true` if 484 rejects it), `s3.endpoint=http://s3:9000`,
`s3.region=${ENV:S3_REGION}`, `s3.path-style-access=true`, `s3.aws-access-key=${ENV:S3_ACCESS_KEY}`,
`s3.aws-secret-key=${ENV:S3_SECRET_KEY}`.

## Compose (project `kafka-streaming-lakehouse`, no `container_name`, network `lake`)

| Service | Image or target | Profile | mem_limit | Health | Depends on |
| --- | --- | --- | --- | --- | --- |
| kafka | cp-kafka | | 1g (heap 512m) | `kafka-broker-api-versions` with a 64m heap | |
| kafka-init | cp-kafka, `create-topics.sh` | | 384m | one-shot | kafka healthy |
| schema-registry | cp-schema-registry (`SCHEMA_REGISTRY_SCHEMA_COMPATIBILITY_LEVEL=backward`, heap 256m) | | 512m | `curl /subjects` | kafka healthy |
| schema-init | app: `register-schemas --version 1` | | 256m | one-shot | schema-registry healthy |
| s3 | rustfs | | 512m | `curl /health` | |
| s3-init | curl, `create-bucket.sh` | | 64m | one-shot | s3 healthy |
| iceberg-rest | fixture, SQLite WAL on volume `catalog_data:/data` | | 384m | `curl /v1/config`, or a bash `/dev/tcp` probe | s3-init completed |
| consumer | app: `consume`, `deploy.replicas: 2`, `restart: on-failure`, no ports | | 384m each | heartbeat file newer than 60 s | kafka-init, schema-init, iceberg-rest |
| trino | trino, `./trino/catalog:/etc/trino/catalog:ro` | | 1536m | built-in `health-check` | iceberg-rest |
| producer | app | tools | 256m | none | kafka-init, schema-init |
| cli | app (`lag`, `check`, `dlq`) | tools | 256m | none | trino |
| spark-job | spark | tools | 2g | disabled | iceberg-rest |
| kafka-ui | kafbat (cluster plus schema registry) | ui | 512m | | kafka |

- Kafka listeners: INTERNAL `kafka:29092`, EXTERNAL advertised as `localhost:${KAFKA_PORT}`,
  CONTROLLER. Host tools and integration tests use the EXTERNAL listener.
- Volumes: `kafka_data`, `s3_data`, `catalog_data`, `spark_checkpoints`, `demo_runs` (at `/runs`).
- Memory with everything long-running up is about 4.7 GB; a Spark job adds 2 GB (about 6.7 GB).
- Dockerfile: the `app` target has no JVM and `HEALTHCHECK NONE`; the `spark` target has the JRE,
  `--extra spark` and runs `lakestream spark-warmup` at build time. Both run as non-root uid
  10001 with entrypoint `lakestream`.

## make demo (`scripts/demo.sh`, printing diagnostics on error)

1. `docker compose down -v --remove-orphans`, then `up -d --build --wait`.
2. `run --rm producer produce --seed 42 --manifest /runs/run1.json`.
3. `run --rm cli lag --wait --timeout 300`: total lag 0 and every partition has a committed
   offset.
4. `run --rm spark-job silver --trigger available-now`, then `spark-job gold`.
5. `run --rm cli check --manifest /runs/run1.json`.
6. Phase 5 adds the evolution run: `register-schemas --version 2`, produce with
   `--schema-version 2 --base-time 2026-01-02T00:00:00Z --seed 43`, then lag, silver, gold, and
   `check` with both manifests.
7. Last, `maintain`, then `check` again: compaction must not change any count.

## E2E check (`lakestream check`, through Trino; the DLQ is read from Kafka)

Every expectation is summed over the manifests.

- Bronze: row count = `produced.avro`; distinct (partition, offset) = row count; sum of
  `ingest.records` over append snapshots (`"events$snapshots"`) = row count.
- DLQ: distinct original coordinates = `produced.malformed`.
- Rejects: count = `invalid.total`; per-reason counts match.
- Silver: `count(*)` and `count(distinct event_id)` = `clean_events`; per-type counts match; every
  `late_event_ids` value is present; the latest silver run in `ops.job_runs` has
  `rows_dropped_late = 0`.
- Gold: the set of `(window_start, event_type, event_count)` rows equals `minute_counts` exactly;
  orders, approved payments and approved amount match; session count = `sessions`, and the sum of
  `event_count` = silver count.
- Phase 5: `count(device_type is not null)` = clean events of the v2 manifests.
- Time travel: print the bronze count `FOR VERSION AS OF` the first append snapshot next to the
  current count and assert first <= current; with two runs, also query gold at its earlier
  snapshot.
- Retry until everything matches or `--timeout` passes; print expected vs actual; exit 1 on any
  mismatch.

## Maintenance (`lakestream maintain`, `make maintain`)

For each table: `CALL lake.system.rewrite_data_files(table => ..., options =>
map('min-input-files','2'))`, then `rewrite_manifests`, then `expire_snapshots(older_than => ...,
retain_last => ...)`. Compaction writes `replace` snapshots, which the silver stream skips (it
also sets the skip-overwrite and skip-delete options). For bronze, the `older_than` cutoff is
clamped so it never passes the snapshot recorded in the latest offset file of
`/checkpoints/silver_events` (`checkpoint.py` parses `snapshot_id` with pure, unit-tested code):
expiring a snapshot silver has not read yet would break the stream. The consumer's resume logic
loses nothing to expiry: offsets in snapshots older than the cutoff are already committed to
Kafka.

## Tests

- Unit (no JVM, Hypothesis where it fits): generator invariants and manifest arithmetic;
  wire-header parsing and error classification; the batch accumulator and per-partition max
  offsets; `resume = max(...)`; Avro to Iceberg type mapping; DLQ header building; settings;
  expectation building; checkpoint offset parsing; reject reasons in `contracts.py` match the
  invalid injections.
- `spark` (WSL or CI, plain pyspark): `validate` for every reason; dedupe on batch DataFrames and
  on a file-source stream with `availableNow`; every gold transform (windows, sessions, flags) on
  fixed rows.
- `integration` (compose stack up; tests fail with a clear message when it is not): produce then a
  schema-registry round trip; a consumer on a unique `it_<uuid>` topic and table with
  `--crash-after-write-batch 2`, then a restart: no duplicate coordinates, DLQ filled; `lag`.

Compose is used instead of testcontainers: the tests exercise the real wiring of six services,
and the testcontainers `get_exposed_port` problem goes away.

## Phases

| # | Branch | Adds | Done when |
| --- | --- | --- | --- |
| 1 | `feat/compose-producer` | tooling (pyproject, uv.lock, ruff, mypy, pytest, Makefile, .env.example, .dockerignore, CHANGELOG, CLAUDE.md, README skeleton); compose kafka, kafka-init, schema-registry, schema-init, kafka-ui, producer; `app` target; `settings`, `logging`, `runmeta`, `contracts`, `registry`, `producer/`, v1 schema; `register-schemas`, `produce`; unit tests and producer integration test | `make lint`, `make test-fast`; `make up` healthy; `produce --seed 42` twice into fresh stacks gives identical manifests (apart from run_id and timestamps); the topic holds `produced.total` records |
| 2 | `feat/consumer-bronze` | s3, s3-init, iceberg-rest, consumer x2; `consumer/`, `lag`; Makefile `lag`, `test-integration` | crash-and-restart integration test passes; `lag --wait` reaches 0; bronze rows = `produced.avro` with unique coordinates (checked through PyIceberg) |
| 3 | `feat/spark-silver-gold` | `spark` target and spark-job; `spark/` (session, tables, silver, gold, maintain, checkpoint, runlog); `silver`, `gold`, `maintain`; Makefile `test-spark`, `maintain` | `make test-spark` in WSL; silver and gold run twice in a row without changing counts |
| 4 | `feat/trino-e2e` | trino and its catalog, cli service, `ops/expectations.py`, `ops/check.py`, `scripts/demo.sh`, `make demo`, README quickstart and queries | `make demo` exits 0 from fresh volumes; a second run gives the same gold |
| 5 | `feat/evolution-dlq` | v2 and v2-breaking schemas, `--schema-version`, bronze `union_by_name`, silver column add, `dlq` command, demo run 2, optional `--too-late`, README "what this demonstrates" | `make demo` passes with both manifests; `register-schemas --check-only v2-breaking` reports incompatible |
| 6 | `feat/ci` | `ci.yml` (lint, test-fast, test-spark with Java 17, compose job: `make demo` then `make test-integration`), `security.yml`, Dependabot, SHA-pinned actions | CI green on the PR |

Two implementers can work in parallel on disjoint files: phase 1 infra (compose, Dockerfile,
scripts, Makefile, `.env`) vs the `src/` package and tests; phase 2 `decode`/`batch`/`dlq`/`run`
vs `bronze.py` and compose; phase 3 pure `silver.py`/`gold.py` and Spark tests vs session, tables,
jobs, maintain and the Dockerfile; phase 4 Trino, compose and `demo.sh` vs `expectations` and
`check`; phase 5 schema, registry, producer and bronze evolution vs `dlq` and the README.

## Verification per phase

Every phase runs `make lint` and `make test-fast` on any machine and `make test-spark` in WSL or
CI. Stack runs hold the machine's heavy lock and end with `make down`.
