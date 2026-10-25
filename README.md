# kafka-streaming-lakehouse

> Kafka end to end on a laptop: Python producers and consumers with Avro and Schema Registry,
> landing events in an Apache Iceberg lakehouse (bronze, silver, gold) that Trino queries.

Work in progress. The project ships in phases, one pull request each:

1. Docker Compose stack and the event producer
2. Consumer group and the bronze landing table
3. Spark Structured Streaming silver and gold tables
4. Trino and the end-to-end check
5. Schema evolution and dead-letter queue demo
6. Continuous integration

Sibling project: [cdc-streaming-demo](https://github.com/emanuel-luis/cdc-streaming-demo)
(Debezium change data capture into Delta Lake).

## License

MIT
