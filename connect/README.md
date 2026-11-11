# connect

Kafka Connect worker that runs Debezium (FR-CDC2).

- `Dockerfile`: Debezium Connect 3.7.0.Final plus the Confluent Avro converter, so connectors write Avro to Redpanda's Confluent-compatible Schema Registry. The converter archive is pinned by version and SHA-256.
- The Confluent Avro converter is under the Confluent Community License (free to use, not OSI open source), like Redpanda's BSL (ADR-001).

```bash
make connect-plugins   # list installed connector and converter plugins
```
