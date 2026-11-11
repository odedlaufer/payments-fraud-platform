# ADR-002: Object store, Iceberg catalog, bronze sink and version matrix

- **Status:** Accepted
- **Date:** 2026-11-11
- **Resolves:** PRD open questions Q1 (Iceberg sink) and Q2 (catalog)
- **Supersedes:** PRD section 3, "Apache Iceberg on MinIO"

## Context

The cold path lands every CDC topic in Iceberg (FR-F5) and is read by Trino (dbt), DuckDB (backtester) and Flink. That needs three choices that have to work together: an S3-compatible object store, an Iceberg catalog, and the process that writes bronze. PRD risk 10.1 calls out Flink, Iceberg and catalog version mismatches, so the versions are pinned here and verified before they are used.

Constraints: one laptop with 16 GB RAM, open source only, no paid services, production-like behaviour.

## Decision 1: Object store is SeaweedFS

| Option | Assessment |
| --- | --- |
| MinIO | Rejected. Repository archived on 2026-04-25. `minio/minio` was deleted from Docker Hub in September 2026 and Quay returns 401, so no image can be pulled. |
| Silo (MinIO fork) | Rejected. Single-vendor fork under AGPL; keeps MinIO's code but not its community. |
| RustFS | Rejected for now. Reached 1.0 in September 2026; too young for the base of the lakehouse. |
| Garage | Rejected. Needs layout, key and bucket bootstrap commands and is designed around multi-node clusters. |
| **SeaweedFS** | **Chosen.** Apache-2.0, maintained since 2012, single container in `mini` mode, creates the bucket on startup from `S3_BUCKET`, static admin credentials from `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY`. |

Consequences:

- Clients must use path-style S3 access and an explicit endpoint (`http://seaweedfs:8333`, region `us-east-1`).
- `mini` runs master, volume, filer and S3 gateway in one process. It is not highly available, which is acceptable for a single-laptop platform.
- SeaweedFS's built-in Iceberg REST catalog and Lance server are disabled (`-s3.port.iceberg=0`, `-s3.port.lance=0`); see Decision 2.

## Decision 2: Catalog is Lakekeeper on its own Postgres

| Option | Assessment |
| --- | --- |
| Iceberg REST fixture | Rejected. A test fixture, not a catalog service; in-memory by default. |
| Project Nessie | Rejected. JVM service with a higher memory cost; its git-style catalog branching is not needed because the training-set publish gate (PRD 6.3) uses Iceberg table branches, which any catalog supports. |
| Apache Polaris | Rejected. JVM service, heavier than needed for one node. |
| SeaweedFS built-in catalog | Rejected. Couples catalog availability to the object store, runs its own server-side compaction and snapshot expiry that would overlap with FR-L5, and is the newest of the options. |
| **Lakekeeper** | **Chosen.** Apache-2.0, written in Rust with a small memory footprint, implements the Iceberg REST specification used by Trino, Flink, DuckDB and PyIceberg, persists state in Postgres, and has a management API for warehouses. |

Lakekeeper gets its own Postgres container (`catalog-db`), not a database inside the OLTP Postgres. The OLTP instance is the CDC source: it runs with `wal_level=logical`, holds a replication slot that can retain WAL, and is restarted and stressed by chaos tests. Keeping the catalog separate means none of that can take the catalog down, which mirrors a real deployment.

## Decision 3: Bronze is written by the Flink Iceberg sink

| Option | Assessment |
| --- | --- |
| Iceberg Kafka Connect sink | Rejected. Apache Iceberg publishes no binary for it; it must be built from source, and its bundled dependencies carried HIGH-severity CVEs in 2026 (apache/iceberg#16286). It would also share a Connect worker with Debezium. |
| **Flink Iceberg sink** | **Chosen**, as written in FR-F5. Published `iceberg-flink-runtime` artifact, exactly-once commits tied to Flink checkpoints, and one stream engine for the platform. |

The bronze sink runs as its own Flink job, separate from the feature jobs, so a failing feature job does not stop the lakehouse from landing data.

## Decision 4: Version matrix

| Component | Version | Released | Notes |
| --- | --- | --- | --- |
| Apache Iceberg | 1.12.0 | 2026-09-30 | Supports Flink 1.20, 2.1, 2.2, 2.3 |
| Trino | 484 | 2026-10-16 | Bundles Iceberg 1.12.0 |
| Apache Flink | 2.1.3 | 2026-06-11 | Newest Flink line with a released Kafka connector |
| flink-connector-kafka | 5.0.0 | 2026-06-02 | Built against Flink 2.1; Kafka client 4.2 |
| iceberg-flink-runtime-2.1 | 1.12.0 | 2026-09-30 | Matches Iceberg and Flink above |
| Lakekeeper | v0.14.2 | 2026-10-25 | |
| SeaweedFS | 4.51 | 2026-10-25 | |

Flink 2.2 and 2.3 are not used: flink-connector-kafka has only a release candidate for 2.2 (5.1.0-rc1) and nothing for 2.3. DuckDB, dbt-trino and Dagster are pinned in the phases that introduce them.

### Pinning policy

- Every image and artifact is pinned to an exact version, never `latest` or a range.
- A version is pinned only once it is at least 14 days old, so a compromised or broken release has time to be caught upstream.
- Image digests are added alongside tags when CI is introduced (PRD ENG-3).

### Verification

The matrix is accepted only when both checks pass, and any change to it repeats both:

1. **Phase 0 gate:** Trino creates, writes and reads an Iceberg table through Lakekeeper on SeaweedFS.
2. **Phase 2, first task:** Flink writes an Iceberg table through Lakekeeper and Trino reads it back.

## Consequences

- Two extra containers (`lakekeeper`, `catalog-db`) in the `lake` profile.
- Every engine is configured with the same three things: the Lakekeeper REST URI, the warehouse name, and SeaweedFS path-style S3 settings.
- If Lakekeeper or SeaweedFS stops being maintained, the replacement is any Iceberg REST catalog or S3 store; the engines do not change.

## Verification record

| Check | Date | Result |
| --- | --- | --- |
| 1. Trino creates, writes, reads an Iceberg table via Lakekeeper on SeaweedFS (`make gate`) | 2026-11-11 | Passed |
| 2. Flink 2.1.3 writes an Iceberg table via Lakekeeper; Trino reads it back (`make flink-check`) | 2026-11-11 | Passed |

Lakekeeper warehouses are created with `remote-signing-enabled: false` and `sts-enabled: false`: Trino and Flink use their own S3 credentials from their environment.
