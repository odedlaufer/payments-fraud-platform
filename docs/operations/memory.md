# Container memory

Docker Desktop: **10 GB**. `tests/test_compose.py` keeps the sum of long-running container limits within 9.5 GB, so every service can sit at its limit at once.

`docker stats` percentages are not a risk measure on their own: JVMs and Go grow into the memory they are given. What matters is whether usage levels off below the limit (8-minute trend under load) and whether the kernel ever kills a container (`docker inspect ... OOMKilled`).

## Per-runtime settings

| Service | Runtime | Setting | Why |
| --- | --- | --- | --- |
| Flink JM, TM, supervisor | JVM + jemalloc (preloaded by the image, including arm64) | `MALLOC_CONF=narenas:4,background_thread:true,dirty_decay_ms:1000,muzzy_decay_ms:0` | jemalloc sizes its arenas from the CPU count and idle arenas hold freed memory outside Flink's accounting (FLINK-39924, affects 2.1.3; fix in flink-docker PR #266). Fewer arenas and fast decay follow jemalloc's TUNING.md low-resource profile |
| Connect, Trino | JVM + glibc malloc | `MALLOC_ARENA_MAX=2` | glibc keeps a malloc arena per thread and only shrinks it from the end, so freed native memory stays resident in long-running multi-threaded JVMs |
| SeaweedFS | Go | `GOMEMLIMIT=420MiB` (82% of 512 MB) | the Go GC is not aware of container limits; a soft limit makes it collect harder near the ceiling |
| SeaweedFS | | `-volume.index=leveldb`, `-s3.readerCacheSizeMB=64` | on-disk file index; bounded S3 read buffers (0 means unlimited) |
| Flink supervisor | SQL client JVM | `JVM_ARGS=-Xmx160m` | a fixed heap for planning jobs inside the 384 MB container |

Trino's heap is 60% of its container and is designed to fill before collecting (Trino deployment docs), so Trino near its limit during queries is expected.

## Measured (8 minutes at 20 tx/s, dbt build halfway, after the settings)

| Service | Limit | Steady | Peak |
| --- | --- | --- | --- |
| flink-taskmanager | 1920 MB | 1.38 GiB | 1.39 GiB |
| flink-jobmanager | 1152 MB | 705 MiB | 709 MiB |
| connect | 1024 MB | 527 MiB | 663 MiB |
| trino | 1792 MB | 1.37 GiB | 1.46 GiB (dbt) |
| seaweedfs | 512 MB | 350 MiB | 367 MiB |

No OOM kills. Remaining growth driver: Iceberg file count in SeaweedFS, addressed by compaction and snapshot expiry (FR-L5, Phase 5).
