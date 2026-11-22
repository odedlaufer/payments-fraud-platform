# 2026-11-19: Trino and the catalog database killed for memory

**Impact:** Trino restarted four times during dbt builds and maintenance; every query in flight failed (dbt errors, "server is still initializing"). Lakekeeper's catalog database had six backends killed, each resetting every catalog connection. No data was lost or corrupted: bronze stayed exactly once and silver matched Postgres throughout.

## What made it slow to find

`docker inspect` reported `OOMKilled=false` for every Trino kill. On cgroup v2 (Docker Desktop) that flag is unreliable: the kernel's own log was the truth (`dmesg`: `Memory cgroup out of memory: Killed process ... (trino-server) anon-rss:3213396kB`, in Trino's own cgroup). Three rounds of fixes chased the wrong cause before reading it.

## Causes

1. **Trino, first:** a heap of 60% of 1.75 GB left Trino's default headroom for untracked allocations at about 320 MB; the JVM ran out of heap (`OutOfMemoryError: Java heap space`).
2. **Trino, then:** raising the heap to a fixed 2.4 GB in a 3.2 GB container left about 800 MB for native memory (metaspace, code cache, thread stacks, GC structures, buffers); two concurrent heavy dbt models pushed the process to 3.2 GB and the kernel killed it.
3. **catalog-db:** Postgres's default 128 MB `shared_buffers` in a 160 MB container, plus Lakekeeper's 15 pooled backends.

## Fixes

- Trino: fixed 2 GB heap in 3.2 GB (about 1.15 GB native room, at least 813 MB measured); `query.max-memory-per-node` and `memory.heap-headroom-per-node` 800 MB each (Trino's constraint: their sum below the heap); dbt runs one model at a time.
- catalog-db: `shared_buffers=32MB`, `work_mem=2MB`, `max_connections=30`; Lakekeeper pools 4 read + 2 write. Peak 132 MB of 160 under the same load, no kills.
- Tests enforce both: Trino's constraint and native room, catalog-db's buffers plus pooled backends within its limit.

## Lessons

- For container memory, read the kernel log (`dmesg | grep "Killed process"`), not Docker's flag.
- A JVM's container must budget native memory explicitly; it grows with the heap and with concurrency.
- The Docker VM is shared with other projects (`lichess`); their containers are outside this budget.

## Follow-up (2026-11-22): the deeper cause was parallelism, not only memory

With a 2 GB heap Trino still hung (no response to `select 1`) on a 1.3-million-row CREATE TABLE AS. Research into Trino's documentation found why every memory adjustment fell short:

- **Trino sizes its parallelism from the CPUs it sees** (Task properties, Trino 484): `task.max-worker-threads` = CPUs x 2, `task.min-drivers` = twice that, `task.concurrency` and `task.max-writer-count` = the physical CPU count. The docs warn more worker threads cause increased heap usage and that too many writers can cause out-of-memory errors.
- **The container saw all 14 of the VM's CPUs** (no CPU limit): 28 worker threads, 56 drivers and up to 14 writers, each holding buffers, in a 2 GB node.
- **Fix:** a CPU limit of 4 on the Trino container (the JVM and every CPU-derived default scale down together) and explicit `task.max-worker-threads=4`, `task.min-drivers=8`, `task.concurrency=4`, `task.max-writer-count=2`. Trino's own JVM defaults (`G1HeapRegionSize=32M`) are kept; a GC log (`/tmp/gc.log`) records evidence for any future stall.

Two related failures in the same period:

- **Kafka Connect stalled** ("Didn't reach end of config log quickly enough", connector `UNASSIGNED`): the worker could not read its config topic from a broker slowed by VM memory pressure (KAFKA-10021 documents the code path). Fix: `scheduled.rebalance.max.delay.ms=0` for this single worker (reassign at once rather than after 5 minutes) and `worker.sync.timeout.ms=10000`; `make health` detects it, a worker restart clears it.
- **Docker Desktop's engine locked up** (HTTP 500 until a full restart): the VM ran out of memory with this stack (9.7 GB of limits) and another project's 10 containers in one 9.7 GB VM. The memory budget assumes the VM runs this stack alone.

## Follow-up (2026-11-22): a 2 GB in-memory file cache, on by default

Native Memory Tracking (`jcmd 1 VM.native_memory summary`) and the kernel's `memory.stat` replaced estimates with measurements, and found the last piece:

- **The official image's `config.properties` enables `cache-manager-memory.properties`: an in-memory file-system cache of up to 2 GB (`fs.memory-cache.max-size=2GB`, TTL 24 h).** Our config was generated from the image's file, so the cache was on throughout, allowed to exceed the whole heap. That fits the first `OutOfMemoryError: Java heap space` and the GC stalls. Turned off; a test fails if it returns.
- **Plugins:** the image loads 57; the lake uses one. The custom image (`trino/Dockerfile`) keeps only `iceberg` and drops the sample catalogs (`jmx`, `memory`, `tpcds`, `tpch`). Measured saving: about 70 MB of class metadata (Metaspace 203 to 151 MB, Class 62 to 46 MB), smaller than first estimated.

| Measure | All plugins, cache on | Iceberg only, cache off |
| --- | --- | --- |
| anon (kernel) | 2,996-3,038 MB of 3,200 | 2,684 MB |
| container limit hits | 18-284 | 0 |
| heap used after the stress | - | 918 of 1,792 MB |
| kernel kills, restarts, full GCs | several | 0 |

Final Trino configuration: 4 CPUs; heap fixed at 1,792 MB in 3,200 MB; query memory and headroom 800 MB each; `task.max-worker-threads=4`, `task.min-drivers=8`, `task.concurrency=4`, `task.max-writer-count=2`; no file cache; Iceberg plugin only; dbt one model at a time.
