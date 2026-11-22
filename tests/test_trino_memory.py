"""Trino's memory settings satisfy its own documented constraint (resource management):
query.max-memory-per-node + memory.heap-headroom-per-node < the JVM heap."""

import re
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def size_mb(value: str) -> float:
    m = re.fullmatch(r"(\d+)\s*([kKmMgG])B?", value.strip())
    assert m, value
    return int(m.group(1)) * {"k": 1 / 1024, "m": 1, "g": 1024}[m.group(2).lower()]


def test_query_memory_plus_headroom_fit_in_the_heap() -> None:
    jvm = (ROOT / "trino/etc/jvm.config").read_text().split()
    (xmx,) = [a[4:] for a in jvm if a.startswith("-Xmx")]
    assert [a[4:] for a in jvm if a.startswith("-Xms")] == [xmx], "fixed heap: -Xms = -Xmx"
    props = dict(
        line.split("=", 1)
        for line in (ROOT / "trino/etc/config.properties").read_text().splitlines()
        if "=" in line and not line.startswith("#")
    )
    query = size_mb(props["query.max-memory-per-node"])
    headroom = size_mb(props["memory.heap-headroom-per-node"])
    assert query + headroom < size_mb(xmx)
    assert headroom >= 768, "untracked allocations crashed a 320 MB headroom"


def test_the_container_leaves_room_beside_the_heap() -> None:
    """Native memory beside the heap: about 990 MB measured at 4 CPUs under the full stress
    (anon 3,041 MB with a 2,048 MB heap, 284 limit hits); 1,792 MB leaves real margin."""
    trino = yaml.safe_load((ROOT / "docker-compose.yml").read_text())["services"]["trino"]
    limit = size_mb(trino["deploy"]["resources"]["limits"]["memory"])
    (xmx,) = [
        a[4:] for a in (ROOT / "trino/etc/jvm.config").read_text().split() if a.startswith("-Xmx")
    ]
    assert limit - size_mb(xmx) >= 1300


def test_parallelism_matches_the_container() -> None:
    """Trino derives threads, drivers and writers from the CPUs it sees (Task properties);
    the container's CPU limit and the explicit settings must agree."""
    trino = yaml.safe_load((ROOT / "docker-compose.yml").read_text())["services"]["trino"]
    cpus = float(trino["deploy"]["resources"]["limits"]["cpus"])
    props = dict(
        line.split("=", 1)
        for line in (ROOT / "trino/etc/config.properties").read_text().splitlines()
        if "=" in line and not line.startswith("#")
    )
    threads = int(props["task.max-worker-threads"])
    assert threads <= 2 * cpus
    assert int(props["task.min-drivers"]) == 2 * threads
    for key in ("task.concurrency", "task.max-writer-count"):
        value = int(props[key])
        assert value & (value - 1) == 0, f"{key} must be a power of two"
        assert value <= cpus


def test_trino_ships_only_the_iceberg_connector() -> None:
    """Class metadata and compiled code of 57 plugins were about 440 MB of native memory."""
    trino = yaml.safe_load((ROOT / "docker-compose.yml").read_text())["services"]["trino"]
    assert trino["build"] == "./trino"
    dockerfile = (ROOT / "trino/Dockerfile").read_text()
    assert "FROM trinodb/trino:484" in dockerfile
    assert "= iceberg ] || rm -rf" in dockerfile
    for sample in ("jmx", "memory", "tpcds", "tpch"):
        assert f"/etc/trino/catalog/{sample}.properties" in dockerfile


def test_no_in_memory_file_cache() -> None:
    """The image's default config enables a 2 GB in-memory file cache
    (cache-manager-memory.properties): larger than the heap, it competed with every query."""
    config = (ROOT / "trino/etc/config.properties").read_text()
    enabled = [
        line for line in config.splitlines() if line.startswith("cache-manager.config-files")
    ]
    assert not enabled
