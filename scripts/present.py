"""make present: everything up for a demo in one command; make present-stop: everything down.

    make present        # checks Docker, offers to stop other projects, starts this project's
                        # containers, waits until healthy, seeds if empty, starts Dagster in the
                        # background, then opens the demo console (Ctrl-C closes the console)
    make present-stop   # stops Dagster and this project's containers (data kept), and offers
                        # to start again the other projects that make present stopped

Only this project's containers are started or stopped (compose project payments-fraud-platform).
Other projects are only stopped when you answer y, and are remembered so they can come back.
"""

import json
import os
import signal
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
PROJECT = "payments-fraud-platform"
STOPPED = ROOT / ".present-stopped.json"  # other projects make present stopped (git-ignored)
DAGSTER_PID = ROOT / "orchestration/dagster_home/.present-dagster.pid"
DAGSTER_LOG = ROOT / "orchestration/dagster_home/present-dagster.log"
MIN_DOCKER_GIB = 11.0  # the stack's memory limits add up to 11.2 GiB; Docker Desktop at 12 GB


def run(*cmd: str, quiet: bool = True) -> subprocess.CompletedProcess[str]:
    return subprocess.run(list(cmd), cwd=ROOT, capture_output=quiet, text=True)


def say(text: str) -> None:
    print(f"==> {text}", flush=True)


def others(ps: str) -> dict[str, list[str]]:
    """Running containers of other projects, by compose project ('' = not from compose).
    ps lines: '<compose project>\\t<container name>'."""
    out: dict[str, list[str]] = {}
    for line in ps.splitlines():
        project, _, name = line.partition("\t")
        if name and project != PROJECT:
            out.setdefault(project, []).append(name)
    return out


def mib(usage: str) -> float:
    """'512.3MiB / 11.7GiB' -> 512.3 (the used part, in MiB)."""
    used = usage.split("/")[0].strip()
    for unit, factor in (("GiB", 1024.0), ("MiB", 1.0), ("KiB", 1 / 1024), ("B", 1 / 1024 / 1024)):
        if used.endswith(unit):
            return float(used[: -len(unit)]) * factor
    return 0.0


def stop_commands(found: dict[str, list[str]]) -> tuple[list[list[str]], list[list[str]]]:
    """(stop, start again) commands: compose projects by name, other containers one by one."""
    stop, start = [], []
    for project, names in sorted(found.items()):
        if project:
            stop.append(["docker", "compose", "-p", project, "stop"])
            start.append(["docker", "compose", "-p", project, "start"])
        else:
            stop.append(["docker", "stop", *names])
            start.append(["docker", "start", *names])
    return stop, start


def ask(question: str) -> bool:
    if not sys.stdin.isatty():
        return False
    return input(f"{question} [y/N] ").strip().lower() in ("y", "yes")


def reachable(url: str) -> bool:
    try:
        with urllib.request.urlopen(url, timeout=3):
            return True
    except Exception:
        return False


def check_docker() -> bool:
    info = run("docker", "info", "--format", "{{.MemTotal}}")
    if info.returncode != 0:
        print("Docker is not running: start Docker Desktop, then make present again")
        return False
    gib = int(info.stdout.strip() or 0) / 1024**3
    if gib < MIN_DOCKER_GIB:
        print(
            f"WARNING: Docker has {gib:.1f} GiB; the stack needs about 11.2 GiB. "
            "Docker Desktop > Settings > Resources > Memory: 12 GB"
        )
    return True


def free_memory() -> None:
    ps = run(
        "docker", "ps", "--format", '{{.Label "com.docker.compose.project"}}\t{{.Names}}'
    ).stdout
    found = others(ps)
    if not found:
        say("no other projects running in Docker")
        return
    stats = run("docker", "stats", "--no-stream", "--format", "{{.Name}}\t{{.MemUsage}}").stdout
    usage = {n: mib(u) for n, _, u in (line.partition("\t") for line in stats.splitlines())}
    print("Other projects are running in Docker and share its memory with this one:")
    for project, names in sorted(found.items()):
        total = sum(usage.get(n, 0.0) for n in names)
        print(f"  {project or '(not from compose)'}: {len(names)} containers, {total:,.0f} MiB")
    stop, start = stop_commands(found)
    if ask("Stop them for the demo (make present-stop offers to start them again)?"):
        for cmd in stop:
            run(*cmd)
        STOPPED.write_text(json.dumps(start) + "\n")
        say("stopped; they come back with make present-stop")
    else:
        say("left running: the demo may be slower, and memory can run out under load")


def seeded() -> bool:
    count = run(
        "docker",
        "compose",
        "exec",
        "-T",
        "postgres",
        "sh",
        "-c",
        'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -tAc "select count(*) from customers"',
    )
    return count.returncode == 0 and count.stdout.strip() not in ("", "0")


def start_dagster() -> None:
    if reachable("http://localhost:3070/server_info"):
        say("Dagster is already running at http://localhost:3070")
        return
    run("uv", "run", "dbt", "parse", "--profiles-dir", "dbt", "--project-dir", "dbt", "--quiet")
    env = {**os.environ, "DAGSTER_HOME": str(ROOT / "orchestration/dagster_home")}
    log = DAGSTER_LOG.open("w")
    proc = subprocess.Popen(
        ["uv", "run", "dagster", "dev", "-m", "orchestration.definitions", "-p", "3070"],
        cwd=ROOT,
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    DAGSTER_PID.write_text(str(proc.pid))
    for _ in range(45):
        if reachable("http://localhost:3070/server_info"):
            say("Dagster started in the background: http://localhost:3070")
            return
        time.sleep(2)
    print(f"WARNING: Dagster did not answer within 90 s; see {DAGSTER_LOG.relative_to(ROOT)}")


def present() -> int:
    if not check_docker():
        return 1
    free_memory()
    say(f"starting {PROJECT}'s containers (only this project)")
    if run("docker", "compose", "up", "-d", quiet=False).returncode != 0:
        return 1
    say("waiting until every service is healthy")
    if run("scripts/health.sh", quiet=False).returncode != 0:
        print("Some services are not healthy: make health shows which; make logs to look closer")
        return 1
    if not seeded():
        say("empty database: seeding customers, cards and merchants")
        run("uv", "run", "simulator", "seed", quiet=False)
    start_dagster()
    say("opening the demo console; Ctrl-C closes it; make present-stop stops everything")
    return run("make", "--no-print-directory", "demo", quiet=False).returncode


def present_stop() -> int:
    if DAGSTER_PID.exists():
        try:
            os.killpg(int(DAGSTER_PID.read_text()), signal.SIGTERM)
            say("Dagster stopped")
        except (ProcessLookupError, ValueError):
            pass
        DAGSTER_PID.unlink()
    say(f"stopping {PROJECT}'s containers (data kept)")
    run("docker", "compose", "stop", quiet=False)
    if STOPPED.exists():
        start = json.loads(STOPPED.read_text())
        print("make present stopped these for the demo:")
        for cmd in start:
            print("  " + " ".join(cmd))
        if ask("Start them again now?"):
            for cmd in start:
                run(*cmd)
            STOPPED.unlink()
            say("started again")
    return 0


if __name__ == "__main__":
    sys.exit(present_stop() if sys.argv[1:] == ["stop"] else present())
