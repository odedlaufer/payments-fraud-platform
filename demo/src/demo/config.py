"""Settings from the repository's .env (the console runs on the host, like Dagster)."""

from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


def load_env(root: Path = ROOT) -> dict[str, str]:
    env: dict[str, str] = {}
    path = root / ".env"
    if not path.exists():
        return env
    for line in path.read_text().splitlines():
        if "=" in line and not line.lstrip().startswith("#"):
            k, v = line.split("=", 1)
            env[k.strip()] = v.strip()
    return env


ENV = load_env()


def port(name: str, default: str) -> str:
    return ENV.get(name, default)


def dsn() -> str:
    """The OLTP database, from the host (the console runs outside Docker)."""
    parts = {
        "host": "localhost",
        "port": ENV.get("POSTGRES_PORT", "5432"),
        "dbname": ENV.get("POSTGRES_DB", ""),
        "user": ENV.get("POSTGRES_USER", ""),
        "password": ENV.get("POSTGRES_PASSWORD", ""),
        "connect_timeout": "3",
    }
    return " ".join(f"{k}={v}" for k, v in parts.items())


LINKS = {
    "Grafana": f"http://localhost:{port('GRAFANA_PORT', '3030')}",
    "Marquez lineage": f"http://localhost:{port('MARQUEZ_WEB_PORT', '3040')}",
    "Dagster": "http://localhost:3070",
    "Flink": f"http://localhost:{port('FLINK_UI_PORT', '8081')}",
    "Redpanda Console": f"http://localhost:{port('REDPANDA_CONSOLE_PORT', '8080')}",
}
