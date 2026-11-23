"""Data contracts (DC-2): ODCS v3.2 YAML for the published datasets.

    make contracts          # write contracts/<name>.odcs.yaml (each keeps its version)
    make contracts-check    # live: schemas still match, quality rules hold, registry BACKWARD

contracts/agreements.yaml holds the human part (owner, purpose, SLOs, quality rules). This
adds columns and types from the sources of truth: the decision service's Avro schema for the
decisions topic, Trino's information_schema for tables, descriptions from dbt's docs.

Regeneration never changes a contract's version: bumping it is a human decision, and
tests/test_contracts.py refuses a breaking change without a major bump (DC-3).
"""

import argparse
import json
import os
import subprocess
import sys
import urllib.request
from collections.abc import Callable
from pathlib import Path
from typing import Any

import yaml

from gen_bronze import ROOT

CONTRACTS = ROOT / "contracts"
DATACONTRACT = ["uvx", "--from", "datacontract-cli[avro,trino]==1.2.9", "datacontract"]
AVRO_LOGICAL = {
    "long": "integer",
    "int": "integer",
    "boolean": "boolean",
    "double": "number",
    "float": "number",
    "record": "object",
    "map": "object",
    "array": "array",
}
Columns = Callable[[str, str], list[tuple[str, str]]]


def trino_logical(physical: str) -> str:
    t = physical.lower()
    for prefix, logical in (
        ("bigint", "integer"),
        ("integer", "integer"),
        ("smallint", "integer"),
        ("decimal", "number"),
        ("double", "number"),
        ("real", "number"),
        ("boolean", "boolean"),
        ("timestamp", "date"),
        ("date", "date"),
        ("array", "array"),
        ("row", "object"),
        ("map", "object"),
    ):
        if t.startswith(prefix):
            return logical
    return "string"


def avro_properties(schema: dict[str, Any]) -> list[dict[str, Any]]:
    props = []
    for field in schema["fields"]:
        kind = field["type"]
        nullable = isinstance(kind, list) and "null" in kind
        base = next(k for k in kind if k != "null") if isinstance(kind, list) else kind
        physical = base if isinstance(base, str) else base.get("type", "record")
        prop = {
            "name": field["name"],
            "physicalType": physical,
            "logicalType": AVRO_LOGICAL.get(physical, "string"),
            "required": not nullable,
        }
        if field.get("doc"):
            prop["description"] = field["doc"]
        props.append(prop)
    return props


def table_properties(
    columns: list[tuple[str, str]], docs: dict[str, str], keys: list[str]
) -> list[dict[str, Any]]:
    props = []
    for name, physical in columns:
        prop: dict[str, Any] = {
            "name": name,
            "physicalType": physical,
            "logicalType": trino_logical(physical),
        }
        if name in keys:
            prop.update(primaryKey=True, primaryKeyPosition=keys.index(name) + 1, required=True)
        if docs.get(name):
            prop["description"] = docs[name]
        props.append(prop)
    return props


def dbt_docs(model: str) -> tuple[str, dict[str, str]]:
    manifest = json.loads((ROOT / "dbt/target/manifest.json").read_text())
    for node in manifest["nodes"].values():
        if node["resource_type"] == "model" and node["name"] == model:
            columns = node.get("columns", {})
            return node.get("description", ""), {
                c: v.get("description", "") for c, v in columns.items()
            }
    return "", {}


def rules(agreement: dict[str, Any], table: str, key: str) -> list[dict[str, Any]]:
    quality = agreement.get("quality", {})
    out = []
    for rule in [*quality.get("*", []), *quality.get(table, [])]:
        rule = dict(rule)
        if "query" in rule:
            rule["query"] = rule["query"].replace("{key}", key)
        out.append(rule)
    return out


def build_one(
    name: str, agreement: dict[str, Any], team: dict[str, Any], version: str, columns: Columns
) -> dict[str, Any]:
    source = agreement["source"]
    schema = []
    if source["type"] == "avro":
        from decision_service.decision import avro_schema

        topic = source["topic"]
        schema.append(
            {
                "name": topic,
                "physicalType": "topic",
                "physicalName": topic,
                "logicalType": "object",
                "properties": avro_properties(avro_schema()),
                "quality": rules(agreement, topic, ""),
            }
        )
    else:
        for table, key in source["tables"].items():
            keys = key if isinstance(key, list) else [key]
            about, docs = dbt_docs(table)
            entry: dict[str, Any] = {
                "name": table,
                "physicalType": "table",
                "physicalName": table,
                "logicalType": "object",
            }
            if about:
                entry["description"] = about
            entry["properties"] = table_properties(columns(source["schema"], table), docs, keys)
            entry["quality"] = rules(agreement, table, keys[0])
            schema.append(entry)
    return {
        "apiVersion": "v3.2.0",
        "kind": "DataContract",
        "id": agreement["id"],
        "name": agreement["name"],
        "version": version,
        "status": "active",
        "description": agreement["description"],
        "team": team,
        "schema": schema,
        "servers": agreement["servers"],
        "slaProperties": agreement["slaProperties"],
    }


def trino_columns() -> Columns:
    import trino

    from cdc_reconcile import load_env

    port = int(load_env().get("TRINO_PORT", "8088"))
    cur = trino.dbapi.connect(host="127.0.0.1", port=port, user="contracts").cursor()  # type: ignore[no-untyped-call]

    def columns(schema: str, table: str) -> list[tuple[str, str]]:
        cur.execute(
            "SELECT column_name, data_type FROM lake.information_schema.columns "
            f"WHERE table_schema = '{schema}' AND table_name = '{table}' ORDER BY ordinal_position"
        )
        rows = [(str(c), str(t)) for c, t in cur.fetchall()]
        if not rows:
            sys.exit(f"STOP: lake.{schema}.{table} not found")
        return rows

    return columns


def version_of(path: Path) -> str:
    return str(yaml.safe_load(path.read_text())["version"]) if path.exists() else "1.0.0"


def build(columns: Columns) -> dict[str, dict[str, Any]]:
    agreements = yaml.safe_load((CONTRACTS / "agreements.yaml").read_text())
    out = {}
    for name, agreement in agreements["contracts"].items():
        path = f"{name}.odcs.yaml"
        out[path] = build_one(
            name, agreement, agreements["team"], version_of(CONTRACTS / path), columns
        )
    return out


def dump(doc: dict[str, Any]) -> str:
    return (
        "# Generated by make contracts from agreements.yaml and the live schemas.\n"
        + yaml.safe_dump(doc, sort_keys=False, allow_unicode=True, width=100)
    )


def check(built: dict[str, dict[str, Any]]) -> bool:
    ok = True
    for path, doc in built.items():
        same = (CONTRACTS / path).exists() and (CONTRACTS / path).read_text() == dump(doc)
        print(
            f"  {'OK   ' if same else 'DRIFT'} contracts/{path}"
            + ("" if same else ": the live schema differs")
        )
        ok &= same
    for path, doc in built.items():
        if doc["servers"][0]["type"] != "trino":
            continue
        env = {
            **os.environ,
            "DATACONTRACT_TRINO_USERNAME": "contracts",
            "DATACONTRACT_TRINO_PASSWORD": "",
        }
        run = subprocess.run(
            [*DATACONTRACT, "test", str(CONTRACTS / path)], capture_output=True, text=True, env=env
        )
        print(f"  {'OK   ' if run.returncode == 0 else 'FAIL '} datacontract test contracts/{path}")
        if run.returncode != 0:
            print("\n".join(run.stdout.splitlines()[-15:]))
        ok &= run.returncode == 0
    from cdc_reconcile import load_env

    url = f"http://localhost:{load_env().get('REDPANDA_SCHEMA_REGISTRY_PORT', '18081')}/config"
    with urllib.request.urlopen(url, timeout=5) as response:
        level = json.load(response).get("compatibilityLevel")
    mark = "OK   " if level == "BACKWARD" else "FAIL "
    print(f"  {mark} schema registry compatibility {level} (DC-1: BACKWARD)")
    return ok and level == "BACKWARD"


def main() -> int:
    parser = argparse.ArgumentParser(
        description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter
    )
    parser.add_argument("--check", action="store_true", help="live: drift, quality rules, registry")
    args = parser.parse_args()
    built = build(trino_columns())
    if args.check:
        return 0 if check(built) else 1
    for path, doc in built.items():
        (CONTRACTS / path).write_text(dump(doc))
        print(f"wrote contracts/{path} (version {doc['version']})")
    return 0


if __name__ == "__main__":
    sys.exit(main())
