"""`features` command line."""

import argparse
import sys
from pathlib import Path

from features.catalogue import render
from features.dbt import OfflineModel, ParityModel
from features.flink import CardJob
from features.model import DEFAULT_PATH, DefinitionError, load

ROOT = DEFAULT_PATH.parents[1]
CATALOGUE = ROOT / "docs/feature-catalogue.md"
CARD_JOB = ROOT / "flink/sql/features/card_features.sql"
OFFLINE_MODEL = ROOT / "dbt/models/features/card_offline.sql"
PARITY_JOINED = ROOT / "dbt/models/marts/parity_joined.sql"
PARITY_MODEL = ROOT / "dbt/models/marts/parity_comparisons.sql"
KAFKA = "redpanda:9092"
REGISTRY = "http://redpanda:8081"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="features", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("validate", help="load and validate definitions.yml")
    cat = sub.add_parser("catalogue", help="write docs/feature-catalogue.md")
    cat.add_argument("--check", action="store_true", help="fail if the catalogue is stale")
    flink = sub.add_parser("flink", help="write flink/sql/features/card_features.sql")
    flink.add_argument("--check", action="store_true", help="fail if the job is stale")
    dbt = sub.add_parser("dbt", help="write the offline features and parity comparison dbt models")
    dbt.add_argument("--check", action="store_true", help="fail if the model is stale")
    args = parser.parse_args(argv)
    try:
        defs = load()
    except DefinitionError as exc:
        print(f"invalid features/definitions.yml: {exc}", file=sys.stderr)
        return 1
    if args.command == "validate":
        print(f"{len(defs.features)} features valid (definitions version {defs.version})")
        return 0
    if args.command == "catalogue":
        outputs = [(render(defs), Path(CATALOGUE))]
    elif args.command == "dbt":
        outputs = [
            (OfflineModel(defs).sql(), Path(OFFLINE_MODEL)),
            (ParityModel(defs).joined_sql(), Path(PARITY_JOINED)),
            (ParityModel(defs).sql(), Path(PARITY_MODEL)),
        ]
    else:
        outputs = [(CardJob(defs).job(KAFKA, REGISTRY), Path(CARD_JOB))]
    stale = False
    for text, path in outputs:
        name = path.relative_to(ROOT)
        if args.check:
            if not path.exists() or path.read_text() != text:
                print(f"{name} is stale: run `uv run features {args.command}`")
                stale = True
            else:
                print(f"{name} is up to date")
            continue
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
        print(f"wrote {name}")
    return 1 if stale else 0


if __name__ == "__main__":
    raise SystemExit(main())
