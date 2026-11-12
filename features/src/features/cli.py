"""`features` command line."""

import argparse
import sys
from pathlib import Path

from features.catalogue import render
from features.model import DEFAULT_PATH, DefinitionError, load

CATALOGUE = DEFAULT_PATH.parents[1] / "docs/feature-catalogue.md"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(prog="features", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("validate", help="load and validate definitions.yml")
    cat = sub.add_parser("catalogue", help="write docs/feature-catalogue.md")
    cat.add_argument("--check", action="store_true", help="fail if the catalogue is stale")
    args = parser.parse_args(argv)
    try:
        defs = load()
    except DefinitionError as exc:
        print(f"invalid features/definitions.yml: {exc}", file=sys.stderr)
        return 1
    if args.command == "validate":
        print(f"{len(defs.features)} features valid (definitions version {defs.version})")
        return 0
    text = render(defs)
    path = Path(CATALOGUE)
    if args.check:
        if not path.exists() or path.read_text() != text:
            print("docs/feature-catalogue.md is stale: run `uv run features catalogue`")
            return 1
        print("feature catalogue is up to date")
        return 0
    path.write_text(text)
    print(f"wrote {path.relative_to(path.parents[1])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
