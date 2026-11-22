"""Iceberg references (tags) through PyIceberg on the Lakekeeper REST catalog.

Trino can create branches only at a branch head, not at an earlier snapshot, so tags on
published training sets are written here. Snapshot expiry never removes a snapshot a tag
references: the tag is what keeps a training set reproducible (FR-PIT4) once maintenance
expires old snapshots (FR-L5).
"""

import logging
from typing import Any

from pyiceberg.catalog import Catalog, load_catalog

# Tagging goes through the catalog only; PyIceberg warns that it cannot read data files.
logging.getLogger("pyiceberg.io").setLevel(logging.ERROR)


def catalog(env: dict[str, str]) -> Catalog:
    return load_catalog(
        "lake",
        type="rest",
        uri=f"http://localhost:{env.get('LAKEKEEPER_PORT', '8181')}/catalog",
        warehouse=env.get("WAREHOUSE_NAME", "lakehouse"),
        **{
            "s3.endpoint": f"http://localhost:{env.get('S3_API_PORT', '8333')}",
            "s3.access-key-id": env["S3_ACCESS_KEY"],
            "s3.secret-access-key": env["S3_SECRET_KEY"],
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )


def tag_name(cutoff: Any, snapshot_id: int) -> str:
    """e.g. training_set_20260330_5386084145022979600: one per published set."""
    day = str(cutoff)[:10].replace("-", "")
    return f"training_set_{day}_{snapshot_id}"


def refs(cat: Catalog, table: str) -> dict[str, int]:
    """Reference name -> snapshot id (main and every branch and tag)."""
    return {k: v.snapshot_id for k, v in cat.load_table(table).metadata.refs.items()}


def protect(cat: Catalog, table: str, snapshot_id: int, name: str) -> bool:
    """Tags the snapshot unless already tagged under that name. True when a tag was added."""
    if refs(cat, table).get(name) == snapshot_id:
        return False
    cat.load_table(table).manage_snapshots().create_tag(snapshot_id, name).commit()
    return True


def unprotected(registered: list[tuple[Any, int]], table_refs: dict[str, int]) -> list[int]:
    """Registered training-set snapshots no reference points at: expiry could remove them."""
    pinned = set(table_refs.values())
    return [snapshot for _, snapshot in registered if snapshot not in pinned]
