"""Loads and validates features/definitions.yml.

Validation is strict on purpose: an unknown key, a missing parameter or a name that
contradicts its window is a definition bug that would otherwise surface as an
online/offline parity mismatch much later.
"""

import re
from dataclasses import dataclass
from datetime import timedelta
from pathlib import Path
from typing import Any

import yaml

DEFAULT_PATH = Path(__file__).resolve().parents[2] / "definitions.yml"

SEMANTICS: dict[str, object] = {
    "event_time": "event_ts",
    "time_precision": "millisecond",
    "window_bounds": "inclusive",
    "includes_current": True,
    "ties": "included",
    "watermark": "30s",
    "amounts": "usd_equivalent",
    "counted_transactions": "all",
}

# kind -> parameters it requires (besides the common ones)
KINDS: dict[str, frozenset[str]] = {
    "window_count": frozenset({"window"}),
    "window_sum": frozenset({"window", "field"}),
    "window_count_distinct": frozenset({"window", "field"}),
    "ratio_to_prior_mean": frozenset({"window", "field"}),
    "distance_from_previous": frozenset({"filter"}),
    "minutes_since_previous": frozenset({"filter"}),
    "first_seen_within": frozenset({"window", "field", "source"}),
    "changed_within": frozenset({"window", "columns", "source"}),
    "status_rate": frozenset({"window", "status", "status_time"}),
}
COMMON = frozenset({"name", "version", "entity", "kind", "dtype", "description"})
OPTIONAL = frozenset({"field", "filter", "source", "columns", "status", "status_time", "window"})
DTYPES = frozenset({"bigint", "double", "boolean", "decimal(18,2)"})
NAME = re.compile(r"^[a-z][a-z0-9_]*$")
WINDOW = re.compile(r"^(\d+)([smhd])$")
WINDOW_SUFFIX = re.compile(r"_(\d+[smhd])$")
UNITS = {"s": "seconds", "m": "minutes", "h": "hours", "d": "days"}


class DefinitionError(ValueError):
    """definitions.yml is invalid."""


def parse_window(value: str) -> timedelta:
    match = WINDOW.match(value)
    if not match:
        raise DefinitionError(f"window {value!r} must look like 30s, 1m, 24h or 7d")
    amount, unit = int(match.group(1)), match.group(2)
    if amount <= 0:
        raise DefinitionError(f"window {value!r} must be positive")
    return timedelta(**{UNITS[unit]: amount})


@dataclass(frozen=True)
class Feature:
    name: str
    version: int
    entity: str
    kind: str
    dtype: str
    description: str
    window: str | None = None
    field: str | None = None
    filter: str | None = None
    source: str | None = None
    columns: tuple[str, ...] = ()
    status: str | None = None
    status_time: str | None = None

    @property
    def window_delta(self) -> timedelta | None:
        return parse_window(self.window) if self.window else None


@dataclass(frozen=True)
class Definitions:
    version: int
    semantics: dict[str, object]
    entities: dict[str, str]  # entity -> key column
    features: tuple[Feature, ...]

    def by_name(self, name: str) -> Feature:
        for feature in self.features:
            if feature.name == name:
                return feature
        raise KeyError(name)

    def for_entity(self, entity: str) -> tuple[Feature, ...]:
        return tuple(f for f in self.features if f.entity == entity)


def _feature(raw: dict[str, Any], entities: dict[str, str]) -> Feature:
    name = raw.get("name", "<unnamed>")
    keys = set(raw)
    if unknown := keys - COMMON - OPTIONAL:
        raise DefinitionError(f"{name}: unknown keys {sorted(unknown)}")
    if missing := COMMON - keys:
        raise DefinitionError(f"{name}: missing {sorted(missing)}")
    if not isinstance(name, str) or not NAME.match(name):
        raise DefinitionError(f"{name!r}: names are lower snake_case")
    kind = raw["kind"]
    if kind not in KINDS:
        raise DefinitionError(f"{name}: unknown kind {kind!r}; one of {sorted(KINDS)}")
    if missing := KINDS[kind] - keys:
        raise DefinitionError(f"{name}: kind {kind} needs {sorted(missing)}")
    if extra := (keys & OPTIONAL) - KINDS[kind]:
        raise DefinitionError(f"{name}: kind {kind} does not take {sorted(extra)}")
    if raw["entity"] not in entities:
        raise DefinitionError(f"{name}: unknown entity {raw['entity']!r}")
    if raw["dtype"] not in DTYPES:
        raise DefinitionError(f"{name}: dtype {raw['dtype']!r} not in {sorted(DTYPES)}")
    if not isinstance(raw["version"], int) or raw["version"] < 1:
        raise DefinitionError(f"{name}: version must be a positive integer")
    if not str(raw["description"]).strip():
        raise DefinitionError(f"{name}: description is empty")
    window = raw.get("window")
    if window is not None:
        parse_window(str(window))
    suffix = WINDOW_SUFFIX.search(name)
    if suffix and window is not None and suffix.group(1) != str(window):
        raise DefinitionError(f"{name}: name says {suffix.group(1)} but window is {window}")
    columns = raw.get("columns", ())
    if kind == "changed_within" and (not isinstance(columns, list) or not columns):
        raise DefinitionError(f"{name}: columns must be a non-empty list")
    return Feature(
        name=name,
        version=raw["version"],
        entity=raw["entity"],
        kind=kind,
        dtype=raw["dtype"],
        description=" ".join(str(raw["description"]).split()),
        window=str(window) if window is not None else None,
        field=raw.get("field"),
        filter=raw.get("filter"),
        source=raw.get("source"),
        columns=tuple(columns),
        status=raw.get("status"),
        status_time=raw.get("status_time"),
    )


def parse(doc: dict[str, Any]) -> Definitions:
    if doc.get("version") != 1:
        raise DefinitionError("definitions version must be 1")
    semantics = doc.get("semantics", {})
    if semantics != SEMANTICS:
        raise DefinitionError(
            f"semantics changed: {semantics!r}; online and offline code implement {SEMANTICS!r}"
        )
    entities = {name: spec["key"] for name, spec in doc.get("entities", {}).items()}
    features = tuple(_feature(raw, entities) for raw in doc.get("features", []))
    if not features:
        raise DefinitionError("no features defined")
    names = [f.name for f in features]
    if duplicates := sorted({n for n in names if names.count(n) > 1}):
        raise DefinitionError(f"duplicate feature names: {duplicates}")
    return Definitions(doc["version"], semantics, entities, features)


def load(path: Path = DEFAULT_PATH) -> Definitions:
    return parse(yaml.safe_load(path.read_text()))
