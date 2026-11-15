"""What a decision sees: card features (Flink), customer features (on demand, ADR-007)
and a few fields of the transaction itself."""

from features.model import load

_DEFS = load()
CARD_FEATURES: tuple[str, ...] = tuple(f.name for f in _DEFS.for_entity("card"))
CUSTOMER_FEATURES: tuple[str, ...] = tuple(f.name for f in _DEFS.for_entity("customer"))
SNAPSHOT_FEATURES: tuple[str, ...] = CARD_FEATURES + CUSTOMER_FEATURES
TRANSACTION_FIELDS: tuple[str, ...] = ("amount", "channel", "device_risk_score")
RULE_NAMES: frozenset[str] = frozenset(SNAPSHOT_FEATURES + TRANSACTION_FIELDS)
