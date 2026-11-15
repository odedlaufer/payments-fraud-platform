"""What a decision sees: card and merchant features (Flink), customer features (on demand,
ADR-007) and a few fields of the transaction itself."""

from features.model import load

_DEFS = load()
CARD_FEATURES: tuple[str, ...] = tuple(f.name for f in _DEFS.for_entity("card"))
MERCHANT_FEATURES: tuple[str, ...] = tuple(f.name for f in _DEFS.for_entity("merchant"))
CUSTOMER_FEATURES: tuple[str, ...] = tuple(f.name for f in _DEFS.for_entity("customer"))
# Flink computes card and merchant features per transaction; customer ones are on demand.
FLINK_FEATURES: tuple[str, ...] = tuple(
    f.name for f in _DEFS.features if f.entity in ("card", "merchant")
)
SNAPSHOT_FEATURES: tuple[str, ...] = FLINK_FEATURES + CUSTOMER_FEATURES
TRANSACTION_FIELDS: tuple[str, ...] = ("amount", "channel", "device_risk_score")
RULE_NAMES: frozenset[str] = frozenset(SNAPSHOT_FEATURES + TRANSACTION_FIELDS)
