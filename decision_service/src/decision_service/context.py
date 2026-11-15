"""What a rule can see: the card features plus a few fields of the transaction itself."""

from features.model import load

CARD_FEATURES: tuple[str, ...] = tuple(f.name for f in load().for_entity("card"))
TRANSACTION_FIELDS: tuple[str, ...] = ("amount", "channel", "device_risk_score")
RULE_NAMES: frozenset[str] = frozenset(CARD_FEATURES + TRANSACTION_FIELDS)
