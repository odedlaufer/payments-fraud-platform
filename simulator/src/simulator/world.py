"""In-memory view of the entities the engine acts on, loaded from the store."""

from dataclasses import dataclass, field
from datetime import date, datetime


@dataclass
class CustomerState:
    customer_id: int
    country_code: str
    home_lat: float
    home_lon: float
    devices: list[str] = field(default_factory=list)
    last_session_id: int | None = None
    last_session_at: datetime | None = None


@dataclass
class CardState:
    card_id: int
    customer_id: int
    card_type: str
    status: str
    expires_on: date


@dataclass
class MerchantState:
    merchant_id: int
    mcc: str
    country_code: str
    lat: float
    lon: float
    status: str


@dataclass
class World:
    customers: dict[int, CustomerState]
    cards: dict[int, CardState]
    merchants: dict[int, MerchantState]
    last_event_ts: datetime | None
    pending_transactions: list[tuple[int, datetime]] = field(default_factory=list)

    def merchants_in(self, country_code: str) -> list[MerchantState]:
        return [m for m in self.merchants.values() if m.country_code == country_code]
