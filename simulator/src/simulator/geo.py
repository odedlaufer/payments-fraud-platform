"""Cities used as customer homes and merchant locations, plus distance."""

import math
from dataclasses import dataclass


@dataclass(frozen=True)
class City:
    name: str
    country_code: str
    lat: float
    lon: float
    currency: str


CITIES: tuple[City, ...] = (
    City("Tel Aviv", "IL", 32.0853, 34.7818, "ILS"),
    City("Jerusalem", "IL", 31.7683, 35.2137, "ILS"),
    City("Haifa", "IL", 32.7940, 34.9896, "ILS"),
    City("New York", "US", 40.7128, -74.0060, "USD"),
    City("San Francisco", "US", 37.7749, -122.4194, "USD"),
    City("Chicago", "US", 41.8781, -87.6298, "USD"),
    City("Miami", "US", 25.7617, -80.1918, "USD"),
    City("Toronto", "CA", 43.6532, -79.3832, "CAD"),
    City("London", "GB", 51.5074, -0.1278, "GBP"),
    City("Manchester", "GB", 53.4808, -2.2426, "GBP"),
    City("Paris", "FR", 48.8566, 2.3522, "EUR"),
    City("Berlin", "DE", 52.5200, 13.4050, "EUR"),
    City("Madrid", "ES", 40.4168, -3.7038, "EUR"),
    City("Rome", "IT", 41.9028, 12.4964, "EUR"),
    City("Amsterdam", "NL", 52.3676, 4.9041, "EUR"),
    City("Dubai", "AE", 25.2048, 55.2708, "AED"),
    City("Mumbai", "IN", 19.0760, 72.8777, "INR"),
    City("Singapore", "SG", 1.3521, 103.8198, "SGD"),
    City("Tokyo", "JP", 35.6762, 139.6503, "JPY"),
    City("Seoul", "KR", 37.5665, 126.9780, "KRW"),
    City("Sydney", "AU", -33.8688, 151.2093, "AUD"),
    City("Sao Paulo", "BR", -23.5505, -46.6333, "BRL"),
    City("Mexico City", "MX", 19.4326, -99.1332, "MXN"),
    City("Lagos", "NG", 6.5244, 3.3792, "NGN"),
    City("Johannesburg", "ZA", -26.2041, 28.0473, "ZAR"),
)

# Most customers live in a few home markets; the rest are spread out.
HOME_WEIGHTS: tuple[float, ...] = tuple(
    {"IL": 6.0, "US": 4.0, "GB": 2.0}.get(c.country_code, 0.5) for c in CITIES
)

EARTH_RADIUS_KM = 6371.0088


def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km (same formula the feature pipeline uses)."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlmb = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlmb / 2) ** 2
    return 2 * EARTH_RADIUS_KM * math.asin(math.sqrt(a))
