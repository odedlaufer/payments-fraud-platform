"""Merchant categories (ISO 18245 MCC) with typical ticket sizes."""

from dataclasses import dataclass


@dataclass(frozen=True)
class Category:
    mcc: str
    name: str
    amount_median: float  # typical ticket in USD-equivalent units
    weight: float  # share of merchants in this category
    online: bool  # mostly card-not-present


CATEGORIES: tuple[Category, ...] = (
    Category("5411", "grocery", 45.0, 18, online=False),
    Category("5812", "restaurants", 35.0, 16, online=False),
    Category("5814", "fast_food", 12.0, 10, online=False),
    Category("5541", "fuel", 55.0, 8, online=False),
    Category("5311", "department_store", 80.0, 6, online=False),
    Category("5732", "electronics", 250.0, 5, online=True),
    Category("5999", "misc_retail", 40.0, 9, online=True),
    Category("4111", "transport", 8.0, 7, online=False),
    Category("4121", "rideshare", 18.0, 6, online=True),
    Category("7011", "hotels", 180.0, 4, online=True),
    Category("4511", "airlines", 420.0, 2, online=True),
    Category("5815", "digital_goods", 10.0, 5, online=True),
    Category("5967", "direct_marketing", 30.0, 2, online=True),
    Category("4829", "money_transfer", 300.0, 2, online=True),
)
