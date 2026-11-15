"""Event rates and lifecycle settings for the simulated payments app."""

from dataclasses import dataclass, replace


@dataclass(frozen=True)
class Rates:
    """Per-entity rates are events per entity per simulated day."""

    # Transaction lifecycle (FR-SIM2)
    status_delay_s: tuple[float, float] = (1.0, 10.0)
    approve_probability: float = 0.97
    reversal_probability: float = 0.01
    reversal_delay_s: tuple[float, float] = (3_600.0, 3 * 86_400.0)

    # Where customers shop: in their current city, or online anywhere
    local_share: float = 0.8

    # Trips: per customer per day; legitimate travel never beats an airliner
    trip_start: float = 0.02
    trip_days: tuple[float, float] = (2.0, 7.0)
    travel_kmh: float = 800.0
    transit_buffer_h: float = 2.0

    # Sessions
    session_reuse_s: float = 1_800.0
    new_device_probability: float = 0.02
    foreign_ip_probability: float = 0.05

    # Reference data changes (FR-SIM5)
    customer_profile_change: float = 0.01
    card_block: float = 0.002
    card_limit_change: float = 0.002
    merchant_update: float = 0.003
    merchant_delete: float = 0.005

    # Injected fraud (FR-SIM3): attacks per simulated day, across the whole population
    card_testing_attacks: float = 4.0
    impossible_travel_attacks: float = 6.0
    account_takeover_attacks: float = 3.0

    def with_fraud(self, multiplier: float) -> "Rates":
        """Scales the injected attack rates (0 turns fraud off)."""
        return replace(
            self,
            card_testing_attacks=self.card_testing_attacks * multiplier,
            impossible_travel_attacks=self.impossible_travel_attacks * multiplier,
            account_takeover_attacks=self.account_takeover_attacks * multiplier,
        )

    def scaled(self, change_multiplier: float) -> "Rates":
        """Scales the reference-data change rates; lifecycle settings stay as they are."""
        return replace(
            self,
            customer_profile_change=self.customer_profile_change * change_multiplier,
            card_block=self.card_block * change_multiplier,
            card_limit_change=self.card_limit_change * change_multiplier,
            merchant_update=self.merchant_update * change_multiplier,
            merchant_delete=self.merchant_delete * change_multiplier,
        )
