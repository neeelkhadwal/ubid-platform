"""
Signal taxonomy for activity classification.
Category 1 = strongest active signal; Category 5 = closure signal.
"""
from enum import IntEnum

SIGNAL_TAXONOMY: dict[str, int] = {
    # Category 1 — Strong active (reset observation window)
    "factories_annual_return":         1,
    "factories_licence_renewal":       1,
    "shops_establishment_renewal":     1,
    "labour_contract_renewal":         1,
    "kspcb_consent_renewal":           1,
    "gst_return_filed":                1,
    "fssai_licence_renewal":           1,

    # Category 2 — Moderate active (extend window by 6 months)
    "factories_inspection_visit":      2,
    "kspcb_inspection":                2,
    "shops_fire_noc_renewal":          2,
    "fire_noc_renewal":                2,
    "labour_inspection":               2,
    "bescom_consumption":              2,
    "bwssb_consumption":               2,
    "bwssb_water_consumption":         2,
    "bescom_meter_reading":            2,

    # Category 3 — Weak active (extend window by 3 months, cannot solo classify)
    "factories_notice_acknowledged":   3,
    "shops_notice_acknowledged":       3,
    "labour_notice_acknowledged":      3,
    "kspcb_notice_acknowledged":       3,
    "correspondence_acknowledged":     3,

    # Category 4 — Dormancy signals (negative evidence)
    "factories_missed_renewal":        4,
    "factories_inspection_not_found":  4,
    "shops_missed_renewal":            4,
    "labour_missed_renewal":           4,
    "kspcb_missed_renewal":            4,
    "returned_mail":                   4,

    # Category 5 — Closure signals (trigger Closed classification)
    "factories_licence_surrender":     5,
    "shops_deregistration":            5,
    "kspcb_consent_withdrawn":         5,
    "gst_cancellation":                5,
    "roc_strike_off":                  5,
    "court_closure_order":             5,
    "premises_vacated":                5,
}


def categorise(event_type: str) -> int:
    """Return the signal category for an event type. Defaults to 3 (weak) if unknown."""
    return SIGNAL_TAXONOMY.get(event_type, 3)


class SignalCategory(IntEnum):
    STRONG_ACTIVE = 1
    MODERATE_ACTIVE = 2
    WEAK_ACTIVE = 3
    DORMANCY = 4
    CLOSURE = 5
