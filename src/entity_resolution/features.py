"""
Feature vector computation for candidate pairs.

Uses a log-odds scoring approach (Fellegi-Sunter inspired):
  score = Σ log(m_prob / u_prob) for each feature agreement level

The total score is passed through a sigmoid to produce a match probability.
Weights are set from domain knowledge and can be re-estimated from labelled data.
"""
import math
from dataclasses import dataclass, field
from typing import Optional

import jellyfish

from src.database.models import SourceRecord

# ---------------------------------------------------------------------------
# Log-odds weights per feature and agreement level
# ---------------------------------------------------------------------------

# Each weight = log(m_prob / u_prob)
# Positive = evidence of match, negative = evidence of non-match.
# The prior log-odds assumes roughly 1 true match per 100 candidate pairs.
PRIOR_LOG_ODDS = math.log(0.01 / 0.99)   # ≈ -4.60

WEIGHTS = {
    # Identifiers — strongest signal
    "pan_exact_match":        6.90,   # log(0.999/0.001) near-certain
    "pan_mismatch_both_valid": -4.60,  # log(0.01/0.999) strong non-match
    "gstin_exact_match":      6.90,
    "gstin_mismatch_both_valid": -4.60,
    "gstin_pan_cross_match":  5.50,   # GSTIN's embedded PAN matches other record's PAN

    # Name similarity bands
    "name_jw_gt_095":         3.50,
    "name_jw_090_095":        2.10,
    "name_jw_080_090":        1.20,
    "name_jw_lt_080":        -0.50,

    "name_jaccard_gt_08":     2.00,
    "name_jaccard_05_08":     1.00,
    "name_jaccard_lt_05":    -0.30,

    "name_phonetic_match":    1.80,   # same phonetic key

    # Address signals
    "pin_exact_match":        1.50,
    "pin_mismatch":          -2.00,

    "door_exact_match":       2.50,
    "door_mismatch":         -0.50,

    "locality_exact_match":   1.20,

    "street_jw_gt_08":        1.00,

    # Entity type
    "entity_type_match":      0.40,
    "entity_type_mismatch":  -1.50,  # strong negative: Proprietorship ≠ Private Limited

    # NIC code
    "nic_2digit_match":       0.30,
}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _safe_jw(a: Optional[str], b: Optional[str]) -> float:
    if not a or not b:
        return 0.0
    return jellyfish.jaro_winkler_similarity(a, b)


def _jaccard(tokens_a: list[str], tokens_b: list[str]) -> float:
    if not tokens_a or not tokens_b:
        return 0.0
    sa, sb = set(tokens_a), set(tokens_b)
    return len(sa & sb) / len(sa | sb)


def _tokens(name_tokens_str: Optional[str]) -> list[str]:
    if not name_tokens_str:
        return []
    return name_tokens_str.split()


# ---------------------------------------------------------------------------
# Feature dataclass
# ---------------------------------------------------------------------------

@dataclass
class FeatureVector:
    contributions: dict = field(default_factory=dict)
    total_log_odds: float = 0.0
    match_probability: float = 0.0

    def add(self, name: str, value, contribution: float):
        self.contributions[name] = {
            "value": value,
            "contribution": round(contribution, 4),
        }
        self.total_log_odds += contribution

    def finalise(self):
        total = PRIOR_LOG_ODDS + self.total_log_odds
        self.match_probability = 1.0 / (1.0 + math.exp(-total))

    @property
    def top_features(self) -> list[dict]:
        return sorted(
            [{"feature": k, **v} for k, v in self.contributions.items()],
            key=lambda x: abs(x["contribution"]),
            reverse=True,
        )[:8]


# ---------------------------------------------------------------------------
# Main feature computation
# ---------------------------------------------------------------------------

def compute_features(a: SourceRecord, b: SourceRecord) -> FeatureVector:
    fv = FeatureVector()

    # ---- Identifiers --------------------------------------------------------
    pan_a_valid = bool(a.pan_valid and a.pan)
    pan_b_valid = bool(b.pan_valid and b.pan)
    gstin_a_valid = bool(a.gstin_valid and a.gstin)
    gstin_b_valid = bool(b.gstin_valid and b.gstin)

    if pan_a_valid and pan_b_valid:
        if a.pan == b.pan:
            fv.add("pan_exact_match", True, WEIGHTS["pan_exact_match"])
        else:
            fv.add("pan_mismatch_both_valid", f"{a.pan} ≠ {b.pan}",
                   WEIGHTS["pan_mismatch_both_valid"])

    if gstin_a_valid and gstin_b_valid:
        if a.gstin == b.gstin:
            fv.add("gstin_exact_match", True, WEIGHTS["gstin_exact_match"])
        else:
            fv.add("gstin_mismatch_both_valid", f"{a.gstin} ≠ {b.gstin}",
                   WEIGHTS["gstin_mismatch_both_valid"])

    # Cross-match: PAN from one against PAN embedded in other's GSTIN
    def _embedded_pan(gstin: str) -> Optional[str]:
        return gstin[2:12] if gstin and len(gstin) == 15 else None

    if pan_a_valid and gstin_b_valid:
        if _embedded_pan(b.gstin) == a.pan:
            fv.add("gstin_pan_cross_match", f"PAN in GSTIN({b.gstin})",
                   WEIGHTS["gstin_pan_cross_match"])
    if pan_b_valid and gstin_a_valid:
        if _embedded_pan(a.gstin) == b.pan:
            fv.add("gstin_pan_cross_match_rev", f"PAN in GSTIN({a.gstin})",
                   WEIGHTS["gstin_pan_cross_match"])

    # ---- Name similarity ----------------------------------------------------
    jw = _safe_jw(a.business_name_std, b.business_name_std)
    if jw >= 0.95:
        fv.add("name_jw", round(jw, 3), WEIGHTS["name_jw_gt_095"])
    elif jw >= 0.90:
        fv.add("name_jw", round(jw, 3), WEIGHTS["name_jw_090_095"])
    elif jw >= 0.80:
        fv.add("name_jw", round(jw, 3), WEIGHTS["name_jw_080_090"])
    else:
        fv.add("name_jw", round(jw, 3), WEIGHTS["name_jw_lt_080"])

    jac = _jaccard(_tokens(a.name_tokens), _tokens(b.name_tokens))
    if jac >= 0.80:
        fv.add("name_jaccard", round(jac, 3), WEIGHTS["name_jaccard_gt_08"])
    elif jac >= 0.50:
        fv.add("name_jaccard", round(jac, 3), WEIGHTS["name_jaccard_05_08"])
    else:
        fv.add("name_jaccard", round(jac, 3), WEIGHTS["name_jaccard_lt_05"])

    if a.phonetic_key and b.phonetic_key and a.phonetic_key == b.phonetic_key:
        fv.add("name_phonetic_match", True, WEIGHTS["name_phonetic_match"])

    # ---- Address signals ----------------------------------------------------
    if a.pin_code and b.pin_code:
        if a.pin_code == b.pin_code:
            fv.add("pin_exact_match", a.pin_code, WEIGHTS["pin_exact_match"])
        else:
            fv.add("pin_mismatch", f"{a.pin_code} ≠ {b.pin_code}", WEIGHTS["pin_mismatch"])

    if a.door_number and b.door_number:
        if a.door_number == b.door_number:
            fv.add("door_exact_match", a.door_number, WEIGHTS["door_exact_match"])
        else:
            fv.add("door_mismatch", f"{a.door_number} ≠ {b.door_number}",
                   WEIGHTS["door_mismatch"])

    if a.locality_std and b.locality_std and a.locality_std == b.locality_std:
        fv.add("locality_exact_match", a.locality_std, WEIGHTS["locality_exact_match"])

    street_jw = _safe_jw(a.street_std, b.street_std)
    if street_jw >= 0.80:
        fv.add("street_jw", round(street_jw, 3), WEIGHTS["street_jw_gt_08"])

    # ---- Entity type --------------------------------------------------------
    if a.entity_type and b.entity_type:
        if a.entity_type == b.entity_type:
            fv.add("entity_type_match", a.entity_type, WEIGHTS["entity_type_match"])
        else:
            fv.add("entity_type_mismatch", f"{a.entity_type} ≠ {b.entity_type}",
                   WEIGHTS["entity_type_mismatch"])

    # ---- NIC code (2-digit sector) ------------------------------------------
    if a.nic_code and b.nic_code and a.nic_code[:2] == b.nic_code[:2]:
        fv.add("nic_2digit_match", a.nic_code[:2], WEIGHTS["nic_2digit_match"])

    fv.finalise()
    return fv
