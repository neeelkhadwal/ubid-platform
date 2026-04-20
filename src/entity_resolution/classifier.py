"""
Scores candidate pairs and assigns them to one of three zones:
  AUTO_LINK  — committed without human review
  REVIEW     — routed to human reviewer
  REJECT     — records remain separate

Special override rules handle near-certain cases regardless of probabilistic score.
"""
from dataclasses import dataclass
from typing import Optional

from src.config import settings
from src.database.models import SourceRecord
from src.entity_resolution.features import FeatureVector, compute_features


@dataclass
class ScoredPair:
    rec_a: SourceRecord
    rec_b: SourceRecord
    blocking_pass: str
    feature_vector: FeatureVector
    zone: str                   # AUTO_LINK | REVIEW | REJECT
    override_reason: Optional[str] = None

    @property
    def match_probability(self) -> float:
        return self.feature_vector.match_probability

    @property
    def priority_score(self) -> float:
        """Priority = variance of Bernoulli → peaks at p=0.5 (most ambiguous)."""
        p = self.match_probability
        return p * (1 - p)


# ---------------------------------------------------------------------------
# Override rules — bypass probabilistic threshold
# ---------------------------------------------------------------------------

def _check_overrides(fv: FeatureVector, a: SourceRecord, b: SourceRecord
                     ) -> tuple[Optional[str], Optional[str]]:
    """
    Returns (zone, reason) if an override applies, else (None, None).
    """
    contribs = fv.contributions

    # Hard auto-link: exact PAN match + same PIN
    if ("pan_exact_match" in contribs and contribs["pan_exact_match"]["value"] is True
            and "pin_exact_match" in contribs):
        return "AUTO_LINK", "Exact PAN match + same PIN code"

    # Hard auto-link: exact GSTIN
    if "gstin_exact_match" in contribs and contribs["gstin_exact_match"]["value"] is True:
        return "AUTO_LINK", "Exact GSTIN match"

    # Hard auto-link: GSTIN-embedded PAN cross-match
    if ("gstin_pan_cross_match" in contribs or
            "gstin_pan_cross_match_rev" in contribs):
        return "AUTO_LINK", "PAN embedded in GSTIN matches counterpart PAN"

    # Hard reject: valid PAN mismatch
    if "pan_mismatch_both_valid" in contribs:
        return "REJECT", "PAN present in both records but values differ"

    # Hard reject: valid GSTIN mismatch
    if "gstin_mismatch_both_valid" in contribs:
        return "REJECT", "GSTIN present in both records but values differ"

    # Hard reject: entity type mismatch (PROPRIETORSHIP vs PVT_LTD)
    sole_types = {"PROPRIETORSHIP", "SOLE_PROPRIETOR"}
    corp_types = {"PVT_LTD", "PUBLIC_LTD", "LLP"}
    if (a.entity_type in sole_types and b.entity_type in corp_types) or \
       (b.entity_type in sole_types and a.entity_type in corp_types):
        if "pan_exact_match" not in contribs and "gstin_exact_match" not in contribs:
            return "REJECT", "Entity type mismatch (Proprietorship vs Company) with no identifier anchor"

    return None, None


# ---------------------------------------------------------------------------
# Zone assignment
# ---------------------------------------------------------------------------

def assign_zone(prob: float) -> str:
    if prob >= settings.t_high:
        return "AUTO_LINK"
    elif prob >= settings.t_low:
        return "REVIEW"
    else:
        return "REJECT"


# ---------------------------------------------------------------------------
# Score a single candidate pair
# ---------------------------------------------------------------------------

def score_pair(rec_a: SourceRecord, rec_b: SourceRecord,
               blocking_pass: str) -> ScoredPair:
    fv = compute_features(rec_a, rec_b)
    override_zone, override_reason = _check_overrides(fv, rec_a, rec_b)

    if override_zone:
        zone = override_zone
    else:
        zone = assign_zone(fv.match_probability)

    return ScoredPair(
        rec_a=rec_a,
        rec_b=rec_b,
        blocking_pass=blocking_pass,
        feature_vector=fv,
        zone=zone,
        override_reason=override_reason,
    )
