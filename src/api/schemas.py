from __future__ import annotations

from datetime import date, datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Source record
# ---------------------------------------------------------------------------

class SourceRecordOut(BaseModel):
    id: str
    source_system: str
    source_record_id: str
    business_name_raw: Optional[str]
    address_raw: Optional[str]
    pin_code: Optional[str]
    pan: Optional[str]
    gstin: Optional[str]
    entity_type: Optional[str]
    nic_code: Optional[str]
    registration_date: Optional[date]

    class Config:
        from_attributes = True


# ---------------------------------------------------------------------------
# UBID
# ---------------------------------------------------------------------------

class UBIDOut(BaseModel):
    ubid: str
    anchor_type: str
    anchor_value: Optional[str]
    status: Optional[str]
    status_confidence: Optional[str]
    status_as_of: Optional[datetime]
    canonical_name: Optional[str]
    canonical_address: Optional[str]
    canonical_pin: Optional[str]
    created_at: datetime

    class Config:
        from_attributes = True


class UBIDDetailOut(UBIDOut):
    source_records: List[SourceRecordOut] = []
    provenance: List[ProvenanceLogOut] = []
    activity_status: Optional[ActivityStatusOut] = None


class UBIDLookupResponse(BaseModel):
    ubid: Optional[str]
    found: bool
    record: Optional[UBIDDetailOut]
    message: str = ""


# ---------------------------------------------------------------------------
# Match candidates / review queue
# ---------------------------------------------------------------------------

class FeatureContribution(BaseModel):
    value: Any
    contribution: float


class MatchCandidateOut(BaseModel):
    id: str
    record_a_system: str
    record_a_id: str
    record_b_system: str
    record_b_id: str
    match_probability: float
    zone: str
    feature_contributions: Dict[str, FeatureContribution]
    blocking_pass: Optional[str]
    priority_score: Optional[float]
    status: str
    created_at: datetime
    reviewed_at: Optional[datetime]
    reviewed_by: Optional[str]
    review_decision: Optional[str]
    review_notes: Optional[str]

    class Config:
        from_attributes = True


class MatchCandidateWithRecords(MatchCandidateOut):
    record_a: Optional[SourceRecordOut] = None
    record_b: Optional[SourceRecordOut] = None


class ReviewDecisionRequest(BaseModel):
    decision: str  # CONFIRM_MATCH | CONFIRM_NON_MATCH | DEFER | ESCALATE
    notes: str = Field(default="", max_length=4000)


# ---------------------------------------------------------------------------
# Activity
# ---------------------------------------------------------------------------

class ActivityEventOut(BaseModel):
    id: str
    ubid: Optional[str]
    source_system: str
    source_record_id: str
    event_type: str
    event_date: date
    signal_category: int
    join_status: str
    event_data: Optional[dict]

    class Config:
        from_attributes = True


class ActivityStatusOut(BaseModel):
    id: str
    ubid: str
    status: str
    confidence: str
    evidence_summary: Optional[dict]
    computed_at: datetime
    is_current: bool

    class Config:
        from_attributes = True


# ---------------------------------------------------------------------------
# Provenance
# ---------------------------------------------------------------------------

class ProvenanceLogOut(BaseModel):
    id: str
    event_type: str
    event_data: Optional[dict]
    actor: str
    created_at: datetime

    class Config:
        from_attributes = True


# ---------------------------------------------------------------------------
# Analytics
# ---------------------------------------------------------------------------

class AnalyticsQueryRequest(BaseModel):
    pin_code: Optional[str] = None
    source_system: Optional[str] = None
    status: Optional[str] = None
    months_without_inspection: Optional[int] = None
    limit: int = Field(default=100, le=500)


class AnalyticsRow(BaseModel):
    ubid: str
    canonical_name: Optional[str]
    canonical_pin: Optional[str]
    status: Optional[str]
    status_confidence: Optional[str]
    days_since_last_inspection: Optional[int]
    source_systems: List[str]


# ---------------------------------------------------------------------------
# Dashboard
# ---------------------------------------------------------------------------

class DashboardStats(BaseModel):
    total_source_records: int
    total_ubids: int
    auto_linked_pairs: int
    review_queue_pending: int
    status_breakdown: Dict[str, int]
    unmatched_events: int
    anchor_breakdown: Dict[str, int]
