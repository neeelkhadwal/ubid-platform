"""
All SQLAlchemy ORM models for the UBID platform.
The UBID registry and provenance log are append-only by convention —
no rows are ever deleted, status changes produce new rows.
"""
import uuid
from datetime import datetime

from sqlalchemy import (
    Boolean, Column, Date, DateTime, Float, Integer,
    String, Text, ForeignKey, JSON, UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, relationship


def _uuid():
    return str(uuid.uuid4())


def _now():
    return datetime.utcnow()


class Base(DeclarativeBase):
    pass


# ---------------------------------------------------------------------------
# Source records ingested from department systems
# ---------------------------------------------------------------------------

class SourceRecord(Base):
    __tablename__ = "source_records"

    id = Column(String(36), primary_key=True, default=_uuid)
    source_system = Column(String(50), nullable=False)   # factories | shops | labour | kspcb
    source_record_id = Column(String(100), nullable=False)

    # Raw fields
    business_name_raw = Column(Text)
    address_raw = Column(Text)

    # Standardised / derived fields
    business_name_std = Column(Text)     # uppercased, abbreviations expanded
    phonetic_key = Column(String(100))   # Double Metaphone of name tokens
    name_tokens = Column(Text)           # space-joined token set

    pin_code = Column(String(10))
    locality_std = Column(String(100))
    door_number = Column(String(50))
    street_std = Column(String(200))

    pan = Column(String(10))
    gstin = Column(String(15))
    pan_valid = Column(Boolean)
    gstin_valid = Column(Boolean)

    entity_type = Column(String(50))     # PROPRIETORSHIP | PARTNERSHIP | PVT_LTD | LLP | PUBLIC_LTD
    nic_code = Column(String(10))
    registration_date = Column(Date)

    raw_data = Column(JSON)
    ingested_at = Column(DateTime, default=_now)

    __table_args__ = (
        UniqueConstraint("source_system", "source_record_id", name="uq_source_record"),
    )



# ---------------------------------------------------------------------------
# UBID registry — one row per unique real-world business
# ---------------------------------------------------------------------------

class UBIDRecord(Base):
    __tablename__ = "ubid_registry"

    ubid = Column(String(60), primary_key=True)
    anchor_type = Column(String(5), nullable=False)   # PAN | GST | INT
    anchor_value = Column(String(20))                 # the PAN or GSTIN value
    status = Column(String(20), default="UNCLASSIFIED")
    status_confidence = Column(String(10))            # HIGH | MEDIUM | LOW | INFERRED
    status_as_of = Column(DateTime)

    created_at = Column(DateTime, default=_now)
    created_by = Column(String(100), default="system")

    # denormalised for fast lookup
    canonical_name = Column(Text)
    canonical_address = Column(Text)
    canonical_pin = Column(String(10))

    source_mappings = relationship("UBIDSourceMapping", back_populates="ubid_record")
    provenance = relationship("UBIDProvenanceLog", back_populates="ubid_record",
                              order_by="UBIDProvenanceLog.created_at")
    activity_statuses = relationship("ActivityStatus", back_populates="ubid_record",
                                     order_by="ActivityStatus.computed_at.desc()")


class UBIDAlias(Base):
    """Old UBIDs that redirect to a current canonical UBID after a re-anchor."""
    __tablename__ = "ubid_aliases"

    alias = Column(String(60), primary_key=True)
    canonical_ubid = Column(String(60), ForeignKey("ubid_registry.ubid"))
    created_at = Column(DateTime, default=_now)


# ---------------------------------------------------------------------------
# Source record → UBID mapping (many-to-one)
# ---------------------------------------------------------------------------

class UBIDSourceMapping(Base):
    __tablename__ = "ubid_source_mappings"

    id = Column(String(36), primary_key=True, default=_uuid)
    ubid = Column(String(60), ForeignKey("ubid_registry.ubid"), nullable=False)
    source_system = Column(String(50), nullable=False)
    source_record_id = Column(String(100), nullable=False)
    match_probability = Column(Float)
    match_method = Column(String(30))   # auto_link | reviewer_confirmed | anchor_match | seed
    mapped_at = Column(DateTime, default=_now)
    mapped_by = Column(String(100), default="system")
    active = Column(Boolean, default=True)  # False after un-merge

    ubid_record = relationship("UBIDRecord", back_populates="source_mappings")

    __table_args__ = (
        UniqueConstraint("source_system", "source_record_id", name="uq_mapping"),
    )


# ---------------------------------------------------------------------------
# Append-only provenance / audit log
# ---------------------------------------------------------------------------

class UBIDProvenanceLog(Base):
    __tablename__ = "ubid_provenance_log"

    id = Column(String(36), primary_key=True, default=_uuid)
    ubid = Column(String(60), ForeignKey("ubid_registry.ubid"), nullable=False)
    event_type = Column(String(50))      # created | merged | anchor_updated | status_changed | unmerged
    event_data = Column(JSON)
    actor = Column(String(100), default="system")
    created_at = Column(DateTime, default=_now)

    ubid_record = relationship("UBIDRecord", back_populates="provenance")


# ---------------------------------------------------------------------------
# Candidate pairs — output of entity resolution, input for review queue
# ---------------------------------------------------------------------------

class MatchCandidate(Base):
    __tablename__ = "match_candidates"

    id = Column(String(36), primary_key=True, default=_uuid)
    record_a_system = Column(String(50))
    record_a_id = Column(String(100))
    record_b_system = Column(String(50))
    record_b_id = Column(String(100))

    match_probability = Column(Float)
    zone = Column(String(15))            # AUTO_LINK | REVIEW | REJECT
    feature_contributions = Column(JSON) # dict of feature → {value, contribution}
    blocking_pass = Column(String(10))
    priority_score = Column(Float)       # for review queue ordering

    status = Column(String(20), default="PENDING")  # PENDING | AUTO_LINKED | REVIEWED | REJECTED
    created_at = Column(DateTime, default=_now)

    reviewed_at = Column(DateTime)
    reviewed_by = Column(String(100))
    review_decision = Column(String(25))  # CONFIRM_MATCH | CONFIRM_NON_MATCH | DEFER | ESCALATE
    review_notes = Column(Text)


# ---------------------------------------------------------------------------
# Activity events joined to UBIDs
# ---------------------------------------------------------------------------

class ActivityEvent(Base):
    __tablename__ = "activity_events"

    id = Column(String(36), primary_key=True, default=_uuid)
    ubid = Column(String(60), ForeignKey("ubid_registry.ubid"))
    source_system = Column(String(50))
    source_record_id = Column(String(100))
    event_type = Column(String(100))
    event_date = Column(Date)
    event_data = Column(JSON)
    signal_category = Column(Integer)    # 1–5 per taxonomy
    join_status = Column(String(30))     # JOINED | AMBIGUOUS | UNMATCHED_KNOWN | UNMATCHED_UNKNOWN
    ingested_at = Column(DateTime, default=_now)

    ubid_record = relationship("UBIDRecord",
                               foreign_keys=[ubid],
                               primaryjoin="ActivityEvent.ubid==UBIDRecord.ubid",
                               viewonly=True)


# ---------------------------------------------------------------------------
# Activity status — current classification per UBID
# ---------------------------------------------------------------------------

class ActivityStatus(Base):
    __tablename__ = "activity_statuses"

    id = Column(String(36), primary_key=True, default=_uuid)
    ubid = Column(String(60), ForeignKey("ubid_registry.ubid"), nullable=False)
    status = Column(String(20))          # ACTIVE | DORMANT | CLOSED | UNCLASSIFIED
    confidence = Column(String(10))      # HIGH | MEDIUM | LOW | INFERRED
    driving_signal_event_id = Column(String(36))
    evidence_summary = Column(JSON)
    computed_at = Column(DateTime, default=_now)
    is_current = Column(Boolean, default=True)

    ubid_record = relationship("UBIDRecord", back_populates="activity_statuses")
