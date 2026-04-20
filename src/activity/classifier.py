"""
Activity status inference engine.

For each UBID, classifies it as ACTIVE / DORMANT / CLOSED / UNCLASSIFIED
based on the signal taxonomy and configurable observation windows.
Produces a structured evidence timeline for every verdict.
"""
from __future__ import annotations

from datetime import date, datetime, timedelta
from typing import Optional

from rich.console import Console
from sqlalchemy.orm import Session

from src.activity.signals import SignalCategory, categorise
from src.config import settings
from src.database.models import (
    ActivityEvent, ActivityStatus, SourceRecord,
    UBIDProvenanceLog, UBIDRecord, UBIDSourceMapping,
)

console = Console()


# ---------------------------------------------------------------------------
# Event ingestion — join events to UBIDs
# ---------------------------------------------------------------------------

def ingest_events(db: Session, raw_events: list[dict]) -> dict:
    stats = {"joined": 0, "unmatched_known": 0, "unmatched_unknown": 0}

    known_record_ids: set[tuple] = {
        (r.source_system, r.source_record_id)
        for r in db.query(SourceRecord).all()
    }

    existing_event_keys: set[tuple] = {
        (e.source_system, e.source_record_id, e.event_type, str(e.event_date))
        for e in db.query(ActivityEvent).all()
    }

    new_events: list[ActivityEvent] = []

    for raw in raw_events:
        sys = raw["source_system"]
        rec_id = raw["source_record_id"]
        evt_type = raw["event_type"]
        evt_date_str = raw["event_date"]

        dedup_key = (sys, rec_id, evt_type, evt_date_str)
        if dedup_key in existing_event_keys:
            continue
        existing_event_keys.add(dedup_key)

        try:
            evt_date = date.fromisoformat(evt_date_str)
        except ValueError:
            continue

        signal_cat = categorise(evt_type)

        mapping = db.query(UBIDSourceMapping).filter_by(
            source_system=sys,
            source_record_id=rec_id,
            active=True,
        ).first()

        if mapping:
            join_status = "JOINED"
            ubid = mapping.ubid
            stats["joined"] += 1
        elif (sys, rec_id) in known_record_ids:
            join_status = "UNMATCHED_KNOWN"
            ubid = None
            stats["unmatched_known"] += 1
        else:
            join_status = "UNMATCHED_UNKNOWN"
            ubid = None
            stats["unmatched_unknown"] += 1

        new_events.append(ActivityEvent(
            ubid=ubid,
            source_system=sys,
            source_record_id=rec_id,
            event_type=evt_type,
            event_date=evt_date,
            event_data=raw.get("event_data", {}),
            signal_category=signal_cat,
            join_status=join_status,
        ))

    db.bulk_save_objects(new_events)
    db.commit()
    return stats


# ---------------------------------------------------------------------------
# Status classification
# ---------------------------------------------------------------------------

def classify_ubid(db: Session, ubid: str) -> ActivityStatus:
    today = date.today()
    window_days = settings.observation_window_months * 30
    closed_threshold = settings.dormant_to_closed_months * 30

    events: list[ActivityEvent] = (
        db.query(ActivityEvent)
        .filter(
            ActivityEvent.ubid == ubid,
            ActivityEvent.join_status == "JOINED",
        )
        .order_by(ActivityEvent.event_date.desc())
        .all()
    )

    # --- Check for closure signal ---
    closure_events = [e for e in events if e.signal_category == SignalCategory.CLOSURE]
    if closure_events:
        driving = closure_events[0]
        status, confidence = "CLOSED", "HIGH"
        evidence = _build_evidence(events, driving, status, today)
        return _persist_status(db, ubid, status, confidence, driving.id, evidence)

    # --- Find latest positive active event ---
    active_events = [
        e for e in events
        if e.signal_category in (
            SignalCategory.STRONG_ACTIVE,
            SignalCategory.MODERATE_ACTIVE,
            SignalCategory.WEAK_ACTIVE,
        )
    ]

    if not active_events:
        # No events at all → UNCLASSIFIED
        evidence = {"reason": "No activity events recorded", "events_total": 0}
        return _persist_status(db, ubid, "UNCLASSIFIED", "INFERRED", None, evidence)

    latest_active = max(active_events, key=lambda e: e.event_date)
    days_since = (today - latest_active.event_date).days

    if days_since <= window_days:
        # Determine confidence by category of driving signal
        driving_cat = latest_active.signal_category
        if driving_cat == SignalCategory.STRONG_ACTIVE:
            confidence = "HIGH"
        elif driving_cat == SignalCategory.MODERATE_ACTIVE:
            confidence = "MEDIUM"
        else:
            confidence = "LOW"
        status = "ACTIVE"
    elif days_since <= closed_threshold:
        status = "DORMANT"
        confidence = "MEDIUM"
    else:
        status = "CLOSED"
        confidence = "INFERRED"

    evidence = _build_evidence(events, latest_active, status, today)
    return _persist_status(db, ubid, status, confidence, latest_active.id, evidence)


def _build_evidence(
    events: list[ActivityEvent],
    driving: ActivityEvent,
    status: str,
    today: date,
) -> dict:
    supporting = [
        e for e in events
        if e.id != driving.id and e.signal_category <= SignalCategory.MODERATE_ACTIVE
    ][:5]

    negative = [e for e in events if e.signal_category == SignalCategory.DORMANCY][:3]

    return {
        "status": status,
        "observation_window_months": settings.observation_window_months,
        "driving_signal": {
            "event_type": driving.event_type,
            "source_system": driving.source_system,
            "event_date": driving.event_date.isoformat(),
            "signal_category": driving.signal_category,
            "event_data": driving.event_data,
        },
        "supporting_signals": [
            {
                "event_type": e.event_type,
                "source_system": e.source_system,
                "event_date": e.event_date.isoformat(),
                "signal_category": e.signal_category,
            }
            for e in supporting
        ],
        "negative_signals": [
            {
                "event_type": e.event_type,
                "event_date": e.event_date.isoformat(),
            }
            for e in negative
        ],
        "total_events": len(events),
        "computed_at": today.isoformat(),
    }


def _persist_status(
    db: Session,
    ubid: str,
    status: str,
    confidence: str,
    driving_event_id: Optional[str],
    evidence: dict,
) -> ActivityStatus:
    # Mark old statuses as not current
    db.query(ActivityStatus).filter_by(ubid=ubid, is_current=True).update(
        {"is_current": False}
    )

    new_status = ActivityStatus(
        ubid=ubid,
        status=status,
        confidence=confidence,
        driving_signal_event_id=driving_event_id,
        evidence_summary=evidence,
        is_current=True,
    )
    db.add(new_status)

    # Update the denormalised status on UBIDRecord for fast queries
    db.query(UBIDRecord).filter_by(ubid=ubid).update({
        "status": status,
        "status_confidence": confidence,
        "status_as_of": datetime.utcnow(),
    })

    # Provenance
    db.add(UBIDProvenanceLog(
        ubid=ubid,
        event_type="status_changed",
        event_data={"new_status": status, "confidence": confidence},
        actor="activity_classifier",
    ))

    db.flush()
    return new_status


# ---------------------------------------------------------------------------
# Batch classification — run for all UBIDs
# ---------------------------------------------------------------------------

def classify_all(db: Session) -> dict:
    ubids = [r.ubid for r in db.query(UBIDRecord.ubid).all()]
    counts: dict[str, int] = {}

    for ubid in ubids:
        status_record = classify_ubid(db, ubid)
        counts[status_record.status] = counts.get(status_record.status, 0) + 1

    db.commit()
    return counts
