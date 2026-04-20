"""
Analytics endpoints — answers cross-system queries impossible without the UBID layer.
"""
from __future__ import annotations

from datetime import date, timedelta
from typing import List, Optional

from fastapi import APIRouter, Depends, Query
from sqlalchemy import func, text
from sqlalchemy.orm import Session

from src.api.schemas import AnalyticsQueryRequest, AnalyticsRow, DashboardStats
from src.database.models import (
    ActivityEvent, ActivityStatus, MatchCandidate,
    SourceRecord, UBIDRecord, UBIDSourceMapping,
)
from src.database.session import get_db

router = APIRouter(prefix="/analytics", tags=["Analytics"])


@router.get("/dashboard", response_model=DashboardStats)
def dashboard(db: Session = Depends(get_db)):
    total_source = db.query(SourceRecord).count()
    total_ubids = db.query(UBIDRecord).count()

    auto_linked = db.query(MatchCandidate).filter_by(status="AUTO_LINKED").count()
    review_pending = db.query(MatchCandidate).filter_by(status="PENDING").count()

    status_rows = (
        db.query(UBIDRecord.status, func.count(UBIDRecord.ubid))
        .group_by(UBIDRecord.status)
        .all()
    )
    status_breakdown = {row[0] or "UNCLASSIFIED": row[1] for row in status_rows}

    unmatched = db.query(ActivityEvent).filter(
        ActivityEvent.join_status.in_(["UNMATCHED_KNOWN", "UNMATCHED_UNKNOWN"])
    ).count()

    anchor_rows = (
        db.query(UBIDRecord.anchor_type, func.count(UBIDRecord.ubid))
        .group_by(UBIDRecord.anchor_type)
        .all()
    )
    anchor_breakdown = {row[0]: row[1] for row in anchor_rows}

    return DashboardStats(
        total_source_records=total_source,
        total_ubids=total_ubids,
        auto_linked_pairs=auto_linked,
        review_queue_pending=review_pending,
        status_breakdown=status_breakdown,
        unmatched_events=unmatched,
        anchor_breakdown=anchor_breakdown,
    )


@router.get("/active-without-inspection", response_model=List[AnalyticsRow])
def active_without_inspection(
    pin_code: Optional[str] = Query(None, description="Filter by PIN code"),
    source_system: str = Query(default="factories", description="Dept system filter"),
    months: int = Query(default=18, description="Months without inspection threshold"),
    limit: int = Query(default=100, le=500),
    db: Session = Depends(get_db),
):
    """
    The canonical UBID query:
    'Active [factories] in PIN [code] with no inspection in the last N months.'
    """
    cutoff = date.today() - timedelta(days=months * 30)

    # Subquery: last inspection date per UBID
    last_inspection = (
        db.query(
            ActivityEvent.ubid,
            func.max(ActivityEvent.event_date).label("last_insp"),
        )
        .filter(ActivityEvent.event_type.like("%inspection%"))
        .group_by(ActivityEvent.ubid)
        .subquery()
    )

    q = (
        db.query(UBIDRecord, last_inspection.c.last_insp)
        .outerjoin(last_inspection, UBIDRecord.ubid == last_inspection.c.ubid)
        .filter(UBIDRecord.status == "ACTIVE")
        .filter(
            (last_inspection.c.last_insp == None) |
            (last_inspection.c.last_insp < cutoff)
        )
    )

    if pin_code:
        q = q.filter(UBIDRecord.canonical_pin == pin_code)

    # Filter by source system membership
    if source_system:
        ubids_in_system = (
            db.query(UBIDSourceMapping.ubid)
            .filter(UBIDSourceMapping.source_system == source_system,
                    UBIDSourceMapping.active == True)
            .subquery()
        )
        q = q.filter(UBIDRecord.ubid.in_(ubids_in_system))

    results = q.limit(limit).all()

    rows = []
    for record, last_insp in results:
        days = (date.today() - last_insp).days if last_insp else None

        # Get source systems for this UBID
        sys_list = [
            m.source_system
            for m in db.query(UBIDSourceMapping.source_system)
            .filter_by(ubid=record.ubid, active=True)
            .all()
        ]

        rows.append(AnalyticsRow(
            ubid=record.ubid,
            canonical_name=record.canonical_name,
            canonical_pin=record.canonical_pin,
            status=record.status,
            status_confidence=record.status_confidence,
            days_since_last_inspection=days,
            source_systems=sys_list,
        ))

    return rows


@router.get("/unmatched-events")
def unmatched_events(
    limit: int = Query(default=50, le=200),
    db: Session = Depends(get_db),
):
    events = (
        db.query(ActivityEvent)
        .filter(ActivityEvent.join_status.in_(["UNMATCHED_KNOWN", "UNMATCHED_UNKNOWN"]))
        .order_by(ActivityEvent.event_date.desc())
        .limit(limit)
        .all()
    )
    return [
        {
            "id": e.id,
            "source_system": e.source_system,
            "source_record_id": e.source_record_id,
            "event_type": e.event_type,
            "event_date": str(e.event_date),
            "join_status": e.join_status,
        }
        for e in events
    ]


@router.get("/status-summary")
def status_summary(db: Session = Depends(get_db)):
    rows = (
        db.query(
            UBIDRecord.canonical_pin,
            UBIDRecord.status,
            func.count(UBIDRecord.ubid).label("count"),
        )
        .group_by(UBIDRecord.canonical_pin, UBIDRecord.status)
        .order_by(UBIDRecord.canonical_pin, UBIDRecord.status)
        .all()
    )
    return [
        {"pin_code": r[0], "status": r[1], "count": r[2]}
        for r in rows
    ]
