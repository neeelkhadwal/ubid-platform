from __future__ import annotations

from typing import List

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.api.schemas import (
    MatchCandidateOut, MatchCandidateWithRecords,
    ReviewDecisionRequest, SourceRecordOut,
)
from src.database.models import MatchCandidate, SourceRecord
from src.database.session import get_db
from src.registry.ubid_registry import UBIDRegistry

router = APIRouter(prefix="/review", tags=["Review Queue"])
registry = UBIDRegistry()


def _enrich(candidate: MatchCandidate, db: Session) -> MatchCandidateWithRecords:
    rec_a = db.query(SourceRecord).filter_by(
        source_system=candidate.record_a_system,
        source_record_id=candidate.record_a_id,
    ).first()
    rec_b = db.query(SourceRecord).filter_by(
        source_system=candidate.record_b_system,
        source_record_id=candidate.record_b_id,
    ).first()
    base = MatchCandidateOut.model_validate(candidate)
    return MatchCandidateWithRecords(
        **base.model_dump(),
        record_a=SourceRecordOut.model_validate(rec_a) if rec_a else None,
        record_b=SourceRecordOut.model_validate(rec_b) if rec_b else None,
    )


@router.get("/queue", response_model=List[MatchCandidateWithRecords])
def get_review_queue(
    status: str = Query(default="PENDING"),
    limit: int = Query(default=50, le=200),
    offset: int = 0,
    db: Session = Depends(get_db),
):
    candidates = (
        db.query(MatchCandidate)
        .filter(MatchCandidate.status == status)
        .order_by(MatchCandidate.priority_score.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )
    return [_enrich(c, db) for c in candidates]


@router.get("/queue/count")
def review_queue_count(db: Session = Depends(get_db)):
    pending = db.query(MatchCandidate).filter_by(status="PENDING").count()
    reviewed = db.query(MatchCandidate).filter_by(status="REVIEWED").count()
    auto_linked = db.query(MatchCandidate).filter_by(status="AUTO_LINKED").count()
    rejected = db.query(MatchCandidate).filter_by(status="REJECTED").count()
    return {
        "pending": pending,
        "reviewed": reviewed,
        "auto_linked": auto_linked,
        "rejected": rejected,
    }


@router.get("/queue/{candidate_id}", response_model=MatchCandidateWithRecords)
def get_candidate(candidate_id: str, db: Session = Depends(get_db)):
    candidate = db.query(MatchCandidate).filter_by(id=candidate_id).first()
    if not candidate:
        raise HTTPException(status_code=404, detail="Candidate not found")
    return _enrich(candidate, db)


@router.post("/queue/{candidate_id}/decision")
def submit_decision(
    candidate_id: str,
    body: ReviewDecisionRequest,
    db: Session = Depends(get_db),
):
    decision = body.decision.upper()
    if decision not in ("CONFIRM_MATCH", "CONFIRM_NON_MATCH", "DEFER", "ESCALATE"):
        raise HTTPException(status_code=422, detail="Invalid decision value")

    if decision == "CONFIRM_MATCH":
        result = registry.apply_reviewer_confirm(
            db, candidate_id, body.reviewer_id, body.notes
        )
    elif decision == "CONFIRM_NON_MATCH":
        result = registry.apply_reviewer_reject(
            db, candidate_id, body.reviewer_id, body.notes
        )
    elif decision == "DEFER":
        result = registry.apply_reviewer_defer(db, candidate_id, body.reviewer_id)
    else:
        # ESCALATE — mark but keep in queue
        candidate = db.query(MatchCandidate).filter_by(id=candidate_id).first()
        if candidate:
            candidate.review_decision = "ESCALATE"
            candidate.reviewed_by = body.reviewer_id
            candidate.review_notes = body.notes
            db.commit()
        result = {"status": "escalated"}

    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result
