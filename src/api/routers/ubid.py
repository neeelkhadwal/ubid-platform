from __future__ import annotations

from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session

from src.api.schemas import (
    ActivityStatusOut, ProvenanceLogOut, SourceRecordOut,
    UBIDDetailOut, UBIDLookupResponse, UBIDOut,
)
from src.database.models import ActivityStatus, UBIDProvenanceLog, UBIDRecord
from src.database.session import get_db
from src.registry.ubid_registry import UBIDRegistry

router = APIRouter(prefix="/ubid", tags=["UBID"])
registry = UBIDRegistry()


def _build_detail(db: Session, record: UBIDRecord) -> UBIDDetailOut:
    source_records = registry.get_source_records(db, record.ubid)
    provenance = (
        db.query(UBIDProvenanceLog)
        .filter_by(ubid=record.ubid)
        .order_by(UBIDProvenanceLog.created_at)
        .all()
    )
    activity_status = (
        db.query(ActivityStatus)
        .filter_by(ubid=record.ubid, is_current=True)
        .first()
    )
    return UBIDDetailOut(
        **UBIDOut.model_validate(record).model_dump(),
        source_records=[SourceRecordOut.model_validate(r) for r in source_records],
        provenance=[ProvenanceLogOut.model_validate(p) for p in provenance],
        activity_status=ActivityStatusOut.model_validate(activity_status) if activity_status else None,
    )


@router.get("/lookup", response_model=UBIDLookupResponse)
def lookup_ubid(
    ubid: Optional[str] = Query(None),
    pan: Optional[str] = Query(None),
    gstin: Optional[str] = Query(None),
    source_system: Optional[str] = Query(None),
    source_record_id: Optional[str] = Query(None),
    name: Optional[str] = Query(None),
    db: Session = Depends(get_db),
):
    record = None

    if ubid:
        record = registry.lookup_by_ubid(db, ubid)
    elif pan:
        results = registry.lookup_by_pan(db, pan.upper())
        record = results[0] if results else None
    elif gstin:
        results = registry.lookup_by_gstin(db, gstin.upper())
        record = results[0] if results else None
    elif source_system and source_record_id:
        record = registry.lookup_by_source_record(db, source_system, source_record_id)
    elif name:
        results = registry.search_by_name(db, name)
        if results:
            record = results[0]

    if not record:
        return UBIDLookupResponse(found=False, message="No UBID found for the given criteria.")

    return UBIDLookupResponse(
        ubid=record.ubid,
        found=True,
        record=_build_detail(db, record),
    )


@router.get("/{ubid}", response_model=UBIDDetailOut)
def get_ubid(ubid: str, db: Session = Depends(get_db)):
    record = registry.lookup_by_ubid(db, ubid)
    if not record:
        raise HTTPException(status_code=404, detail=f"UBID {ubid} not found")
    return _build_detail(db, record)


@router.get("/", response_model=List[UBIDOut])
def list_ubids(
    status: Optional[str] = Query(None),
    pin_code: Optional[str] = Query(None),
    anchor_type: Optional[str] = Query(None),
    limit: int = Query(default=50, le=500),
    offset: int = 0,
    db: Session = Depends(get_db),
):
    q = db.query(UBIDRecord)
    if status:
        q = q.filter(UBIDRecord.status == status)
    if pin_code:
        q = q.filter(UBIDRecord.canonical_pin == pin_code)
    if anchor_type:
        q = q.filter(UBIDRecord.anchor_type == anchor_type)
    records = q.offset(offset).limit(limit).all()
    return [UBIDOut.model_validate(r) for r in records]


@router.post("/{ubid}/unmerge")
def unmerge_record(
    ubid: str,
    source_system: str,
    source_record_id: str,
    actor: str = "system",
    db: Session = Depends(get_db),
):
    result = registry.unmerge(db, ubid, source_system, source_record_id, actor)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result


@router.post("/{ubid}/anchor")
def update_anchor(
    ubid: str,
    pan: Optional[str] = None,
    gstin: Optional[str] = None,
    actor: str = "system",
    db: Session = Depends(get_db),
):
    result = registry.update_anchor(db, ubid, pan=pan, gstin=gstin, actor=actor)
    if "error" in result:
        raise HTTPException(status_code=400, detail=result["error"])
    return result
