"""
UBID Registry — creates, looks up, and manages UBID lifecycle.

The registry is append-only: every state change produces a new provenance
log entry. Merges and un-merges are explicit operations with full audit trails.
"""
from __future__ import annotations

import re
import uuid
from datetime import datetime
from typing import Optional

from sqlalchemy.orm import Session

from src.database.models import (
    MatchCandidate, SourceRecord, UBIDAlias,
    UBIDProvenanceLog, UBIDRecord, UBIDSourceMapping,
)

_SEQ_COUNTER: dict[str, int] = {}   # in-memory seq per anchor type (reset on restart; fine for demo)


def _next_seq(anchor_type: str) -> str:
    _SEQ_COUNTER[anchor_type] = _SEQ_COUNTER.get(anchor_type, 0) + 1
    return f"{_SEQ_COUNTER[anchor_type]:08d}"


def _generate_ubid(anchor_type: str, anchor_value: Optional[str]) -> str:
    seq = _next_seq(anchor_type)
    if anchor_type == "PAN" and anchor_value:
        return f"UBID-KA-PAN-{anchor_value}-{seq}"
    elif anchor_type == "GST" and anchor_value:
        return f"UBID-KA-GST-{anchor_value}-{seq}"
    else:
        return f"UBID-KA-INT-{seq}"


class UBIDRegistry:

    # -----------------------------------------------------------------------
    # Create / get
    # -----------------------------------------------------------------------

    def get_or_create_ubid(
        self,
        db: Session,
        source_records: list[SourceRecord],
        pan: Optional[str] = None,
        gstin: Optional[str] = None,
        actor: str = "system",
    ) -> dict:
        # Check if any source record already has a mapping
        for rec in source_records:
            mapping = db.query(UBIDSourceMapping).filter_by(
                source_system=rec.source_system,
                source_record_id=rec.source_record_id,
                active=True,
            ).first()
            if mapping:
                # Add other records to this existing UBID
                self._add_records_to_ubid(db, mapping.ubid, source_records, actor)
                return {"ubid": mapping.ubid, "created": False}

        # Determine anchor type
        anchor_type = "INT"
        anchor_value = None
        if pan:
            anchor_type = "PAN"
            anchor_value = pan
        elif gstin:
            anchor_type = "GST"
            anchor_value = gstin

        ubid = _generate_ubid(anchor_type, anchor_value)

        # Pick canonical name/address from the most complete record
        best = max(source_records, key=lambda r: len(r.business_name_raw or ""))
        record = UBIDRecord(
            ubid=ubid,
            anchor_type=anchor_type,
            anchor_value=anchor_value,
            canonical_name=best.business_name_raw,
            canonical_address=best.address_raw,
            canonical_pin=best.pin_code,
            created_by=actor,
        )
        db.add(record)
        db.flush()

        # Add provenance
        db.add(UBIDProvenanceLog(
            ubid=ubid,
            event_type="created",
            event_data={
                "anchor_type": anchor_type,
                "anchor_value": anchor_value,
                "source_count": len(source_records),
            },
            actor=actor,
        ))

        # Map source records
        for rec in source_records:
            self._map_record(db, ubid, rec, method="auto_link" if pan or gstin else "clustered",
                             actor=actor)

        return {"ubid": ubid, "created": True}

    def _add_records_to_ubid(
        self, db: Session, ubid: str, source_records: list[SourceRecord], actor: str
    ):
        for rec in source_records:
            existing = db.query(UBIDSourceMapping).filter_by(
                source_system=rec.source_system,
                source_record_id=rec.source_record_id,
                active=True,
            ).first()
            if not existing:
                self._map_record(db, ubid, rec, method="clustered", actor=actor)

    def _map_record(
        self, db: Session, ubid: str, rec: SourceRecord,
        method: str = "auto_link", actor: str = "system",
        probability: float = 1.0,
    ):
        db.add(UBIDSourceMapping(
            ubid=ubid,
            source_system=rec.source_system,
            source_record_id=rec.source_record_id,
            match_probability=probability,
            match_method=method,
            mapped_by=actor,
        ))

    # -----------------------------------------------------------------------
    # Lookup
    # -----------------------------------------------------------------------

    def lookup_by_source_record(
        self, db: Session, source_system: str, source_record_id: str
    ) -> Optional[UBIDRecord]:
        mapping = db.query(UBIDSourceMapping).filter_by(
            source_system=source_system,
            source_record_id=source_record_id,
            active=True,
        ).first()
        if mapping:
            return db.query(UBIDRecord).filter_by(ubid=mapping.ubid).first()
        return None

    def lookup_by_ubid(self, db: Session, ubid: str) -> Optional[UBIDRecord]:
        record = db.query(UBIDRecord).filter_by(ubid=ubid).first()
        if record:
            return record
        # Check aliases
        alias = db.query(UBIDAlias).filter_by(alias=ubid).first()
        if alias:
            return db.query(UBIDRecord).filter_by(ubid=alias.canonical_ubid).first()
        return None

    def lookup_by_pan(self, db: Session, pan: str) -> list[UBIDRecord]:
        return db.query(UBIDRecord).filter_by(anchor_value=pan, anchor_type="PAN").all()

    def lookup_by_gstin(self, db: Session, gstin: str) -> list[UBIDRecord]:
        return db.query(UBIDRecord).filter_by(anchor_value=gstin, anchor_type="GST").all()

    def search_by_name(self, db: Session, name_fragment: str, limit: int = 20) -> list[UBIDRecord]:
        return (
            db.query(UBIDRecord)
            .filter(UBIDRecord.canonical_name.ilike(f"%{name_fragment}%"))
            .limit(limit)
            .all()
        )

    def get_source_records(self, db: Session, ubid: str) -> list[SourceRecord]:
        mappings = db.query(UBIDSourceMapping).filter_by(ubid=ubid, active=True).all()
        result = []
        for m in mappings:
            rec = db.query(SourceRecord).filter_by(
                source_system=m.source_system,
                source_record_id=m.source_record_id,
            ).first()
            if rec:
                result.append(rec)
        return result

    # -----------------------------------------------------------------------
    # Reviewer decision — confirm match
    # -----------------------------------------------------------------------

    def apply_reviewer_confirm(
        self,
        db: Session,
        candidate_id: str,
        reviewer_id: str,
        notes: str = "",
    ) -> dict:
        candidate = db.query(MatchCandidate).filter_by(id=candidate_id).first()
        if not candidate:
            return {"error": "candidate not found"}

        rec_a = db.query(SourceRecord).filter_by(
            source_system=candidate.record_a_system,
            source_record_id=candidate.record_a_id,
        ).first()
        rec_b = db.query(SourceRecord).filter_by(
            source_system=candidate.record_b_system,
            source_record_id=candidate.record_b_id,
        ).first()

        if not rec_a or not rec_b:
            return {"error": "source records not found"}

        mapping_a = db.query(UBIDSourceMapping).filter_by(
            source_system=candidate.record_a_system,
            source_record_id=candidate.record_a_id,
            active=True,
        ).first()
        mapping_b = db.query(UBIDSourceMapping).filter_by(
            source_system=candidate.record_b_system,
            source_record_id=candidate.record_b_id,
            active=True,
        ).first()

        if mapping_a and mapping_b and mapping_a.ubid == mapping_b.ubid:
            # Already the same UBID
            pass
        elif mapping_a and mapping_b and mapping_a.ubid != mapping_b.ubid:
            # Merge B into A
            target_ubid = mapping_a.ubid
            source_ubid = mapping_b.ubid
            self._merge_ubids(db, target_ubid, source_ubid, reviewer_id)
        elif mapping_a and not mapping_b:
            self._map_record(db, mapping_a.ubid, rec_b, "reviewer_confirmed", reviewer_id,
                             probability=candidate.match_probability)
        elif mapping_b and not mapping_a:
            self._map_record(db, mapping_b.ubid, rec_a, "reviewer_confirmed", reviewer_id,
                             probability=candidate.match_probability)
        else:
            # Neither has a UBID yet — create one
            pan = rec_a.pan if rec_a.pan_valid else (rec_b.pan if rec_b.pan_valid else None)
            gstin = rec_a.gstin if rec_a.gstin_valid else (rec_b.gstin if rec_b.gstin_valid else None)
            self.get_or_create_ubid(db, [rec_a, rec_b], pan=pan, gstin=gstin, actor=reviewer_id)

        candidate.status = "REVIEWED"
        candidate.review_decision = "CONFIRM_MATCH"
        candidate.reviewed_by = reviewer_id
        candidate.reviewed_at = datetime.utcnow()
        candidate.review_notes = notes
        db.commit()
        return {"status": "merged"}

    def apply_reviewer_reject(
        self,
        db: Session,
        candidate_id: str,
        reviewer_id: str,
        notes: str = "",
    ) -> dict:
        candidate = db.query(MatchCandidate).filter_by(id=candidate_id).first()
        if not candidate:
            return {"error": "candidate not found"}
        candidate.status = "REVIEWED"
        candidate.review_decision = "CONFIRM_NON_MATCH"
        candidate.reviewed_by = reviewer_id
        candidate.reviewed_at = datetime.utcnow()
        candidate.review_notes = notes
        db.commit()
        return {"status": "rejected"}

    def apply_reviewer_defer(
        self, db: Session, candidate_id: str, reviewer_id: str
    ) -> dict:
        candidate = db.query(MatchCandidate).filter_by(id=candidate_id).first()
        if not candidate:
            return {"error": "candidate not found"}
        candidate.status = "PENDING"   # back in queue
        candidate.reviewed_by = reviewer_id
        candidate.review_decision = "DEFER"
        candidate.reviewed_at = datetime.utcnow()
        db.commit()
        return {"status": "deferred"}

    # -----------------------------------------------------------------------
    # Merge / Un-merge
    # -----------------------------------------------------------------------

    def _merge_ubids(self, db: Session, target_ubid: str, source_ubid: str, actor: str):
        """Migrate all source record mappings from source_ubid to target_ubid."""
        mappings = db.query(UBIDSourceMapping).filter_by(ubid=source_ubid, active=True).all()
        for m in mappings:
            m.ubid = target_ubid
            m.match_method = "reviewer_confirmed"

        # Alias the old UBID
        db.add(UBIDAlias(alias=source_ubid, canonical_ubid=target_ubid))

        # Provenance
        db.add(UBIDProvenanceLog(
            ubid=target_ubid,
            event_type="merged",
            event_data={"source_ubid": source_ubid, "record_count": len(mappings)},
            actor=actor,
        ))
        db.flush()

    def unmerge(
        self,
        db: Session,
        ubid: str,
        source_system: str,
        source_record_id: str,
        actor: str,
    ) -> dict:
        """Split one source record out of a UBID cluster into a new singleton UBID."""
        mapping = db.query(UBIDSourceMapping).filter_by(
            ubid=ubid,
            source_system=source_system,
            source_record_id=source_record_id,
            active=True,
        ).first()
        if not mapping:
            return {"error": "mapping not found"}

        rec = db.query(SourceRecord).filter_by(
            source_system=source_system,
            source_record_id=source_record_id,
        ).first()
        if not rec:
            return {"error": "source record not found"}

        mapping.active = False

        pan = rec.pan if rec.pan_valid else None
        gstin = rec.gstin if rec.gstin_valid else None
        result = self.get_or_create_ubid(db, [rec], pan=pan, gstin=gstin, actor=actor)

        db.add(UBIDProvenanceLog(
            ubid=ubid,
            event_type="unmerged",
            event_data={"detached_record": f"{source_system}:{source_record_id}",
                        "new_ubid": result["ubid"]},
            actor=actor,
        ))
        db.commit()
        return {"new_ubid": result["ubid"]}

    # -----------------------------------------------------------------------
    # Anchor update
    # -----------------------------------------------------------------------

    def update_anchor(
        self,
        db: Session,
        ubid: str,
        pan: Optional[str] = None,
        gstin: Optional[str] = None,
        actor: str = "system",
    ) -> dict:
        record = db.query(UBIDRecord).filter_by(ubid=ubid).first()
        if not record:
            return {"error": "UBID not found"}

        old_anchor = record.anchor_value
        if pan:
            record.anchor_type = "PAN"
            record.anchor_value = pan
        elif gstin:
            record.anchor_type = "GST"
            record.anchor_value = gstin

        db.add(UBIDProvenanceLog(
            ubid=ubid,
            event_type="anchor_updated",
            event_data={"old_anchor": old_anchor, "new_anchor": record.anchor_value},
            actor=actor,
        ))
        db.commit()
        return {"ubid": ubid, "anchor": record.anchor_value}
