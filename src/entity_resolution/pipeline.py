"""
Orchestrates the full entity resolution pipeline:
1. Load standardised source records from the DB.
2. Generate candidate pairs via blocking.
3. Score each pair.
4. Persist MatchCandidate rows.
5. Auto-link high-confidence pairs.
6. Cluster auto-links via union-find and assign UBIDs.
"""
from __future__ import annotations

from collections import defaultdict
from datetime import datetime
from typing import Optional

from rich.console import Console
from rich.progress import track
from sqlalchemy.orm import Session

from src.database.models import MatchCandidate, SourceRecord
from src.entity_resolution.blocker import generate_candidates
from src.entity_resolution.classifier import ScoredPair, score_pair
from src.registry.ubid_registry import UBIDRegistry

console = Console()


# ---------------------------------------------------------------------------
# Union-Find for clustering
# ---------------------------------------------------------------------------

class UnionFind:
    def __init__(self):
        self._parent: dict[str, str] = {}

    def _ensure(self, x: str):
        if x not in self._parent:
            self._parent[x] = x

    def find(self, x: str) -> str:
        self._ensure(x)
        if self._parent[x] != x:
            self._parent[x] = self.find(self._parent[x])  # path compression
        return self._parent[x]

    def union(self, x: str, y: str):
        rx, ry = self.find(x), self.find(y)
        if rx != ry:
            self._parent[ry] = rx

    def clusters(self) -> dict[str, list[str]]:
        result: dict[str, list[str]] = defaultdict(list)
        for node in self._parent:
            result[self.find(node)].append(node)
        return dict(result)


# ---------------------------------------------------------------------------
# Record key helper
# ---------------------------------------------------------------------------

def _rec_key(rec: SourceRecord) -> str:
    return f"{rec.source_system}:{rec.source_record_id}"


# ---------------------------------------------------------------------------
# Pipeline
# ---------------------------------------------------------------------------

def run_entity_resolution(db: Session, registry: UBIDRegistry) -> dict:
    stats = {
        "total_source_records": 0,
        "candidate_pairs": 0,
        "auto_linked": 0,
        "review_queue": 0,
        "rejected": 0,
        "ubids_created": 0,
        "ubids_existing": 0,
    }

    # 1. Load standardised records
    records: list[SourceRecord] = db.query(SourceRecord).all()
    stats["total_source_records"] = len(records)
    console.print(f"[cyan]ER Pipeline:[/] {len(records)} source records loaded")

    if len(records) < 2:
        console.print("[yellow]Too few records for entity resolution.[/]")
        return stats

    # 2. Generate candidates
    candidates = generate_candidates(records)
    stats["candidate_pairs"] = len(candidates)
    console.print(f"[cyan]ER Pipeline:[/] {len(candidates)} candidate pairs generated")

    # 3. Score each pair and persist MatchCandidate rows
    uf = UnionFind()
    scored_pairs: list[ScoredPair] = []

    # Ensure all records are individually in the UnionFind (singletons)
    for r in records:
        uf._ensure(_rec_key(r))

    existing_candidate_keys: set[tuple] = set()
    for mc in db.query(MatchCandidate).all():
        existing_candidate_keys.add(
            (mc.record_a_system, mc.record_a_id, mc.record_b_system, mc.record_b_id)
        )

    new_candidates: list[MatchCandidate] = []

    for pair in track(candidates, description="Scoring pairs…"):
        sp = score_pair(pair.rec_a, pair.rec_b, pair.blocking_pass)
        scored_pairs.append(sp)

        # Deduplicate against existing DB candidates
        key_fwd = (sp.rec_a.source_system, sp.rec_a.source_record_id,
                   sp.rec_b.source_system, sp.rec_b.source_record_id)
        key_rev = (sp.rec_b.source_system, sp.rec_b.source_record_id,
                   sp.rec_a.source_system, sp.rec_a.source_record_id)
        if key_fwd in existing_candidate_keys or key_rev in existing_candidate_keys:
            continue

        mc = MatchCandidate(
            record_a_system=sp.rec_a.source_system,
            record_a_id=sp.rec_a.source_record_id,
            record_b_system=sp.rec_b.source_system,
            record_b_id=sp.rec_b.source_record_id,
            match_probability=round(sp.match_probability, 4),
            zone=sp.zone,
            feature_contributions=sp.feature_vector.contributions,
            blocking_pass=sp.blocking_pass,
            priority_score=round(sp.priority_score, 4),
            status="AUTO_LINKED" if sp.zone == "AUTO_LINK" else
                   ("REJECTED" if sp.zone == "REJECT" else "PENDING"),
        )
        new_candidates.append(mc)
        existing_candidate_keys.add(key_fwd)

        if sp.zone == "AUTO_LINK":
            uf.union(_rec_key(sp.rec_a), _rec_key(sp.rec_b))
            stats["auto_linked"] += 1
        elif sp.zone == "REVIEW":
            stats["review_queue"] += 1
        else:
            stats["rejected"] += 1

    db.bulk_save_objects(new_candidates)
    db.commit()
    console.print(
        f"[cyan]ER Pipeline:[/] {stats['auto_linked']} auto-linked, "
        f"{stats['review_queue']} → review queue, "
        f"{stats['rejected']} rejected"
    )

    # 4. Cluster and assign UBIDs
    clusters = uf.clusters()
    console.print(f"[cyan]ER Pipeline:[/] {len(clusters)} clusters formed")

    # Build lookup: rec_key → SourceRecord
    rec_lookup: dict[str, SourceRecord] = {_rec_key(r): r for r in records}

    for root, members in clusters.items():
        recs = [rec_lookup[m] for m in members if m in rec_lookup]
        if not recs:
            continue

        # Find the best anchor (PAN → GSTIN → INT)
        pan = next((r.pan for r in recs if r.pan_valid and r.pan), None)
        gstin = next((r.gstin for r in recs if r.gstin_valid and r.gstin), None)

        result = registry.get_or_create_ubid(
            db=db,
            source_records=recs,
            pan=pan,
            gstin=gstin,
        )
        if result["created"]:
            stats["ubids_created"] += 1
        else:
            stats["ubids_existing"] += 1

    db.commit()
    stats["total_ubids"] = stats["ubids_created"] + stats["ubids_existing"]
    console.print(
        f"[cyan]ER Pipeline:[/] {stats['ubids_created']} UBIDs created, "
        f"{stats['ubids_existing']} existing updated"
    )
    return stats
