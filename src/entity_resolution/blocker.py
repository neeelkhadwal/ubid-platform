"""
Blocking strategies that reduce the O(n²) comparison space to manageable
candidate pairs. Multiple overlapping passes maximise recall.
"""
from collections import defaultdict
from dataclasses import dataclass
from typing import Iterator

from src.database.models import SourceRecord


@dataclass(frozen=True)
class CandidatePair:
    rec_a: SourceRecord
    rec_b: SourceRecord
    blocking_pass: str

    def key(self) -> tuple[str, str]:
        ids = sorted([
            f"{self.rec_a.source_system}:{self.rec_a.source_record_id}",
            f"{self.rec_b.source_system}:{self.rec_b.source_record_id}",
        ])
        return (ids[0], ids[1])


def _index_by(records: list[SourceRecord], key_fn) -> dict[str, list[SourceRecord]]:
    idx = defaultdict(list)
    for rec in records:
        k = key_fn(rec)
        if k:
            idx[k].append(rec)
    return idx


def _emit_pairs(idx: dict, pass_name: str) -> Iterator[CandidatePair]:
    for bucket in idx.values():
        if len(bucket) < 2:
            continue
        for i in range(len(bucket)):
            for j in range(i + 1, len(bucket)):
                a, b = bucket[i], bucket[j]
                # Never compare a record to itself
                if (a.source_system == b.source_system and
                        a.source_record_id == b.source_record_id):
                    continue
                yield CandidatePair(a, b, pass_name)


# ---------------------------------------------------------------------------
# Individual blocking passes
# ---------------------------------------------------------------------------

def block_by_pan(records: list[SourceRecord]) -> Iterator[CandidatePair]:
    idx = _index_by(records, lambda r: r.pan if r.pan_valid else None)
    yield from _emit_pairs(idx, "B1")


def block_by_gstin(records: list[SourceRecord]) -> Iterator[CandidatePair]:
    idx = _index_by(records, lambda r: r.gstin if r.gstin_valid else None)
    yield from _emit_pairs(idx, "B2")


def block_by_pin_phonetic(records: list[SourceRecord]) -> Iterator[CandidatePair]:
    def key(r: SourceRecord):
        if r.pin_code and r.phonetic_key:
            # Use first 6 chars of phonetic key to allow partial phonetic variation
            return f"{r.pin_code}|{r.phonetic_key[:6]}"
        return None
    idx = _index_by(records, key)
    yield from _emit_pairs(idx, "B3")


def block_by_pin_door(records: list[SourceRecord]) -> Iterator[CandidatePair]:
    def key(r: SourceRecord):
        if r.pin_code and r.door_number:
            return f"{r.pin_code}|{r.door_number}"
        return None
    idx = _index_by(records, key)
    yield from _emit_pairs(idx, "B4")


def block_by_phonetic_entity(records: list[SourceRecord]) -> Iterator[CandidatePair]:
    def key(r: SourceRecord):
        if r.phonetic_key and r.entity_type:
            return f"{r.phonetic_key}|{r.entity_type}"
        return None
    idx = _index_by(records, key)
    yield from _emit_pairs(idx, "B5")


def block_by_name_prefix_pin(records: list[SourceRecord]) -> Iterator[CandidatePair]:
    """Backup pass: first 5 chars of std name + PIN code."""
    def key(r: SourceRecord):
        if r.business_name_std and r.pin_code:
            prefix = r.business_name_std.replace(" ", "")[:5]
            return f"{prefix}|{r.pin_code}"
        return None
    idx = _index_by(records, key)
    yield from _emit_pairs(idx, "B6")


# ---------------------------------------------------------------------------
# Combined blocking — union of all passes, deduped by pair key
# ---------------------------------------------------------------------------

def generate_candidates(records: list[SourceRecord]) -> list[CandidatePair]:
    seen: set[tuple[str, str]] = set()
    candidates: list[CandidatePair] = []

    passes = [
        block_by_pan,
        block_by_gstin,
        block_by_pin_phonetic,
        block_by_pin_door,
        block_by_phonetic_entity,
        block_by_name_prefix_pin,
    ]

    for blocking_fn in passes:
        for pair in blocking_fn(records):
            k = pair.key()
            if k not in seen:
                seen.add(k)
                candidates.append(pair)

    return candidates
