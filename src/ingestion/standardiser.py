"""
Standardises raw business name, address, and identifier fields
into canonical forms suitable for blocking and feature comparison.
"""
import re
import unicodedata
from dataclasses import dataclass, field
from typing import Optional

import jellyfish

# ---------------------------------------------------------------------------
# Lookup tables
# ---------------------------------------------------------------------------

NAME_ABBREVIATIONS = {
    r"\bPVT\b": "PRIVATE",
    r"\bLTD\b": "LIMITED",
    r"\bMFG\b": "MANUFACTURING",
    r"\bENGG\b": "ENGINEERING",
    r"\bIND\b": "INDUSTRIES",
    r"\bINDS\b": "INDUSTRIES",
    r"\bCO\b": "COMPANY",
    r"\bCORP\b": "CORPORATION",
    r"\bINTL\b": "INTERNATIONAL",
    r"\bNATL\b": "NATIONAL",
    r"\bENTERPS\b": "ENTERPRISES",
    r"\bENTERP\b": "ENTERPRISES",
    r"\bBROS\b": "BROTHERS",
    r"\bSERV\b": "SERVICES",
    r"\bASSO\b": "ASSOCIATES",
    r"\bASST\b": "ASSOCIATES",
    r"\bTRDG\b": "TRADING",
    r"\bTRADE\b": "TRADING",
    r"\bAGRI\b": "AGRICULTURE",
    r"\bPHARMA\b": "PHARMACEUTICALS",
    r"\bELEC\b": "ELECTRICAL",
    r"\bTECH\b": "TECHNOLOGIES",
}

LEGAL_SUFFIXES = {
    "PRIVATE LIMITED", "PVT LTD", "PUBLIC LIMITED", "LIMITED LIABILITY PARTNERSHIP",
    "LLP", "PROPRIETORSHIP", "PARTNERSHIP", "SOLE PROPRIETOR", "HUF",
}

ADDRESS_ABBREVIATIONS = {
    r"\bRD\b": "ROAD",
    r"\bST\b": "STREET",
    r"\bAVE\b": "AVENUE",
    r"\bNGR\b": "NAGAR",
    r"\bNG\b": "NAGAR",
    r"\bIND AREA\b": "INDUSTRIAL AREA",
    r"\bIND EST\b": "INDUSTRIAL ESTATE",
    r"\bINDL AREA\b": "INDUSTRIAL AREA",
    r"\bINDL EST\b": "INDUSTRIAL ESTATE",
    r"\bLAY\b": "LAYOUT",
    r"\bEXT\b": "EXTENSION",
    r"\bMG\b": "MAHATMA GANDHI",
    r"\bNO\b": "",  # "No. 14" → "14"
}

PAN_PATTERN = re.compile(r"^[A-Z]{5}[0-9]{4}[A-Z]$")
GSTIN_PATTERN = re.compile(r"^[0-9]{2}[A-Z]{5}[0-9]{4}[A-Z][0-9A-Z]{3}$")


# ---------------------------------------------------------------------------
# Dataclasses for standardised output
# ---------------------------------------------------------------------------

@dataclass
class StandardisedName:
    raw: str
    cleaned: str          # uppercase, punctuation stripped, abbreviations expanded
    tokens: list[str]     # sorted token set for Jaccard
    phonetic_key: str     # Double Metaphone key of token set
    legal_suffix: str     # extracted legal suffix
    without_suffix: str   # name without legal suffix (for comparison)


@dataclass
class StandardisedAddress:
    raw: str
    door_number: str
    street: str
    locality: str
    pin_code: str
    full_std: str         # concatenated standardised address


@dataclass
class StandardisedRecord:
    source_system: str
    source_record_id: str
    name: StandardisedName
    address: StandardisedAddress
    pan: Optional[str]
    gstin: Optional[str]
    pan_valid: bool
    gstin_valid: bool
    entity_type: str
    nic_code: str
    registration_date: Optional[str]
    raw_data: dict


# ---------------------------------------------------------------------------
# Name standardisation
# ---------------------------------------------------------------------------

def _to_ascii(text: str) -> str:
    return unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode("ascii")


def standardise_name(raw: str) -> StandardisedName:
    if not raw:
        return StandardisedName("", "", [], "", "", "")

    s = _to_ascii(raw).upper().strip()

    # Remove punctuation except spaces
    s = re.sub(r"[.,\-/\\()\[\]{}\"'`]", " ", s)

    # Expand abbreviations
    for pattern, replacement in NAME_ABBREVIATIONS.items():
        s = re.sub(pattern, replacement, s)

    # Normalise & → AND
    s = re.sub(r"\s*&\s*", " AND ", s)

    # Collapse whitespace
    s = re.sub(r"\s+", " ", s).strip()

    # Extract legal suffix
    legal_suffix = ""
    without_suffix = s
    for suffix in sorted(LEGAL_SUFFIXES, key=len, reverse=True):
        if s.endswith(suffix):
            legal_suffix = suffix
            without_suffix = s[: -len(suffix)].strip()
            break

    # Token set (sorted, for Jaccard)
    tokens = sorted(set(without_suffix.split()))

    # Phonetic key: Double Metaphone of each token, joined
    phonetic_key = " ".join(
        jellyfish.metaphone(t) for t in tokens if len(t) > 1
    )

    return StandardisedName(
        raw=raw,
        cleaned=s,
        tokens=tokens,
        phonetic_key=phonetic_key,
        legal_suffix=legal_suffix,
        without_suffix=without_suffix,
    )


# ---------------------------------------------------------------------------
# Address standardisation
# ---------------------------------------------------------------------------

_DOOR_PATTERN = re.compile(
    r"(?:NO\.?\s*|#\s*)?(\d+[\w/\-]*)",
    re.IGNORECASE,
)


def standardise_address(raw: str, pin_code: str = "") -> StandardisedAddress:
    if not raw:
        return StandardisedAddress(raw, "", "", "", pin_code or "", "")

    s = _to_ascii(raw).upper().strip()
    s = re.sub(r"[.,\-/\\()\[\]{}'`]", " ", s)

    for pattern, replacement in ADDRESS_ABBREVIATIONS.items():
        s = re.sub(pattern, replacement, s)

    s = re.sub(r"\s+", " ", s).strip()

    # Naive door number extraction: first token matching digit pattern
    door_number = ""
    m = _DOOR_PATTERN.match(s)
    if m:
        door_number = m.group(1).strip()

    # PIN from raw if not supplied
    if not pin_code:
        pin_match = re.search(r"\b(\d{6})\b", raw)
        if pin_match:
            pin_code = pin_match.group(1)

    # Remove the pin from the address body
    body = re.sub(r"\b\d{6}\b", "", s).strip()

    # Very rough split: first part = street, rest = locality
    parts = body.split()
    mid = max(1, len(parts) // 2)
    street = " ".join(parts[:mid]).strip()
    locality = " ".join(parts[mid:]).strip()

    return StandardisedAddress(
        raw=raw,
        door_number=door_number,
        street=street,
        locality=locality,
        pin_code=pin_code,
        full_std=body,
    )


# ---------------------------------------------------------------------------
# Identifier validation
# ---------------------------------------------------------------------------

def validate_pan(pan: Optional[str]) -> tuple[Optional[str], bool]:
    if not pan:
        return None, False
    cleaned = _to_ascii(str(pan)).upper().strip().replace(" ", "")
    return cleaned, bool(PAN_PATTERN.match(cleaned))


def validate_gstin(gstin: Optional[str]) -> tuple[Optional[str], bool]:
    if not gstin:
        return None, False
    cleaned = _to_ascii(str(gstin)).upper().strip().replace(" ", "")
    return cleaned, bool(GSTIN_PATTERN.match(cleaned))


def pan_from_gstin(gstin: str) -> Optional[str]:
    """Extract the embedded PAN from a valid GSTIN (chars 3–12 are the PAN)."""
    if gstin and len(gstin) == 15:
        return gstin[2:12]
    return None


# ---------------------------------------------------------------------------
# Top-level standardiser for a raw record dict
# ---------------------------------------------------------------------------

def standardise_record(raw: dict) -> StandardisedRecord:
    pan_raw, pan_valid = validate_pan(raw.get("pan"))
    gstin_raw, gstin_valid = validate_gstin(raw.get("gstin"))

    # Cross-check: if GSTIN valid but PAN missing, derive PAN from GSTIN
    if gstin_valid and not pan_valid:
        derived = pan_from_gstin(gstin_raw)
        if derived:
            pan_raw = derived
            pan_valid = True

    return StandardisedRecord(
        source_system=raw.get("source_system", ""),
        source_record_id=raw.get("source_record_id", ""),
        name=standardise_name(raw.get("business_name", "")),
        address=standardise_address(
            raw.get("address", ""),
            raw.get("pin_code", ""),
        ),
        pan=pan_raw,
        gstin=gstin_raw,
        pan_valid=pan_valid,
        gstin_valid=gstin_valid,
        entity_type=raw.get("entity_type", "UNKNOWN"),
        nic_code=raw.get("nic_code", ""),
        registration_date=raw.get("registration_date"),
        raw_data=raw,
    )
