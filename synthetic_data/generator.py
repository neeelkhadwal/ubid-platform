"""
Generates realistic synthetic master data and activity events
for the UBID hackathon demo.

Produces 120 "real" businesses across 2 Bengaluru PIN codes,
represented as records in 4 department systems with realistic name /
address / identifier variations. Also generates 12 months of activity
events for each business.
"""
import random
import uuid
from copy import deepcopy
from dataclasses import dataclass, field
from datetime import date, timedelta
from typing import Optional

random.seed(42)

# ---------------------------------------------------------------------------
# Corpus data
# ---------------------------------------------------------------------------

BUSINESS_PREFIXES = [
    "Karnataka", "Bangalore", "Bengaluru", "Peenya", "Rajajinagar",
    "Mysore", "Hubli", "South India", "Deccan", "Cauvery",
    "Sri", "Shri", "Sri Sai", "Sri Vinayaka", "Lakshmi",
    "Saraswati", "Ganesha", "Mahalakshmi", "Hanuman",
    "National", "Indian", "Bharatha", "Bharat",
    "Modern", "Progressive", "Premier", "Quality", "Excel",
    "Global", "Universal", "Supreme", "Pioneer", "Innovative",
]

BUSINESS_CORE = [
    "Steel", "Pipes", "Tubes", "Castings", "Forgings",
    "Plastics", "Chemicals", "Pharma", "Paints", "Coatings",
    "Textiles", "Garments", "Fabrics", "Leather", "Footwear",
    "Electronics", "Electricals", "Components", "Switches", "Cables",
    "Foods", "Beverages", "Spices", "Flour", "Oils",
    "Printing", "Packaging", "Paper", "Cardboard",
    "Tools", "Dies", "Moulds", "Jigs", "Fixtures",
    "Engineering", "Fabrication", "Welding", "Machining",
    "Granite", "Marble", "Tiles", "Ceramics",
    "Auto", "Auto Parts", "Spares", "Motors",
    "Software", "IT Services", "Data", "Tech",
    "Logistics", "Transport", "Courier",
    "Construction", "Builders", "Infrastructure",
    "Trading", "Exports", "Imports",
]

BUSINESS_SUFFIXES = [
    "Industries", "Enterprises", "Company", "Works",
    "Manufacturing", "Products", "Solutions", "Services",
    "Associates", "Brothers", "Group",
]

LEGAL_TYPES = [
    ("Private Limited", "PVT_LTD", 0.35),
    ("LLP", "LLP", 0.10),
    ("Proprietorship", "PROPRIETORSHIP", 0.40),
    ("Partnership", "PARTNERSHIP", 0.15),
]

STREETS_560058 = [  # Rajajinagar
    "Sampige Road", "10th Cross Road", "18th Cross Road",
    "8th Block Main Road", "1st Main Road", "3rd Stage Road",
    "Dr Rajkumar Road", "Chord Road", "Magadi Road",
    "Palace Road",
]

STREETS_560086 = [  # Peenya
    "Peenya Industrial Area 1st Stage", "Peenya Industrial Area 2nd Stage",
    "Tumkur Road", "Peenya 2nd Phase", "Hegganahalli Road",
    "KSSIDC Industrial Estate", "Yeshwanthpur Industrial Suburb",
    "BEL Road", "Jalahalli Cross", "Sarakki Industrial Layout",
]

NIC_CODES = {
    "factories": ["25910", "25120", "27900", "22190", "24200", "28140",
                  "29100", "30200", "13100", "10610"],
    "shops":     ["47191", "47299", "47710", "46100", "56101", "72200",
                  "62010", "77110", "52100", "46900"],
    "labour":    ["25910", "13100", "47191", "62010", "52100", "24200"],
    "kspcb":     ["25910", "25120", "24200", "22190", "28140", "13100"],
}

ENTITY_TYPES_BY_SYSTEM = {
    "factories": ["PVT_LTD", "LLP", "PARTNERSHIP", "PROPRIETORSHIP"],
    "shops":     ["PROPRIETORSHIP", "PARTNERSHIP", "PVT_LTD"],
    "labour":    ["PVT_LTD", "PROPRIETORSHIP", "PARTNERSHIP"],
    "kspcb":     ["PVT_LTD", "PARTNERSHIP", "LLP"],
}

EVENT_TYPES = {
    1: [  # Strong active
        ("factories", "factories_annual_return"),
        ("factories", "factories_licence_renewal"),
        ("shops", "shops_establishment_renewal"),
        ("labour", "labour_contract_renewal"),
        ("kspcb", "kspcb_consent_renewal"),
    ],
    2: [  # Moderate active
        ("factories", "factories_inspection_visit"),
        ("kspcb", "kspcb_inspection"),
        ("shops", "fire_noc_renewal"),
        ("labour", "labour_inspection"),
        ("bescom", "bescom_consumption"),
        ("bwssb", "bwssb_consumption"),
    ],
    3: [  # Weak active
        ("factories", "factories_notice_acknowledged"),
        ("shops", "shops_notice_acknowledged"),
        ("bescom", "bescom_meter_reading"),
    ],
    4: [  # Dormancy signals
        ("factories", "factories_missed_renewal"),
        ("factories", "factories_inspection_not_found"),
        ("shops", "shops_missed_renewal"),
    ],
    5: [  # Closure signals
        ("factories", "factories_licence_surrender"),
        ("shops", "shops_deregistration"),
        ("kspcb", "kspcb_consent_withdrawn"),
    ],
}

# ---------------------------------------------------------------------------
# Name variation helpers
# ---------------------------------------------------------------------------

_ABBREV_MAP = {
    "Private Limited": ["Pvt Ltd", "Pvt. Ltd.", "P. Ltd", "Private Ltd", "Pvt Limited"],
    "LLP": ["L.L.P", "L L P"],
    "Proprietorship": ["Prop", "Propr"],
    "Industries": ["Inds", "Ind", "Inds."],
    "Enterprises": ["Entr", "Enterp", "Entps"],
    "Manufacturing": ["Mfg", "Mfg.", "Mfrs"],
    "Engineering": ["Engg", "Engg.", "Engg", "Engg"],
    "Company": ["Co", "Co."],
    "Services": ["Serv", "Srvs"],
    "Associates": ["Assoc", "Asso"],
    "Brothers": ["Bros", "Bros."],
}


def _abbreviate(name: str) -> str:
    for full, abbrevs in _ABBREV_MAP.items():
        if full in name:
            return name.replace(full, random.choice(abbrevs), 1)
    return name


def _vary_name(base_name: str) -> str:
    """Apply one of several realistic variations to a business name."""
    choice = random.random()
    if choice < 0.30:
        return _abbreviate(base_name)
    elif choice < 0.50:
        # Drop one word
        parts = base_name.split()
        if len(parts) > 2:
            idx = random.randint(0, len(parts) - 2)
            parts.pop(idx)
            return " ".join(parts)
    elif choice < 0.65:
        # Introduce a typo (swap adjacent chars in one token)
        parts = base_name.split()
        idx = random.randint(0, len(parts) - 1)
        w = parts[idx]
        if len(w) > 3:
            i = random.randint(0, len(w) - 2)
            w = w[:i] + w[i + 1] + w[i] + w[i + 2:]
            parts[idx] = w
        return " ".join(parts)
    return base_name


def _vary_address(base_address: str, door: str) -> str:
    """Apply a realistic address format variation."""
    formats = [
        f"No. {door}, {base_address}",
        f"{door}, {base_address}",
        f"#{door} {base_address}",
        f"{door}/{random.randint(1,5)}, {base_address}",
        base_address,
    ]
    return random.choice(formats)


# ---------------------------------------------------------------------------
# Core business generator
# ---------------------------------------------------------------------------

@dataclass
class RealBusiness:
    biz_id: str
    canonical_name: str
    canonical_address: str
    door_number: str
    pin_code: str
    legal_type: str
    entity_type: str
    pan: Optional[str]
    gstin: Optional[str]
    nic_code: str
    reg_date: date
    systems: list[str]          # which dept systems this business appears in
    is_closed: bool = False
    is_dormant: bool = False


def _random_pan() -> str:
    chars = "ABCDEFGHJKLMNPQRSTUVWXYZ"
    return (
        random.choice(chars)
        + random.choice(chars)
        + random.choice(chars)
        + random.choice("ABCFGHLJPT")  # 4th char encodes entity type
        + random.choice(chars)
        + "".join(str(random.randint(0, 9)) for _ in range(4))
        + random.choice(chars)
    )


def _random_gstin(pan: str, state_code: str = "29") -> str:
    entity_num = str(random.randint(1, 5))
    z_char = "Z"
    check = str(random.randint(0, 9))
    return f"{state_code}{pan}{entity_num}{z_char}{check}"


def generate_businesses(n: int = 120) -> list[RealBusiness]:
    businesses = []
    systems_pool = ["factories", "shops", "labour", "kspcb"]

    for i in range(n):
        prefix = random.choice(BUSINESS_PREFIXES)
        core = random.choice(BUSINESS_CORE)
        suffix = random.choice(BUSINESS_SUFFIXES)
        legal_type, entity_type, _ = random.choices(
            LEGAL_TYPES, weights=[w for _, _, w in LEGAL_TYPES]
        )[0]

        name = f"{prefix} {core} {suffix} {legal_type}"

        pin = random.choice(["560058", "560086"])
        streets = STREETS_560058 if pin == "560058" else STREETS_560086
        street = random.choice(streets)
        door = f"{random.randint(1, 200)}"

        address = f"{door}, {street}, Bengaluru"

        # ~45% have PAN, 30% also have GSTIN
        has_pan = random.random() < 0.45
        pan = _random_pan() if has_pan else None
        gstin = _random_gstin(pan) if (has_pan and random.random() < 0.65) else None

        nic_pool = random.choice(["factories", "shops", "labour", "kspcb"])
        nic = random.choice(NIC_CODES[nic_pool])

        reg_date = date(
            random.randint(2005, 2022),
            random.randint(1, 12),
            random.randint(1, 28),
        )

        # Which systems does this business appear in?
        n_systems = random.choices([1, 2, 3, 4], weights=[0.15, 0.35, 0.35, 0.15])[0]
        systems = random.sample(systems_pool, n_systems)

        is_closed = i < 8           # first 8 are closed
        is_dormant = 8 <= i < 20    # next 12 are dormant

        businesses.append(RealBusiness(
            biz_id=str(uuid.uuid4()),
            canonical_name=name,
            canonical_address=address,
            door_number=door,
            pin_code=pin,
            legal_type=legal_type,
            entity_type=entity_type,
            pan=pan,
            gstin=gstin,
            nic_code=nic,
            reg_date=reg_date,
            systems=systems,
            is_closed=is_closed,
            is_dormant=is_dormant,
        ))

    return businesses


# ---------------------------------------------------------------------------
# Source record generator (adds noise/variation per system)
# ---------------------------------------------------------------------------

def generate_source_records(businesses: list[RealBusiness]) -> list[dict]:
    records = []
    for biz in businesses:
        for sys in biz.systems:
            rec_id = f"{sys.upper()[:3]}-BLR-{len(records):05d}"

            # Apply name variation ~70% of the time
            name = _vary_name(biz.canonical_name) if random.random() < 0.70 else biz.canonical_name

            # Apply address variation
            address = _vary_address(biz.canonical_address.split(",")[1].strip(), biz.door_number)
            if random.random() < 0.30:
                address = address  # no pin in address field (stored separately)

            # PAN: present in record ~60% of cases where business has one
            pan = biz.pan if (biz.pan and random.random() < 0.60) else None
            # Introduce PAN typo ~5% of the time
            if pan and random.random() < 0.05:
                idx = random.randint(5, 8)
                pan = pan[:idx] + str(random.randint(0, 9)) + pan[idx + 1:]

            gstin = biz.gstin if (biz.gstin and pan == biz.pan and random.random() < 0.50) else None

            entity_type = biz.entity_type
            if sys in ENTITY_TYPES_BY_SYSTEM and random.random() < 0.10:
                entity_type = random.choice(ENTITY_TYPES_BY_SYSTEM[sys])

            records.append({
                "biz_id": biz.biz_id,         # ground truth link (not in production)
                "source_system": sys,
                "source_record_id": rec_id,
                "business_name": name,
                "address": address,
                "pin_code": biz.pin_code,
                "pan": pan,
                "gstin": gstin,
                "entity_type": entity_type,
                "nic_code": random.choice(NIC_CODES[sys]),
                "registration_date": biz.reg_date.isoformat(),
                "is_closed": biz.is_closed,
                "is_dormant": biz.is_dormant,
            })

    return records


# ---------------------------------------------------------------------------
# Activity event generator
# ---------------------------------------------------------------------------

def generate_activity_events(
    source_records: list[dict],
    months: int = 12,
) -> list[dict]:
    today = date.today()
    start = date(today.year - 1, today.month, 1)
    events = []

    # Group records by biz_id to get per-business ground-truth status
    biz_meta: dict[str, dict] = {}
    for rec in source_records:
        biz_id = rec["biz_id"]
        if biz_id not in biz_meta:
            biz_meta[biz_id] = {
                "is_closed": rec["is_closed"],
                "is_dormant": rec["is_dormant"],
                "records": [],
            }
        biz_meta[biz_id]["records"].append(rec)

    for biz_id, meta in biz_meta.items():
        recs = meta["records"]
        is_closed = meta["is_closed"]
        is_dormant = meta["is_dormant"]

        for rec in recs:
            sys = rec["source_system"]
            rec_id = rec["source_record_id"]

            if is_closed:
                # Emit a closure signal early in the period, then nothing
                closure_events = [e for e in EVENT_TYPES[5] if e[0] == sys]
                if closure_events:
                    _, evt_type = random.choice(closure_events)
                    evt_date = start + timedelta(days=random.randint(0, 90))
                    events.append({
                        "source_system": sys,
                        "source_record_id": rec_id,
                        "event_type": evt_type,
                        "event_date": evt_date.isoformat(),
                        "signal_category": 5,
                        "event_data": {"outcome": "closed"},
                    })
                continue

            if is_dormant:
                # 1–2 weak signals, nothing recent
                weak_candidates = [e for e in EVENT_TYPES[3] if e[0] == sys]
                if weak_candidates:
                    _, evt_type = random.choice(weak_candidates)
                    evt_date = start + timedelta(days=random.randint(0, 60))
                    events.append({
                        "source_system": sys,
                        "source_record_id": rec_id,
                        "event_type": evt_type,
                        "event_date": evt_date.isoformat(),
                        "signal_category": 3,
                        "event_data": {"note": "last recorded contact"},
                    })
                continue

            # Active business — generate a mix of events
            # At least one Category 1 signal (renewal)
            cat1 = [e for e in EVENT_TYPES[1] if e[0] == sys]
            if cat1:
                _, evt_type = random.choice(cat1)
                days_ago = random.randint(10, 300)
                evt_date = today - timedelta(days=days_ago)
                events.append({
                    "source_system": sys,
                    "source_record_id": rec_id,
                    "event_type": evt_type,
                    "event_date": evt_date.isoformat(),
                    "signal_category": 1,
                    "event_data": {"outcome": "renewed", "validity_years": 1},
                })

            # Random Cat 2 events
            cat2 = [e for e in EVENT_TYPES[2] if e[0] in (sys, "bescom", "bwssb")]
            for _ in range(random.randint(1, 4)):
                if cat2:
                    _, evt_type = random.choice(cat2)
                    days_ago = random.randint(5, 360)
                    evt_date = today - timedelta(days=days_ago)
                    extra = {}
                    if "consumption" in evt_type:
                        extra["kwh"] = round(random.uniform(200, 5000), 1)
                    elif "inspection" in evt_type:
                        extra["outcome"] = random.choice(
                            ["Business operating", "Business operating", "Minor violation noted"]
                        )
                    events.append({
                        "source_system": sys,
                        "source_record_id": rec_id,
                        "event_type": evt_type,
                        "event_date": evt_date.isoformat(),
                        "signal_category": 2,
                        "event_data": extra,
                    })

    # Add ~5% unmatched events (events for unknown record IDs)
    for _ in range(len(events) // 20):
        sys = random.choice(["factories", "shops", "labour", "kspcb"])
        events.append({
            "source_system": sys,
            "source_record_id": f"{sys.upper()[:3]}-UNKNOWN-{random.randint(9000, 9999)}",
            "event_type": random.choice(EVENT_TYPES[2])[1],
            "event_date": (today - timedelta(days=random.randint(1, 200))).isoformat(),
            "signal_category": 2,
            "event_data": {"note": "unmatched event for demo"},
        })

    return events


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def generate_all(n_businesses: int = 120) -> tuple[list[dict], list[dict]]:
    businesses = generate_businesses(n_businesses)
    source_records = generate_source_records(businesses)
    activity_events = generate_activity_events(source_records)
    return source_records, activity_events
