# UBID Platform — Unified Business Identifier

**Karnataka Commerce & Industry · Hackathon Round 1 & 2**

A system that links business records across 40+ Karnataka State department systems, assigns each real-world business a single Unified Business Identifier (UBID), and classifies every business as **Active**, **Dormant**, or **Closed** from a stream of activity events — all without modifying any source system.

---

## The Problem

Karnataka runs 40+ independent regulatory systems (Shop Establishment, Factories, Labour, KSPCB, BESCOM, BWSSB, and more). Each was built in isolation. The same business exists as different records in different databases with no shared key. As a result:

- There is no reliable way to join data across departments.
- It is impossible to answer basic questions like *"how many factories in PIN 560058 have had no inspection in 18 months?"*
- Activity data — inspections, renewals, consumption — cannot be aggregated per business.

UBID solves both problems without touching any source system.

---

## How It Works

### Part A — Entity Resolution (UBID Assignment)

```
Source Systems  →  Standardise  →  Block  →  Score  →  Threshold
(read-only)         name/address    6 passes   log-odds   AUTO_LINK
                    PAN/GSTIN                  per pair   REVIEW
                                                          REJECT
                                                  ↓
                                          Union-Find cluster
                                                  ↓
                                          UBID assigned
```

1. **Standardise** — business names (abbreviation expansion, Double Metaphone phonetics), addresses (door/street/locality decomposition), PAN and GSTIN validation.
2. **Block** — 6 overlapping blocking passes (exact PAN, exact GSTIN, PIN+phonetic name, PIN+door number, phonetic+entity type, name prefix+PIN) reduce the O(n²) comparison space to manageable candidate pairs.
3. **Score** — each pair receives a log-odds score across 15 interpretable features (Jaro-Winkler name similarity, Jaccard token overlap, phonetic key match, PAN/GSTIN cross-match, door number match, entity type consistency).
4. **Threshold** — three zones: **AUTO_LINK** (≥0.92), **REVIEW** (0.65–0.92), **REJECT** (<0.65). Hard override rules catch near-certain cases (exact PAN + same PIN → always auto-link; PAN mismatch when both valid → always reject).
5. **Cluster** — auto-linked pairs are connected via Union-Find. Each cluster gets one UBID, anchored to PAN or GSTIN where available.

### Part B — Activity Status Inference

Every incoming event is classified by a 5-category signal taxonomy:

| Category | Signal Strength | Examples |
|---|---|---|
| 1 | Strong Active | Licence renewal, annual return filed |
| 2 | Moderate Active | Inspection visit, electricity consumption |
| 3 | Weak Active | Notice acknowledged |
| 4 | Dormancy | Missed renewal, premises not found |
| 5 | Closure | Licence surrender, deregistration |

Each UBID is classified as **ACTIVE** / **DORMANT** / **CLOSED** / **UNCLASSIFIED** with a structured evidence timeline. Events that cannot be joined to a UBID surface in a review queue — never silently dropped.

---

## Architecture

```
┌─ Source Systems (read-only) ──────────────────────────┐
│  Factories │ Shops │ Labour │ KSPCB │ BESCOM │ BWSSB  │
└────────────────────┬──────────────────────────────────┘
                     │ read replicas / exports
                     ▼
         ┌─ Ingestion + Standardisation ─┐
         └──────────────┬────────────────┘
                        ▼
         ┌─ Entity Resolution Engine ────┐
         │  Blocking → Features → Score  │
         │  Auto-link │ Review │ Reject  │
         └──────────────┬────────────────┘
                        ▼
              ┌─ UBID Registry ──┐
              │  Append-only     │
              │  Provenance log  │
              └────────┬─────────┘
                       │
         ┌─────────────┴─────────────┐
         ▼                           ▼
┌─ Event Stream ──────┐   ┌─ Activity Classifier ─┐
│  Join events→UBIDs  │   │  ACTIVE/DORMANT/CLOSED │
│  Unmatched → queue  │   │  Evidence timeline      │
└─────────────────────┘   └────────────────────────┘
                       │
              ┌────────┴────────┐
              ▼                 ▼
       ┌─ REST API ──┐   ┌─ Reviewer UI ──────────┐
       │  FastAPI    │   │  Dashboard              │
       │  /docs      │   │  UBID Lookup            │
       └─────────────┘   │  Review Queue           │
                         │  Analytics Query Runner  │
                         └────────────────────────┘
```

---

## Project Structure

```
UBID/
├── src/
│   ├── config.py                      # Settings from environment
│   ├── database/
│   │   ├── models.py                  # All SQLAlchemy ORM models
│   │   └── session.py                 # DB engine + session factory
│   ├── ingestion/
│   │   └── standardiser.py            # Name, address, identifier normalisation
│   ├── entity_resolution/
│   │   ├── blocker.py                 # 6 blocking passes
│   │   ├── features.py                # Log-odds feature vector computation
│   │   ├── classifier.py              # Zone assignment + override rules
│   │   └── pipeline.py               # Orchestration + Union-Find clustering
│   ├── registry/
│   │   └── ubid_registry.py           # UBID lifecycle: create, merge, un-merge, lookup
│   ├── activity/
│   │   ├── signals.py                 # 5-category signal taxonomy
│   │   └── classifier.py             # Status inference + evidence timeline
│   └── api/
│       ├── main.py                    # FastAPI app entry point
│       ├── schemas.py                 # Pydantic request/response models
│       └── routers/
│           ├── ubid.py                # UBID lookup, list, un-merge, anchor update
│           ├── review.py              # Review queue: get, decide, escalate
│           └── analytics.py           # Dashboard, active-without-inspection, heatmap
├── synthetic_data/
│   └── generator.py                   # Generates realistic Karnataka business data
├── scripts/
│   └── run_pipeline.py               # End-to-end CLI runner
├── ui/
│   └── index.html                    # Single-page reviewer UI (no build step)
├── docker-compose.yml
├── requirements.txt
├── Makefile
└── UBID_Solution_Document.md         # Full Round 1 written proposal
```

---

## Quick Start

### Requirements

- Python 3.9+
- pip

### Run locally (SQLite — no Docker needed)

```bash
# 1. Clone and install
git clone <repo-url>
cd UBID
pip install -r requirements.txt

# 2. Generate data, run entity resolution, classify activity
python scripts/run_pipeline.py

# 3. Start the API + UI
uvicorn src.api.main:app --reload --port 8000
```

Open **http://localhost:8000** for the Reviewer UI.
Open **http://localhost:8000/docs** for the interactive API docs.

### Run with PostgreSQL (Docker)

```bash
docker compose up -d db
DATABASE_URL=postgresql://ubid:ubid_secret@localhost:5432/ubid_db python scripts/run_pipeline.py
docker compose up api
```

### Makefile shortcuts

```bash
make install        # pip install -r requirements.txt
make run-pipeline   # generate data + entity resolution + activity classification
make serve          # start API server on :8000
make demo           # run-pipeline then serve
make reset          # delete DB and re-run pipeline from scratch
```

---

## Configuration

Copy `.env.example` to `.env` and edit as needed:

```env
DATABASE_URL=sqlite:///./ubid.db        # or postgresql://...
T_HIGH=0.92                             # auto-link threshold
T_LOW=0.65                              # reject threshold
OBSERVATION_WINDOW_MONTHS=12            # window for ACTIVE classification
DORMANT_TO_CLOSED_MONTHS=36             # inactivity before implied CLOSED
```

---

## API Endpoints

| Method | Path | Description |
|---|---|---|
| `GET` | `/api/v1/ubid/lookup` | Lookup by UBID, PAN, GSTIN, name, or source record ID |
| `GET` | `/api/v1/ubid/{ubid}` | Full UBID detail with source records, provenance, activity status |
| `GET` | `/api/v1/ubid/` | List UBIDs with optional status/PIN/anchor filters |
| `POST` | `/api/v1/ubid/{ubid}/unmerge` | Split a source record out of a cluster |
| `POST` | `/api/v1/ubid/{ubid}/anchor` | Add or update PAN/GSTIN anchor |
| `GET` | `/api/v1/review/queue` | Paginated review queue (PENDING / REVIEWED / AUTO_LINKED) |
| `GET` | `/api/v1/review/queue/count` | Queue counts by status |
| `GET` | `/api/v1/review/queue/{id}` | Single candidate with both source records |
| `POST` | `/api/v1/review/queue/{id}/decision` | Submit CONFIRM_MATCH / CONFIRM_NON_MATCH / DEFER / ESCALATE |
| `GET` | `/api/v1/analytics/dashboard` | Summary stats |
| `GET` | `/api/v1/analytics/active-without-inspection` | The canonical cross-system query |
| `GET` | `/api/v1/analytics/unmatched-events` | Events that could not be joined to a UBID |
| `GET` | `/api/v1/analytics/status-summary` | PIN × Status breakdown |

### Example: the canonical query

```
GET /api/v1/analytics/active-without-inspection
    ?pin_code=560058
    &source_system=factories
    &months=18
```

Returns active factories in PIN 560058 that have had no inspection in the last 18 months — a query impossible without the UBID layer.

---

## Reviewer UI

The single-page UI (served at `/`) has five views:

| View | What it shows |
|---|---|
| **Dashboard** | Source records, UBIDs, auto-linked pairs, review queue size, status breakdown by PIN |
| **UBID Lookup** | Search by any identifier — returns UBID detail, source records, activity evidence, provenance log |
| **Review Queue** | Side-by-side comparison of candidate records with feature contribution waterfall chart; confirm / reject / defer / escalate |
| **Analytics** | Active-without-inspection query runner + PIN × Status heatmap |
| **Unmatched Events** | Events that arrived but could not be joined to any UBID |

---

## Key Design Decisions

**No source system changes.** All connectors are read-only. The UBID registry is a new system of record layered on top — it never writes back to any source system.

**A wrong merge is worse than a missed one.** The entity resolution defaults to conservative thresholds. Ambiguous cases go to the review queue rather than being silently committed.

**Every decision is explainable and reversible.** Each match carries a feature contribution breakdown (the waterfall chart). Every UBID state change is recorded in an append-only provenance log. Merges can be undone via the un-merge endpoint.

**Unmatched events surface, never sink.** Events that cannot be joined to a UBID are flagged `UNMATCHED_KNOWN` or `UNMATCHED_UNKNOWN` and appear in the unmatched events view.

**No LLM calls on raw PII.** The synthetic data generator produces scrambled names and addresses that preserve linkage structure but contain no real business information.

---

## Pipeline Output (120 synthetic businesses)

```
Source records ingested : 301  (across 4 department systems)
Total UBIDs created     : 140
  PAN-anchored          :  49
  Internal (no anchor)  :  91
Auto-linked pairs       : 231  (no human review needed)
Review queue            :  26  (ambiguous, surfaced for reviewer)
Rejected pairs          : 1347 (correctly kept separate)
Activity — ACTIVE       : 125
Activity — DORMANT      :   3
Activity — CLOSED       :   8
Unmatched events        :  48  (surfaced, not dropped)
```

---

## Non-Negotiables Compliance

| Requirement | How it is met |
|---|---|
| Source systems not modified | Read-only connectors; UBID platform has no write credentials to any source schema |
| Works on scrambled/synthetic data | Synthetic generator preserves linkage structure with no real PII; all pipeline components work identically on scrambled input |
| Every decision explainable | Feature contribution JSON stored with every match; evidence timeline stored with every activity status |
| Every decision reversible | Append-only provenance log; explicit un-merge endpoint; status changes produce new rows, not updates |
| Unmatched events surfaced | `join_status` field on every event; unmatched events appear in dedicated review view |

---

## Tech Stack

| Layer | Technology |
|---|---|
| API | FastAPI + Uvicorn |
| ORM / DB | SQLAlchemy 2 + SQLite (dev) / PostgreSQL (prod) |
| String similarity | jellyfish (Jaro-Winkler, Metaphone) |
| Data processing | pandas, numpy |
| Synthetic data | faker + custom corpus |
| UI | Bootstrap 5 + vanilla JS (no build step) |
| Containerisation | Docker Compose |

---

## Extending for Round 2

- **Swap SQLite → PostgreSQL** by setting `DATABASE_URL` in `.env`.
- **Add a real department connector** by implementing a connector that reads from the department's read replica and calls `standardise_record()` — no other changes needed.
- **Retrain confidence thresholds** by collecting reviewer decisions and running the calibration notebook (to be added in Round 2).
- **Add Kafka event streaming** by replacing the batch event ingest in `run_pipeline.py` with a Kafka consumer that calls `ingest_events()` per message.

---

## Licence

MIT — see `LICENSE` for details.
