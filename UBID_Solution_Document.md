# Unified Business Identifier (UBID) and Active Business Intelligence
## Solution Proposal — Karnataka Commerce & Industry
### Round 1 Submission

---

## Table of Contents

1. [Problem Understanding](#1-problem-understanding)
2. [Entity Resolution — Linking Businesses Across Systems (Part A)](#2-entity-resolution--linking-businesses-across-systems-part-a)
3. [Confidence Calibration and Threshold Design](#3-confidence-calibration-and-threshold-design)
4. [Human-in-the-Loop Review Design](#4-human-in-the-loop-review-design)
5. [Activity Status Inference (Part B)](#5-activity-status-inference-part-b)
6. [Respecting the Non-Negotiables](#6-respecting-the-non-negotiables)
7. [Architecture Overview](#7-architecture-overview)
8. [Technology and Model Choices](#8-technology-and-model-choices)
9. [Risks, Trade-offs, and Mitigations](#9-risks-trade-offs-and-mitigations)
10. [Round 2 Implementation Plan](#10-round-2-implementation-plan)

---

## 1. Problem Understanding

### The Core Problem in Plain Terms

Karnataka runs 40+ independent regulatory systems. Each was built to serve a single department's workflow, not to describe a shared reality — the business. A factory operating in Peenya Industrial Area exists as five separate database rows in five separate systems, each row with a slightly different name ("Peenya Tools Pvt Ltd", "Peenya Tools Private Limited", "Peenya Tools"), a different primary key, and no shared foreign key. Nothing in the current infrastructure makes those rows point at the same entity.

The consequence is not just a reporting inconvenience. It means Karnataka C&I cannot answer operationally important questions: Is a business that holds a Factories licence actually operating? Has a shop-establishment registrant renewed its trade licence? Are there businesses in a flood-risk PIN code that have never been inspected? These are questions regulators need answered, and they cannot be answered today.

### What Makes This Hard

**The data quality problem is structural, not incidental.** Business names in Indian regulatory data are stored in ways that reflect decades of offline form-filling, manual entry, transliteration inconsistency (Kannada to English), common abbreviation patterns (Pvt/Private, Ltd/Limited, & vs and), and outright typos. Addresses in India lack the structured precision assumed by Western address-matching algorithms — a "door number" may be "14/3B", "14-3B", or "14 3B" across three systems, and the locality name may appear in Kannada or English depending on the officer who entered it.

**PAN and GSTIN exist but are not reliable join keys today.** They are present in some systems, absent in others, and where present they may have been entered with transcription errors. They are anchor points, not silver bullets.

**The system cannot be modified.** Source systems are in production, operated by separate departments, and typically governed by separate IT contracts. Any solution must read from them, never write to them, and survive schema changes in them.

**A wrong merge is worse than a missed one.** If the system incorrectly links two businesses and one of them has compliance violations, the clean business inherits the dirty record. This destroys trust in the UBID platform irreversibly. The design must be conservative: when in doubt, separate and surface for review.

### What Success Actually Looks Like (Operationally)

A single lookup by Shop Establishment number, PAN, GSTIN, or even a fuzzy name+address+PIN combination returns one UBID, with the evidence that supports it. That UBID has a status — Active, Dormant, or Closed — and a timeline of events that justify the status. Ambiguous matches are not hidden; they appear in a queue where a human makes the call, and that decision improves the system's future behaviour. Karnataka C&I can run the "active factories with no inspection in 18 months" query and get a real, evidence-backed answer.

---

## 2. Entity Resolution — Linking Businesses Across Systems (Part A)

### 2.1 High-Level Pipeline

Entity resolution (also called record linkage or deduplication) is a well-studied problem in data science. The approach here adapts the Fellegi-Sunter probabilistic model — extended with supervised learning and rule-based anchoring — to the specific characteristics of Indian regulatory data.

The pipeline has five stages:

```
Stage 1: Ingest & Standardise
Stage 2: Block (reduce the comparison space)
Stage 3: Compare (generate pairwise feature vectors)
Stage 4: Classify (score and threshold)
Stage 5: Cluster & Assign UBID
```

Each stage is described below.

### 2.2 Stage 1 — Ingest and Standardise

A read-only connector extracts master data from each source system via an agreed mechanism (database replica, API, file export). No writes to source systems at any point.

Raw records are landed in an immutable raw zone in the data lake. From there, a standardisation layer applies the following transformations to produce a canonical representation:

**Business Name Normalisation**

- Convert to uppercase.
- Expand known abbreviations to canonical forms: `PVT` → `PRIVATE`, `LTD` → `LIMITED`, `MFG` → `MANUFACTURING`, `ENGG` → `ENGINEERING`.
- Remove legal suffixes for comparison purposes (`PRIVATE LIMITED`, `LLP`, `PROPRIETORSHIP`) but store them separately as entity type signals.
- Remove punctuation (`.`, `,`, `&`, `-`, `/`).
- Replace `AND` with `&` and normalise to a canonical token.
- Apply phonetic encoding (Double Metaphone) to handle transliteration variants — "Srinivasa" and "Shrinivasa" map to the same phonetic key.
- Produce both a cleaned string and a phonetic token set for downstream comparison.

**Address Normalisation**

- Separate the address into sub-fields: door/plot number, street/road name, locality/area, PIN code, district.
- Map PIN codes to canonical village/town/city using the India Post PIN database — this fixes the common case where a business in "560058" appears as "Rajajinagar" in one system and "West Rajajinagar" in another.
- Expand known locality abbreviations: `Ind Area` → `Industrial Area`, `Nagar` variants normalised.
- Street numbers: strip whitespace and punctuation variation from door numbers for comparison.

**Identifier Cleaning**

- PAN: validate against the 10-character alphanum pattern `[A-Z]{5}[0-9]{4}[A-Z]`. Flag and quarantine invalid entries rather than propagating them.
- GSTIN: validate the 15-character pattern and cross-check the embedded PAN (characters 3–12 of a GSTIN are the PAN). This alone catches a large category of entry errors.
- CIN, LLPIN: validate format where known.

**Intra-Department Deduplication**

Before cross-system linking, deduplicate within each department. It is common for a business to appear twice in the same department's system (lapsed registration renewed under a new record ID). The same entity resolution pipeline runs intra-system first, with a tighter threshold, and the surviving canonical records proceed to cross-system linkage.

### 2.3 Stage 2 — Blocking

Blocking is the technique that makes entity resolution computationally tractable. Without blocking, every pair of records across all systems must be compared — at 100,000 records per system with 4 systems, that is 40 billion pairs. Blocking creates "candidate buckets" where comparison is restricted to records that share at least one blocking key.

Multiple overlapping blocking passes are used to maximise recall (avoid missed true matches) while controlling precision:

| Blocking Pass | Key | Rationale |
|---|---|---|
| B1 | Exact PAN | Highest-confidence anchor |
| B2 | Exact GSTIN | Highest-confidence anchor |
| B3 | PIN code + first 6 chars of phonetic name | Same area, similar name |
| B4 | PIN code + normalised door number | Same plot, same PIN |
| B5 | Double Metaphone name token + entity type | Same phonetic name, same entity type |
| B6 | Phone number (where present) | Same contact |

A pair that appears in any blocking pass proceeds to Stage 3. The union of all blocking passes is the candidate set.

### 2.4 Stage 3 — Feature Vector Construction

For each candidate pair, a feature vector is computed. Features are designed to be interpretable — each feature maps to a human-understandable signal about similarity.

**Name Features**
- Jaro-Winkler similarity of cleaned business names (0–1).
- Jaccard similarity of name token sets (handles word order variation: "Karnataka Pipes Industries" vs "Industries Karnataka Pipes").
- Exact match on phonetic key (boolean).
- Longest Common Subsequence ratio.

**Address Features**
- Exact PIN code match (boolean).
- Jaro-Winkler on normalised street/road field.
- Exact door/plot number match (boolean).
- Canonical locality match after PIN-to-locality mapping (boolean).

**Identifier Features**
- Exact PAN match (boolean, high weight).
- Exact GSTIN match (boolean, high weight).
- GSTIN-embedded PAN cross-match: GSTIN present in one record, PAN present in other — if the PAN embedded in the GSTIN matches, treat as near-certain match.
- Levenshtein distance on PAN (catches single-character transcription errors).

**Contextual Features**
- Entity type consistency (sole proprietor vs private limited — mismatches are a negative signal).
- Industry/sector consistency (NIC code where available).
- Registration date proximity (two records registered within 30 days of each other and otherwise similar is a positive signal).

### 2.5 Stage 4 — Classification

**The Model**

The primary classifier is a probabilistic model trained using the Splink framework (open-source, Python, based on the Fellegi-Sunter/EM algorithm). Splink learns the probability that a pair with a given feature vector is a true match without requiring labelled training data — it uses the EM algorithm to estimate match and non-match probability distributions from the data itself.

For bootstrapping and calibration, a small set of manually verified ground-truth pairs is collected during system setup (approximately 200–500 pairs across the blocking strata). This seed set trains a supervised calibration layer on top of the probabilistic scores, ensuring that the output score is a true posterior probability (P(match | features)), not just a relative ranking.

**Why Splink specifically:**
- Purpose-built for this exact problem class (large-scale probabilistic record linkage).
- Explainability is first-class: it produces a waterfall chart showing how each feature contributed to the final score.
- Scales to tens of millions of record pairs via Apache Spark integration.
- Open-source, no vendor lock-in, runs entirely on-premises.

**Score Output**

Each candidate pair receives a match probability between 0 and 1, along with a feature-level breakdown. This breakdown is the explainability artefact: it shows, for a given pair, that "the PAN matched exactly (+40 points), the name similarity was 0.92 (+20 points), but the PIN codes differ (-15 points), net score: 0.78".

### 2.6 Stage 5 — Clustering and UBID Assignment

Once pairs are scored and thresholded (see Section 3), the auto-linked pairs are passed to a connected components clustering step. If Record A matches Record B, and Record B matches Record C, then A, B, and C all refer to the same business — they form one cluster, one UBID.

The clustering algorithm is a simple union-find (disjoint set) over auto-confirmed matches. Each resulting cluster is assigned a UBID.

**UBID Format**

```
UBID-KA-{TYPE}-{ANCHOR}-{SEQUENCE}

Where:
  TYPE     = PAN | GST | INT  (anchor type)
  ANCHOR   = first 10 chars of PAN, or first 15 of GSTIN, or empty for internal
  SEQUENCE = 8-char zero-padded sequential integer

Examples:
  UBID-KA-PAN-AAACB1234K-00000001   (PAN-anchored)
  UBID-KA-GST-29AAACB1234K1ZX-00000002  (GSTIN-anchored)
  UBID-KA-INT-----------00000003   (internal, no central anchor yet)
```

When a reviewer later confirms that an INT-typed UBID corresponds to a known PAN or GSTIN, the UBID is re-anchored. The old UBID is retained as an alias — lookups by old UBID always resolve to the current canonical UBID.

**UBID Registry**

The UBID registry is an append-only ledger. Every state change — creation, merge, anchor update, status change — is a new row with a timestamp and the actor (system or human reviewer ID). There is no UPDATE or DELETE in the registry; history is complete and auditable.

---

## 3. Confidence Calibration and Threshold Design

### 3.1 Three-Zone Threshold Model

The output probability from Stage 4 is mapped to one of three zones:

```
Score ≥ T_HIGH  → Auto-Link (committed without human review)
T_LOW ≤ Score < T_HIGH → Review Queue (routed to human reviewer)
Score < T_LOW  → Reject (records remain separate)
```

**Initial Threshold Values**

| Threshold | Value | Rationale |
|---|---|---|
| T_HIGH | 0.92 | False positive rate < 1% at this level given typical data quality |
| T_LOW | 0.65 | Below this, expected precision drops below 50% — more harm than benefit |

These are starting values. They must be calibrated against the actual dataset during Round 2 using precision-recall curves derived from the ground-truth seed set.

**Special-Case Rules (Override the Probabilistic Model)**

Some feature combinations are so definitive that they bypass the probabilistic model:

- **Exact PAN match + same PIN code**: Auto-link regardless of score (PAN is a legal identifier; two records with the same PAN in the same geography are almost certainly the same business).
- **Exact GSTIN match**: Auto-link (GSTIN is unique by law).
- **PAN in one record matches the PAN embedded in the GSTIN of the other**: Auto-link.
- **Entity type mismatch (Proprietorship vs Private Limited) + no PAN/GSTIN match**: Hard block — never auto-link even if name/address similarity is high.

### 3.2 Calibration Methodology

Calibration is the process of ensuring that a model that outputs "0.80" is correct approximately 80% of the time — not just that 0.80 is "more likely than 0.70". This is essential for the thresholds to have operational meaning.

The calibration process:

1. Extract 500–1000 candidate pairs spanning the full score range (sampled to be uniform over score deciles).
2. Have two independent reviewers label each pair as Match / Non-Match.
3. Plot the reliability diagram (mean predicted probability vs observed match rate per bin).
4. Apply Platt scaling or isotonic regression to correct systematic over- or under-confidence.
5. Re-evaluate T_HIGH and T_LOW on the calibrated scores: set T_HIGH such that false positive rate ≤ 1%, T_LOW such that precision ≥ 50%.

Calibration is repeated every 90 days or after 1,000 new reviewer decisions, whichever comes first.

### 3.3 Confidence Score Decomposition

Every match decision stores a structured evidence object alongside the score:

```json
{
  "ubid_a": "UBID-KA-INT-----------00000041",
  "ubid_b": null,
  "source_record_a": { "system": "factories", "id": "FAC-BLR-00234" },
  "source_record_b": { "system": "kspcb", "id": "KSPCB-2019-00891" },
  "match_probability": 0.87,
  "zone": "REVIEW",
  "feature_contributions": {
    "pan_exact_match": { "value": false, "weight": 0.00 },
    "gstin_exact_match": { "value": false, "weight": 0.00 },
    "name_jaro_winkler": { "value": 0.94, "weight": 0.31 },
    "name_token_jaccard": { "value": 0.83, "weight": 0.18 },
    "pin_exact_match": { "value": true, "weight": 0.22 },
    "door_number_match": { "value": true, "weight": 0.16 },
    "entity_type_match": { "value": true, "weight": 0.00 }
  },
  "blocking_pass": "B3",
  "created_at": "2025-06-01T14:23:00Z"
}
```

This object is stored permanently and is the input the reviewer sees. It is also the artefact that satisfies explainability and auditability requirements.

---

## 4. Human-in-the-Loop Review Design

### 4.1 Design Principles

The review workflow is not a fallback for when the algorithm fails. It is a first-class part of the system. The design goals are:

1. **Make the right decision easy.** Show the reviewer exactly the evidence that matters — not raw database records.
2. **Make the decision reversible.** A reviewer who merges two records must be able to un-merge them later.
3. **Capture the decision, not just the outcome.** A reviewer who says "same business, different branch" is giving different information than one who says "same business, this is just a typo". Both teach the system different things.
4. **Do not overload reviewers.** The queue should be prioritised by impact — a high-activity business with an ambiguous linkage matters more than a dormant micro-enterprise.

### 4.2 Review Queue Structure

Each item in the review queue contains:

- The two (or more) candidate records displayed side-by-side, with source system and record ID.
- The feature contribution waterfall chart (which features drove the score up or down).
- Any PAN/GSTIN present in either record, flagged if they differ.
- The business's event history (if any events are already associated with either record).
- A priority score: P_priority = match_probability × (1 - match_probability) × activity_weight. This surfaces genuinely ambiguous cases that matter most.

The reviewer can take one of four actions:

| Action | Meaning | System Effect |
|---|---|---|
| **Confirm Match** | These are the same business | Merge clusters, assign single UBID, record decision |
| **Confirm Non-Match** | These are different businesses | Permanently separate, add to rejection training set |
| **Defer** | Not enough information to decide | Return to queue, flag for data enrichment |
| **Escalate** | Requires senior review or field verification | Routes to Level 2 queue |

### 4.3 Feedback Loop — How Decisions Improve the Model

Every reviewer decision is a labelled training example. The feedback loop operates on two time scales:

**Short-term (rule updates, near-real-time):**
If a reviewer consistently confirms matches where a specific PAN digit pattern appears, a rule can be added to the identifier validator to catch that pattern as a known transcription error. These rule updates can be proposed automatically and activated by a model administrator.

**Medium-term (model retraining, quarterly):**
After 500+ new labelled pairs accumulate, the calibration layer is retrained and the probabilistic model is re-estimated with the expanded ground truth. Threshold values are re-evaluated against the updated precision-recall curve.

**Long-term (blocking strategy evolution):**
Analysis of Defer and Escalate decisions may reveal that certain classes of businesses (e.g., home-based service businesses) are systematically harder to link because no reliable blocking key exists for them. New blocking passes or additional features can be added to address systematic gaps.

All model updates are versioned. The system records which model version produced each decision, so that a future audit can reconstruct the state of the system at any point in time.

### 4.4 Audit Trail

Every UBID carries a complete provenance chain:

```
UBID-KA-PAN-AAACB1234K-00000001
├── Created: 2025-06-01T14:30:00Z by system v1.2.0
├── Source records: [FAC-BLR-00234 (factories), KSPCB-2019-00891 (kspcb)]
├── Match probability: 0.87 → Reviewed by reviewer_id:R042 on 2025-06-02
│   └── Decision: Confirm Match, reason: "same PAN visible in KSPCB physical file scan"
├── Anchor updated: 2025-06-02T09:00:00Z, PAN added from reviewer input
└── Status: Active (last event: 2025-11-14)
```

---

## 5. Activity Status Inference (Part B)

### 5.1 The Status Model

Every UBID is classified into one of three states:

| Status | Definition |
|---|---|
| **Active** | Has received at least one positive activity signal within the configured observation window (default: 12 months) |
| **Dormant** | Has a registration that has not been formally closed, but no positive activity signal within the observation window |
| **Closed** | Has received an explicit closure signal (licence surrender, deregistration, strike-off) OR has had no activity signal for an extended period (configurable, default: 36 months) AND has a final-demand compliance event |

A fourth operational state, **Unclassified**, applies to UBIDs that have never received any event — typically newly created UBIDs from historical master data with no associated transaction history.

### 5.2 Signal Taxonomy

Not all activity signals are equal. A signal taxonomy defines the strength and category of each event type:

**Category 1 — Strong Active Signals (reset observation window)**
- Successful licence/registration renewal
- Annual return filing (Factories Act, Shops & Establishment)
- KSPCB consent renewal
- GST return filed (if accessible via state portal)
- Compliance certificate issued

**Category 2 — Moderate Active Signals (extend observation window by 6 months)**
- Inspection visited and inspection report filed (business was present)
- Electricity/water consumption above baseline threshold
- Fire NOC renewed
- Food safety licence renewed

**Category 3 — Weak Active Signals (extend window by 3 months, cannot solo classify as Active)**
- Any correspondence or notice acknowledged
- Electricity/water consumption above zero but below threshold

**Category 4 — Dormancy Signals (negative, reduce confidence in Active)**
- Missed renewal (renewal deadline passed with no renewal)
- Inspection visited, business not found at premises
- Returned mail / undeliverable notice

**Category 5 — Closure Signals (trigger Closed classification)**
- Explicit deregistration / licence surrender
- Court order or government order for closure
- Strike-off from ROC records (where CIN is available)
- GSTIN cancellation (where accessible)
- Inspection report explicitly noting "business closed" or "premises vacated"

### 5.3 Classification Engine

The classification engine runs as a scheduled job (daily) and also in near-real-time when a new event arrives for a UBID.

**Algorithm:**

```
For each UBID:
  1. Retrieve all events within the trailing 36-month window, joined to this UBID.
  2. Check for any Category 5 signals → if present, classify CLOSED.
  3. Compute the effective last-active date:
       max(event_date) over events with category IN (1, 2, 3)
  4. If effective last-active date is within observation_window_months → ACTIVE
  5. Else if registration is not formally closed → DORMANT
  6. Else → CLOSED (implied, by inactivity threshold)
```

Configurable parameters (editable without code changes):
- `observation_window_months`: default 12
- `dormant_to_closed_months`: default 36
- `consumption_active_threshold_kwh`: default 100/month
- Signal category weights (for future ML enhancement)

### 5.4 Explainability — The Evidence Timeline

Every status classification is accompanied by a structured evidence object:

```json
{
  "ubid": "UBID-KA-PAN-AAACB1234K-00000001",
  "status": "ACTIVE",
  "status_as_of": "2025-12-01",
  "confidence": "HIGH",
  "observation_window_months": 12,
  "driving_signal": {
    "event_type": "factories_annual_return",
    "source_system": "factories",
    "event_date": "2025-09-15",
    "category": 1,
    "description": "Annual return filed for FY 2024-25"
  },
  "supporting_signals": [
    { "event_type": "bescom_consumption", "event_date": "2025-11-01",
      "value": "1240 kWh", "category": 2 },
    { "event_type": "kspcb_inspection", "event_date": "2025-07-20",
      "outcome": "Business operating", "category": 2 }
  ],
  "negative_signals": [],
  "events_outside_window": 14
}
```

The confidence field reflects signal strength:
- `HIGH`: At least one Category 1 signal within the window.
- `MEDIUM`: Only Category 2 signals within the window.
- `LOW`: Only Category 3 signals within the window.
- `INFERRED`: No signals, status derived by timeout rule.

### 5.5 Handling Events That Cannot Be Joined to a UBID

A critical non-negotiable from the problem statement: events that cannot be joined must surface for review, not be silently dropped.

The event processing pipeline assigns each incoming event a join status:

| Join Status | Condition | Action |
|---|---|---|
| `JOINED` | Event's source record ID maps to exactly one UBID | Apply to UBID, done |
| `AMBIGUOUS` | Source record ID maps to multiple UBIDs (split/ambiguous cluster) | Route to event review queue |
| `UNMATCHED_KNOWN_RECORD` | Record ID exists in master data but has no UBID yet (new record not yet processed) | Trigger incremental entity resolution |
| `UNMATCHED_UNKNOWN` | Record ID does not exist in any ingested master data | Route to data quality queue for investigation |

The event review queue mirrors the UBID review queue in design. Reviewers can manually assign an event to a UBID or flag it for master data investigation.

---

## 6. Respecting the Non-Negotiables

### 6.1 No Source System Changes

This is enforced by architecture, not just by policy.

All connectors operate in read-only mode. The ETL/ingestion layer connects to read replicas of source databases (or consumes exports/APIs provided by each department). The UBID platform has no write credentials to any source system. This is verified at the infrastructure layer: database grants are read-only, and the ingestion service runs under a separate service account with no write privileges to any source schema.

The platform adds a new system of record — the UBID registry — without modifying any existing one.

### 6.2 Working on Scrambled or Synthetic Data

The architecture cleanly separates the data processing pipeline from the model training and calibration loop. The scramblers/synthesisers are applied at the boundary: when data is extracted from source systems for non-production use (development, testing, model training), it passes through a deterministic scrambler before leaving the production perimeter.

The deterministic scrambler:
- Replaces PAN characters with a deterministic substitution cipher (same input always produces the same output, so linkages are preserved).
- Replaces business names with a synthetic name generator seeded by the hash of the original name (phonetic and token structure are preserved).
- Replaces addresses with real addresses drawn from the same PIN code, so geographic signals remain valid.
- Replaces contact details (phone, email) with synthetic equivalents.

Crucially, the linkage structure is preserved: if Record A and Record B were the same business in real data, they remain the same business in scrambled data. The scrambling enables model development and reviewer training on non-sensitive data.

### 6.3 LLM Usage — Compliant Design

LLMs offer genuine value in two areas: address parsing and business name disambiguation. Both are deployed in ways that never process raw PII:

**Permitted LLM Use:**
- **Address parsing**: A local, fine-tuned small language model (running on-premises) parses free-text address strings into structured sub-fields (door number, street, locality, PIN). This runs on scrambled data during training and on production data only after the address has been stripped of personal identifiers (individual names, mobile numbers embedded in address fields).
- **Review assist**: In the reviewer UI, a prompt to a local LLM (running on-premises, never a hosted API) can suggest the most likely reason two records are similar but not identical (e.g., "Address differs by one digit in door number — possible transcription error"). This suggestion is advisory only and is shown after the reviewer's own comparison, not before.

**Not Permitted:**
- Any API call to an external hosted LLM (OpenAI, Google, Anthropic, etc.) with raw record data.
- Any LLM processing of PAN, GSTIN, individual names, mobile numbers, or addresses in production data outside the scrambled pipeline.

### 6.4 Every Decision is Explainable and Reversible

**Explainability** is addressed by the feature contribution waterfall stored with every linkage decision (Section 2.5) and the evidence timeline stored with every status classification (Section 5.4).

**Reversibility** is addressed by:
- Append-only UBID registry (no destructive updates).
- All merges recorded with the model version and reviewer ID that authorised them.
- A formal "un-merge" operation that splits a cluster, re-runs entity resolution on the fragments, and routes the resulting sub-clusters back through the review queue.
- Status classifications are stored as time-stamped events; rewinding to any past state is a query against the event log.

---

## 7. Architecture Overview

### 7.1 Logical Architecture

```
┌──────────────────────────────────────────────────────────────────────────────┐
│  SOURCE SYSTEMS (read-only)                                                  │
│  Shop Establishment │ Factories │ Labour │ KSPCB │ BESCOM │ BWSSB │ others  │
└───────────────────────────────────┬──────────────────────────────────────────┘
                                    │ read replicas / exports / APIs
                                    ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│  INGESTION LAYER                                                             │
│  ┌────────────────────┐  ┌────────────────────┐  ┌─────────────────────┐   │
│  │ Batch connectors   │  │ Streaming connectors│  │ File/SFTP importers │   │
│  │ (nightly extracts) │  │ (Kafka consumers)   │  │ (for legacy systems)│   │
│  └────────────────────┘  └────────────────────┘  └─────────────────────┘   │
│              │                       │                        │              │
│              └───────────────────────┴────────────────────────┘              │
│                                      │                                       │
│                                      ▼                                       │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │  RAW ZONE (immutable, append-only, partitioned by source + date)      │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└────────────────────────────────────────────────────────────────────────────┬─┘
                                                                             │
                                                                             ▼
┌────────────────────────────────────────────────────────────────────────────┐
│  ENTITY RESOLUTION ENGINE                                                  │
│                                                                            │
│  ┌──────────────────┐  ┌──────────────┐  ┌─────────────┐  ┌───────────┐ │
│  │ Standardisation  │→ │   Blocking   │→ │  Comparison │→ │ Classifier│ │
│  │ (name, address,  │  │ (6 passes)   │  │  (feature   │  │ (Splink + │ │
│  │  identifier)     │  │              │  │   vectors)  │  │  rules)   │ │
│  └──────────────────┘  └──────────────┘  └─────────────┘  └─────┬─────┘ │
│                                                                   │       │
│                           ┌───────────────┬───────────────────────┘       │
│                           │               │                               │
│                     Score ≥ T_HIGH   T_LOW ≤ Score < T_HIGH              │
│                           │               │                               │
│                           ▼               ▼                               │
│                    ┌────────────┐  ┌─────────────────┐                   │
│                    │ Auto-Link  │  │  REVIEW QUEUE   │                   │
│                    └─────┬──────┘  └────────┬────────┘                   │
│                          │                  │ reviewer decisions          │
│                          └──────────┬───────┘                            │
│                                     ▼                                     │
│                          ┌─────────────────────┐                         │
│                          │  Clustering Engine  │                         │
│                          │ (Union-Find, UBID   │                         │
│                          │  assignment)        │                         │
│                          └──────────┬──────────┘                         │
└─────────────────────────────────────┼──────────────────────────────────┘
                                      │
                                      ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  UBID REGISTRY (append-only ledger, PostgreSQL + TimescaleDB)            │
│  UBID table │ Source record mapping │ Provenance log │ Alias table        │
└──────────────────────────────────────────────────────────────────────────┘
                       ▲                           ▲
                       │                           │
         ┌─────────────┘                           └──────────────┐
         │                                                         │
┌────────┴────────────────────────┐     ┌───────────────────────────────┐
│  EVENT STREAM PROCESSOR         │     │  ACTIVITY CLASSIFIER          │
│  (Apache Kafka + Flink)         │     │  (scheduled job + streaming   │
│                                 │     │   trigger, rule engine)       │
│  ┌─────────┐  ┌──────────────┐  │     │                               │
│  │ Ingest  │  │  Join to     │  │     │  Active / Dormant / Closed    │
│  │ events  │→ │  UBID        │→─┼─────│  with evidence timeline       │
│  └─────────┘  └──────────────┘  │     └───────────────────────────────┘
│                    │             │
│             UNMATCHED events     │
│                    ↓             │
│             Event Review Queue   │
└─────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  QUERY & ANALYTICS LAYER                                                 │
│  ┌───────────────────┐  ┌──────────────────┐  ┌──────────────────────┐ │
│  │  UBID Lookup API  │  │  BI Dashboard    │  │  Bulk Query Engine   │ │
│  │  (REST + GraphQL) │  │  (Metabase/      │  │  (SQL over UBID      │ │
│  │                   │  │   Superset)      │  │   registry)          │ │
│  └───────────────────┘  └──────────────────┘  └──────────────────────┘ │
└──────────────────────────────────────────────────────────────────────────┘
                       │
                       ▼
┌──────────────────────────────────────────────────────────────────────────┐
│  REVIEWER UI (React SPA + FastAPI backend)                               │
│  Match Review Queue │ Event Review Queue │ UBID Detail View             │
│  Model Admin Panel  │ Threshold Config   │ Audit Log Viewer             │
└──────────────────────────────────────────────────────────────────────────┘
```

### 7.2 Data Flow — Nightly Batch (Master Data)

1. Batch connector extracts delta of new/updated master records from each source system since the last run.
2. Records land in the raw zone with a source-system tag and a run timestamp.
3. Standardisation job runs: normalised records written to the standardised zone.
4. Incremental blocking: new records are compared against all existing records in the same blocking buckets.
5. New candidate pairs are scored by the classifier.
6. Auto-link pairs trigger cluster updates and UBID assignments.
7. Review-queue pairs are inserted into the review queue with priority scores.
8. The UBID registry is updated. New UBIDs are created for clusters with no prior UBID.

### 7.3 Data Flow — Event Stream (Activity Data)

1. Department systems emit events (inspections, renewals, filings) to a Kafka topic (one topic per department, or a unified topic with department tag).
2. The Flink stream processor consumes each event, looks up the source record ID in the UBID registry mapping table.
3. If joined: event is written to the UBID event log, activity classifier is triggered.
4. If unmatched: event is written to the unmatched event queue for review.
5. Activity classifier updates the UBID's status and evidence timeline.

---

## 8. Technology and Model Choices

### 8.1 Core Stack

| Component | Technology | Reason |
|---|---|---|
| **Entity Resolution** | Splink (Python) | Purpose-built for probabilistic record linkage; explainability built-in; Spark-native; open-source |
| **Data Processing** | Apache Spark (PySpark) | Scales to 10M+ record pairs; integrates with Splink; familiar in data engineering |
| **Event Streaming** | Apache Kafka + Apache Flink | Kafka for durable, partitioned event ingestion; Flink for stateful stream processing (UBID join state) |
| **UBID Registry** | PostgreSQL + TimescaleDB | PostgreSQL for relational integrity and audit log; TimescaleDB for time-series event data |
| **Data Lake** | Apache Iceberg on object storage | Table format with time-travel (point-in-time queries); schema evolution; works on-premises |
| **Data Transformation** | dbt | SQL-based transformations, lineage tracking, documentation |
| **Data Quality** | Great Expectations | Automated schema and distribution checks on ingested data |
| **API Layer** | FastAPI (Python) | Async, high-performance; OpenAPI spec auto-generated for documentation |
| **Reviewer UI** | React + TypeScript | Mature ecosystem; component libraries for complex data views |
| **BI / Analytics** | Apache Superset | Open-source; SQL-based; supports Karnataka's SQL query use cases |
| **Orchestration** | Apache Airflow | DAG-based; proven for complex multi-step batch pipelines |
| **Infrastructure** | Kubernetes on on-premises cloud | Portable; no vendor lock-in; fits Karnataka's data residency requirements |

### 8.2 Why Not a Big-Bang Approach (MDM / Commercial)

Commercial Master Data Management (MDM) platforms (Informatica, Reltio, Profisee) are designed for organisations that can mandate a single golden record. Karnataka's constraint — that source systems cannot be changed — makes traditional MDM inappropriate. MDM typically requires source systems to write to the MDM system and consume the golden record back. That is explicitly out of scope.

The proposed approach is a **virtual MDM**: the UBID registry provides the golden linkage without touching source systems. This is technically more complex to build but is the only approach that respects the non-negotiables.

### 8.3 Why Not a Graph Database for Entity Resolution

Graph databases (Neo4j, Amazon Neptune) are sometimes proposed for entity resolution. They are excellent for *storing* the cluster relationships once computed. However, the computational bottleneck in entity resolution is the pairwise comparison step — which is fundamentally a Cartesian product problem that benefits from columnar processing (Spark) rather than graph traversal. The cluster representation in the UBID registry does use graph-like relationships (many source records → one UBID), but the computation is done in Spark, not a graph engine.

### 8.4 Name Matching Specifics for Indian Data

Standard Western string similarity metrics (Levenshtein, Jaro-Winkler) perform adequately on English text but underperform on Indian transliterated names for two reasons: (a) transliteration is not standardised, and (b) the information is in the consonant skeleton, not the vowels. The following additions are made:

- **IndicNLP / custom phonetic encoder**: Maps Kannada transliterations to a consonant-skeleton representation that is invariant to common vowel-placement variation.
- **Gazetteer of known business name patterns**: A small lookup table of common Karnataka business name patterns (area names, industry suffixes, family name variants) that appear frequently in regulatory data, used to improve blocking key quality.

---

## 9. Risks, Trade-offs, and Mitigations

### 9.1 Technical Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| **Source system schema changes break ingestion** | High (40+ systems) | Medium (data gap until fixed) | Schema-aware connectors with automated schema-drift detection; alerting on unexpected column changes; each connector versioned independently |
| **PAN/GSTIN not present in majority of records, making anchor-based linking insufficient** | High | High (more records in review queue, lower auto-link rate) | Invest in blocking and phonetic matching quality; accept higher review volume in early phases; prioritise anchor capture in onboarding |
| **Blocking misses true matches (false negatives in blocking)** | Medium | High (permanent miss — two records never compared) | Multiple overlapping blocking passes; recall-focused blocking (designed to over-include); periodic "oracle" sampling to estimate blocking recall |
| **Model drift as data quality improves over time** | Medium | Medium (thresholds become miscalibrated) | Quarterly re-calibration; monitoring of review queue score distribution for shifts |
| **Review queue volume overwhelms available reviewers** | High (in early phases) | High (bottleneck blocks UBID creation) | Auto-link threshold starts conservative, then loosened as calibration improves; reviewer queue management (SLAs, routing, bulk-approval for high-confidence-but-above-threshold clusters) |
| **Kafka/Flink event stream lag during peak department filing periods** | Medium | Low (eventual consistency — lag is acceptable) | Backpressure handling in Flink; status classifications marked with lag indicator if event processing is >24h behind |

### 9.2 Organisational Risks

| Risk | Mitigation |
|---|---|
| **Departments do not provide read access to source systems** | Engage C&I as the mandate-holder; design connectors to work with file exports as a fallback (many departments already export to MIS portals) |
| **Departments dispute UBID assignments for their businesses** | Reviewable, reversible decisions; department-facing portal to flag disputes; dispute resolution workflow in the reviewer UI |
| **Reviewer quality varies; inconsistent decisions degrade the model** | Inter-reviewer agreement metrics; calibration sessions for reviewers; "honeypot" known pairs inserted into review queue to measure reviewer accuracy |

### 9.3 Key Trade-offs

**Precision vs. Recall in Auto-Linking**

Setting T_HIGH conservatively (say, 0.95) means fewer auto-links and more review queue volume, but fewer wrong merges. Setting T_HIGH aggressively (say, 0.85) reduces review burden but increases the risk of wrong merges. The recommendation is to start conservative (T_HIGH = 0.92) and loosen only after calibration demonstrates safety. *A wrong merge is more costly than a missed one* — as the problem statement says.

**Streaming vs. Batch for Activity Events**

Streaming (Kafka/Flink) provides near-real-time status updates but is operationally more complex than a nightly batch job. Given that the use case is regulatory intelligence (not real-time alerts), a hybrid approach is appropriate: near-real-time for high-signal events (licence surrender, explicit closure), nightly batch for low-signal events (consumption data, routine inspections). This reduces operational complexity without sacrificing meaningful timeliness.

**Central Identifiers as Anchors vs. as Evidence**

When a PAN is present and valid in two records, the system auto-links. But PAN data in government systems is not always correct — a PAN may have been entered incorrectly and never corrected because the source system had no validation. The risk of auto-linking on a wrong PAN is handled by the identifier validation step (Section 2.2): invalid PANs are quarantined and treated as missing, not as evidence. For valid PANs, the risk of two different businesses sharing a correctly-entered PAN is negligibly low (it is a legal violation to use another entity's PAN).

---

## 10. Round 2 Implementation Plan

### 10.1 Assumptions

- A sandbox environment is provided with representative data: master records from 4 department systems across 2 PIN codes in Bengaluru Urban, plus 12 months of activity events.
- Data is deterministically scrambled (PII replaced as described in Section 6.2).
- APIs or file exports for each department system are available in the sandbox.
- Up to 3 reviewers are available for ground-truth labelling during calibration.

### 10.2 Phases

#### Phase 0 — Foundation (Weeks 1–2)

- Stand up the Kubernetes cluster in the sandbox.
- Deploy PostgreSQL, Kafka, Iceberg object store, Airflow, and the raw zone.
- Build and test read-only connectors for each of the 4 department systems.
- Run first full extract; verify record counts and basic data quality.
- Deploy Great Expectations checks for schema and null rate monitoring.

#### Phase 1 — Entity Resolution MVP (Weeks 3–5)

- Build the standardisation pipeline for names, addresses, and identifiers.
- Implement the 6 blocking passes; measure blocking recall against manually identified known matches.
- Configure and run Splink; review initial score distribution.
- Run calibration session with reviewers (500 pairs across score range).
- Set initial thresholds T_HIGH and T_LOW.
- Auto-link high-confidence clusters; populate the UBID registry with first set of UBIDs.
- Build the reviewer UI MVP (tabular comparison view, confirm/reject actions).
- Route review-queue items to reviewers; collect first 200 reviewer decisions.

#### Phase 2 — Activity Classification MVP (Weeks 6–7)

- Ingest 12 months of activity events from department systems.
- Build the UBID join step; measure join rate; route unmatched events to the event review queue.
- Implement the rule-based activity classifier.
- Compute Active/Dormant/Closed classifications for all UBIDs with events.
- Build the evidence timeline API endpoint.
- Verify the "active factories without recent inspection" query against the dataset.

#### Phase 3 — Calibration and Hardening (Week 8)

- Retrain calibration layer with accumulated reviewer decisions.
- Re-evaluate thresholds; tighten or loosen based on observed precision-recall.
- Implement the model feedback loop (reviewer decisions → training log → quarterly retraining pipeline).
- Run end-to-end tests: lookup by department ID, PAN, GSTIN, name+address+PIN.
- Performance test: measure p95 latency for UBID lookup API.
- Security review: confirm read-only connector credentials, no PII in logs.

#### Phase 4 — Demo and Handoff Preparation (Week 9)

- Build the Superset dashboard with the representative query ("active factories in PIN 560058 with no inspection in 18 months").
- Demonstrate the reviewer workflow with a live ambiguous case.
- Demonstrate an un-merge and re-review.
- Document operational runbook: connector deployment, schema drift handling, reviewer onboarding, threshold adjustment.
- Deliver the architecture decision record (ADR) documenting all major design choices.

### 10.3 Success Criteria for Round 2

| Criterion | Target |
|---|---|
| Auto-link rate | ≥ 60% of cross-system matches resolved without human review |
| Auto-link precision | ≥ 99% (verified against reviewer ground truth) |
| UBID lookup latency (p95) | < 200ms for exact ID lookup |
| Event join rate | ≥ 85% of events joined to a UBID without manual review |
| Activity classification coverage | ≥ 90% of UBIDs with at least one event have a non-Unclassified status |
| Reviewer queue processing | All items in queue receive a decision within 5 working days |

---

## Appendix A — Sample Query: Active Factories Without Recent Inspection

The following SQL query, run against the UBID analytics view, answers the target question:

```sql
SELECT
    u.ubid,
    u.business_name,
    u.primary_address,
    u.pin_code,
    u.status,
    u.status_confidence,
    MAX(e.event_date) FILTER (WHERE e.event_type = 'factories_inspection') AS last_inspection_date,
    CURRENT_DATE - MAX(e.event_date) FILTER (WHERE e.event_type = 'factories_inspection') AS days_since_inspection
FROM
    ubid_registry u
    JOIN ubid_source_records sr ON u.ubid = sr.ubid
    LEFT JOIN ubid_events e ON u.ubid = e.ubid
WHERE
    u.status = 'ACTIVE'
    AND sr.source_system = 'factories'
    AND u.pin_code = '560058'
GROUP BY
    u.ubid, u.business_name, u.primary_address, u.pin_code, u.status, u.status_confidence
HAVING
    MAX(e.event_date) FILTER (WHERE e.event_type = 'factories_inspection') < CURRENT_DATE - INTERVAL '18 months'
    OR MAX(e.event_date) FILTER (WHERE e.event_type = 'factories_inspection') IS NULL
ORDER BY
    days_since_inspection DESC NULLS FIRST;
```

This query is impossible today because there is no `ubid_registry` that joins across the Factories system's records and its inspection events. The UBID platform makes it a single SQL query returning a result in under a second.

---

## Appendix B — Glossary

| Term | Definition |
|---|---|
| **UBID** | Unified Business Identifier — the single canonical identifier for a real-world business across all Karnataka department systems |
| **Entity Resolution** | The process of determining which records in different datasets refer to the same real-world entity |
| **Blocking** | A technique to reduce the comparison space in entity resolution by restricting comparisons to records that share at least one blocking key |
| **Fellegi-Sunter Model** | A probabilistic framework for record linkage, originating from a 1969 paper, that models matching as a classification problem with M-probability (given match, probability of observing this feature value) and U-probability (given non-match) |
| **Splink** | An open-source Python library implementing the Fellegi-Sunter model with Spark backend, developed by the UK Ministry of Justice |
| **T_HIGH / T_LOW** | The upper and lower thresholds that define the three zones: auto-link, review, and reject |
| **PAN** | Permanent Account Number — a 10-character alphanumeric identifier issued by the Income Tax Department to every taxpayer in India |
| **GSTIN** | Goods and Services Tax Identification Number — a 15-character identifier that encodes the state code, PAN, entity number, check digit |
| **Jaro-Winkler** | A string similarity metric that gives higher weight to matches at the start of strings; well-suited for business name comparison |
| **Double Metaphone** | A phonetic algorithm that encodes words as phonetic keys, tolerating spelling variations and transliterations |
| **Active** | A UBID status indicating at least one positive activity signal within the configured observation window |
| **Dormant** | A UBID status indicating a live registration with no positive activity signal within the observation window |
| **Closed** | A UBID status indicating either an explicit deregistration signal or implied closure by extended inactivity |

---

*Document prepared for Karnataka Commerce & Industry — UBID Hackathon Round 1*
*Architecture and implementation details are subject to refinement based on Round 2 sandbox data characteristics.*
