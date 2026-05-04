# O7 Historical Credit Restoration — Method Note

**Card**: `15_extension_reports/07_o7_historical`
**Status**: Active (2026-05-02)
**Audience**: 文化庁文化財第二課, NFAJ, 国立国会図書館
**Report file**: `scripts/report_generators/reports/o7_historical.py`

---

## 1. Problem statement

Pre-1990 anime (particularly pre-1963 theatrical works) frequently have sparse
or absent credit records in the SILVER database.  Physical records held by
studios have been lost to fires, floods, and organisational dissolution.
Digitisation of industry journals (映画旬報, アニメージュ, etc.) is incomplete.

This module implements a best-effort structural restoration: it does not claim
to recover ground truth, only to surface cross-source agreement patterns that
may warrant further archival investigation.

---

## 2. Confidence tier design

| Tier | Condition | Insertion path |
|------|-----------|----------------|
| **HIGH** | Passed existing entity_resolution 5-stage pipeline | Normal SILVER ingest (unchanged) |
| **MEDIUM** | 2+ sources agree on person–role–anime; progression_consistency = True | `insert_restored_credits()` (future promotion path) |
| **LOW** | 1 source only; title similarity >= 0.85 (rapidfuzz token_sort_ratio) | `insert_restored_credits()` |
| **RESTORED** | Estimation only; evidence_source = 'restoration_estimated' | `insert_restored_credits()` — current card scope |

All rows inserted by this pipeline carry:
- `confidence_tier = 'RESTORED'`
- `evidence_source = 'restoration_estimated'`

Existing SILVER credits (HIGH / MEDIUM / LOW) are **never mutated**.

---

## 3. Multi-source fuzzy match algorithm

### 3.1 Sources consulted

| Source | Table (BRONZE) | Title column | Person column | Role column |
|--------|---------------|--------------|---------------|-------------|
| ANN (Anime News Network) | `src_ann_credits` | `title` | `person_name` | `role` |
| mediaarts / MADB | `src_mediaarts_credits` | `title_ja` | `person_name` | `role_ja` |
| SeesaaWiki | `src_seesaawiki_credits` | `anime_title` | `name` | `role` |
| allcinema | `src_allcinema_credits` | `title` | `name` | `role` |

Tables may be absent (e.g., allcinema is scraped selectively).  Missing tables
are silently skipped.

### 3.2 Title matching

```
title_sim = rapidfuzz.fuzz.token_sort_ratio(bronze_title, silver_title_ja) / 100
```

Fallback (when rapidfuzz unavailable): Dice coefficient on character sets.

Threshold: `title_sim >= 0.85` (configurable via `threshold` parameter).

**False-positive rate**: If spot-check review reveals > 20% false positives,
raise threshold to 0.90 and enforce mandatory manual review gate.

### 3.3 Person entity resolution

Thin resolution: exact match on `persons.name_ja` / `persons.name_en`, then
fuzzy fallback.  The canonical 5-stage pipeline
(`src.analysis.entity_resolution`) is **not modified** (H3).

Unresolved names receive a synthetic person_id: `restored:<normalised_name>`.
These are stub rows in the `persons` table (`name_en = raw_name`).

### 3.4 Role progression consistency

A heuristic check flags implausible credits (e.g., a SENIOR role 20+ years
before any other recorded credit).  When data are insufficient (< 1 existing
credit for the person), the check defaults to `True` (benefit of the doubt).

This check is informational only; it does not block insertion.

---

## 4. Claim correction flow

### 4.1 Purpose

Allow individuals, family members, researchers, and institutions to submit
corrections to restored or missing credits.

### 4.2 Data model

```sql
CREATE TABLE meta_credit_corrections (
    claim_id        INTEGER PRIMARY KEY AUTOINCREMENT,
    anime_id        TEXT NOT NULL,
    person_id       TEXT,
    corrected_field TEXT NOT NULL,
    old_value       TEXT,
    new_value       TEXT NOT NULL,
    claimer_role    TEXT NOT NULL DEFAULT 'unknown'
                        CHECK (claimer_role IN
                            ('individual','family','researcher','institution','unknown')),
    evidence_url    TEXT,
    status          TEXT NOT NULL DEFAULT 'pending'
                        CHECK (status IN ('pending','approved','rejected','superseded')),
    reviewer_note   TEXT,
    submitted_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    reviewed_at     TIMESTAMP,
    source_credit_id INTEGER
);
```

### 4.3 Submission paths (design only — implementation is a separate card)

- **Web form**: FastAPI endpoint `POST /api/v1/corrections` (not yet implemented)
- **Email**: akizora.biz@gmail.com with structured subject line
- **Google Form**: External link → manual ingestion by admin

### 4.4 Review process

1. Claim submitted → `status = 'pending'`
2. Admin review: check `evidence_url`, consult NFAJ/NFC archives if needed
3. Approved: `status = 'approved'`; UPDATE corresponding `credits` row or
   INSERT new HIGH-tier row; `source_credit_id` links back
4. Rejected: `status = 'rejected'`; `reviewer_note` records reason
5. Superseded: when a later claim provides better evidence

**Reviewer authority**: At minimum one of — NFAJ archivist, NFJ-certified
researcher, or studio official representative.

### 4.5 Audit trail

`meta_credit_corrections` is append-only.  Corrections to a correction use
`status = 'superseded'` on the old row and insert a new `pending` row.

---

## 5. Hard constraints

| Constraint | Implementation |
|------------|---------------|
| H1: anime.score excluded | No reference to `anime.score` in any SQL or formula |
| H2: No ability framing | lint_vocab gate; "失われた" + "人材" bigram forbidden |
| H3: entity_resolution unchanged | Only `persons` table consulted; no modification to resolution logic |
| H4: evidence_source tag | `EVIDENCE_SOURCE = 'restoration_estimated'` constant in `insert_restored.py` |
| H5: No existing row mutation | INSERT OR IGNORE; HIGH/MEDIUM/LOW rows never updated |

---

## 6. Verification queries

```python
# Check confidence_tier distribution
import duckdb
c = duckdb.connect('result/silver.duckdb', read_only=True)
print(c.execute(
    "SELECT confidence_tier, COUNT(*) FROM credits GROUP BY 1 ORDER BY 1"
).fetchall())

# Verify no evidence_source = 'restoration_estimated' rows exist with tier != 'RESTORED'
rows = c.execute(
    """SELECT COUNT(*) FROM credits
       WHERE evidence_source = 'restoration_estimated'
         AND confidence_tier != 'RESTORED'"""
).fetchone()
assert rows[0] == 0, "Invariant violated"
```

---

## 7. References

- Library of Congress, National Film Registry — credit attestation standards
- NFAJ (国立映画アーカイブ) — 映画フィルム原版情報
- 映画旬報データベース (1919–)
- ANN Encyclopedia credit data model

---

*Generated: 2026-05-02 — Animetor Eval O7 implementation.*
