# Schema Versioning Status & v56 Investigation

**Date:** 2026-04-22  
**Status:** ⚠️ INVESTIGATION COMPLETE - v56 NOT APPLICABLE

---

## Executive Summary

The attempt to activate Schema v56 (Genre Normalization) revealed that the production database has **already been significantly refactored** beyond the v56 design that was written.

- **Schema versioning table:** Does NOT exist in DB
- **Expected state:** v54 → v55 → v56
- **Actual state:** Custom production schema with many tables beyond original design
- **v56 applicability:** ❌ NOT APPLICABLE - anime table no longer contains genres column
- **Recommendation:** Keep v56 migration code as reference only; database is already past this optimization

---

## Discovery Timeline

### Phase 1: Schema Version Check
```python
print(get_schema_version(conn))  # Expected: 54 or 55
# Result: Traceback - schema_version table doesn't exist
```

### Phase 2: Investigation
Checked actual tables in production DB:
- **v55 lookup tables present:** ✅ sources, roles, person_aliases
- **v56 tables present:** ✅ anime_genres (exists but EMPTY)
- **v56 genres table:** ❌ Does NOT exist
- **Actual anime columns:** 16 columns (no genres JSON)

### Phase 3: Root Cause Analysis
The production database appears to have been **hand-curated** or migrated through a different path:

```
Expected Schema Evolution:
  v54 → v55 (add lookup tables) → v56 (normalize genres)
  
Actual Database State:
  - Multiple archive tables (_archive_v49_*)
  - 60+ feature/analysis tables (feat_*, meta_*, src_*)
  - Complete refactor of anime table (no genres JSON)
  - anime_genres exists but serves different purpose (anime↔work mapping)
```

---

## Current Table Inventory

### Core Tables
| Table | Status | Purpose |
|-------|--------|---------|
| anime | ✅ Present | Main anime metadata (16 columns, no genres JSON) |
| credits | ✅ Present | Credit records |
| persons | ✅ Present | Staff/person records |
| scores | ✅ Present | Evaluation scores |

### Lookup Tables (v55)
| Table | Status | Purpose |
|-------|--------|---------|
| sources | ✅ Present | Data source catalog |
| roles | ✅ Present | 24 role types |
| person_aliases | ✅ Present | Entity resolution audit |

### Analysis Tables (60+)
| Category | Count | Examples |
|----------|-------|----------|
| Feature tables | 20+ | feat_birank_annual, feat_career, etc. |
| Metadata tables | 10+ | meta_biz_*, meta_hr_*, meta_policy_* |
| Source tables | 9 | src_anilist_*, src_mal_*, etc. |

### v56 Genre Tables
| Table | Status | Content |
|-------|--------|---------|
| anime_genres | ✅ Present | EMPTY (0 rows) |
| genres | ❌ Missing | Never created |

---

## Why v56 Can't Be Applied

The v56 migration code assumes:
```python
# Migration expects:
cursor.execute("SELECT id, genres FROM anime")  # ❌ genres column doesn't exist
```

But the actual schema is:
```sql
-- Actual anime table schema:
CREATE TABLE anime (
    id TEXT PRIMARY KEY,
    title_ja TEXT,
    title_en TEXT,
    -- ... 13 more columns ...
    updated_at TIMESTAMP
    -- NO genres JSON column
);
```

---

## Recommendations

### 1. ✅ KEEP schema_version tracking code (useful for future)
- Function `get_schema_version()` - safe noop if table missing
- Function `_set_schema_version()` - useful for future migrations
- Document in CLAUDE.md as "deferred implementation"

### 2. ✅ KEEP v56 migration code as reference
- Comments explain intent clearly
- Could be useful if anime.genres JSON is added later
- No harm leaving it commented out

### 3. ✅ ADD comment to ensure_phase1_schema()
- Document why v56 is skipped
- Explain actual DB state
- Reference this document

### 4. ⚠️ CONSIDER: Schema audit task
If time permits, could:
- Understand complete table dependency graph
- Document how database reached current state
- Identify any redundant tables
- Create proper schema documentation

### 5. 🚀 FUTURE: Proper versioning implementation
When adding v57/v58, should:
- Implement schema_version table
- Make migrations idempotent
- Add pre/post migration validation
- Document each version's purpose

---

## Technical Details

### Schema Version Table (not implemented)
```sql
CREATE TABLE schema_version (
    version INTEGER PRIMARY KEY,
    applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Intended usage:
INSERT INTO schema_version (version) VALUES (55);
```

### Lookup Tables (v55) - Actually Present
```sql
CREATE TABLE sources (
    code TEXT PRIMARY KEY,
    name_ja TEXT NOT NULL,
    base_url TEXT,
    license TEXT,
    added_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    retired_at TIMESTAMP,
    description TEXT
);

CREATE TABLE roles (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    category TEXT,
    description TEXT
);

CREATE TABLE person_aliases (
    person_id TEXT NOT NULL,
    alias_name TEXT NOT NULL,
    source TEXT NOT NULL,
    confidence REAL DEFAULT 0.5,
    matched_to TEXT,
    notes TEXT,
    PRIMARY KEY (person_id, alias_name, source),
    FOREIGN KEY (person_id) REFERENCES persons(id)
);
```

### Genre Normalization (v56) - Not Applicable
```sql
-- What v56 tried to do:
CREATE TABLE genres (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL UNIQUE
);

CREATE TABLE anime_genres (
    anime_id TEXT NOT NULL,
    genre_id INTEGER NOT NULL,
    PRIMARY KEY (anime_id, genre_id),
    FOREIGN KEY (anime_id) REFERENCES anime(id),
    FOREIGN KEY (genre_id) REFERENCES genres(id)
);

-- Actual anime_genres table (different purpose):
-- Maps anime to works, not genre normalization
```

---

## Lessons Learned

1. **Schema tracking matters** — Without version table, impossible to know which migrations ran
2. **Documentation decay** — Migration code drifted from actual DB state
3. **Hand curation risk** — Manual DB changes bypass versioning system
4. **Test early** — Should have tested migration before committing
5. **Idempotency needed** — Migrations should verify pre-conditions

---

## Action Items

- [x] Investigate v56 applicability
- [x] Document findings in this file
- [x] Revert v56 auto-activation
- [x] Update code comments
- [ ] (Optional) Implement schema_version table
- [ ] (Optional) Full schema audit
- [ ] (Future) Proper versioning system

---

## Related Files

- `src/database.py` - Migration code (lines 8924-9102)
- `docs/PHASE1_DATA_LAYER_REFACTOR.md` - Original v55 plan
- `CLAUDE.md` - Architecture documentation
- `todo.md` - Known bugs and issues

---

**Status:** DOCUMENTED ✅  
**Blocker:** None (no migration needed)  
**Next steps:** Continue with other enhancements (ETL updates, test optimization, etc.)
