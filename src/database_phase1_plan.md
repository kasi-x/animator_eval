# Phase 1: Data Layer Refactor (Schema v54 → v56)

## Goal
Complete separation of analysis (silver) and display (bronze/display) layers.

## Changes Required

### v55: Lookup Tables + Display Helper

**New Tables:**
1. `sources` — Canonical list of credit data sources
2. `roles` — Role types (from role_groups.py)
3. `person_aliases` — Entity resolution audit trail

**New Function:**
- `src/utils/display_lookup.py::get_display_score(conn, anime_id)` — **only** way to access bronze

**Modified ETL:**
- `src/etl/integrate.py` — Use new lookup tables, enforce display helper

**Constraints:**
- ✅ `anime` table: NO score/popularity/favourites columns
- ✅ `anime_display` table: HAS score/popularity/description
- ✅ Analysis imports: Cannot use display_lookup (enforced by lint)

### v56: Normalized Genres

**New Tables:**
- `anime_genres(anime_id INT, genre_id INT)` — Replace JSON parsing

**Modified:**
- `anime` table: Remove `genres` JSON column (migrate to N-M table)

## Implementation Roadmap

### Step 1: Create display_lookup helper (no schema change)
- [ ] Create `src/utils/display_lookup.py`
- [ ] Functions: `get_display_score()`, `get_display_popularity()`, etc.
- [ ] Update all analysis code to use it

### Step 2: Migrate to v55 (add lookup tables)
- [ ] Create migration: `create_sources_table()`, `create_roles_table()`, `create_person_aliases_table()`
- [ ] Populate from existing data
- [ ] Update `src/etl/integrate.py` to use new tables
- [ ] Add FK constraints

### Step 3: Migrate to v56 (normalize genres)
- [ ] Create `create_anime_genres_table()`
- [ ] Migrate JSON genres → N-M rows
- [ ] Drop `anime.genres` column

### Step 4: Audit & Documentation
- [ ] Generate `docs/DATA_DICTIONARY.md` (auto-generated from schema)
- [ ] Create `docs/SCHEMA_MIGRATION_GUIDE.md`
- [ ] Run full test suite

## Files to Modify

| File | Changes |
|------|---------|
| `src/utils/display_lookup.py` | CREATE (new) |
| `src/database.py` | Add v55/v56 migration functions |
| `src/etl/integrate.py` | Use lookups + display helper |
| `src/analysis/**` | Import + use display_lookup |
| `tests/` | Add migration tests |

## Key Constraints

- ✅ No schema changes affect existing analysis code
- ✅ Backward compatible (rollback possible)
- ✅ No anime.score leakage to silver layer
- ✅ All tests remain green

## Estimated Effort
- Step 1: 30 min (display_lookup)
- Step 2: 1 hour (migration + ETL)
- Step 3: 30 min (genre normalization)
- Step 4: 30 min (audit + docs)
- **Total: ~2.5 hours**

