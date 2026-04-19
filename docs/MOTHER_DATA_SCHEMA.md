# MOTHER_DATA_SCHEMA.md — Silver Layer Canonical Schema (v50)

**Status**: DESIGN DRAFT — Phase 1 Tasks 1-1 and 1-2 (non-destructive skeleton).
Destructive migration (Task 1-3, v49→v50) is deferred to a checkpoint-gated session.

**Related**:
- `detailed_todo.md` §1.2-§1.4, §2.1-§2.2 (authoritative spec)
- `CLAUDE.md` (hard constraints, `anime.score` contamination rules)
- `docs/REPORT_PHILOSOPHY.md` (v2 gates)

---

## 1. Goals

1. **Zero `anime.score` contamination at the silver layer.** A silver schema that simply does not contain the column makes accidental `SELECT score FROM anime` impossible.
2. **Single canonical silver table per entity.** Today, three parallel `anime*` tables coexist (legacy `anime`, `anime_analysis`, `anime_display`). Collapse to one.
3. **Self-documenting DDL.** `CHECK` constraints encode value domains; column comments encode non-obvious semantics; lookup tables replace hardcoded CHECK enumerations.
4. **Display-layer access has exactly one entrance**: `src/utils/display_lookup.py`. Analysis code must never import it (enforced by import-guard lint in Phase 1-7).
5. **Extensible via row-inserts, not ALTER TABLE.** External IDs and sources are normalized so adding a new scraper is an `INSERT` into lookup tables, not a migration.

---

## 2. AS-IS (schema v49) — Problem Statement

Three parallel `anime*` tables exist:

| Table | Role | Problem |
|-------|------|---------|
| `anime` (legacy) | original scraper target | Carries `score REAL` — direct contamination risk |
| `anime_analysis` | silver (clean, no score) | Correct shape but non-canonical name |
| `anime_display` | silver (display: score, cover, description, JSON blobs) | Defeats point of stripping score from silver; duplicates bronze |

Additional schema-smell:
- `credits.source` collides in meaning with `anime.source` (one is scraper provenance, the other is original-work type: MANGA/NOVEL/ORIGINAL).
- `credits.episode DEFAULT -1` as a sentinel — unreadable to outside reviewers.
- Horizontal external-ID columns (`mal_id, anilist_id, ann_id, allcinema_id, madb_id`) — new ID = ALTER TABLE.
- Hardcoded `CHECK (source IN ('anilist','ann',...))` duplicated across tables — new source = migration.
- `anime.genres`, `anime.tags`, `anime.studios` as JSON blobs — `GROUP BY genre` requires JSON parse on every row.
- `scores` table collides semantically with `anime.score` (they are person-level, but the name does not distinguish).

---

## 3. TO-BE (schema v50) Overview

```
Silver layer (canonical; no score/popularity/description/cover):
  anime                          -- single canonical anime table (was anime_analysis)
  persons
  credits                        -- credits.source → credits.evidence_source
  studios                        -- existing, unchanged
  anime_studios                  -- existing, unchanged
  anime_genres                   -- NEW (normalized from bronze JSON)
  anime_tags                     -- NEW (normalized from bronze JSON)
  anime_external_ids             -- NEW (N-4: replaces inline mal_id/anilist_id/...)
  person_external_ids            -- NEW (N-4)
  person_aliases                 -- NEW (N-3: entity-resolution audit trail)
  sources                        -- NEW (N-1: lookup, replaces hardcoded CHECK)
  roles                          -- NEW (N-2: lookup, replaces role_groups.py hardcoded enum)
  person_scores                  -- RENAMED from `scores` (avoid anime.score confusion)
  anime_relations, characters, character_voice_actors  -- unchanged

Bronze layer (unchanged; retains everything):
  src_anilist_anime, src_anilist_persons, src_anilist_credits
  src_ann_anime,     src_ann_persons,     src_ann_credits
  src_allcinema_anime, src_allcinema_persons, src_allcinema_credits
  src_seesaawiki_anime, src_seesaawiki_credits
  src_keyframe_anime, src_keyframe_credits
  (all viewer-facing fields stay here: score, popularity, favourites,
   description, cover_*, banner, site_url, genres JSON, tags JSON,
   studios JSON, synonyms JSON)

Dropped:
  anime (legacy, score-bearing)
  anime_display
```

### 3.1 anime_id convention (load-bearing)

Silver uses composite string IDs of the form `{source}:{external_id}`:
- `anilist:123`
- `ann:456`
- `allcinema:789`
- `seesaawiki:slug-string` (TEXT PK on bronze)
- `keyframe:slug-string`

The `display_lookup` helper parses this prefix to route to the correct bronze table.

---

## 4. Target DDL

### 4.1 Sources lookup (N-1)

```sql
CREATE TABLE sources (
    code         TEXT PRIMARY KEY,
    name_ja      TEXT NOT NULL,
    base_url     TEXT NOT NULL,
    license      TEXT NOT NULL,
    added_at     TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    retired_at   TIMESTAMP,
    description  TEXT NOT NULL
);

INSERT INTO sources (code, name_ja, base_url, license, description) VALUES
    ('anilist',    'AniList',             'https://anilist.co',              'proprietary', 'GraphQL で structured staff 情報が最も豊富'),
    ('ann',        'Anime News Network',  'https://www.animenewsnetwork.com','proprietary', 'historical depth と職種粒度'),
    ('allcinema',  'allcinema',           'https://www.allcinema.net',       'proprietary', '邦画・OVA の網羅性'),
    ('seesaawiki', 'SeesaaWiki',          'https://seesaawiki.jp',           'CC-BY-SA',    'fan-curated 詳細エピソード情報'),
    ('keyframe',   'Sakugabooru',         'https://www.sakugabooru.com',     'CC',          'sakuga コミュニティ別名情報');
```

### 4.2 Roles lookup (N-2)

```sql
CREATE TABLE roles (
    code            TEXT PRIMARY KEY,
    name_ja         TEXT NOT NULL,
    name_en         TEXT NOT NULL,
    role_group      TEXT NOT NULL CHECK (role_group IN (
                        'director','animator','sound','production',
                        'writer','voice_actor','other')),
    weight_default  REAL NOT NULL CHECK (weight_default > 0),
    description_ja  TEXT NOT NULL
);
-- Seeded from src/utils/role_groups.py (ROLE_CATEGORY, _ROLE_WEIGHT).
```

### 4.3 Canonical anime (silver)

```sql
CREATE TABLE anime (
    id            TEXT    PRIMARY KEY,
                                            -- format: '{source_code}:{external_id}'
                                            --   anilist:123 / ann:456 / allcinema:789
                                            --   seesaawiki:slug / keyframe:slug
    title_ja      TEXT    NOT NULL DEFAULT '',
    title_en      TEXT    NOT NULL DEFAULT '',
    year          INTEGER CHECK (year IS NULL OR year BETWEEN 1910 AND 2100),
    season        TEXT    CHECK (season IS NULL OR season IN ('WINTER','SPRING','SUMMER','FALL')),
    quarter       INTEGER CHECK (quarter IS NULL OR quarter BETWEEN 1 AND 4),
    episodes      INTEGER CHECK (episodes IS NULL OR episodes > 0),
    format        TEXT    CHECK (format IS NULL OR format IN ('TV','MOVIE','OVA','ONA','SPECIAL','MUSIC')),
    duration      INTEGER CHECK (duration IS NULL OR duration > 0),
                                            -- 1 話あたり分。production_scale 計算の入力
    start_date    TEXT    CHECK (start_date IS NULL OR start_date GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
                                            -- ISO 8601 'YYYY-MM-DD' (N-5)
    end_date      TEXT    CHECK (end_date   IS NULL OR end_date   GLOB '[0-9][0-9][0-9][0-9]-[0-9][0-9]-[0-9][0-9]'),
    status        TEXT    CHECK (status IS NULL OR status IN
                          ('FINISHED','RELEASING','NOT_YET_RELEASED','CANCELLED','HIATUS')),
    source        TEXT,                     -- 原作タイプ (ORIGINAL/MANGA/LIGHT_NOVEL/...);
                                            --   注: C-1 後も anime.source は残る。
                                            --   credits 側が evidence_source に改名される。
    work_type     TEXT    CHECK (work_type IS NULL OR work_type IN ('tv','tanpatsu')),
    scale_class   TEXT    CHECK (scale_class IS NULL OR scale_class IN ('large','medium','small')),
    updated_at    TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_anime_year     ON anime(year);
CREATE INDEX idx_anime_format   ON anime(format);
CREATE INDEX idx_anime_year_fmt ON anime(year, format);

-- INTENTIONALLY ABSENT (contamination prevention):
--   score, popularity, popularity_rank, favourites, mean_score
--   description, cover_large, cover_extra_large, cover_medium, banner, site_url
--   genres, tags, studios, synonyms (JSON)   → moved to junction tables
--   mal_id, anilist_id, ann_id, allcinema_id, madb_id → anime_external_ids
```

### 4.4 Genres / tags junction (JSON normalization)

```sql
CREATE TABLE anime_genres (
    anime_id   TEXT NOT NULL,
    genre_name TEXT NOT NULL,
    PRIMARY KEY (anime_id, genre_name),
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
);
CREATE INDEX idx_anime_genres_genre ON anime_genres(genre_name, anime_id);

CREATE TABLE anime_tags (
    anime_id TEXT NOT NULL,
    tag_name TEXT NOT NULL,
    rank     INTEGER CHECK (rank IS NULL OR rank BETWEEN 0 AND 100),
    PRIMARY KEY (anime_id, tag_name),
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
);
CREATE INDEX idx_anime_tags_tag ON anime_tags(tag_name, rank DESC, anime_id);
```

### 4.5 External IDs (N-4, replaces horizontal columns)

```sql
CREATE TABLE anime_external_ids (
    anime_id     TEXT NOT NULL,
    source       TEXT NOT NULL REFERENCES sources(code),
    external_id  TEXT NOT NULL,
    PRIMARY KEY (anime_id, source),
    UNIQUE (source, external_id),
    FOREIGN KEY (anime_id) REFERENCES anime(id) ON DELETE CASCADE
);
CREATE INDEX idx_anime_ext_ids_source ON anime_external_ids(source, external_id);

CREATE TABLE person_external_ids (
    person_id    TEXT NOT NULL,
    source       TEXT NOT NULL REFERENCES sources(code),
    external_id  TEXT NOT NULL,
    PRIMARY KEY (person_id, source),
    UNIQUE (source, external_id),
    FOREIGN KEY (person_id) REFERENCES persons(id) ON DELETE CASCADE
);
CREATE INDEX idx_person_ext_ids_source ON person_external_ids(source, external_id);
```

### 4.6 Person aliases (N-3, entity-resolution audit trail)

```sql
CREATE TABLE person_aliases (
    person_id   TEXT NOT NULL,
    alias       TEXT NOT NULL,
    source      TEXT NOT NULL REFERENCES sources(code),
                                            -- extended semantics: in addition to scraper
                                            -- sources, 'romaji_auto' / 'ai_merge' are
                                            -- seeded rows in `sources` to track algorithmic origin.
    confidence  REAL CHECK (confidence IS NULL OR confidence BETWEEN 0 AND 1),
    added_at    TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (person_id, alias, source),
    FOREIGN KEY (person_id) REFERENCES persons(id) ON DELETE CASCADE
);
CREATE INDEX idx_person_aliases_alias ON person_aliases(alias, person_id);
```

### 4.7 Credits (C-1 rename + lookup FKs)

```sql
CREATE TABLE credits (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    person_id         TEXT    NOT NULL,
    anime_id          TEXT    NOT NULL,
    role              TEXT    NOT NULL REFERENCES roles(code),
    raw_role          TEXT,                           -- original string from source
    episode           INTEGER CHECK (episode IS NULL OR episode > 0),
                                                     -- NULL = 作品レベル (全話通し)
                                                     -- integer = 特定話
    evidence_source   TEXT    NOT NULL REFERENCES sources(code),
                                                     -- was: `source`. Renamed (C-1) so as not to
                                                     -- collide with anime.source (= 原作タイプ).
    updated_at        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE(person_id, anime_id, role, episode, evidence_source),
    FOREIGN KEY (person_id) REFERENCES persons(id),
    FOREIGN KEY (anime_id)  REFERENCES anime(id)
);

CREATE INDEX idx_credits_person          ON credits(person_id);
CREATE INDEX idx_credits_anime           ON credits(anime_id);
CREATE INDEX idx_credits_role            ON credits(role);
CREATE INDEX idx_credits_anime_role      ON credits(anime_id, role);
CREATE INDEX idx_credits_person_evidence ON credits(person_id, evidence_source);
```

### 4.8 Rename: `scores` → `person_scores`

```sql
ALTER TABLE scores RENAME TO person_scores;
-- All FROM scores / UPDATE scores / JOIN scores references must be
-- bulk-replaced in src/analysis/, src/pipeline_phases/, src/api.py.
```

---

## 5. Column-by-Column Rationale for `anime`

| Legacy column (v49) | v50 disposition | Rationale |
|---|---|---|
| `id` | kept | Canonical composite string ID |
| `title_ja`, `title_en` | kept | Structural metadata |
| `year`, `season`, `quarter` | kept (tightened CHECK) | Needed for temporal/cohort analysis |
| `episodes`, `format`, `duration` | kept (tightened CHECK) | Inputs to `production_scale` |
| `start_date`, `end_date` | kept (added GLOB CHECK, N-5) | Enforce ISO 8601 format |
| `status` | kept (tightened CHECK) | Structural state |
| `source` (= 原作タイプ) | kept | Distinct meaning from credits.evidence_source |
| `work_type`, `scale_class` | kept | Derived classifications |
| `updated_at` | kept | Lineage |
| **`score`** | **DROPPED → bronze only** | H1: anime.score must not exist in silver |
| **`popularity`** | **DROPPED → bronze only** | H1: audience metric, not production |
| **`popularity_rank`** | **DROPPED → bronze only** | H1 |
| **`favourites`** | **DROPPED → bronze only** | H1 |
| **`mean_score`** | **DROPPED → bronze only** | H1 |
| **`description`** | **DROPPED → bronze only** | Non-analytical; display helper on demand |
| **`cover_large`, `cover_extra_large`, `cover_medium`, `cover_large_path`** | **DROPPED → bronze only** | Images are display concern |
| **`banner`, `banner_path`, `site_url`** | **DROPPED → bronze only** | Display concern |
| **`genres` (JSON)** | **DROPPED → `anime_genres` junction** | Normalization; `GROUP BY genre` becomes indexable |
| **`tags` (JSON)** | **DROPPED → `anime_tags` junction** | Same |
| **`studios` (JSON)** | **DROPPED → `anime_studios` (already exists)** | De-duplicate |
| **`synonyms` (JSON)** | **DROPPED → bronze only** | Display alias list |
| **`mal_id`** | **DROPPED → `anime_external_ids(source='anilist')` or retained as separate source row** | N-4 (horizontal → normalized) |
| **`anilist_id`** | **DROPPED → `anime_external_ids(source='anilist')`** | N-4 |
| **`ann_id`** | **DROPPED → `anime_external_ids(source='ann')`** | N-4 |
| **`allcinema_id`** | **DROPPED → `anime_external_ids(source='allcinema')`** | N-4 |
| **`madb_id`** | **DROPPED → `anime_external_ids(source='madb')`** (when added) | N-4 |
| **`country_of_origin`, `is_adult`, `relations_json`, `external_links_json`, `rankings_json`** (ex-`anime_display`) | **DROPPED → bronze only** | Display/meta; not used in scoring |

**Sanity gate** (automatable post-migration):
```
sqlite> PRAGMA table_info(anime);
   -- Must NOT return any column named: score, popularity, popularity_rank,
   --   favourites, mean_score, description, cover_*, banner, site_url,
   --   genres, tags, studios, synonyms, mal_id, anilist_id, ann_id,
   --   allcinema_id, madb_id
```

---

## 6. What Bronze Retains

All "interesting-to-display" fields continue to live in bronze:

| Field | Lives in | Accessed via |
|---|---|---|
| score | `src_anilist_anime.score` | `display_lookup.get_display_score()` |
| popularity | `src_anilist_anime.popularity` | `display_lookup.get_display_popularity()` |
| favourites | `src_anilist_anime.favourites` | `display_lookup.get_display_favourites()` |
| description | `src_anilist_anime.description`, `src_allcinema_anime.synopsis` | `display_lookup.get_display_description()` |
| cover image URL | `src_anilist_anime.cover_large`, `cover_medium` | `display_lookup.get_display_cover_url()` |
| banner URL | `src_anilist_anime.banner` | (future helper) |
| site URL | `src_anilist_anime.site_url` | (future helper) |
| genres (as raw JSON) | `src_anilist_anime.genres`, `src_ann_anime.genres` | (analysis uses `anime_genres` junction) |
| tags (raw JSON) | `src_anilist_anime.tags` | (analysis uses `anime_tags`) |
| synonyms (raw JSON) | `src_anilist_anime.synonyms` | display only |
| synopsis (allcinema) | `src_allcinema_anime.synopsis` | description fallback |

Bronze is **read-only** after the initial scrape. Bronze schema is frozen as of v48 and does not change in v50.

---

## 7. Display Access: Single-Door Rule

The only code path from **any downstream consumer** to bronze score/popularity/description/cover is `src/utils/display_lookup.py`.

```python
# Allowed
from src.utils.display_lookup import get_display_score  # in scripts/report_generators/**

# Forbidden (Phase 1-7 import-guard lint will fail)
from src.utils.display_lookup import *  # in src/analysis/** or src/pipeline_phases/**
```

The lint check (to land in Phase 1-7) is a one-liner:

```bash
rg -l 'display_lookup' src/analysis/ src/pipeline_phases/ && exit 1 || exit 0
```

### 7.1 Fallback precedence (design decision)

For fields that multiple bronze tables could answer, the helper tries them in this order:

| Field | Precedence | Rationale |
|---|---|---|
| score | anilist ONLY | Only AniList provides aggregated viewer rating; MAL is discarded (not scraped as bronze) |
| popularity | anilist ONLY | Same |
| favourites | anilist ONLY | Same |
| description | anilist > allcinema (synopsis) | AniList has richer English descriptions; allcinema has JP synopses as fallback |
| cover URL | anilist ONLY | Bronze image fields exist on AniList only |

**Routing behaviour**:
1. Parse prefix from `anime_id` (`anilist:`, `ann:`, `allcinema:`, `seesaawiki:`, `keyframe:`).
2. If the prefix's source offers the requested field, query that source first.
3. If the row is missing OR the source cannot answer the field (e.g., ann has no score), consult the fallback chain above in order — **but only when doing so is semantically safe** (e.g., description can fall back; score cannot — score is a property of a specific viewer population, not cross-mappable).
4. Return `None` if nothing answers.

---

## 8. Migration Plan (v49 → v50)

Destructive. Execute only inside Task 1-3's checkpoint session.

### 8.1 Step order

1. **Checkpoint**: `cp data/animetor.db data/animetor.db.pre-v50.bak`.
2. **Archive snapshots** (E-3 reversibility):
   ```sql
   CREATE TABLE _archive_v49_anime         AS SELECT * FROM anime;
   CREATE TABLE _archive_v49_anime_display AS SELECT * FROM anime_display;
   CREATE TABLE _archive_v49_credits       AS SELECT * FROM credits;
   CREATE TABLE _archive_v49_scores        AS SELECT * FROM scores;
   ```
3. **Create lookups** (N-1, N-2):
   - `sources` + seed 5 rows (+ `romaji_auto`, `ai_merge` for alias origin tracking).
   - `roles` + seed from `role_groups.py`.
4. **Create external_ids + aliases** (N-3, N-4):
   - `anime_external_ids`, `person_external_ids`, `person_aliases`.
   - Backfill from `anime_analysis.mal_id/anilist_id/...` and `persons.aliases`.
5. **Create junction tables** (4.4):
   - `anime_genres`, `anime_tags`; backfill from `src_anilist_anime.genres`/`tags` JSON.
6. **Rename silver analysis table**:
   ```sql
   DROP TABLE anime_display;
   DROP TABLE anime;                  -- legacy (score-bearing); archived in step 2
   ALTER TABLE anime_analysis RENAME TO anime;
   -- then drop the now-redundant mal_id / anilist_id / ann_id / allcinema_id / madb_id columns
   -- (SQLite: CREATE TABLE _new → INSERT SELECT → DROP old → RENAME)
   ```
7. **Rename credits.source → credits.evidence_source** (C-1):
   ```sql
   ALTER TABLE credits RENAME COLUMN source TO evidence_source;
   UPDATE credits SET episode = NULL WHERE episode = -1;  -- sentinel cleanup
   ```
   Then rebuild table to install FK on `roles(code)` and `sources(code)` (SQLite requires table-recreate for FK changes).
8. **Rename `scores` → `person_scores`**.
9. **Enable FK**: every `get_connection()` call must now execute `PRAGMA foreign_keys = ON` (code change in `src/database.py`).
10. **Bump SCHEMA_VERSION**: 49 → 50.
11. **Validation gate** (blocks the migration if any fails):
    - `PRAGMA table_info(anime)` does not contain any forbidden column (see §5 sanity gate).
    - `SELECT COUNT(*) FROM _archive_v49_anime` equals `SELECT COUNT(*) FROM anime` (no row loss).
    - `PRAGMA foreign_key_check;` returns zero rows.
    - Spot-check 10 random `anime.id` values: `anime_external_ids` join reconstructs the original `anilist_id`.
12. **Code sweeps** (mechanical, high-churn):
    - `rg -l 'FROM scores\b'` → replace with `FROM person_scores`.
    - `rg -l 'credits\.source\b'` / `rg -l '"source"\s*FROM credits'` → replace with `evidence_source`.
    - `rg -l 'FROM anime_analysis\b'` → replace with `FROM anime`.
    - `rg -l 'FROM anime_display\b'` → replace with a `display_lookup` helper call.
    - `rg -l 'anime\.(mal|anilist|ann|allcinema|madb)_id'` → replace with `anime_external_ids` join.
13. **Test run**: `pixi run test` (all 1947 tests must pass; any failures block commit).

### 8.2 Rollback strategy (E-3)

If validation gate in step 11 fails, or tests fail in step 13:

```sql
-- Option A (during same session, no commits yet): restore from _archive_v49_* snapshots.
DROP TABLE anime;
ALTER TABLE _archive_v49_anime RENAME TO anime;
-- ...etc for anime_display, credits, scores
-- Then revert SCHEMA_VERSION to 49.

-- Option B (catastrophic): restore the pre-migration backup file.
cp data/animetor.db.pre-v50.bak data/animetor.db
```

Archive tables are retained until the next pipeline run succeeds end-to-end. `scripts/maintenance/purge_archive.py` (deferred) cleans them up after a safety window.

### 8.3 Non-goals of v50

- Schema YAML SSoT (E-1) is deferred to Task 1-12.
- `meta_lineage` reproducibility fields (E-2) land in Phase 2.
- Entity-resolution logic is **not** touched (H3).
- `meta_entity_resolution_audit` (V-2) lands in Phase 2 Task 2-6.

---

## 9. Cross-References

- Source of truth for task ordering: `detailed_todo.md` §1.4.x and §2.x.
- Hard constraints: `detailed_todo.md` §0.4.
- Helper module spec: `detailed_todo.md` §2.2 → `src/utils/display_lookup.py`.
- Tests: `tests/test_display_lookup.py` (smoke) + Task 1-3 will add schema-shape tests.
