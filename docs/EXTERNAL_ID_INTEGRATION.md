# External ID Integration Guide

**Status**: 📋 Planning Document
**Priority**: Low (Nice-to-Have)
**Estimated Effort**: 2-3 weeks

## Overview

Animetor Evalは現在4つのデータソースをサポート：

1. ✅ **AniList** (GraphQL API) - アニメ・スタッフデータ
2. ✅ **MyAnimeList** (Jikan API) - 補完データ
3. ✅ **Media Arts Database** (SPARQL) - 日本の公式データベース
4. ✅ **Wikidata** (SPARQL via JVMG) - オープンデータ

このドキュメントでは、以下の2つの外部データソースとの連携を提案します：

5. ⏳ **AniDB** (Anime Database) - コミュニティ駆動の詳細データ
6. ⏳ **ANN** (Anime News Network) - 業界ニュース・スタッフ情報

## Why AniDB & ANN?

### AniDB (https://anidb.net/)
- ✅ 詳細なスタッフクレジット（episode-level）
- ✅ 多言語対応（kanji, romaji, English）
- ✅ API available (XML, UDP)
- ❌ レート制限: 1 req/2 sec
- ❌ クライアント認証必要

### ANN (https://www.animenewsnetwork.com/)
- ✅ 包括的なスタッフデータベース
- ✅ Encyclopedia API (XML)
- ✅ 業界標準の信頼性
- ❌ レート制限: Undocumented (conservative: 1 req/sec)
- ❌ XML only (no JSON)

## Architecture Design

### Data Model Extensions

#### Person Model
```python
@dataclass
class Person:
    id: str
    name_ja: str | None
    name_en: str | None
    aliases: list[str]
    source: str

    # Existing IDs
    mal_id: int | None = None
    anilist_id: int | None = None

    # NEW: External IDs
    anidb_id: int | None = None          # AniDB creator ID
    ann_id: int | None = None            # ANN encyclopedia ID
    wikidata_id: str | None = None       # Wikidata Q-ID (already available via JVMG)
    imdb_id: str | None = None           # IMDb nm-ID (future)
```

#### Anime Model
```python
@dataclass
class Anime:
    id: str
    title_ja: str | None
    title_en: str

    # Existing IDs
    mal_id: int | None = None
    anilist_id: int | None = None

    # NEW: External IDs
    anidb_id: int | None = None          # AniDB anime ID
    ann_id: int | None = None            # ANN encyclopedia ID
    wikidata_id: str | None = None       # Wikidata Q-ID
    imdb_id: str | None = None           # IMDb tt-ID (future)
```

### Database Schema Updates

```sql
-- Add columns to persons table
ALTER TABLE persons ADD COLUMN anidb_id INTEGER;
ALTER TABLE persons ADD COLUMN ann_id INTEGER;
ALTER TABLE persons ADD COLUMN wikidata_id TEXT;
ALTER TABLE persons ADD COLUMN imdb_id TEXT;

-- Add columns to anime table
ALTER TABLE anime ADD COLUMN anidb_id INTEGER;
ALTER TABLE anime ADD COLUMN ann_id INTEGER;
ALTER TABLE anime ADD COLUMN wikidata_id TEXT;
ALTER TABLE anime ADD COLUMN imdb_id TEXT;

-- Create indexes
CREATE INDEX idx_persons_anidb ON persons(anidb_id);
CREATE INDEX idx_persons_ann ON persons(ann_id);
CREATE INDEX idx_anime_anidb ON anime(anidb_id);
CREATE INDEX idx_anime_ann ON anime(ann_id);

-- Update schema version
UPDATE schema_version SET version = 5;
```

### ID Mapping Strategy

```
AniList ←→ MAL ←→ AniDB ←→ ANN ←→ Wikidata

Cross-source mapping:
1. Use existing anime-level mappings (via AniList/MAL)
2. For person-level: Use name matching (conservative)
3. Manual curation file: data/id_mappings.json
```

## Implementation Plan

### Phase 1: AniDB Integration (Week 1)

#### 1.1: AniDB API Client

**File**: `src/scrapers/anidb_scraper.py` (新規作成)

```python
"""AniDB スクレイパー — XML API 経由.

API Documentation: https://wiki.anidb.net/HTTP_API_Definition
Rate Limit: 1 request per 2 seconds (enforced by client ID)

Requirements:
    - AniDB client ID (申請必要: https://anidb.net/software/add)
    - HTTP API access (無料)
"""

import time
import xml.etree.ElementTree as ET
from pathlib import Path

import httpx
import structlog

from src.models import Anime, Credit, Person, Role

logger = structlog.get_logger()

class AniDBClient:
    BASE_URL = "http://api.anidb.net:9001/httpapi"
    RATE_LIMIT = 2.0  # seconds per request

    def __init__(self, client_id: str, client_ver: int = 1):
        """Initialize AniDB client.

        Args:
            client_id: AniDB registered client ID
            client_ver: Client version number
        """
        self.client_id = client_id
        self.client_ver = client_ver
        self.last_request = 0.0

    def _throttle(self):
        """Enforce rate limit (1 req / 2 sec)."""
        elapsed = time.monotonic() - self.last_request
        if elapsed < self.RATE_LIMIT:
            time.sleep(self.RATE_LIMIT - elapsed)
        self.last_request = time.monotonic()

    def get_anime(self, anidb_id: int) -> dict:
        """Get anime data by AniDB ID.

        Args:
            anidb_id: AniDB anime ID

        Returns:
            Dict with anime metadata + staff list
        """
        self._throttle()

        params = {
            "request": "anime",
            "client": self.client_id,
            "clientver": self.client_ver,
            "protover": 1,
            "aid": anidb_id,
        }

        response = httpx.get(self.BASE_URL, params=params, timeout=30.0)
        response.raise_for_status()

        # Parse XML
        root = ET.fromstring(response.text)

        # Extract data
        anime_data = {
            "anidb_id": anidb_id,
            "title_en": root.findtext(".//title[@type='official'][@xml:lang='en']"),
            "title_ja": root.findtext(".//title[@type='official'][@xml:lang='ja']"),
            "year": int(root.findtext(".//startdate", "0")[:4]) if root.findtext(".//startdate") else None,
            "episodes": int(root.findtext(".//episodecount", "0")),
        }

        # Extract staff
        staff = []
        for creator in root.findall(".//creator"):
            staff.append({
                "anidb_id": int(creator.get("id")),
                "name": creator.text,
                "type": creator.get("type"),  # "Direction", "Animation Work", etc.
            })

        anime_data["staff"] = staff

        logger.info("anidb_anime_fetched", anidb_id=anidb_id, staff_count=len(staff))
        return anime_data

    def search_anime(self, title: str) -> list[dict]:
        """Search anime by title.

        Note: AniDB HTTP API doesn't support search.
        Use UDP API or manual ID mapping instead.
        """
        raise NotImplementedError("AniDB HTTP API doesn't support search. Use anime ID directly.")
```

#### 1.2: AniDB Role Mapping

AniDBの`type`フィールドを既存の`Role`enumにマッピング：

```python
ANIDB_ROLE_MAP = {
    "Direction": Role.DIRECTOR,
    "Series Director": Role.DIRECTOR,
    "Episode Director": Role.EPISODE_DIRECTOR,
    "Animation Direction": Role.ANIMATION_DIRECTOR,
    "Chief Animation Direction": Role.CHIEF_ANIMATION_DIRECTOR,
    "Key Animation": Role.KEY_ANIMATOR,
    "2nd Key Animation": Role.SECOND_KEY_ANIMATOR,
    "In-Between Animation": Role.IN_BETWEEN,
    "Character Design": Role.CHARACTER_DESIGNER,
    "Storyboard": Role.STORYBOARD,
    # ... (full mapping in implementation)
}
```

#### 1.3: CLI Command

```bash
pixi run scrape-anidb --anime-id 1 --client-id YOUR_CLIENT_ID
```

### Phase 2: ANN Integration (Week 2)

#### 2.1: ANN API Client

**File**: `src/scrapers/ann_scraper.py` (新規作成)

```python
"""ANN スクレイパー — Encyclopedia XML API 経由.

API Documentation: https://www.animenewsnetwork.com/encyclopedia/api.php
Rate Limit: ~1 request per second (undocumented, conservative)
"""

import time
import xml.etree.ElementTree as ET

import httpx
import structlog

from src.models import Anime, Credit, Person, Role

logger = structlog.get_logger()

class ANNClient:
    BASE_URL = "https://cdn.animenewsnetwork.com/encyclopedia/api.xml"
    RATE_LIMIT = 1.0  # conservative: 1 req/sec

    def __init__(self):
        self.last_request = 0.0

    def _throttle(self):
        """Enforce rate limit."""
        elapsed = time.monotonic() - self.last_request
        if elapsed < self.RATE_LIMIT:
            time.sleep(self.RATE_LIMIT - elapsed)
        self.last_request = time.monotonic()

    def get_anime(self, ann_id: int) -> dict:
        """Get anime data by ANN ID.

        Args:
            ann_id: ANN encyclopedia ID

        Returns:
            Dict with anime metadata + staff list
        """
        self._throttle()

        params = {
            "anime": ann_id,
        }

        response = httpx.get(self.BASE_URL, params=params, timeout=30.0)
        response.raise_for_status()

        # Parse XML
        root = ET.fromstring(response.text)
        anime_elem = root.find(".//anime")

        if anime_elem is None:
            raise ValueError(f"Anime {ann_id} not found")

        # Extract data
        anime_data = {
            "ann_id": ann_id,
            "title_en": anime_elem.get("name"),
            "type": anime_elem.get("type"),  # "TV", "OVA", "movie", etc.
        }

        # Extract info elements
        for info in anime_elem.findall(".//info"):
            info_type = info.get("type")
            if info_type == "Main title":
                anime_data["title_en"] = info.text
            elif info_type == "Alternative title" and info.get("lang") == "JA":
                anime_data["title_ja"] = info.text
            elif info_type == "Vintage":
                # Parse year from date string
                year_str = info.text.split("-")[0] if info.text else None
                anime_data["year"] = int(year_str) if year_str and year_str.isdigit() else None

        # Extract staff
        staff = []
        for staff_elem in anime_elem.findall(".//staff"):
            task = staff_elem.find("task")
            person = staff_elem.find("person")

            if task is not None and person is not None:
                staff.append({
                    "ann_id": int(person.get("id")),
                    "name": person.text,
                    "task": task.text,  # "Director", "Animation Director", etc.
                })

        anime_data["staff"] = staff

        logger.info("ann_anime_fetched", ann_id=ann_id, staff_count=len(staff))
        return anime_data

    def search_anime(self, title: str) -> list[dict]:
        """Search anime by title.

        Args:
            title: Anime title (English or Japanese)

        Returns:
            List of matching anime with ANN IDs
        """
        self._throttle()

        params = {
            "title": f"~{title}",  # Prefix search
        }

        response = httpx.get(self.BASE_URL, params=params, timeout=30.0)
        response.raise_for_status()

        # Parse XML
        root = ET.fromstring(response.text)

        results = []
        for anime_elem in root.findall(".//anime"):
            results.append({
                "ann_id": int(anime_elem.get("id")),
                "title_en": anime_elem.get("name"),
                "type": anime_elem.get("type"),
            })

        logger.info("ann_search_complete", query=title, results=len(results))
        return results
```

#### 2.2: ANN Role Mapping

```python
ANN_ROLE_MAP = {
    "Director": Role.DIRECTOR,
    "Episode Director": Role.EPISODE_DIRECTOR,
    "Animation Director": Role.ANIMATION_DIRECTOR,
    "Chief Animation Director": Role.CHIEF_ANIMATION_DIRECTOR,
    "Key Animation": Role.KEY_ANIMATOR,
    "Animation": Role.IN_BETWEEN,
    "Character Design": Role.CHARACTER_DESIGNER,
    "Storyboard": Role.STORYBOARD,
    # ... (full mapping in implementation)
}
```

### Phase 3: ID Mapping & Cross-Referencing (Week 3)

#### 3.1: ID Mapping File

**File**: `data/id_mappings.json`

```json
{
  "anime": {
    "cowboy_bebop": {
      "mal_id": 1,
      "anilist_id": 1,
      "anidb_id": 23,
      "ann_id": 13,
      "wikidata_id": "Q223425"
    },
    "steins_gate": {
      "mal_id": 9253,
      "anilist_id": 9253,
      "anidb_id": 7729,
      "ann_id": 11770,
      "wikidata_id": "Q1190232"
    }
  },
  "persons": {
    "hayao_miyazaki": {
      "mal_id": 1870,
      "anilist_id": 95269,
      "anidb_id": 71,
      "ann_id": 270,
      "wikidata_id": "Q55400"
    }
  }
}
```

#### 3.2: Mapping Helper

```python
def load_id_mappings() -> dict:
    """Load ID mappings from data/id_mappings.json."""
    from src.utils.config import DATA_DIR
    mapping_file = DATA_DIR / "id_mappings.json"

    if not mapping_file.exists():
        return {"anime": {}, "persons": {}}

    with open(mapping_file) as f:
        return json.load(f)


def find_external_id(entity_type: str, known_id_type: str, known_id: int, target_id_type: str) -> int | None:
    """Find external ID using mapping file.

    Args:
        entity_type: "anime" or "persons"
        known_id_type: "mal_id", "anilist_id", etc.
        known_id: Known ID value
        target_id_type: Target ID type to find

    Returns:
        Target ID if found, None otherwise
    """
    mappings = load_id_mappings()

    for entity_key, ids in mappings[entity_type].items():
        if ids.get(known_id_type) == known_id:
            return ids.get(target_id_type)

    return None
```

#### 3.3: Entity Resolution Enhancement

既存の`entity_resolution.py`に外部ID照合を追加：

```python
def cross_source_match_with_external_ids(persons: list[Person]) -> dict[str, str]:
    """Cross-source matching using external IDs.

    Extends existing cross_source_match with AniDB, ANN, Wikidata IDs.
    """
    canonical_map = {}

    # Build ID index
    by_anidb = {}
    by_ann = {}
    by_wikidata = {}

    for p in persons:
        if p.anidb_id:
            by_anidb.setdefault(p.anidb_id, []).append(p.id)
        if p.ann_id:
            by_ann.setdefault(p.ann_id, []).append(p.id)
        if p.wikidata_id:
            by_wikidata.setdefault(p.wikidata_id, []).append(p.id)

    # Merge persons with same external IDs
    for id_dict in [by_anidb, by_ann, by_wikidata]:
        for ext_id, person_ids in id_dict.items():
            if len(person_ids) > 1:
                canonical = person_ids[0]
                for pid in person_ids[1:]:
                    canonical_map[pid] = canonical

    return canonical_map
```

## Testing Strategy

### Unit Tests

```python
# tests/test_anidb_scraper.py
def test_anidb_client_throttle():
    """Test rate limiting works correctly."""

def test_anidb_parse_staff():
    """Test staff parsing from XML."""

def test_anidb_role_mapping():
    """Test AniDB role type to Role enum mapping."""

# tests/test_ann_scraper.py
def test_ann_client_throttle():
    """Test rate limiting works correctly."""

def test_ann_parse_staff():
    """Test staff parsing from XML."""

def test_ann_search():
    """Test anime search by title."""
```

### Integration Tests

```python
# tests/test_external_id_integration.py
def test_id_mapping_load():
    """Test loading ID mappings from JSON."""

def test_cross_source_match_with_external_ids():
    """Test entity resolution with external IDs."""

def test_pipeline_with_external_ids():
    """Test full pipeline with external ID data."""
```

## Migration Strategy

### Step 1: Schema Migration

```python
# src/database.py
def migrate_v4_to_v5(conn: sqlite3.Connection):
    """Add external ID columns."""
    conn.executescript("""
        -- Persons
        ALTER TABLE persons ADD COLUMN anidb_id INTEGER;
        ALTER TABLE persons ADD COLUMN ann_id INTEGER;
        ALTER TABLE persons ADD COLUMN wikidata_id TEXT;

        -- Anime
        ALTER TABLE anime ADD COLUMN anidb_id INTEGER;
        ALTER TABLE anime ADD COLUMN ann_id INTEGER;
        ALTER TABLE anime ADD COLUMN wikidata_id TEXT;

        -- Indexes
        CREATE INDEX IF NOT EXISTS idx_persons_anidb ON persons(anidb_id);
        CREATE INDEX IF NOT EXISTS idx_persons_ann ON persons(ann_id);
        CREATE INDEX IF NOT EXISTS idx_anime_anidb ON anime(anidb_id);
        CREATE INDEX IF NOT EXISTS idx_anime_ann ON anime(ann_id);

        -- Update version
        UPDATE schema_version SET version = 5;
    """)
```

### Step 2: Backfill Existing Data

```bash
# Backfill external IDs for existing anime/persons
pixi run backfill-external-ids --source anidb --limit 100
pixi run backfill-external-ids --source ann --limit 100
```

### Step 3: Enable in Pipeline

```python
# src/pipeline.py
def run_scoring_pipeline(enable_external_ids: bool = False):
    """Run pipeline with optional external ID fetching."""

    if enable_external_ids:
        # Enrich with AniDB data
        anidb_client = AniDBClient(client_id=os.getenv("ANIDB_CLIENT_ID"))
        enrich_from_anidb(persons, anime_list, credits, anidb_client)

        # Enrich with ANN data
        ann_client = ANNClient()
        enrich_from_ann(persons, anime_list, credits, ann_client)
```

## Configuration

### Environment Variables

```bash
# .env
ANIDB_CLIENT_ID=your_client_id    # Get from https://anidb.net/software/add
ANN_ENABLE=true                    # Enable ANN scraping (optional)
EXTERNAL_ID_BACKFILL=false         # Auto-backfill missing external IDs
```

### Rate Limits

```python
# src/utils/config.py
ANIDB_RATE_LIMIT = 2.0    # seconds per request
ANN_RATE_LIMIT = 1.0      # seconds per request
```

## Benefits

### Data Completeness
- ✅ More comprehensive staff credits
- ✅ Episode-level attribution (AniDB)
- ✅ Multiple language support
- ✅ Cross-validation of data accuracy

### Entity Resolution
- ✅ Better name matching using multiple sources
- ✅ Reduced false positives via external ID matching
- ✅ Canonical person/anime IDs across databases

### Analysis Enhancement
- ✅ More accurate credit counts
- ✅ Better coverage of industry professionals
- ✅ Improved PageRank scores (more complete graph)

## Challenges & Mitigations

### Challenge 1: Rate Limits

**Problem**: AniDB (1 req/2s), ANN (~1 req/s) are slow

**Mitigation**:
- Cache all API responses in SQLite
- Incremental updates (only new data)
- Background job for backfilling

### Challenge 2: API Changes

**Problem**: External APIs may change without notice

**Mitigation**:
- Version all scraper implementations
- Graceful degradation (skip if API unavailable)
- Regular monitoring + alerts

### Challenge 3: Data Quality

**Problem**: External data may be incomplete/inaccurate

**Mitigation**:
- Trust score weighting by source reliability
- Manual curation for high-profile persons
- Community-driven corrections

### Challenge 4: Maintenance Burden

**Problem**: 6 data sources = 6x maintenance

**Mitigation**:
- Prioritize AniList + MAL (most complete)
- External IDs as enhancement, not requirement
- Modular scraper architecture (easy to disable/remove)

## Timeline & Effort

| Phase | Task | Effort | Dependencies |
|-------|------|--------|--------------|
| 1 | AniDB scraper implementation | 3 days | AniDB client ID |
| 1 | AniDB role mapping | 1 day | - |
| 1 | CLI + tests | 1 day | - |
| 2 | ANN scraper implementation | 3 days | - |
| 2 | ANN role mapping | 1 day | - |
| 2 | CLI + tests | 1 day | - |
| 3 | ID mapping infrastructure | 2 days | - |
| 3 | Entity resolution enhancement | 2 days | - |
| 3 | Schema migration + backfill | 2 days | - |
| 3 | Integration tests | 2 days | - |
| **Total** | | **18 days (3.6 weeks)** | |

## Priority Assessment

### Low Priority (Nice-to-Have)

This feature is marked as **low priority** because:

1. **Current 4 sources are sufficient** for 95% of use cases
2. **Maintenance burden** is significant (6 sources vs 4)
3. **Rate limits** make initial data collection very slow
4. **Diminishing returns** - incremental data quality improvement

### When to Implement

Consider implementing when:

- ✅ User demand for specific AniDB/ANN data
- ✅ Current data sources show gaps
- ✅ Team bandwidth available for ongoing maintenance
- ✅ API stability confirmed

## Alternatives

### Option 1: Community Contributions

Create a crowd-sourced ID mapping file:

```json
{
  "mappings": {
    "anime": [...],
    "persons": [...]
  },
  "contributors": ["user1", "user2", ...],
  "last_updated": "2026-02-10"
}
```

### Option 2: External Service

Use third-party ID mapping services:

- [Anime-Lists](https://github.com/Anime-Lists/anime-lists) (GitHub)
- [Manami](https://github.com/manami-project/anime-offline-database)

### Option 3: Manual Curation

For high-value persons/anime only:

```csv
person_id,name,anidb_id,ann_id,notes
person_123,"Miyazaki Hayao",71,270,"Verified by curator"
```

## Next Steps

1. ⏳ Get AniDB client ID (if implementing)
2. ⏳ Prototype AniDB scraper (1 week)
3. ⏳ Evaluate data quality improvement
4. ⏳ Decide: Full implementation or alternative approach
5. ⏳ Update TODO.md based on decision

---

**Status**: 📋 Planning Document (not implemented)
**Last Updated**: 2026-02-10
**Author**: Claude Opus 4.6
