# Task: MAL/Jikan BRONZE 28 テーブル schema 確定

**ID**: `12_mal_scraper_jikan/01_schema_design`
**Priority**: 🟠
**Estimated changes**: 約 +500 / -0 lines, 1 file (`src/scrapers/parsers/mal.py` に dataclass 追加)
**Requires senior judgment**: yes (列命名、`display_*` prefix 全列洗い出し、raw vs parsed の境界)
**Blocks**: `12_mal_scraper_jikan/02_parser_extend`
**Blocked by**: (なし)

---

## Goal

Jikan v4 API 全 endpoint から取得可能な情報を raw 近い形で BRONZE 保存するための **28 テーブル構造** を dataclass として確定する (コード生成のみ、データ書き込みは 03 で)。

---

## Hard constraints

- **H1**: viewer rating 系列は **`display_*` prefix 必須**。scoring / edge_weight 参照 禁止 (各 dataclass docstring に明記)。
- **H3**: entity_resolution 既存ロジック不変。person_id = `mal:p{mal_person_id}`、anime_id = `mal:{mal_id}`、character_id = `mal:c{mal_character_id}`、producer_id = `mal:s{mal_producer_id}`、manga_id = `mal:m{mal_manga_id}`。
- **raw 保存原則**: position 文字列 / role 名 / language 名 / relation_type / theme song 行は **正規化なし** で BRONZE 格納。SILVER 移行時に解釈する。
- **既存互換**: `parse_anime_data` / `parse_staff_data` の戻り値 (`BronzeAnime` / `Person` / `Credit`) は変更しない (`mal_scraper.py` 既存呼び出し維持)。新 parser は別関数で追加。

---

## Pre-conditions

- [ ] `git status` clean
- [ ] 既存 `src/scrapers/parsers/mal.py` 把握 (82 行、`parse_anime_data` / `parse_staff_data`)
- [ ] `BronzeWriter` 仕様確認 (`src/scrapers/bronze_writer.py`: `source=mal` ALLOWED 済)
- [ ] Jikan v4 公式 docs 参照: `https://docs.api.jikan.moe/`

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/scrapers/parsers/mal.py` | dataclass 28 個新規追加 (既存 2 関数は不変) |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/scrapers/mal_scraper.py` | 03 で結合 |
| `src/db/schema.py` | BRONZE は parquet 直書きで SQLModel スキーマ外 |
| `src/runtime/models.py` | `BronzeAnime` / `Person` / `Credit` 既存維持 |

---

## 確定 schema (28 テーブル)

### A. anime 系 (13 テーブル)

#### `mal_anime` (1 row / anime)

| 列 | 型 | 出典 | 備考 |
|----|----|------|------|
| `mal_id` | `int` | `mal_id` | PK |
| `url` | `str` | `url` | |
| `title` | `str` | `title` | default title |
| `title_english` | `str\|None` | `title_english` | |
| `title_japanese` | `str\|None` | `title_japanese` | |
| `titles_alt_json` | `str` | `titles[]` 全部 | `[{"type":..., "title":...}]` |
| `synonyms_json` | `str` | type=Synonym | list |
| `type` | `str\|None` | `type` | TV / Movie / OVA / ONA / Special |
| `source` | `str\|None` | `source` | 原作種別 (Manga / Light novel / Original / Game / ...) |
| `episodes` | `int\|None` | `episodes` | |
| `status` | `str\|None` | `status` | Finished Airing / Currently Airing / ... |
| `airing` | `bool` | `airing` | |
| `aired_from` | `str\|None` | `aired.from` ISO | |
| `aired_to` | `str\|None` | `aired.to` ISO | |
| `aired_string` | `str\|None` | `aired.string` | 原文 |
| `duration_raw` | `str\|None` | `duration` | "23 min per ep" 等原文 |
| `rating` | `str\|None` | `rating` | G / PG / R-17+ / ... |
| `season` | `str\|None` | `season` | winter/spring/summer/fall |
| `year` | `int\|None` | `year` | |
| `broadcast_day` | `str\|None` | `broadcast.day` | "Sundays" 等 |
| `broadcast_time` | `str\|None` | `broadcast.time` | "23:30" |
| `broadcast_timezone` | `str\|None` | `broadcast.timezone` | "Asia/Tokyo" |
| `broadcast_string` | `str\|None` | `broadcast.string` | 原文 |
| `synopsis` | `str\|None` | `synopsis` | 原文 |
| `background` | `str\|None` | `background` | 原文 |
| `approved` | `bool` | `approved` | |
| `display_score` | `float\|None` | `score` | **H1: display only** |
| `display_scored_by` | `int\|None` | `scored_by` | **H1** |
| `display_rank` | `int\|None` | `rank` | **H1** |
| `display_popularity` | `int\|None` | `popularity` | **H1** |
| `display_members` | `int\|None` | `members` | **H1** |
| `display_favorites` | `int\|None` | `favorites` | **H1** |
| `image_url` | `str\|None` | `images.jpg.image_url` | |
| `image_url_large` | `str\|None` | `images.jpg.large_image_url` | |
| `trailer_youtube_id` | `str\|None` | `trailer.youtube_id` | |
| `fetched_at` | `str` | runtime | ISO |
| `content_hash` | `str` | runtime | 差分検知用 |

#### `mal_anime_genres` (1 row / anime × genre)

```python
@dataclass
class MalAnimeGenre:
    mal_id: int
    genre_id: int
    name: str
    kind: str  # "genre" / "explicit_genre" / "theme" / "demographic"
```

#### `mal_anime_relations` (1 row / relation)

```python
@dataclass
class MalAnimeRelation:
    mal_id: int
    relation_type: str        # raw "Sequel" / "Prequel" / "Side story" / "Adaptation" / ...
    target_type: str          # "anime" / "manga"
    target_mal_id: int
    target_name: str
    target_url: str | None
```

#### `mal_anime_themes` (1 row / theme song)

```python
@dataclass
class MalAnimeTheme:
    mal_id: int
    kind: str                 # "opening" / "ending"
    position: int             # array index 順
    raw_text: str             # "Tank! by The Seatbelts (eps 1-26)" 原文
```

#### `mal_anime_external` (1 row / external link)

```python
@dataclass
class MalAnimeExternal:
    mal_id: int
    name: str                 # "Official Site" / "AnimeNewsNetwork" / "Wikipedia" / ...
    url: str
```

#### `mal_anime_streaming` (1 row / streaming platform)

```python
@dataclass
class MalAnimeStreaming:
    mal_id: int
    name: str                 # "Crunchyroll" / "Netflix" / ...
    url: str
```

#### `mal_anime_videos_promo` (1 row / promo video)

```python
@dataclass
class MalAnimeVideoPromo:
    mal_id: int
    title: str
    youtube_id: str | None
    url: str | None
    embed_url: str | None
    image_url: str | None     # サムネ URL のみ (画像 binary なし)
```

#### `mal_anime_videos_ep` (1 row / episode video)

```python
@dataclass
class MalAnimeVideoEp:
    mal_id: int
    mal_episode_id: int | None
    episode_label: str        # "Episode 1" 等原文
    url: str | None
    image_url: str | None
```

#### `mal_anime_episodes` (1 row / episode)

```python
@dataclass
class MalAnimeEpisode:
    mal_id: int
    episode_no: int
    title: str | None
    title_japanese: str | None
    title_romanji: str | None
    aired: str | None         # ISO
    filler: bool
    recap: bool
    forum_url: str | None
    synopsis: str | None      # /episodes/{ep} で取れた場合のみ
    display_score: float | None  # H1
```

#### `mal_anime_pictures` (1 row / picture URL)

```python
@dataclass
class MalAnimePicture:
    mal_id: int
    image_url: str
    small_image_url: str | None
    large_image_url: str | None
```

#### `mal_anime_statistics` (1 row / anime, ★ 全列 display_*)

```python
@dataclass
class MalAnimeStatistics:
    mal_id: int
    display_watching: int
    display_completed: int
    display_on_hold: int
    display_dropped: int
    display_plan_to_watch: int
    display_total: int
    display_scores_json: str  # [{"score":10, "votes":..., "percentage":...}, ...]
```

#### `mal_anime_moreinfo` (1 row / anime)

```python
@dataclass
class MalAnimeMoreinfo:
    mal_id: int
    moreinfo: str | None
```

#### `mal_anime_recommendations` (1 row / recommendation pair)

```python
@dataclass
class MalAnimeRecommendation:
    mal_id: int
    recommended_mal_id: int
    recommended_url: str
    votes: int
```

### B. relation tables (4 テーブル)

#### `mal_anime_studios`

```python
@dataclass
class MalAnimeStudio:
    mal_id: int
    mal_producer_id: int
    name: str                 # raw
    kind: str                 # "studio" / "producer" / "licensor"
    url: str | None
```

#### `mal_staff_credits`

```python
@dataclass
class MalStaffCredit:
    mal_id: int
    mal_person_id: int
    person_name: str          # raw "Last, First" 形式
    position: str             # raw "Director" / "Original Creator" / ... 文字列
```

#### `mal_anime_characters`

```python
@dataclass
class MalAnimeCharacter:
    mal_id: int
    mal_character_id: int
    character_name: str
    character_url: str | None
    role: str                 # "Main" / "Supporting"
    display_favorites: int    # H1
    image_url: str | None
```

#### `mal_va_credits`

```python
@dataclass
class MalVaCredit:
    mal_id: int
    mal_character_id: int
    mal_person_id: int
    person_name: str          # raw
    language: str             # "Japanese" / "English" / ... raw
```

### C. persons (2 テーブル)

#### `mal_persons`

```python
@dataclass
class MalPerson:
    mal_person_id: int
    url: str
    name: str                 # "Last, First"
    given_name: str | None
    family_name: str | None
    name_kanji: str | None
    alternate_names_json: str # ["...", "..."]
    websites_json: str        # [{"name":..., "url":...}]
    birthday: str | None      # ISO
    display_favorites: int    # H1
    about: str | None
    image_url: str | None
    fetched_at: str
    content_hash: str
```

#### `mal_person_pictures`

```python
@dataclass
class MalPersonPicture:
    mal_person_id: int
    image_url: str
```

### D. characters (2 テーブル)

#### `mal_characters`

```python
@dataclass
class MalCharacter:
    mal_character_id: int
    url: str
    name: str
    name_kanji: str | None
    nicknames_json: str       # [...]
    display_favorites: int    # H1
    about: str | None
    image_url: str | None
    fetched_at: str
    content_hash: str
```

#### `mal_character_pictures`

```python
@dataclass
class MalCharacterPicture:
    mal_character_id: int
    image_url: str
```

### E. producers / studios (2 テーブル)

#### `mal_producers`

```python
@dataclass
class MalProducer:
    mal_producer_id: int
    url: str
    titles_json: str          # [{"type":..., "title":...}]
    title_default: str
    title_japanese: str | None
    established: str | None   # ISO date (設立年月日)
    about: str | None
    count: int                # 作品数
    display_favorites: int    # H1
    image_url: str | None
    fetched_at: str
    content_hash: str
```

#### `mal_producer_external`

```python
@dataclass
class MalProducerExternal:
    mal_producer_id: int
    name: str
    url: str
```

### F. manga (3 テーブル)

#### `mal_manga`

```python
@dataclass
class MalManga:
    mal_manga_id: int
    url: str
    title: str
    title_english: str | None
    title_japanese: str | None
    titles_alt_json: str
    type: str | None          # Manga / Novel / One-shot / Doujinshi / Manhwa / Manhua
    chapters: int | None
    volumes: int | None
    status: str | None
    publishing: bool
    published_from: str | None
    published_to: str | None
    synopsis: str | None
    background: str | None
    display_score: float | None    # H1
    display_scored_by: int | None  # H1
    display_rank: int | None       # H1
    display_popularity: int | None # H1
    display_members: int | None    # H1
    display_favorites: int | None  # H1
    image_url: str | None
    fetched_at: str
    content_hash: str
```

#### `mal_manga_authors`

```python
@dataclass
class MalMangaAuthor:
    mal_manga_id: int
    mal_person_id: int
    name: str                 # raw
    role: str                 # raw "Story" / "Art" / "Story & Art" / "Original Creator"
```

#### `mal_manga_serializations`

```python
@dataclass
class MalMangaSerialization:
    mal_manga_id: int
    mal_magazine_id: int
    name: str
    url: str | None
```

### G. event / temporal (2 テーブル)

#### `mal_anime_news`

```python
@dataclass
class MalAnimeNews:
    mal_id: int
    mal_news_id: int          # url から抽出
    url: str
    title: str
    date: str                 # ISO
    author_username: str | None
    author_url: str | None
    forum_url: str | None
    intro: str | None
    image_url: str | None
```

#### `mal_anime_schedule`

```python
@dataclass
class MalAnimeSchedule:
    mal_id: int
    day_of_week: str          # "monday" .. "sunday" / "unknown" / "other"
    snapshot_date: str        # 取得時点 ISO date
```

### H. masters (2 テーブル)

#### `mal_master_genres`

```python
@dataclass
class MalMasterGenre:
    genre_id: int
    name: str
    url: str
    count: int
    kind: str                 # "genre" / "explicit_genre" / "theme" / "demographic"
```

#### `mal_master_magazines`

```python
@dataclass
class MalMasterMagazine:
    mal_magazine_id: int
    name: str
    url: str
    count: int
```

---

## H1 (display_*) prefix 全列リスト (検算用)

| table | 列 |
|-------|----|
| `mal_anime` | display_score, display_scored_by, display_rank, display_popularity, display_members, display_favorites |
| `mal_anime_episodes` | display_score |
| `mal_anime_statistics` | display_watching, display_completed, display_on_hold, display_dropped, display_plan_to_watch, display_total, display_scores_json |
| `mal_anime_characters` | display_favorites |
| `mal_persons` | display_favorites |
| `mal_characters` | display_favorites |
| `mal_producers` | display_favorites |
| `mal_manga` | display_score, display_scored_by, display_rank, display_popularity, display_members, display_favorites |

合計: **8 テーブル × 25 列**。lint 候補 (将来): `grep -E "score\|popularity\|favorites\|members\|rank" src/analysis/` で `mal_*.display_*` 参照が SILVER scoring path に流入していないか検査。

---

## Steps

### Step 1: dataclass 28 個書き出し

`src/scrapers/parsers/mal.py` 既存 import / 既存 `parse_anime_data` / `parse_staff_data` の **直前** に dataclass 群を追記 (既存関数は変更しない)。

### Step 2: docstring 追加

各 dataclass の冒頭に:

- `"""raw Jikan v4 response の最小変換版。SILVER 解釈は別タスク。"""`
- 該当 dataclass で `display_*` 列を持つ場合: `"""H1 (No viewer ratings in scoring): display_* 列は scoring / edge_weight に参入してはならない。"""`

### Step 3: enum 候補の文字列定数化

```python
class MalProducerKind:
    STUDIO = "studio"
    PRODUCER = "producer"
    LICENSOR = "licensor"

class MalGenreKind:
    GENRE = "genre"
    EXPLICIT = "explicit_genre"
    THEME = "theme"
    DEMOGRAPHIC = "demographic"

class MalRelationTargetType:
    ANIME = "anime"
    MANGA = "manga"
```

raw 文字列 (`relation_type` / `position` / `language` / `role`) は **enum 化しない** (raw 原則)。

---

## Verification

```bash
# 1. import 確認
pixi run python -c "from src.scrapers.parsers.mal import (
    MalAnimeGenre, MalAnimeRelation, MalAnimeTheme, MalAnimeExternal,
    MalAnimeStreaming, MalAnimeVideoPromo, MalAnimeVideoEp, MalAnimeEpisode,
    MalAnimePicture, MalAnimeStatistics, MalAnimeMoreinfo, MalAnimeRecommendation,
    MalAnimeStudio, MalStaffCredit, MalAnimeCharacter, MalVaCredit,
    MalPerson, MalPersonPicture, MalCharacter, MalCharacterPicture,
    MalProducer, MalProducerExternal, MalManga, MalMangaAuthor,
    MalMangaSerialization, MalAnimeNews, MalAnimeSchedule,
    MalMasterGenre, MalMasterMagazine,
); print('OK')"

# 2. 既存 parser test 影響なし
pixi run test-scoped tests/unit/test_mal_scraper_parse.py

# 3. lint
pixi run lint
```

---

## Stop-if conditions

- [ ] Jikan v4 公式 schema 変更 (新フィールド追加) を検知 → 02 と一緒に再設計
- [ ] `display_*` prefix 列で SILVER 既存読み込みが必要と判明 → user 確認 (Hard Rule H1 違反なら絶対不可)
- [ ] 列数 50+ の dataclass で Python dataclass の制約に当たる → split 検討

---

## Rollback

```bash
git checkout src/scrapers/parsers/mal.py
```

---

## Completion signal

- [ ] 全 28 dataclass 定義が import 可能
- [ ] docstring に raw 原則 + H1 注意書き
- [ ] 既存 `parse_anime_data` / `parse_staff_data` 不変、既存 unit test pass
- [ ] `lint` pass
- [ ] `DONE: 12_mal_scraper_jikan/01_schema_design` 記録
