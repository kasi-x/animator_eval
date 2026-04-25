# Task: ANN BRONZE parquet 多テーブル schema 決定

**ID**: `10_ann_scraper_extend/01_schema_design`
**Priority**: 🟠
**Estimated changes**: 約 +250 / -0 lines, 1 file (`src/scrapers/parsers/ann.py` に dataclass 追加のみ)
**Requires senior judgment**: yes (列命名、raw vs parsed の境界、Hard Rule 遵守)
**Blocks**: `10_ann_scraper_extend/02_parser_extend`
**Blocked by**: (なし)

---

## Goal

ANN XML/HTML から抽出可能な全フィールドを raw 近い形で BRONZE に格納するための **8 テーブル構造** を dataclass として確定する (コード生成のみ、データ書き込みは 03 で)。

---

## Hard constraints

- **H1**: `ratings` 列群は `src_ann_anime` に `display_*` prefix で格納。`scoring` / `edge_weight` 参照 禁止 (docstring で明記)。
- **H3**: entity_resolution 既存ロジック不変。person_id は ANN 内一意 ID (`ann_person_id`) をそのまま key。
- **raw 保存原則**: parser 内の文字列変換は **最小限** (日付正規化、int cast 等)。役職文字列 / 関係種別 / cast role 等は ANN の原文を維持。

---

## Pre-conditions

- [ ] `git status` clean
- [ ] 既存 `AnnAnimeRecord` / `AnnStaffEntry` / `AnnPersonDetail` 列把握 (`src/scrapers/parsers/ann.py:46-80`)
- [ ] `BronzeWriter` 仕様確認 (`src/scrapers/bronze_writer.py`: `source=ann` は `ALLOWED_SOURCES` に既存)

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/scrapers/parsers/ann.py` | dataclass 6 個新規 + 既存 3 個拡張。parser 関数のシグネチャはまだ書かない (02 で実装) |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/scrapers/ann_scraper.py` | 03 で結合 |
| `src/db/schema.py` | BRONZE は parquet 直書きで SQLModel スキーマ外 |

---

## 確定 schema

### 既存テーブル拡張

#### `src_ann_anime` (1 row / anime)

既存列: `ann_id, title_en, title_ja, year, episodes, format, genres, start_date, end_date, titles_alt, fetched_at, content_hash`

追加列:

| 列 | 型 | 出典 | 備考 |
|----|----|------|------|
| `themes` | `list[str]` | `<info type="Themes">` | `;` 区切り split |
| `plot_summary` | `str\|None` | `<info type="Plot Summary">` | 切詰なし |
| `running_time_raw` | `str\|None` | `<info type="Running time">` | 原文 ("23 minutes per episode" 等) |
| `objectionable_content` | `str\|None` | `<info type="Objectionable content">` | TA/TV/MA |
| `opening_themes_json` | `str` | `<info type="Opening Theme">` 複数 | `[{"title":..., "artist":...}]` JSON |
| `ending_themes_json` | `str` | `<info type="Ending Theme">` 複数 | 同上 |
| `insert_songs_json` | `str` | `<info type="Insert song">` 複数 | 同上 |
| `official_websites_json` | `str` | `<info type="Official website">` 複数 | `[{"lang":..., "url":...}]` |
| `vintage_raw` | `str\|None` | `<info type="Vintage">` 原文 | 既存 parse 結果と並存 |
| `image_url` | `str\|None` | `<info type="Picture">` src 属性 | 単一 (複数なら最初) |
| `display_rating_votes` | `int\|None` | `<ratings nb_votes>` | Hard Rule: display のみ |
| `display_rating_weighted` | `float\|None` | `<ratings weighted_score>` | 同上 |
| `display_rating_bayesian` | `float\|None` | `<ratings bayesian_score>` | 同上 |

#### `src_ann_credits` (1 row / staff 参加)

既存: `ann_anime_id, ann_person_id, name_en, task, role`

追加:

| 列 | 型 | 備考 |
|----|----|------|
| `task_raw` | `str` | ANN 原文 (既存 `task` と同値だが明示命名) — 将来 `task` を parsed 版に振替える場合の布石 |
| `gid` | `int\|None` | `<staff gid="N">` ソース固有 ID。ED 順不一致につき `ann_staff_position` として扱う。sorting 目的使用 禁止 (docstring) |

#### `src_ann_persons` (1 row / person)

既存: `ann_id, name_en, name_ja, name_ko, name_zh, names_alt, date_of_birth, hometown, blood_type, website, description`

追加:

| 列 | 型 | 出典 | 備考 |
|----|----|------|------|
| `gender` | `str\|None` | `<div id="infotype-N"><strong>Gender</strong>` | raw ("Male"/"Female"/"Non-binary" 等) |
| `nickname` | `str\|None` | 同様 | |
| `family_name_ja` | `str\|None` | `infotype-12` | 個別 |
| `given_name_ja` | `str\|None` | `infotype-13` | 個別 |
| `height_raw` | `str\|None` | raw (`"5'8\""` / `"173 cm"` 混在許容) | |
| `image_url` | `str\|None` | `<img id="pic" src=...>` | |
| `description_raw` | `str\|None` | 2000 char truncate 撤廃。原文丸ごと | `description` は deprecated、将来削除 |
| `credits_json` | `str` | `<div id="credit_list">` raw HTML → JSON 配列 | `[{"anime_id":..., "task":..., "year":...}]` — 作品横断キャリア |
| `alt_names_json` | `str` | alternative names 節 | `[{"lang":..., "name":...}]` |

### 新規テーブル 6 種

#### `src_ann_cast` (1 row / anime × character × VA)

```python
@dataclass
class AnnCastEntry:
    ann_anime_id: int
    ann_person_id: int            # voice actor
    voice_actor_name: str         # <person>text</person>
    cast_role: str                # <cast lang="JA">, raw ("Main" / "Supporting" / "Japanese" 等)
    character_name: str           # <character>text</character>
    character_id: int | None      # <character id="N"> 無い場合あり
```

Hard Rule 4: 声優はクレジット構造的事実のみ。「人気度」類 不参入。

#### `src_ann_company` (1 row / anime × company 関係)

```python
@dataclass
class AnnCompanyEntry:
    ann_anime_id: int
    company_id: int | None        # <company id="N"> 無い場合あり (外部企業等)
    company_name: str             # <company>text</company>
    task: str                     # raw ("Animation Production" / "Distributor" / "Licensed by" 等)
```

#### `src_ann_episodes` (1 row / anime × episode × lang)

```python
@dataclass
class AnnEpisodeEntry:
    ann_anime_id: int
    episode_num: str              # "1" / "1a" / "OP" 等あり得る → str
    lang: str                     # "EN" / "JA" / ... raw
    title: str                    # <title>text</title>
    aired_date: str | None        # <episode><aired> あれば
```

#### `src_ann_releases` (1 row / DVD-BD リリース)

```python
@dataclass
class AnnReleaseEntry:
    ann_anime_id: int
    release_date: str | None      # ISO or 原文
    product_title: str            # raw
    href: str | None              # ANN 内リンク
    region: str | None            # "NA" / "JP" 等, element 名から派生
```

#### `src_ann_news` (1 row / news 言及)

```python
@dataclass
class AnnNewsEntry:
    ann_anime_id: int
    datetime: str                 # ISO
    title: str                    # raw
    href: str | None
```

件数上限は XML の返却分のみ (scraper 側で絞らない)。

#### `src_ann_related` (1 row / 関連作品関係)

```python
@dataclass
class AnnRelatedEntry:
    ann_anime_id: int             # source
    target_ann_id: int            # related target
    rel: str                      # raw ("sequel" / "prequel" / "spinoff" / "remake" / "summary" / ...)
    direction: str                # "prev" / "next" (XML 要素名から派生)
```

---

## Steps

### Step 1: dataclass 書き出し

`src/scrapers/parsers/ann.py` 既存 dataclass 群の直後に以下を追記:

- `AnnCastEntry`, `AnnCompanyEntry`, `AnnEpisodeEntry`, `AnnReleaseEntry`, `AnnNewsEntry`, `AnnRelatedEntry`
- `AnnAnimeRecord` に新規 12 列追加 (field default あり、既存位置変えない)
- `AnnStaffEntry` に `task_raw`, `gid` 追加
- `AnnPersonDetail` に新規 8 列追加

### Step 2: `_HTML_LABEL_MAP` 拡張

`gender` / `nickname` / `height` / `family name` / `given name` / `picture` / `credit list` のラベル文字列を追加。正規化ロジック (05 以降の parser 側) のフック用。

### Step 3: docstring 追記

各 dataclass に「raw 原則」「Hard Rule: rating は display のみ」「gid は ED 順不一致」を明記。

---

## Verification

```bash
# 1. import 確認
pixi run python -c "from src.scrapers.parsers.ann import (
    AnnAnimeRecord, AnnStaffEntry, AnnPersonDetail,
    AnnCastEntry, AnnCompanyEntry, AnnEpisodeEntry,
    AnnReleaseEntry, AnnNewsEntry, AnnRelatedEntry,
); print('OK')"

# 2. lint
pixi run lint

# 3. 既存 test 影響なし確認 (parser test のみ)
pixi run test-scoped tests/scrapers/test_ann_parser.py  # ファイル存在するなら
```

---

## Stop-if conditions

- [ ] 既存 `parse_anime_xml` / `parse_person_html` の戻り値 shape 変更必要と判明 → 02 と一緒に設計するため中断
- [ ] `BronzeWriter` が `ALLOWED_TABLES` 制約を持つと判明 → 別 card 起票 (現状は source のみ制約)
- [ ] Hard Rule の display rating 保存が HR 責任者的に NG と判明 → user に確認

---

## Rollback

```bash
git checkout src/scrapers/parsers/ann.py
```

---

## Completion signal

- [ ] 全 dataclass 定義が import 可能
- [ ] 列ドキュメント (本カード § 確定 schema) が実装と一致
- [ ] `lint` pass
- [ ] `DONE: 10_ann_scraper_extend/01_schema_design` 記録
