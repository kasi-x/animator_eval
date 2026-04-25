# Task: MAL/Jikan parser 拡張 (18+ 関数 / 28 テーブル全網羅)

**ID**: `12_mal_scraper_jikan/02_parser_extend`
**Priority**: 🟠
**Estimated changes**: 約 +900 / -10 lines, 1 file (`src/scrapers/parsers/mal.py`) + tests
**Requires senior judgment**: yes (Jikan 応答の null 安全 / display_* 流入禁止 / titles_alt 多言語)
**Blocks**: `12_mal_scraper_jikan/03_scraper_phases`
**Blocked by**: `12_mal_scraper_jikan/01_schema_design`

---

## Goal

Jikan v4 全 endpoint レスポンスから 28 dataclass を埋める parser 関数群を実装する。**追加 HTTP fetch なし** (引数で渡された raw dict のみ消費)。

---

## Hard constraints

- **H1**: viewer rating 系列は dataclass 定義通り `display_*` prefix 列に格納。raw key からのコピー時に必ず prefix 付与。
- **raw 原則**: `position` / `relation_type` / `role` / `language` / `theme song raw_text` は **正規化しない** (`.strip()` のみ可)。
- **null 安全**: Jikan は欠損フィールドを `null` で返す。全 `.get()` を `or {}` / `or []` でガード。
- **既存関数互換**: `parse_anime_data` / `parse_staff_data` の戻り値 shape は変更しない (`mal_scraper.py` 既存呼び出し維持)。新 parser は別関数で追加。

---

## Pre-conditions

- [ ] Card 01 完了 (28 dataclass 定義済)
- [ ] サンプル取得 (`tests/fixtures/scrapers/mal/` に保存):

```bash
mkdir -p tests/fixtures/scrapers/mal
curl -s 'https://api.jikan.moe/v4/anime/1/full' > tests/fixtures/scrapers/mal/anime_1_full.json
curl -s 'https://api.jikan.moe/v4/anime/1/staff' > tests/fixtures/scrapers/mal/anime_1_staff.json
curl -s 'https://api.jikan.moe/v4/anime/1/characters' > tests/fixtures/scrapers/mal/anime_1_characters.json
curl -s 'https://api.jikan.moe/v4/anime/1/episodes' > tests/fixtures/scrapers/mal/anime_1_episodes.json
curl -s 'https://api.jikan.moe/v4/anime/1/external' > tests/fixtures/scrapers/mal/anime_1_external.json
curl -s 'https://api.jikan.moe/v4/anime/1/streaming' > tests/fixtures/scrapers/mal/anime_1_streaming.json
curl -s 'https://api.jikan.moe/v4/anime/1/videos' > tests/fixtures/scrapers/mal/anime_1_videos.json
curl -s 'https://api.jikan.moe/v4/anime/1/pictures' > tests/fixtures/scrapers/mal/anime_1_pictures.json
curl -s 'https://api.jikan.moe/v4/anime/1/statistics' > tests/fixtures/scrapers/mal/anime_1_statistics.json
curl -s 'https://api.jikan.moe/v4/anime/1/moreinfo' > tests/fixtures/scrapers/mal/anime_1_moreinfo.json
curl -s 'https://api.jikan.moe/v4/anime/1/recommendations' > tests/fixtures/scrapers/mal/anime_1_recs.json
curl -s 'https://api.jikan.moe/v4/anime/1/news' > tests/fixtures/scrapers/mal/anime_1_news.json
curl -s 'https://api.jikan.moe/v4/people/1/full' > tests/fixtures/scrapers/mal/person_1_full.json
curl -s 'https://api.jikan.moe/v4/people/1/pictures' > tests/fixtures/scrapers/mal/person_1_pictures.json
curl -s 'https://api.jikan.moe/v4/characters/1/full' > tests/fixtures/scrapers/mal/character_1_full.json
curl -s 'https://api.jikan.moe/v4/characters/1/pictures' > tests/fixtures/scrapers/mal/character_1_pictures.json
curl -s 'https://api.jikan.moe/v4/producers/1/full' > tests/fixtures/scrapers/mal/producer_1_full.json
curl -s 'https://api.jikan.moe/v4/producers/1/external' > tests/fixtures/scrapers/mal/producer_1_external.json
curl -s 'https://api.jikan.moe/v4/manga/1/full' > tests/fixtures/scrapers/mal/manga_1_full.json
curl -s 'https://api.jikan.moe/v4/genres/anime' > tests/fixtures/scrapers/mal/master_genres.json
curl -s 'https://api.jikan.moe/v4/magazines' > tests/fixtures/scrapers/mal/master_magazines.json
curl -s 'https://api.jikan.moe/v4/schedules?filter=monday' > tests/fixtures/scrapers/mal/schedules_monday.json
sleep 0.4 # 各 curl 間に rate limit 配慮
```

→ 22 fixture json (~1MB)。コミット対象。

---

## Files to modify

| File | 変更 |
|------|------|
| `src/scrapers/parsers/mal.py` | 18 parser 関数追加 |
| `tests/unit/test_mal_scraper_parse.py` | 50+ test ケース追加 |
| `tests/fixtures/scrapers/mal/*.json` | fixture 22 件追加 |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/scrapers/mal_scraper.py` | 03 で結合 |
| 既存 `parse_anime_data` / `parse_staff_data` | 互換維持 |

---

## 実装する parser 関数

### A. anime 系 (12 関数)

```python
def parse_anime_full(raw: dict) -> tuple[
    MalAnimeRecord,
    list[MalAnimeGenre],
    list[MalAnimeRelation],
    list[MalAnimeTheme],
    list[MalAnimeExternal],
    list[MalAnimeStreaming],
    list[MalAnimeStudio],
]:
    """`/anime/{id}/full` 全フィールド抽出。relations / themes / external / streaming / studios も同時抽出 (full に含まれるため)。

    H1: score / scored_by / rank / popularity / members / favorites は display_* prefix へ。
    """

def parse_anime_external(mal_id: int, raw: dict) -> list[MalAnimeExternal]: ...
def parse_anime_streaming(mal_id: int, raw: dict) -> list[MalAnimeStreaming]: ...
def parse_anime_videos(mal_id: int, raw: dict) -> tuple[list[MalAnimeVideoPromo], list[MalAnimeVideoEp]]: ...
def parse_anime_episodes(mal_id: int, raw: dict) -> list[MalAnimeEpisode]: ...
def parse_anime_pictures(mal_id: int, raw: dict) -> list[MalAnimePicture]: ...
def parse_anime_statistics(mal_id: int, raw: dict) -> MalAnimeStatistics: ...
def parse_anime_moreinfo(mal_id: int, raw: dict) -> MalAnimeMoreinfo: ...
def parse_anime_recommendations(mal_id: int, raw: dict) -> list[MalAnimeRecommendation]: ...
def parse_anime_news(mal_id: int, raw: dict) -> list[MalAnimeNews]: ...
def parse_anime_characters_va(mal_id: int, raw: dict) -> tuple[list[MalAnimeCharacter], list[MalVaCredit]]: ...
def parse_anime_staff_full(mal_id: int, raw: dict) -> list[MalStaffCredit]: ...
```

`parse_anime_full` 詳細:
- `data` field 1 段下り
- `images.jpg` から `image_url` / `large_image_url` 抽出
- `titles[]` 全部 → `titles_alt_json` (json.dumps)
- `genres + explicit_genres + themes + demographics` → `MalAnimeGenre` 4 種 kind 区別
- `relations[].entry[]` → `MalAnimeRelation` (relation_type は raw "Adaptation" / "Sequel" / ...)
- `theme.openings[]` / `theme.endings[]` → `MalAnimeTheme` (raw_text そのまま)
- `external[]` → `MalAnimeExternal`
- `streaming[]` → `MalAnimeStreaming`
- `studios + producers + licensors` → `MalAnimeStudio` (kind 分けて 3 種)
- `trailer.youtube_id` のみ抽出 (動画 binary 不要)
- score / scored_by / rank / popularity / members / favorites → `display_*` prefix
- `aired.from` / `aired.to` ISO 文字列維持
- `broadcast.day/time/timezone/string` 全列保存
- `content_hash` = sha1(json.dumps(raw, sort_keys=True))

### B. persons 系 (2 関数)

```python
def parse_person_full(raw: dict) -> MalPerson:
    """`/people/{id}/full`。anime / voices / manga 出演リストは別 endpoint で credit として生成。"""

def parse_person_pictures(mal_person_id: int, raw: dict) -> list[MalPersonPicture]: ...
```

注意: `/people/{id}/full` の `anime / voices / manga` 配列は **credit 生成に使わない** (Phase A の `/anime/{id}/staff` + `/anime/{id}/characters` で取得済の正の credit と重複)。Person の static info (name / kanji / birthday / about / favorites) のみ抽出。

### C. characters 系 (2 関数)

```python
def parse_character_full(raw: dict) -> MalCharacter: ...
def parse_character_pictures(mal_character_id: int, raw: dict) -> list[MalCharacterPicture]: ...
```

`anime / voices` 配列は同上、credit 生成不使用。

### D. producers 系 (2 関数)

```python
def parse_producer_full(raw: dict) -> tuple[MalProducer, list[MalProducerExternal]]: ...
def parse_producer_external(mal_producer_id: int, raw: dict) -> list[MalProducerExternal]: ...
```

`/producers/{id}/full` の `external[]` は full レスポンスに含まれるが、completeness 確保のため `/producers/{id}/external` も別途叩く設計 (Card 03)。parser は両方扱える。

### E. manga 系 (1 関数)

```python
def parse_manga_full(raw: dict) -> tuple[
    MalManga,
    list[MalMangaAuthor],
    list[MalMangaSerialization],
    list[MalAnimeRelation],   # manga 同士 + manga→anime relations
]:
    """`/manga/{id}/full`。authors[] / serializations[] / relations[] も同時抽出。"""
```

manga relations は anime と同じ `MalAnimeRelation` dataclass を再利用 (mal_id=manga_id, target_type="anime"/"manga")。テーブル分離が必要なら別 dataclass `MalMangaRelation` 起こす (本カードでは共有で書き始め、Card 03 で要分離なら refactor)。

### F. event / temporal (1 関数)

```python
def parse_schedules(raw: dict, day_of_week: str, snapshot_date: str) -> list[MalAnimeSchedule]:
    """`/schedules?filter={day}` のレスポンスから (mal_id, day_of_week) ペアを抽出。

    snapshot_date: 取得時点 (Card 03 で `datetime.now().date().isoformat()`)。
    """
```

### G. masters (2 関数)

```python
def parse_master_genres(raw: dict, kind: str) -> list[MalMasterGenre]:
    """kind ∈ {"genre", "explicit_genre", "theme", "demographic"}"""

def parse_master_magazines(raw: dict) -> list[MalMasterMagazine]: ...
```

---

## Steps

### Step 1: A 群 (anime 系 12 関数)

`parse_anime_full` から着手。null 安全確認のため `tests/fixtures/scrapers/mal/anime_1_full.json` で 1 件 round-trip。

### Step 2: B-G 群 (10 関数)

順次。各関数 5-10 unit test。

### Step 3: 既存 `parse_anime_data` リファクタ判定

新 `parse_anime_full` と機能重複。**統合しない** (既存呼び出し側 = `mal_scraper.py` の互換維持)。コメントで `parse_anime_data` を deprecated 扱いとし、Card 03 で `parse_anime_full` への移行を実施。

### Step 4: テスト

```bash
pixi run test-scoped tests/unit/test_mal_scraper_parse.py -v
```

最低 50 ケース:
- 各 parser 関数 1 happy + 1 null 欠損
- `display_*` prefix 検証 5 ケース (parser が score を display_* に格納していること)
- `titles_alt_json` JSON 整形 1
- `relations` 多重 / 空 1 + 1
- `theme.openings` 配列順保持 1
- `studios + producers + licensors` kind 分離 1

### Step 5: lint

```bash
pixi run lint
pixi run format
```

---

## Verification

```bash
# 1. parser unit test all pass
pixi run test-scoped tests/unit/test_mal_scraper_parse.py

# 2. fixture round-trip (1 件 anime full → dataclass → 全列埋まり確認)
pixi run python -c "
import json
from src.scrapers.parsers.mal import parse_anime_full
raw = json.load(open('tests/fixtures/scrapers/mal/anime_1_full.json'))
anime, genres, rels, themes, ext, stream, studios = parse_anime_full(raw)
assert anime.mal_id == 1
assert anime.display_score is not None or anime.display_score is None  # 型のみ
assert all(s.kind in {'studio','producer','licensor'} for s in studios)
print('OK', anime.title, len(genres), len(rels), len(themes))
"

# 3. lint
pixi run lint
```

---

## Stop-if conditions

- [ ] fixture 取得段階で 429 連発 → `sleep 1.0` に上げて再試行、それでもダメなら `cache_store` 既存応答利用
- [ ] Jikan API スキーマが docs と乖離 → 実応答 fixture を正とし parser を実応答に合わせる
- [ ] 同一 person が `staff` と `characters[].voice_actors` で重複 → BRONZE では別テーブルなので重複 OK (SILVER で名寄せ)

---

## Rollback

```bash
git checkout src/scrapers/parsers/mal.py tests/unit/test_mal_scraper_parse.py
rm -rf tests/fixtures/scrapers/mal
```

---

## Completion signal

- [ ] 18 parser 関数実装、各 5-10 unit test pass
- [ ] fixture 22 件コミット
- [ ] H1 chk: `grep -E "anime\.score\s*=" src/scrapers/parsers/mal.py` が **0 件** (全て `display_score` 経由)
- [ ] `lint` / `format` pass
- [ ] 既存 `parse_anime_data` / `parse_staff_data` の test 不変
- [ ] `DONE: 12_mal_scraper_jikan/02_parser_extend` 記録
