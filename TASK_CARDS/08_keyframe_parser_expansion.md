# Task: keyframe parser / scraper 拡張 — 落としてる 40+ フィールド回収

**ID**: `08_keyframe_parser_expansion`
**Priority**: 🟠 (構造情報の主要漏れ、特に person→studio 所属関係)
**Estimated changes**: 約 +400 lines, 4-5 files
**Requires senior judgment**: yes (schema 拡張方針 / dataclass 設計)
**Blocks**: keyframe 全件 scrape の本実施
**Blocked by**: なし

---

## 背景と目的

### 現状

`src/scrapers/parsers/keyframe.py` の `parse_credits_from_data()` は preloadData JSON から **4 フィールド** (episode, role_ja, role_en, person_id, name_ja, name_en) のみ抽出。実際の preloadData には **40+ フィールド** あり、約 90% を捨てている。

**サンプル調査結果** (one-piece): preloadData top-level + nested `anilist` + `staff[].studio` + `settings.categories` で大量の構造情報が利用可能。

### 狙い

落としているフィールドを全部回収し、再 scrape で **完全な BRONZE keyframe** を構築する。raw HTML をローカル保存することで、将来 parser 改修時に再 scrape を不要化。

---

## Hard constraints

- H1 anime.score 等の主観メタは scoring path に入れない (CLAUDE.md)
- H3 entity resolution 不変
- 既存不完全 BRONZE (`result/bronze/source=keyframe/`) は wipe 前に **退避** (`result/bronze/source=keyframe.bak-<date>/`)
- 既存 checkpoint (`data/keyframe/checkpoint.json.bak-20260425`) は touch 禁止
- HTTP delay は **5.0s 以上**。3.0s で連続 429 確認済 (2026-04-25)
- max_retries=3 → 5 に引き上げ、`Retry-After` header 尊重

---

## Pre-conditions

- [ ] `git status` clean
- [ ] `data/keyframe/checkpoint.json.bak-20260425` 存在
- [ ] keyframe-staff-list.com sitemap 到達可能 (`curl -sI <sitemap>` → 200)
- [ ] BRONZE 退避先未占有

---

## 取得対象フィールド (現状未取得 / 抽出仕様)

### A. anime レベル (preloadData top-level + anilist nest)

| 出力列 | preloadData path | 型 | 備考 |
|--------|------------------|----|----|
| `kf_uuid` | `uuid` | str | ページ unique id |
| `kf_saving_id` | `savingId` | int | |
| `kf_author` | `author` | str | 編集者 list (カンマ区切り文字列) |
| `kf_status` | `status` | str | IN_PROGRESS / COMPLETE |
| `kf_comment` | `comment` | str / null | 編集者ノート |
| `title_ja` | `anilist.title.native` | str | 日本語正式名 |
| `title_en` | `anilist.title.english` | str | (現行: top-level `title` を使用、anilist 経由が正確) |
| `title_romaji` | `anilist.title.romaji` | str | 国際標準ロマ字 |
| `synonyms` | `anilist.synonyms` | list[str] | 多言語旧称 (entity resolution に必須) |
| `format` | `anilist.format` | str | TV/MOVIE/OVA/SPECIAL |
| `episodes` | `anilist.episodes` | int / null | 全話数 |
| `season` | `anilist.season` | str | FALL/WINTER/SPRING/SUMMER |
| `season_year` | `anilist.seasonYear` | int | (現行取得済) |
| `start_date` | `anilist.startDate` | dict | {year, month, day} |
| `end_date` | `anilist.endDate` | dict | {year, month, day} |
| `cover_image_url` | `anilist.coverImage.extraLarge` | str | |
| `is_adult` | `anilist.isAdult` | bool | |
| `anilist_status` | `anilist.status` | str | RELEASING / FINISHED |

### B. anime studios (anilist.studios.edges[])

専用テーブル `bronze_keyframe_anime_studios`:

| 出力列 | path | 備考 |
|--------|------|------|
| `anime_id` | (parent) | keyframe:slug |
| `studio_name` | `node.name` | |
| `is_main` | `isMain` | 主制作 vs 協力/放送局区分 |

### C. credit レベル拡張 (現行 dict に列追加)

| 出力列 | path | 備考 |
|--------|------|------|
| `section_name` | `menu.credits[*].name` | 例: Main Staff / Cast / Animation / Sound / Opening / Ending |
| `episode_title` | `menu.name` | 各話タイトル付きの場合 (例: "#01「夜明けの冒険！」") から episode 番号以外を抽出 |
| `menu_note` | `menu.note` | 各話注釈 |
| `studio_ja` | `staff.studio.ja` | person 所属スタジオ ja |
| `studio_en` | `staff.studio.en` | person 所属スタジオ en |
| `studio_id` | `staff.studio.id` or `staff.id` (when isStudio=true) | keyframe studio numeric id |
| `studio_is_studio` | `staff.studio.isStudio` | bool、true なら独立スタジオ entry |
| `is_studio_role` | derived | `staff.isStudio` 自体を保持 (現状 skip → 保持に変更) |

### D. studio master (専用テーブル `bronze_keyframe_studios`)

isStudio=true の独立 entry を集約:

| 出力列 | 備考 |
|--------|------|
| `studio_id` | keyframe numeric id |
| `name_ja` | |
| `name_en` | |

### E. settings (anime レベルメタ、専用テーブル `bronze_keyframe_settings`)

20 件の `categories[].name` を anime ごとに保持。role 正規化マッピングに使用。

| 出力列 | 備考 |
|--------|------|
| `anime_id` | |
| `category_name` | "Main Staff" 等 |
| `category_order` | list 内 index |

`delimiters`, `episodeDelimiters`, `roleDelimiters`, `staffDelimiters` は anime レベルカラムとして 1 行 1 anime で保持 (parsing 規則のソース別 metadata)。

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/scrapers/parsers/keyframe.py` | A〜E 全フィールド抽出、`parse_credits_from_data` 戻り値拡張、新関数 `parse_anime_meta`, `parse_studios`, `parse_settings` 追加 |
| `src/scrapers/keyframe_scraper.py` | A 全フィールド書き出し、studios / studios_master / settings の `BronzeWriter` 追加、raw HTML 保存ロジック追加、delay 既定 5.0s に引き上げ、max_retries=5 に拡張、`Retry-After` 尊重 |
| `src/runtime/models.py` | keyframe 専用 dataclass 追加 (詳細下記) |
| `src/db/schema.py` | `bronze_keyframe_anime`, `bronze_keyframe_anime_studios`, `bronze_keyframe_studios`, `bronze_keyframe_settings` 追加 (or 共通テーブル拡張、下記判断) |
| `tests/scrapers/test_keyframe_parser.py` | 拡張内容の unit test 追加 |
| `tests/fixtures/scrapers/keyframe/sample_one-piece.html` | one-piece の preloadData 含む fixture HTML (実物 1 件) |

### 判断: schema 拡張方針

**推奨 (a)**: keyframe 専用テーブル `bronze_keyframe_anime`, `bronze_keyframe_credits` 等を追加 (共通 BronzeAnime / Credit には触らない)

理由:
- 他 source に keyframe specific フィールド (kf_uuid, settings 等) が混入すると汚染
- v55 以降の共通 schema は安定運用、column 追加は costly
- SILVER 統合時にだけ共通スキーマへ正規化すればよい

**代替案 (b)**: 共通 BronzeAnime に optional column 追加 (`source_specific_meta` JSON 列等)

採用は (a) で進める。Step 3 で実装。

---

## Files to NOT touch

| File | 理由 |
|------|------|
| `data/keyframe/checkpoint.json.bak-20260425` | safety net |
| `result/bronze/source=keyframe.bak-<date>/` (退避後) | safety net |
| 他 source の parsers / scrapers | scope 外 |
| 共通 `BronzeAnime` / `Credit` dataclass | (a) 案では触らない |

---

## Steps

### Step 0: 既存不完全 BRONZE 退避

```bash
mv result/bronze/source=keyframe result/bronze/source=keyframe.bak-$(date +%Y%m%d)
mv data/keyframe/checkpoint.json data/keyframe/checkpoint.json.dirty-$(date +%Y%m%d)  # .bak は既にある
ls result/bronze/source=keyframe.bak-*/  # 確認
```

### Step 1: fixture 作成

```bash
curl -s -A "Mozilla/5.0" --max-time 10 https://keyframe-staff-list.com/staff/one-piece > tests/fixtures/scrapers/keyframe/sample_one-piece.html
ls -la tests/fixtures/scrapers/keyframe/
```

### Step 2: parser 拡張

`src/scrapers/parsers/keyframe.py`:

```python
# 新関数群
def parse_anime_meta(data: dict, slug: str) -> dict:
    """preloadData top-level + anilist nest から anime メタを抽出。"""
    a = data.get("anilist") or {}
    title = a.get("title") or {}
    return {
        "kf_uuid": data.get("uuid"),
        "kf_saving_id": data.get("savingId"),
        "kf_author": data.get("author"),
        "kf_status": data.get("status"),
        "kf_comment": data.get("comment"),
        "title_ja": title.get("native"),
        "title_en": title.get("english") or data.get("title"),
        "title_romaji": title.get("romaji"),
        "synonyms": a.get("synonyms") or [],
        "format": a.get("format"),
        "episodes": a.get("episodes"),
        "season": a.get("season"),
        "season_year": a.get("seasonYear"),
        "start_date": a.get("startDate") or {},
        "end_date": a.get("endDate") or {},
        "cover_image_url": (a.get("coverImage") or {}).get("extraLarge"),
        "is_adult": a.get("isAdult"),
        "anilist_status": a.get("status"),
        "anilist_id": a.get("id") or _parse_int(data.get("anilistId")),
        "slug": slug,
        "delimiters": (data.get("settings") or {}).get("delimiters"),
        "episode_delimiters": (data.get("settings") or {}).get("episodeDelimiters"),
        "role_delimiters": (data.get("settings") or {}).get("roleDelimiters"),
        "staff_delimiters": (data.get("settings") or {}).get("staffDelimiters"),
    }


def parse_anime_studios(data: dict) -> list[dict]:
    """anilist.studios.edges[] から studio リスト。"""
    edges = ((data.get("anilist") or {}).get("studios") or {}).get("edges") or []
    return [
        {"studio_name": e["node"]["name"], "is_main": bool(e.get("isMain"))}
        for e in edges
        if e.get("node", {}).get("name")
    ]


def parse_settings_categories(data: dict) -> list[dict]:
    """settings.categories[] の role 標準分類。"""
    cats = (data.get("settings") or {}).get("categories") or []
    return [
        {"category_name": c.get("name"), "category_order": i}
        for i, c in enumerate(cats)
        if c.get("name")
    ]


def parse_credits_from_data(data: dict, slug: str) -> list[dict]:
    """拡張: section_name / studio / menu_note / episode_title / is_studio_role 追加。
    isStudio=true entry も保持 (現 skip 削除)。
    """
    credits: list[dict] = []
    studio_master: dict[int, dict] = {}  # studio_id -> {name_ja, name_en}

    for menu in data.get("menus", []):
        menu_name = menu.get("name", "")
        episode_num = _extract_episode_num(menu_name)
        episode_title = _extract_episode_title(menu_name)  # 新 helper
        menu_note = menu.get("note")

        for section in menu.get("credits", []):
            section_name = section.get("name")

            for role_entry in section.get("roles", []):
                role_ja = role_entry.get("original", "")
                role_en = role_entry.get("name", "")

                for staff in role_entry.get("staff", []):
                    is_studio_role = bool(staff.get("isStudio"))
                    studio_obj = staff.get("studio") or {}
                    person_id = staff.get("id")
                    name_ja = staff.get("ja", "")
                    name_en = staff.get("en", "")

                    if person_id is None and not (name_ja or name_en):
                        continue

                    # studio master collection
                    if is_studio_role and person_id:
                        studio_master.setdefault(person_id, {
                            "studio_id": person_id,
                            "name_ja": name_ja,
                            "name_en": name_en,
                        })

                    credits.append({
                        "episode": episode_num,
                        "episode_title": episode_title,
                        "menu_note": menu_note,
                        "section_name": section_name,
                        "role_ja": role_ja,
                        "role_en": role_en,
                        "person_id": person_id,
                        "name_ja": name_ja,
                        "name_en": name_en,
                        "is_studio_role": is_studio_role,
                        "studio_ja": studio_obj.get("ja"),
                        "studio_en": studio_obj.get("en"),
                        "studio_id": studio_obj.get("id"),
                        "studio_is_studio": bool(studio_obj.get("isStudio")) if studio_obj else None,
                    })

    # studio_master を別出力するため、tuple で返すか、scraper 側で再集計
    return credits  # studio_master は scraper 側で再構築 (parse_credits の純粋関数性を保つ)


def _extract_episode_title(menu_name: str) -> str | None:
    """'#01「夜明けの冒険！」' から '夜明けの冒険！' を抽出。"""
    m = re.search(r"[「『](.+?)[」』]", menu_name)
    return m.group(1) if m else None


def _parse_int(v) -> int | None:
    if v is None: return None
    try: return int(v)
    except (ValueError, TypeError): return None
```

### Step 3: 専用 dataclass 追加 (`src/runtime/models.py`)

```python
class BronzeKeyframeAnime(BaseModel):
    # 共通 BronzeAnime とは独立、keyframe 全フィールド保持
    id: str  # keyframe:slug
    slug: str
    kf_uuid: str | None
    kf_saving_id: int | None
    kf_author: str | None
    kf_status: str | None
    kf_comment: str | None
    title_ja: str | None
    title_en: str | None
    title_romaji: str | None
    synonyms: list[str] = Field(default_factory=list)
    format: str | None
    episodes: int | None
    season: str | None
    season_year: int | None
    start_date: dict | None
    end_date: dict | None
    cover_image_url: str | None
    is_adult: bool | None
    anilist_status: str | None
    anilist_id: int | None
    delimiters: str | None
    episode_delimiters: str | None
    role_delimiters: str | None
    staff_delimiters: str | None


class BronzeKeyframeCredit(BaseModel):
    # 共通 Credit と独立
    person_id: str  # keyframe:p_<id>
    anime_id: str
    episode: int
    episode_title: str | None
    menu_note: str | None
    section_name: str | None
    role_ja: str | None
    role_en: str | None
    name_ja: str | None
    name_en: str | None
    is_studio_role: bool
    studio_ja: str | None
    studio_en: str | None
    studio_id: int | None
    studio_is_studio: bool | None
    source: str = "keyframe"


class BronzeKeyframeAnimeStudio(BaseModel):
    anime_id: str
    studio_name: str
    is_main: bool


class BronzeKeyframeStudio(BaseModel):
    studio_id: int
    name_ja: str | None
    name_en: str | None


class BronzeKeyframeSettingsCategory(BaseModel):
    anime_id: str
    category_name: str
    category_order: int
```

### Step 4: scraper 拡張 (`src/scrapers/keyframe_scraper.py`)

- `BronzeWriter("keyframe", table="anime")` → 拡張 dataclass に切り替え
- 新 `BronzeWriter` 追加: `anime_studios`, `studios`, `settings_categories`
- `fetch_anime_page` を改修して raw HTML 保存:
  ```python
  raw_dir = data_dir / "raw"
  raw_dir.mkdir(parents=True, exist_ok=True)
  (raw_dir / f"{slug}.html").write_text(resp.text, encoding="utf-8")
  ```
- DEFAULT_DELAY を 5.0 に
- max_retries 5、`Retry-After` を `wait` の下限値として尊重 (現状: `max(retry_after, 10*attempt)` → `max(retry_after, 30, 10*attempt)`)
- `scrape_keyframe()` 内で各 dataclass instance を append、studio_master を credit ループ後に集計

### Step 5: tests

`tests/scrapers/test_keyframe_parser.py`:
- (既存 6 ケース) + 以下追加
- `parse_anime_meta`: 全 19 列が dict に揃う
- `parse_anime_studios`: edges 解釈、isMain フラグ
- `parse_settings_categories`: 順序保持
- credits 拡張: studio_ja/en/id 抽出、is_studio_role 保持、section_name、episode_title 抽出 (鉤括弧パターン)、menu_note
- raw HTML 保存: scraper integration test で `raw/<slug>.html` 出現を確認
- 既存 isStudio skip 仕様 → 保持仕様への変更が壊さない事を確認

### Step 6: smoke test (3 件)

```bash
mkdir -p data/keyframe/raw
pixi run python -m src.scrapers.keyframe_scraper --fresh --max-anime 3 --delay 5.0 --checkpoint 1

# 確認
ls data/keyframe/raw/  # 3 ファイル想定
python3 -c "
import duckdb, glob
con = duckdb.connect(':memory:')
for t in ['anime', 'credits', 'anime_studios', 'studios', 'settings_categories']:
    f = glob.glob(f'result/bronze/source=keyframe/table={t}/**/*.parquet', recursive=True)
    if f:
        n = con.execute(f'SELECT count(*) FROM read_parquet({f!r})').fetchone()[0]
        print(f'{t}: {n}')
        cols = con.execute(f'DESCRIBE SELECT * FROM read_parquet({f!r})').fetchall()
        print('  cols:', [c[0] for c in cols])
"
```

期待: 3 anime / studios > 3 / credits > 100 / settings_categories = 60 (3×20) / studios > 0、各列が空でない。

### Step 7: 全件 fresh 再 scrape

```bash
pixi run python -m src.scrapers.keyframe_scraper --fresh --delay 5.0 --checkpoint 20 2>&1 | tee logs/scrapers/keyframe_$(date +%Y%m%d).log
```

- 約 5,512 slug × 5.0s = **約 7-9 時間** (rate limit 込み)
- 進捗 monitor: 100 件ごとの checkpoint log
- 中断時は resume 可能 (raw HTML が残るので、parser 改修時に再 parse のみで済む)

### Step 8: SILVER 統合

`src/db/integrate_duckdb.py` に keyframe 専用テーブル → 共通 SILVER (anime, persons, credits, studios, anime_studios) への正規化を追加。

- credit.studio_id → SILVER `studios` 1 行 → SILVER `anime_studios` (anime ↔ studio リンク)
- credit.studio_ja/en → person.studio_affiliation 列 (SILVER persons 拡張) または SILVER 新テーブル `person_studio_affiliations`
- synonyms → SILVER anime.synonyms 列 (JSON)
- start_date / end_date → SILVER anime.broadcast_start / broadcast_end (現行 year のみ)

詳細は別タスクカード `08b_keyframe_silver_integration.md` で分割可。

---

## Verification

```bash
# lint
pixi run lint

# parser unit test
pixi run test-scoped tests/scrapers/test_keyframe_parser.py

# scraper integration test
pixi run test-scoped tests/scrapers/test_keyframe_scraper.py

# smoke (3 件取得確認)
ls data/keyframe/raw/*.html | wc -l  # 3 想定

# 全件取得後の整合性
python3 -c "
import duckdb, glob
con = duckdb.connect(':memory:')
for t in ['anime', 'credits', 'anime_studios', 'studios', 'settings_categories']:
    f = glob.glob(f'result/bronze/source=keyframe/table={t}/**/*.parquet', recursive=True)
    n = con.execute(f'SELECT count(*) FROM read_parquet({f!r})').fetchone()[0] if f else 0
    print(f'{t}: {n}')
"
```

期待値:
- anime: ~5,000 (sitemap slug 数 - 404)
- credits: > 1,500,000 (前回 stats: 1,020,730 を上回る、各話レベルまで含むので倍程度)
- studios: > 500 (独立スタジオ entry の集約)
- anime_studios: > 5,000 (anime あたり 1-3 個)
- settings_categories: ~5,000 × 20 = 100,000

---

## Stop-if conditions

- [ ] keyframe-staff-list.com sitemap が 404/500 → サイト構造変更の可能性、user に報告
- [ ] preloadData 構造が one-piece fixture と乖離 (例: anilist nest 消失) → サイト改修、parser 設計見直し必要
- [ ] 全件 scrape で 429 が delay 5.0s でも継続 → delay 10s+ に引き上げ、複数日に分割
- [ ] BronzeWriter のスキーマ違反 (新 dataclass の Pydantic v2 制約引っかかり) → models.py 設計修正
- [ ] raw HTML 保存ディスク容量不足 (5,512 × ~200KB = 約 1.1GB 想定) → 別ディスクへの shim、または gzip 化検討

---

## Rollback

```bash
# parser / scraper コード破棄
git checkout src/scrapers/parsers/keyframe.py src/scrapers/keyframe_scraper.py src/runtime/models.py src/db/schema.py

# tests / fixtures 破棄
rm -rf tests/scrapers/test_keyframe_parser.py tests/fixtures/scrapers/keyframe/sample_one-piece.html

# 新 BRONZE 削除、退避を復元
rm -rf result/bronze/source=keyframe
mv result/bronze/source=keyframe.bak-* result/bronze/source=keyframe

# raw HTML 削除 (必要なら)
rm -rf data/keyframe/raw

# checkpoint 復元
mv data/keyframe/checkpoint.json.bak-20260425 data/keyframe/checkpoint.json
```

---

## Completion signal

- [ ] Step 1-5 (parser + scraper + schema + tests) の PR が lint / test pass
- [ ] Step 6 smoke test で 3 anime の全 5 テーブルに値が入っている事を確認
- [ ] Step 7 全件 scrape 完走、`scrape_complete` log 出現、anime ~5000 / credits > 1.5M
- [ ] raw HTML が `data/keyframe/raw/` に全 slug 分保存済 (再 parse 可能)
- [ ] commit (例: `scraper: keyframe parser 拡張 — anime メタ19/credits 8/studios 2 テーブル追加 (08_keyframe_parser_expansion)`)
- [ ] 作業ログに `DONE: 08_keyframe_parser_expansion` 記録
- [ ] Step 8 (SILVER 統合) は別カード `08b_keyframe_silver_integration` に切り出し
- [ ] 残る判断 (delay 5s で十分か、夜間連続 scrape 可否) は user に投げる
