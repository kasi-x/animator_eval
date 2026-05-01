# Task: TMDb 劇場アニメ国際配信 scraper

**ID**: `16_new_sources/02_tmdb`
**Priority**: 🟢
**Estimated changes**: 約 +500 / -0 lines, 5 files
**Requires senior judgment**: yes (API key 管理 / rate limit 設計)
**Blocks**: `15_extension_reports/06_o8_soft_power` (海外配信メタの一部)
**Blocked by**: なし

---

## Goal

TMDb (The Movie Database) API から劇場アニメ作品の国際配信メタデータ (海外公開日 / 配給会社 / 国別タイトル) を BRONZE parquet として取得する。

---

## Hard constraints

- **H1**: TMDb の `vote_average` / `popularity` を SILVER scoring に絶対入れない。display_* prefix のみ
- **H4**: `evidence_source = 'tmdb'`
- **H5**: 既存テスト破壊禁止
- **API key**: 環境変数 `TMDB_API_KEY` 必須、コミット禁止

---

## Pre-conditions

- [ ] `git status` clean
- [ ] TMDb API key 取得 (https://www.themoviedb.org/settings/api)
- [ ] `.env.example` に `TMDB_API_KEY=` 追加 (実 key はコミットしない)
- [ ] `pixi run test` baseline pass

---

## データ要件

TMDb は劇場映画 (movie) と TV (tv) の両 endpoint。本カードでは **anime 映画 (劇場アニメ)** を主対象。

| フィールド | 用途 |
|----------|------|
| tmdb_id | PK |
| title (original / international) | 国別 |
| release_date (国別 release_dates) | 海外公開日 |
| production_companies (国別) | 配給会社 |
| genres | (anime 識別) |
| vote_average / popularity | display のみ (H1) |
| poster_path / backdrop_path | display |
| imdb_id_link | cross-source mapping |

---

## BRONZE スキーマ

`result/bronze/source=tmdb/table=*/date=YYYY-MM-DD/*.parquet`

- `table=anime_movies`: tmdb_id, original_title, release_date_jp, genres, imdb_id, runtime_minutes
- `table=international_releases`: tmdb_id, country, release_date, release_type (theatrical / digital / physical)
- `table=production_companies`: tmdb_id, company_id, company_name, country
- `table=alternative_titles`: tmdb_id, country, title

---

## Files to create

| File | 内容 |
|------|------|
| `src/scrapers/tmdb_scraper.py` | ScrapeRunner 利用、anime genre filter (id=16) |
| `src/scrapers/parsers/tmdb.py` | JSON parser + dataclass |
| `src/scrapers/queries/tmdb.py` | endpoint 定義 (discover / movie / release_dates) |
| `tests/scrapers/test_tmdb_parser.py` | parser unit test |
| `tests/scrapers/test_tmdb_scraper.py` | E2E integration (録画 fixture) |

## Files to modify

| File | 変更内容 |
|------|---------|
| `pixi.toml` | (httpx 既存、依存追加なし想定) |
| `.env.example` | `TMDB_API_KEY=` |
| `docs/scraper_ethics.md` | TMDb ToS 記録 (Attribution Required) |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/etl/integrate_duckdb.py` | SILVER 統合は別カード |
| `.env` (もし存在) | 実 key コミット禁止 |

---

## Steps

### Step 1: API key 取得 + ToS 確認

- TMDb API: 30 req/sec、attribution 必須 (powered by TMDb ロゴ表示要件)
- `docs/scraper_ethics.md` に記録 (attribution 表示位置: report HTML footer)

### Step 2: dataclass + parser

`src/scrapers/parsers/tmdb.py`:
- `TmdbAnimeMovie`
- `TmdbInternationalRelease`
- `TmdbProductionCompany`
- `TmdbAlternativeTitle`

parser unit test。

### Step 3: discover + movie endpoint

```
GET /3/discover/movie?with_genres=16&with_origin_country=JP&...
GET /3/movie/{tmdb_id}/release_dates
GET /3/movie/{tmdb_id}/alternative_titles
```

ScrapeRunner で 3 phase (discover / detail / release_dates)。

### Step 4: E2E test

- 録画 fixture (実際の API レスポンス JSON を `tests/fixtures/tmdb/`)
- BronzeWriterGroup の parquet 出力確認

### Step 5: 全件 scrape スクリプト

`pixi run python -m src.scrapers.tmdb_scraper --year-from 1960 --year-to 2025` で起動可能に。本カード範囲外で全件取得実行。

---

## Verification

```bash
# 1. lint
pixi run lint

# 2. テスト
pixi run test-scoped tests/scrapers/test_tmdb_parser.py tests/scrapers/test_tmdb_scraper.py

# 3. dry-run (sample 10 件)
TMDB_API_KEY=$(cat ~/.config/secrets/tmdb_key) \
  pixi run python -m src.scrapers.tmdb_scraper --limit 10

# 4. BRONZE 出力確認
ls result/bronze/source=tmdb/table=*/date=*/

# 5. invariant
rg 'vote_average|popularity' src/scrapers/parsers/tmdb.py | rg -v 'display_\|raw_'   # display 隔離
```

---

## Stop-if conditions

- [ ] API key 取得不可
- [ ] rate limit 30 req/sec で全件取得 > 30 日
- [ ] anime genre (id=16) filter で TV シリーズ大量混入 → release_type filter 追加
- [ ] `pixi run test` 既存テスト失敗

---

## Rollback

```bash
rm src/scrapers/tmdb_scraper.py
rm src/scrapers/parsers/tmdb.py
rm src/scrapers/queries/tmdb.py
rm tests/scrapers/test_tmdb_*.py
git checkout pixi.toml .env.example docs/scraper_ethics.md
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] BRONZE parquet 取得確認 (limit 10 で row > 0)
- [ ] DONE: `16_new_sources/02_tmdb`
