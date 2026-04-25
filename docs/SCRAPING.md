# Scraping Guide

## Playwright scrapers

Used for Cloudflare-protected sites where raw httpx cannot pass the JS challenge.

### Scrapers using Playwright

| Scraper | Target |
| --- | --- |
| `sakuga_atwiki_scraper.py` | www18.atwiki.jp/sakuga/ |

### Initial setup

```bash
pixi run install-playwright   # downloads Chromium (~300MB)
```

Re-install after `pixi install` updates:

```bash
pixi run install-playwright
```

### Headful debug mode

Set `HEADFUL=1` to open a visible browser window:

```bash
HEADFUL=1 pixi run python -c "
import asyncio
from src.scrapers.http_playwright import PlaywrightFetcher
async def main():
    async with PlaywrightFetcher(headless=False) as f:
        html = await f.fetch('https://www18.atwiki.jp/sakuga/pages/1.html')
        print(len(html))
asyncio.run(main())
"
```

### CF challenge troubleshooting

`PlaywrightFetcher.fetch()` detects `"Just a moment"` / `"Attention Required"` titles and waits up to 30 s for the challenge to clear. If it still fails:

1. Run headful (`HEADFUL=1`) to watch what the browser sees.
2. Check that Chromium is up to date: `pixi run install-playwright`.
3. Try a longer `timeout_ms` (e.g. `timeout_ms=60_000`).
4. If CF fingerprinting has been strengthened, consider rotating the profile dir (`data/playwright_profile/`) to get a fresh session.

### Rate limiting

`PlaywrightFetcher.fetch()` does **not** sleep internally. The caller controls delay:

```python
async with PlaywrightFetcher() as f:
    for url in urls:
        html = await f.fetch(url)
        await asyncio.sleep(3.0)   # >= 3s per robots.txt convention
```

## 作画@wiki 月次差分更新

### 推奨スケジュール

月次 (毎月第1日曜日 AM 3:00) を想定。cron 登録例:

```cron
0 3 * * 0 [ "$(date +\%d)" -le 7 ] && cd /path/to/animetor_eval && pixi run python -m src.scrapers.sakuga_atwiki_scraper incremental --cache-dir data/sakuga/ --output result/bronze/ --date $(date +%Y%m%d) >> logs/scrapers/sakuga_incremental.log 2>&1
```

### コマンド

```bash
# 月次差分更新 (通常)
pixi run python -m src.scrapers.sakuga_atwiki_scraper incremental \
    --cache-dir data/sakuga/ \
    --output result/bronze/ \
    --date $(date +%Y%m%d)

# 上限付きテスト実行 (10ページ)
pixi run python -m src.scrapers.sakuga_atwiki_scraper incremental \
    --cache-dir data/sakuga/ --output /tmp/bronze_inc \
    --date $(date +%Y%m%d) --max-pages 10
```

出力サマリ例:
```
fetched=42 unchanged=39 changed=3 new_pages=1 errors=0
```

### 失敗時リトライ

- CF 通過失敗 (fetch error): 個別ページは skip + errors カウント増加。次回 run で再試行
- `unchanged + changed + new_pages != fetched` → hash ロジック異常、コード確認
- CF 通過率 < 80% (= errors/fetched > 0.20) → Chromium 更新 or profile dir ローテーション

### アラート基準

- `changed / fetched == 1.00` かつ `fetched > 10` → hash 判定バグの可能性。ログ確認
- `errors / fetched > 0.20` → CF 強化、headful デバッグ実施

## httpx scrapers

All other scrapers use `src/scrapers/http_client.py` (`RetryingHttpClient`) backed by httpx async.

## bangumi

Source: [bangumi.tv](https://bgm.tv) — largest anime/manga database with Chinese-language credits.

### Hybrid dump + API approach

bangumi publishes a weekly Archive zip at `github.com/bangumi/Archive`. The dump's README
lists 9 jsonlines files, but the actual weekly zip contains **only `subject.jsonlines`**
(verified by local-file-header scan of the ZIP structure). All relation, person, and character
data must be scraped via the `/v0` REST API.

| Data | Method | Notes |
|------|--------|-------|
| subjects (anime metadata) | Archive dump — `subject.jsonlines` | type=2 filter for anime |
| subject × persons | `GET /v0/subjects/{id}/persons` | ~3-5k requests per backfill |
| subject × characters + actors | `GET /v0/subjects/{id}/characters` | actors array nested inside |
| person detail | `GET /v0/persons/{id}` | referenced person_ids only |
| character detail | `GET /v0/characters/{id}` | referenced character_ids only |

The streaming zip extractor in `src/scrapers/bangumi_dump.py` handles ZIP structures without a
central-directory End-Of-Central-Directory record (EOCD), which was a real constraint during
initial design. It is retained even though current dumps are single-file, in case bangumi
reverts to multi-file EOCD-less archives.

### BRONZE output paths

```
result/bronze/source=bangumi/table={subjects,subject_persons,subject_characters,person_characters,persons,characters}/date=YYYYMMDD/*.parquet
```

### BronzeWriter UUID pattern

bangumi uses the same `BronzeWriter` as other scrapers. Each checkpoint flush writes a
UUID-named parquet file (`{uuid4}.parquet`) rather than an incrementing `part-N.parquet`
— this prevents concurrent writers from colliding and makes partial backfill recovery safe.
A bug where BronzeWriter was unconditionally overwriting `part-0.parquet` was fixed in
commit 960323d.

### Rate limiter design

`BangumiClient` enforces a **1 req/sec floor** using `asyncio.Lock` + `time.monotonic`:
before each request the client calculates elapsed time since the previous request and sleeps
for the remaining fraction of the 1-second window. If the server returns a `Retry-After`
header (HTTP 429), that value takes precedence. The rate limit is conservative relative to
bangumi's published guidance (which is vague), ensuring long backfills do not trigger blocks.

### Known sentinel: `last_modified == "0001-01-01T00:00:00Z"`

bangumi's `/v0` API returns `"0001-01-01T00:00:00Z"` for persons whose `last_modified` field
is unset. The BRONZE parquet stores this raw string as-is. SILVER ETL must convert it to
`NULL` (or `NaT`) before loading into the `persons` table.

### Raw label preservation

`position` values in `subject_persons` are stored as raw Chinese strings (e.g. "导演",
"原画", "音乐制作人"). These are preserved verbatim in BRONZE. Normalization to the
project's role taxonomy (`src/utils/role_groups.py`) will be done in SILVER via a
`bangumi/common` YAML mapping file — this is a TODO tracked in `TODO.md §13.6`.

### GraphQL client (main path as of 2026-04-25)

The bangumi server exposes a GraphQL endpoint at `https://api.bgm.tv/v0/graphql`
(POST, Content-Type: application/json). This is now the **default client** for all
three orchestrator scripts. The v0 REST client remains available as `--client v0`.

Key properties:
- Batched queries: `scrape_bangumi_relations.py` sends 25 subjects per POST using
  aliased GraphQL queries (`s100: subject(id:100) {...}`) — ~25x throughput vs v0.
- Rate budget: `_HOST_RATE_LIMITER` (module-level singleton in `bangumi_scraper.py`)
  is shared between `BangumiClient` (v0) and `BangumiGraphQLClient` so total
  throughput on `api.bgm.tv` never exceeds 1 req/sec regardless of which client is active.
- Response adapters: `adapt_person_gql_to_v0()` and `adapt_character_gql_to_v0()` in
  `bangumi_graphql_scraper.py` normalise GraphQL camelCase field names to the snake_case
  shape expected by the existing BRONZE row builders — no changes to row builder logic needed.
- H1 constraint: `SubjectRating.score / rank / total` are fetched and kept in BRONZE as
  display metadata only; they do not flow into the scoring path.

Rollback: pass `--client v0` to any script to revert to the legacy REST path instantly.
