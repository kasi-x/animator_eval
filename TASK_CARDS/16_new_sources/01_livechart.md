# Task: LiveChart.me 放送スケジュール scraper

**ID**: `16_new_sources/01_livechart`
**Priority**: 🟡
**Estimated changes**: 約 +600 / -0 lines, 5 files
**Requires senior judgment**: yes (ToS 確認 / scraping 倫理)
**Blocks**: なし
**Blocked by**: なし

---

## Goal

LiveChart.me から放送スケジュール (放送局・時間帯・先行配信プラットフォーム) を BRONZE parquet として取得し、SILVER `anime` 表の放送メタを精密化する基盤を作る。

---

## Hard constraints

- **H1**: scoring に score / popularity 不参入。display 系のみ
- **H4**: `evidence_source = 'livechart'`
- **H5**: 既存テスト破壊禁止
- **倫理**: robots.txt + ToS 確認必須

---

## Pre-conditions

- [ ] `git status` clean
- [ ] LiveChart ToS / robots.txt 確認、`docs/scraper_ethics.md` に記録
- [ ] API 提供有無確認 (公式 API / RSS / HTML scraping のどれか)
- [ ] `pixi run test` baseline pass

---

## データ要件

| フィールド | 用途 |
|----------|------|
| anime_title (JA / EN) | ID マッピング (entity_resolution H3) |
| 放送開始日 / 終了日 | 既存 SILVER 補強 |
| 放送局 (TV Tokyo / MBS / Tokyo MX 等) | 新規情報 |
| 放送時間帯 (時刻) | 新規情報 |
| 先行配信プラットフォーム + 国 | 新規情報 (O8 soft power 連携) |
| 制作 studio | 既存補強 |
| シーズン (Spring 2025 等) | 既存補強 |

---

## BRONZE スキーマ

`result/bronze/source=livechart/table=*/date=YYYY-MM-DD/*.parquet`

- `table=anime`: anime_title, livechart_id, season, broadcast_start, broadcast_end, anilist_id_link, mal_id_link
- `table=broadcast_slots`: livechart_id, network, slot_time, region (JP only or global)
- `table=streaming_distribution`: livechart_id, platform, country, simulcast_flag

---

## Files to create

| File | 内容 |
|------|------|
| `src/scrapers/livechart_scraper.py` | ScrapeRunner 利用、3 phase (anime / broadcast / streaming) |
| `src/scrapers/parsers/livechart.py` | HTML/JSON parser |
| `src/scrapers/queries/livechart.py` | (API がある場合) GraphQL/REST query 定義 |
| `tests/scrapers/test_livechart_parser.py` | parser unit test |
| `tests/scrapers/test_livechart_scraper.py` | E2E integration (録画 fixture) |

## Files to modify

| File | 変更内容 |
|------|---------|
| `pixi.toml` | scraper 依存追加 (httpx は既存) |
| `docs/scraper_ethics.md` | LiveChart ToS / robots.txt 記録 |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/etl/integrate_duckdb.py` | SILVER 統合は別カード |
| 既存 scraper (`anilist_scraper.py` 等) | 独立統合 |

---

## Steps

### Step 1: ToS / robots.txt 確認

```bash
curl -s https://www.livechart.me/robots.txt | head -50
```

- API 提供の有無確認 (LiveChart Pro API があれば優先)
- scraping 許可範囲 (robots.txt) を `docs/scraper_ethics.md` に記録

### Step 2: dataclass + parser

`src/scrapers/parsers/livechart.py` に dataclass:
- `LivechartAnime`
- `LivechartBroadcastSlot`
- `LivechartStreamingDistribution`

parser 関数 + unit test (`tests/scrapers/test_livechart_parser.py`)。

### Step 3: scraper 実装

ScrapeRunner + BronzeWriterGroup パターン (`TASK_CARDS/13` 完了済 abstraction):

```python
async with BronzeWriterGroup("livechart", date=today) as writers:
    runner = ScrapeRunner(http_client, rate_limiter, ...)
    await runner.run(...)
```

### Step 4: 整合性テスト

- 録画 fixture で E2E
- entity_resolution との整合 (anilist_id_link が SILVER `anime.anilist_id` に存在すること)

### Step 5: 全件 scrape (オプション、別実行)

`pixi run python -m src.scrapers.livechart_scraper` で起動可能な状態にして、全件 scrape は本カード範囲外。

---

## Verification

```bash
# 1. lint
pixi run lint

# 2. テスト
pixi run test-scoped tests/scrapers/test_livechart_parser.py tests/scrapers/test_livechart_scraper.py

# 3. dry-run
pixi run python -m src.scrapers.livechart_scraper --limit 10 --dry-run

# 4. BRONZE 出力確認
ls result/bronze/source=livechart/table=*/date=*/
```

---

## Stop-if conditions

- [ ] ToS / robots.txt で scraping 禁止 → 公式 API 検討、なければ Stop
- [ ] rate limit が厳しすぎて全件取得不可 (推定完走時間 > 30 日)
- [ ] `pixi run test` 既存テスト失敗

---

## Rollback

```bash
rm src/scrapers/livechart_scraper.py
rm src/scrapers/parsers/livechart.py
rm src/scrapers/queries/livechart.py
rm tests/scrapers/test_livechart_*.py
git checkout pixi.toml docs/scraper_ethics.md
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] BRONZE parquet 取得確認 (limit 10 でも row > 0)
- [ ] DONE: `16_new_sources/01_livechart`
