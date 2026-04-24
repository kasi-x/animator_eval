# Task: bangumi 差分更新 API (cron)

**ID**: `08_bangumi_scraper/06_incremental_update`
**Priority**: 🟢
**Estimated changes**: 約 +200 lines, 2 files 新規
**Requires senior judgment**: yes (rate limit 設計)
**Blocks**: (なし)
**Blocked by**: `02..05` 全完了、dump で一次データ揃った後のみ着手

---

## Goal

dump は週次更新 → 1週間遅れのデータしか持てない → 公式 API `/v0` で日次差分を取得し BRONZE に append する cron。

**この card は dump 運用が回り始めてから着手**。急がない。

---

## Hard constraints

- User-Agent: `animetor_eval/<version> (https://github.com/kashi-x)` 必須
- rate limit: **遵守必須**。bangumi 公式ガイドは明記薄いが安全側で 1 req/sec + burst 上限 5
- 差分のみ: 前回 cron 以降に `last_modified` 更新された subject/person のみ取得
- 既存 dump parquet を**上書きしない**。date パーティション分離で append

---

## Pre-conditions

- [ ] `01..05` 全完了
- [ ] dump-based BRONZE が安定運用できている
- [ ] `src/scrapers/retrying_http_client.py` が bangumi domain に対応していることを確認

---

## Steps (概要のみ、実装時に詳細化)

### Step 1: API client (`src/scrapers/bangumi_scraper.py`)

- `fetch_subject(id)` / `fetch_person(id)` / `fetch_subject_persons(id)` / `fetch_subject_characters(id)`
- httpx async + 既存 retry util
- rate limiter: `asyncio.Semaphore(1) + sleep(1.0)`

### Step 2: 差分検出

- state ファイル `data/bangumi/incremental_state.json` に `last_run_at` 保持
- subject: `/v0/subjects?since=<last_run_at>` があればそれ、無ければ recent changes ページを scrape
- **要仕様調査**: bangumi API に `since` filter があるか不明 → Step 0 で確認、無ければ個別 id で `last_modified` 比較方式

### Step 3: cron wrapper (`scripts/bangumi_incremental_cron.py`)

- 出力: `result/bronze/source=bangumi/table=<X>/date=YYYYMMDD/part-N.parquet` (日付パーティション)
- manifest に `cron_run_id` 記録

### Step 4: schedule

- `schedule` skill で日次 cron 登録 (この card 完了後、別途 user 承認)

---

## Verification

```bash
pixi run python scripts/bangumi_incremental_cron.py --dry-run --since 1d
# → 取得対象数のみ表示
pixi run python scripts/bangumi_incremental_cron.py --since 1d
ls result/bronze/source=bangumi/table=*/date=$(date +%Y%m%d)/
pixi run lint
```

---

## Stop-if conditions

- [ ] rate limit 違反 (429 連続) → 即停止、sleep 倍増して再検討
- [ ] bangumi が User-Agent ポリシー変更 (ban されたら停止)
- [ ] 差分が 10000 件超/日 → 仕様理解誤り、初回 backfill と誤認している可能性

---

## Rollback

```bash
git checkout src/scrapers/bangumi_scraper.py scripts/bangumi_incremental_cron.py
rm -rf result/bronze/source=bangumi/table=*/date=<今回>/
# cron 登録後なら schedule delete
```

---

## Completion signal

- [ ] dry-run + 実 run 両方成功
- [ ] 1 週間連続運用して rate limit エラー 0
- [ ] DONE 記録
