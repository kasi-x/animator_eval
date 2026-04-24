# Task: bangumi 差分更新 (週次 dump diff + 関係 re-scrape)

**ID**: `08_bangumi_scraper/06_incremental_update`
**Priority**: 🟢
**Estimated changes**: 約 +250 lines, 2 files 新規
**Requires senior judgment**: yes (差分検出ロジック)
**Blocks**: (なし)
**Blocked by**: `02..05` 全完了、初回 backfill 成功後のみ着手

---

## Goal (2026-04-25 改訂)

全データ再取得は rate limit で現実的でない (~3-5k subject × 3 endpoint = 週次完走不可)。→ 2 層差分:

1. **subject 差分**: `/aux/latest.json` で新 tag 検知 → dump DL → 前週 subject parquet と diff → 新規 + 更新 subject_id 抽出
2. **関係/人物 再 scrape**: 差分 subject のみ `/v0/subjects/{id}/persons` + `/characters` を再 fetch。person/character detail は新規 id のみ fetch

---

## Hard constraints

- rate limit 1 req/sec 遵守継続
- User-Agent 継続必須
- 既存 BRONZE parquet 上書きしない → **新 date partition** に追記
- 差分 subject_id 集合は date 毎に記録 (後から rollback / 検証可能)

---

## Pre-conditions

- [ ] `01..05` 全完了、初回 backfill 完走
- [ ] dump-based subject BRONZE が 2 weeks 分以上溜まっていること (diff 検証のため)

---

## Steps (概要)

### Step 1: 差分 subject 検出

```python
# last week parquet vs this week parquet
old = duckdb_read("table=subjects/date=<prev_week>")
new = duckdb_read("table=subjects/date=<this_week>")
diff_ids = (new.id - old.id) | {r.id for r in new if hash(r) != hash(old[r.id])}
```

hash 比較は `last_modified` 列優先、無ければ infobox+score の複合 hash。

### Step 2: 差分 subject 分だけ関係 re-scrape

Card 03 の `scripts/scrape_bangumi_relations.py` に `--subject-ids-file` option 追加 → diff_ids 注入

### Step 3: 新規 person/character id 検出 + detail fetch

Card 04/05 の script に `--only-new` option 追加 → 既存 persons/characters parquet と diff した集合のみ fetch

### Step 4: cron wrapper `scripts/bangumi_weekly_cron.py`

毎週水曜朝 6 時 (JST)。dump 更新は GMT+8 水曜 5 時なので JST 水曜 6 時で safe margin。

- 1. `fetch_bangumi_dump.py` (Card 01)
- 2. `migrate_bangumi_subjects_to_parquet.py` (Card 02)
- 3. diff 検出 → diff_ids.txt
- 4. `scrape_bangumi_relations.py --subject-ids-file diff_ids.txt`
- 5. `scrape_bangumi_persons.py --only-new`
- 6. `scrape_bangumi_characters.py --only-new`
- 7. manifest に週次 run_id 記録

### Step 5: schedule 登録

`schedule` skill で cron 登録 (user 承認必須)。

---

## Verification

```bash
pixi run python scripts/bangumi_weekly_cron.py --dry-run
# → 差分 subject 数・予測 req 数・ETA のみ表示

pixi run python scripts/bangumi_weekly_cron.py
ls result/bronze/source=bangumi/table=*/date=$(date +%Y%m%d)/
pixi run lint
```

---

## Stop-if conditions

- [ ] 差分 subject 数が 1 週間で > 1000 → diff ロジック誤り (初回 backfill 誤認の可能性)
- [ ] 429 連続 → sleep 増
- [ ] dump release が 2 週連続更新されない → bangumi/Archive 側停止、cron 一時停止

---

## Rollback

```bash
rm -rf result/bronze/source=bangumi/table=*/date=<今回>/
# schedule 登録後なら cron delete
```

---

## Completion signal

- [ ] dry-run + 実 run 両方成功
- [ ] 2 週連続完走、rate limit エラー 0
- [ ] DONE 記録
