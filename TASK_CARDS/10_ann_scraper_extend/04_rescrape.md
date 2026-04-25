# Task: ANN 全件再 scrape (checkpoint リセット)

**ID**: `10_ann_scraper_extend/04_rescrape`
**Priority**: 🟠
**Estimated changes**: code +0 / data +数 GB (parquet), 1 checkpoint reset
**Requires senior judgment**: yes (完走 ~18 時間、rate limit 監視、途中停止判断)
**Blocks**: (なし — 後続は SILVER 移行側 TASK_CARD)
**Blocked by**: `10_ann_scraper_extend/03_scraper_integration`

---

## Goal

27,000 anime + 対応 persons を新 parser/writer で全件 scrape、8 テーブルの parquet を BRONZE に生成する。

---

## Hard constraints

- **rate limit**: `DEFAULT_DELAY=1.5s` を下回らない。ANN 非公式 limit、過度な送信は禁止。
- **Option B 再確認**: raw cache 層は今回は入れない (ユーザ 2026-04-24 確定)。今回限り再 scrape で完結、次回以降も必要なら同方針。
- **resume 可能**: 完走せず中断しても `completed_ids` に積まれた ID からの resume が必須。

---

## Pre-conditions

- [ ] Card 03 完了 (dry-run `--limit 1` で 8 parquet partition 生成確認済)
- [ ] ディスク 10GB 以上空き (parquet 全件で ~5GB 見積)
- [ ] 旧 parquet 退避: `mv result/bronze/source=ann result/bronze/source=ann.bak-YYYYMMDD`
- [ ] ネットワーク安定 (WiFi 切断で数時間無駄になる)

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `data/ann/anime_checkpoint.json` | `completed_ids` を `[]` にリセット、`all_ids` は維持 |
| `data/ann/persons_checkpoint.json` | 同じく `completed_ids` を `[]` にリセット |
| `TODO.md` | §11 を「Card 10 で管理」に置換 |

## Files to NOT touch

| File | 理由 |
|------|------|
| すべてのコード | Card 03 時点で完了している想定 |

---

## Steps

### Step 1: checkpoint リセット

```bash
pixi run python -c "
import json, pathlib
for name in ('anime_checkpoint.json', 'persons_checkpoint.json'):
    p = pathlib.Path('data/ann') / name
    if not p.exists(): continue
    d = json.loads(p.read_text())
    d['completed_ids'] = []
    p.write_text(json.dumps(d, indent=2))
    print(f'{name}: reset to 0 completed, {len(d.get(\"all_ids\", []))} pending')
"
```

### Step 2: 旧 parquet 退避

```bash
# 旧データは削除せず退避 (比較検証に使う)
DATE=$(date +%Y%m%d)
mv result/bronze/source=ann "result/bronze/source=ann.bak-${DATE}"
```

### Step 3: anime phase 実行

```bash
# フォアグラウンド実行だと 18h ブロックなので nohup + log 化
nohup pixi run python -m src.scrapers.ann_scraper scrape-anime \
    --delay 1.5 \
    --checkpoint-interval 10 \
    > logs/scrapers/ann_rescrape_anime_${DATE}.log 2>&1 &

# 進捗監視
tail -f logs/scrapers/ann_rescrape_anime_${DATE}.log | grep ann_anime_progress
```

見積: 27,000 anime / (50 per batch × (1/1.5s per req)) ≈ 約 13.5 min + retry 余裕 → 15-30 min。
staff 含むフル parse で多少遅延あり、最大 1 時間と想定。

### Step 4: persons phase 実行

anime phase 完了を `ann_anime_scrape_done` ログで確認後:

```bash
nohup pixi run python -m src.scrapers.ann_scraper scrape-persons \
    --delay 1.5 \
    --checkpoint-interval 10 \
    > logs/scrapers/ann_rescrape_persons_${DATE}.log 2>&1 &
```

見積: 全 person ~50,000 × 1.5s = 20h。必ず夜間帯開始。分割実行容認 (`--limit 10000` 等)。

### Step 5: 完走確認

```bash
# 8 テーブル全て parquet 存在
for tbl in anime credits persons cast company episodes releases news related; do
    n=$(find "result/bronze/source=ann/table=${tbl}" -name "*.parquet" 2>/dev/null | wc -l)
    echo "${tbl}: ${n} files"
done

# 行数サンプル
pixi run python -c "
import pyarrow.dataset as ds
for tbl in ('anime', 'credits', 'persons', 'cast', 'company', 'episodes', 'releases', 'news', 'related'):
    path = f'result/bronze/source=ann/table={tbl}'
    try:
        n = ds.dataset(path, format='parquet').count_rows()
        print(f'{tbl:10s}: {n:>8d} rows')
    except FileNotFoundError:
        print(f'{tbl:10s}: MISSING')
"
```

### Step 6: 旧 vs 新 比較検証

```bash
# 旧 anime / 新 anime の ann_id 差分
pixi run python -c "
import pyarrow.dataset as ds
old = set(ds.dataset('result/bronze/source=ann.bak-${DATE}/table=anime', format='parquet').to_table(columns=['ann_id']).column('ann_id').to_pylist())
new = set(ds.dataset('result/bronze/source=ann/table=anime', format='parquet').to_table(columns=['ann_id']).column('ann_id').to_pylist())
print(f'old only: {len(old - new)}, new only: {len(new - old)}, both: {len(old & new)}')
"
```

減少が多い (> 5%) なら parser regression 疑い。

### Step 7: TODO.md 更新

```bash
# §11 全体を以下で置換:
```
```markdown
## SECTION 11: ANN scraper / parser 改修

Card `TASK_CARDS/10_ann_scraper_extend/` へ全面移管。2026-04-24 完了。

- 01_schema_design: DONE
- 02_parser_extend: DONE
- 03_scraper_integration: DONE
- 04_rescrape: DONE
```

### Step 8: バックアップ削除 判断

新 parquet の行数・品質確認後、ユーザに確認してから `result/bronze/source=ann.bak-${DATE}` を削除する (本カード自動削除しない)。

---

## Verification

```bash
# 1. 全 8 テーブル存在
for tbl in anime credits persons cast company episodes releases news related; do
    ls "result/bronze/source=ann/table=${tbl}/date="*/*.parquet > /dev/null && echo "${tbl}: OK"
done

# 2. 行数サニティ
pixi run python -c "
import pyarrow.dataset as ds
anime = ds.dataset('result/bronze/source=ann/table=anime', format='parquet').count_rows()
credits = ds.dataset('result/bronze/source=ann/table=credits', format='parquet').count_rows()
assert anime > 20000, f'anime too few: {anime}'
assert credits > anime * 5, f'credits too few: {credits} / anime {anime}'
print(f'anime={anime}, credits={credits}, OK')
"

# 3. Hard Rule invariant
rg -n 'anime\.score' src/analysis/ src/pipeline_phases/  # 0 件維持
```

---

## Stop-if conditions

- [ ] 最初の 1000 anime 処理時点で 50% 超が parse 失敗 → 即中断、02 に差し戻し
- [ ] 429 / 503 が連続 10 回以上 → delay を 3.0s に引き上げ再開
- [ ] ディスク空き < 2GB → 即中断、旧 bak 削除判断
- [ ] scrape 途中で process 落ちる → 原因調査 (OOM / network / ANN 側) → checkpoint から resume

---

## Rollback

```bash
# 新 parquet 破棄、旧を戻す
DATE=$(date +%Y%m%d)
rm -rf result/bronze/source=ann
mv "result/bronze/source=ann.bak-${DATE}" result/bronze/source=ann

# checkpoint も旧に戻す (git history から)
git checkout data/ann/anime_checkpoint.json data/ann/persons_checkpoint.json
```

---

## Completion signal

- [ ] anime / credits / persons / cast / company / episodes / releases / news / related 9 table 全て parquet 存在
- [ ] anime 行数 ≥ 旧 parquet の 95% (regression 無し)
- [ ] cast / company / episodes / related の行数 > 0 (新規追加テーブル機能確認)
- [ ] TODO.md §11 を本 Card 参照に書換
- [ ] `DONE: 10_ann_scraper_extend/04_rescrape` 記録
