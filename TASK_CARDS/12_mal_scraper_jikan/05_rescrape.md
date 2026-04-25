# Task: MAL/Jikan 全件 scrape 実行 (~7.6 日)

**ID**: `12_mal_scraper_jikan/05_rescrape`
**Priority**: 🟠
**Estimated changes**: code +0 / data ~15GB (parquet) / ~183h 完走
**Requires senior judgment**: yes (4 日超完走、rate limit 監視、phase 別停止判断、ディスク監視)
**Blocks**: (なし — 後続は SILVER 移行側 TASK_CARD)
**Blocked by**: `12_mal_scraper_jikan/04_rate_limit_strict`

---

## Goal

Jikan v4 全 endpoint を 28 BRONZE テーブルへ書き出す。完走目安:

| Phase | 対象 | req 数 | 時間 (60/min) |
|-------|------|--------|---------------|
| A | anime ~26,000 × 13 endpoint | ~340,000 | ~95h |
| B | persons ~50,000 × 2 + characters ~150,000 × 2 | ~400,000 | ~111h ★ |
| C | producers ~1,000 × 2 + manga ~70,000 × 1 + masters ~10 | ~72,000 | ~20h |
| **計** | | **~812,000** | **~226h ≒ 9.4 日** |

★ 元見積 ~183h は内訳粗。Phase B が支配的。Phase 単位で中断・継続可能。

---

## Hard constraints

- **rate limit 厳守**: 60 req/min 上限を超えない (Card 04 適用済確認)
- **resume 可能**: 任意中断後、`completed_*_ids` set から差分 resume
- **ディスク監視**: 想定 15GB。`df -h result/bronze/source=mal` を監視 script で 1h 毎チェック (50GB 切ったら停止)
- **phase 単位停止可**: Phase A 完了後に B 開始判断、B 完了後に C 開始判断 (一気通貫しない)
- **partial failure log**: 失敗 endpoint は log に記録、~5% 程度なら無視 (Jikan 一時 503 は珍しくない)。10% 超えたら停止調査

---

## Pre-conditions

- [ ] Card 04 完了 (`DualWindowRateLimiter` 統合済)
- [ ] ディスク 50GB 以上空き (`df -h .`)
- [ ] 旧 `result/bronze/source=mal/` 不在確認 (`ls result/bronze/source=mal 2>/dev/null` 何も出ない)
- [ ] ネットワーク安定 (有線推奨、VPN 不要)
- [ ] 過去 cache (`data/scraper_cache/mal/rest/`) があれば保持 (resume 時に hit)
- [ ] log rotation 設定確認 (`logs/scrapers/mal_*.jsonl` 1 ファイルで GB 級になる可能性 → 日次 rotate)

---

## 実行手順

### Phase A 開始 (~95h)

```bash
# 別 tmux session で実行 (4 日継続のため)
tmux new -s mal-phase-a
pixi run python -m src.scrapers.mal_scraper --phase A --resume false 2>&1 | tee -a logs/scrapers/mal_phase_a_$(date +%Y%m%d).log
# Ctrl-B D で detach
```

進捗監視 (別 terminal):

```bash
# 1h 毎 progress
watch -n 3600 '
echo "==== checkpoint ===="
jq ".phase, .completed_anime_ids | length, .discovered_person_ids | length, .discovered_character_ids | length, .discovered_producer_ids | length, .discovered_manga_ids | length" data/mal/checkpoint.json
echo "==== disk ===="
du -sh result/bronze/source=mal
df -h . | tail -1
echo "==== last 5 errors ===="
grep -E "ERROR|429|503" logs/scrapers/mal_*.jsonl | tail -5
'
```

### Phase A 中断時 resume

```bash
tmux attach -t mal-phase-a
# Ctrl-C 中断後
pixi run python -m src.scrapers.mal_scraper --phase A --resume true
```

### Phase B 開始判定

Phase A 完了 (`checkpoint.phase == "B"`) 後:

```bash
# discover 数確認
jq '.discovered_person_ids | length' data/mal/checkpoint.json
# 例: 50000 → Phase B 工期 ~33h × 2 (person + character) = ~66h と再見積
```

問題なければ:

```bash
tmux new -s mal-phase-b
pixi run python -m src.scrapers.mal_scraper --phase B --resume true 2>&1 | tee -a logs/scrapers/mal_phase_b_$(date +%Y%m%d).log
```

### Phase C 開始

Phase B 完了 (`checkpoint.phase == "C"`) 後:

```bash
tmux new -s mal-phase-c
pixi run python -m src.scrapers.mal_scraper --phase C --resume true 2>&1 | tee -a logs/scrapers/mal_phase_c_$(date +%Y%m%d).log
```

### 完了確認

```bash
jq '.phase' data/mal/checkpoint.json   # "DONE"

# 全 28 table partition 存在確認
find result/bronze/source=mal -name "*.parquet" | awk -F/ '{print $4}' | sort -u | wc -l
# 期待: 28 (実装での table 名差で 28-30 範囲)

# データ件数確認
pixi run python -c "
import duckdb
con = duckdb.connect()
for tbl in ['anime', 'persons', 'characters', 'producers', 'manga']:
    r = con.execute(f\"SELECT COUNT(*) FROM read_parquet('result/bronze/source=mal/table={tbl}/**/*.parquet')\").fetchone()
    print(f'{tbl}: {r[0]:,}')
"
# 期待:
#   anime: 25000-27000
#   persons: 40000-60000
#   characters: 100000-200000
#   producers: 800-1200
#   manga: 60000-80000
```

---

## 監視スクリプト

`scripts/watch_mal_scrape.sh` (新規、Card 内に実装):

```bash
#!/bin/bash
set -euo pipefail
while true; do
    echo "=== $(date) ==="
    if [ -f data/mal/checkpoint.json ]; then
        jq '{
            phase, last_page_anime_list,
            anime_done: (.completed_anime_ids | length),
            person_disc: (.discovered_person_ids | length),
            char_disc: (.discovered_character_ids | length),
            producer_disc: (.discovered_producer_ids | length),
            manga_disc: (.discovered_manga_ids | length),
            person_done: (.completed_person_ids | length),
            char_done: (.completed_character_ids | length)
        }' data/mal/checkpoint.json
    fi
    echo "disk: $(du -sh result/bronze/source=mal 2>/dev/null | awk '{print $1}')"
    free=$(df --output=avail -B1G . | tail -1)
    echo "free: ${free}GB"
    if [ "$free" -lt 50 ]; then
        echo "!! DISK LOW (<50GB) — STOPPING !!"
        pkill -f "src.scrapers.mal_scraper" || true
        exit 1
    fi
    err=$(grep -cE 'ERROR|429|503' logs/scrapers/mal_*.jsonl 2>/dev/null | awk -F: '{s+=$NF} END {print s+0}')
    echo "error count cumulative: $err"
    sleep 3600
done
```

実行:

```bash
chmod +x scripts/watch_mal_scrape.sh
tmux new -s mal-watch
./scripts/watch_mal_scrape.sh
```

---

## Stop-if conditions

- [ ] **失敗率 > 10%**: `errors / completed > 0.1` → 停止して原因調査
- [ ] **ディスク <50GB**: 自動停止
- [ ] **24h 経過で 1000 anime 未完** (= 進捗 4%/day 未達) → rate limit 過剰 / network 問題
- [ ] **Jikan サービス 200 連続 5xx**: 24h 待機後再開
- [ ] **checkpoint json 100MB 超**: ID set 巨大化 → SQLite 移行検討 (別 card 起票)

---

## Rollback

```bash
# Phase 中断 + cleanup
tmux kill-session -t mal-phase-a 2>/dev/null || true
tmux kill-session -t mal-phase-b 2>/dev/null || true
tmux kill-session -t mal-phase-c 2>/dev/null || true
tmux kill-session -t mal-watch 2>/dev/null || true

# データ完全リセット (慎重に)
read -p "MAL BRONZE データ全削除する? (yes/NO): " confirm
[ "$confirm" = "yes" ] && rm -rf result/bronze/source=mal data/mal/checkpoint.json
```

---

## Completion signal

- [ ] `data/mal/checkpoint.json` の `phase == "DONE"`
- [ ] 28+ table partition 存在 (`find result/bronze/source=mal -name "*.parquet" | awk -F/ '{print $4}' | sort -u`)
- [ ] anime 25k+ / persons 40k+ / characters 100k+ / producers 800+ / manga 60k+
- [ ] 失敗率 < 5%
- [ ] DONE.md / TODO.md §12.3 を完了に更新
- [ ] `DONE: 12_mal_scraper_jikan/05_rescrape` 記録

---

## 後続 (本カード scope 外)

1. **SILVER 移行**: 28 BRONZE → SILVER `anime` / `persons` / `credits` 統合 (`src/etl/integrate_duckdb.py` に MAL parser 追加 — 別 TASK_CARD)
2. **画像 binary DL**: scope 外。必要なら別 TASK_CARD 起票 (bangumi 08 と同方針、画像 URL 列を入力に並列 DL)
3. **manga master 統合**: 原作 manga staff (漫画家) を SILVER `persons` に統合する場合の cross-source dedupe 設計 — 別 TASK_CARD
