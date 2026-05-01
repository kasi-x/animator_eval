# Task: display 系列 SILVER 取込拡充 (bgm / ANN / MAL rating)

**ID**: `20_silver_quality/03_display_columns_extend`
**Priority**: 🟡
**Estimated changes**: 約 +200 / -20 lines, 4-5 files
**Requires senior judgment**: no (パターン定型)
**Blocks**: なし
**Blocked by**: なし

---

## Goal

各 source の rating / popularity / member 数 を `display_*_<source>` prefix で SILVER `anime` 表に取り込む。**scoring 経路に絶対入れない** (H1)。

---

## Hard constraints

- **H1**: 各列は `display_*_<source>` prefix 必須 (例: `display_score_bgm`, `display_rating_ann`, `display_score_mal`)
- **H4**: `evidence_source` は credits / characters のみ、anime 表は ID prefix で source 識別
- **H5**: 既存テスト破壊禁止
- **H8**: 行番号信頼禁止

---

## Pre-conditions

- [ ] `git status` clean
- [ ] 各 source の現状確認:
```bash
duckdb result/silver.duckdb -c "
SELECT column_name FROM information_schema.columns 
WHERE table_name='anime' AND column_name LIKE 'display_%' ORDER BY 1
"
```
- [ ] BRONZE 側で各 rating 列存在確認
- [ ] `pixi run test` baseline pass

---

## 対象列

### bangumi (`bgm`)
- `display_score_bgm`: BRONZE `subjects.score`
- `display_rank_bgm`: BRONZE `subjects.rank`
- `display_collect_count_bgm`: BRONZE `subjects.collection_total` (or 同等)
- `display_total_episodes_bgm`: BRONZE 総話数 (display 用)

### ANN (`ann`)
- `display_rating_avg_ann`: BRONZE `anime.rating_avg`
- `display_rating_count_ann`: BRONZE `anime.rating_count`
- `display_rating_weighted_ann`: BRONZE `anime.rating_weighted`
- `display_rating_bayesian_ann`: BRONZE `anime.rating_bayesian`

### MAL (既存だが要確認、Card 14/08 で `display_*_mal` 6 列実装済)
- 既に: `display_score_mal`, `display_popularity_mal`, `display_members_mal`, `display_favorites_mal`, `display_rank_mal`, `display_scored_by_mal`
- 未実装あれば追加

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/etl/silver_loaders/bangumi.py` | display 系列を SILVER UPDATE に追加 |
| `src/etl/silver_loaders/ann.py` | display 系列を SILVER UPDATE に追加 |
| `src/etl/silver_loaders/mal.py` | (既存確認、不足あれば追加) |
| `src/db/schema.py` | `_BANGUMI_EXTENSION_COLUMNS` / `_ANN_EXTENSION_COLUMNS` に display 列追加 (末尾追記) |
| `tests/test_etl/test_silver_<source>.py` | 各 loader 回帰テスト追加 |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/analysis/scoring/*` | H1 |
| `src/analysis/structural_estimation.py` | H1 |

---

## Steps

### Step 1: BRONZE 列存在確認

各 source で対象列が parquet に含まれるか:

```bash
for src in bangumi ann; do
  echo "--- $src ---"
  duckdb -c "DESCRIBE SELECT * FROM read_parquet('result/bronze/source=$src/table=*/date=*/*.parquet', union_by_name=true) LIMIT 0" 2>&1 | grep -iE "score|rating|popular|rank|collect"
done
```

### Step 2: schema.py に列追加

末尾 `# ===== bangumi extension =====` 内 (or `_BANGUMI_EXTENSION_COLUMNS` リスト) に display 列追加。

### Step 3: 各 loader の UPDATE 文修正

bangumi.py / ann.py の `_ANIME_*_SQL` (anime UPDATE) に display 列マッピング追加。

### Step 4: 回帰テスト

合成 BRONZE で新 display 列が SILVER に反映されること確認。

### Step 5: 実 SILVER で検証

```bash
duckdb result/silver.duckdb -c "
SELECT 
  COUNT(*) FILTER (WHERE display_score_bgm IS NOT NULL) AS bgm_score,
  COUNT(*) FILTER (WHERE display_rating_avg_ann IS NOT NULL) AS ann_rating
FROM anime
"
```

---

## Verification

```bash
pixi run lint
pixi run test-scoped tests/test_etl/test_silver_bangumi.py tests/test_etl/test_silver_ann.py tests/test_etl/test_silver_mal.py
# H1 invariant
rg 'display_score_bgm|display_rating_avg_ann' src/analysis/  # 0 件 (scoring 経路不参入)
```

---

## Stop-if conditions

- [ ] BRONZE 側に対象列が存在しない (parser 未対応) → scrape 系タスク化、本カード Stop
- [ ] H1 違反検出 (scoring 経路に流入)
- [ ] `pixi run test` 既存テスト失敗

---

## Rollback

```bash
git checkout src/etl/silver_loaders/ src/db/schema.py tests/test_etl/
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] 新 display 列の row > 0
- [ ] DONE: `20_silver_quality/03_display_columns_extend`
