# Task: AKM 再計算 + GOLD 反映

**ID**: `21_silver_enrichment/01_akm_refresh`
**Priority**: 🟠
**Estimated changes**: 約 +100 / -10 lines, 1-2 files
**Requires senior judgment**: yes (AKM 結果の妥当性検証)
**Blocks**: なし
**Blocked by**: なし

---

## Goal

最新 SILVER credits (3,237,230 行、20/02 dedup 後) で AKM (Abowd-Kramarz-Margolis) を再計算し、`theta_i` (person FE) / `psi_j` (studio FE) を更新、`feat_person_scores` / `scores` / `score_history` に反映する。

---

## Hard constraints

- **H1**: AKM 結果変数は `production_scale` (構造的) のみ、`anime.score` 不使用
- **H5**: 既存テスト破壊禁止
- **H8**: 行番号信頼禁止
- silver.duckdb / gold.duckdb backup 必須

---

## Pre-conditions

- [ ] `git status` clean
- [ ] credits 3.2M 確認: `duckdb result/silver.duckdb -c "SELECT COUNT(*) FROM credits"`
- [ ] gold.duckdb pipeline 健全 (19/01 完了)
- [ ] `pixi run test` baseline pass

---

## 設計

### AKM 再実行手順

1. silver.duckdb / gold.duckdb backup
2. `pixi run pipeline` (or `pipeline-inc`) を full mode で実行 — credits の変更を Phase 5 (core_scoring) が検知して再計算
3. AKM 結果検証:
   - theta_i 分布 (mean ≈ 0、std 妥当性)
   - psi_j 分布 (mean ≈ 0、std 妥当性)
   - limited mobility bias 警告 (Andrews et al. 2008 criteria)
4. feat_person_scores / scores / score_history 更新確認
5. bench 結果記録

### 妥当性チェック

- `theta_i` の極端値 (|z-score| > 5) を sample で抽出
- 主要 director (例: 宮崎駿) の theta_i が妥当範囲か (sanity check)
- 連結成分 (connected component) サイズ確認

### bench 比較

19/01 の `gold_refresh_summary.md` の数値と比較:
- person_scores 行数変化
- AKM 計算時間
- top 10 theta_i 変化

---

## Files to modify (potential)

| File | 変更内容 |
|------|---------|
| `src/pipeline_phases/core_scoring.py` | (バグ発見時のみ) |
| `src/analysis/structural_estimation.py` | (AKM ロジック不変、改修禁止 — H 系列) |
| `result/audit/akm_refresh_summary.md` | 結果記録 (新規) |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/analysis/structural_estimation.py` | AKM ロジック不変 |
| `src/etl/silver_loaders/*` | SILVER 不変 |

---

## Steps

### Step 1: backup + baseline 取得

```bash
cp result/silver.duckdb result/silver.duckdb.bak.$(date +%Y%m%d-%H%M%S)
cp result/gold.duckdb result/gold.duckdb.bak.$(date +%Y%m%d-%H%M%S)
duckdb result/gold.duckdb -c "
SELECT COUNT(*) AS person_scores FROM person_scores;
SELECT COUNT(*) AS scores FROM scores;
SELECT MIN(theta_i), AVG(theta_i), MAX(theta_i), STDDEV(theta_i) FROM feat_person_scores
" > /tmp/akm_before.txt
```

### Step 2: pipeline 実行 (full mode、AKM 再計算)

```bash
pixi run pipeline 2>&1 | tee /tmp/akm_refresh.log
```

`--force-recompute` 等のフラグがあれば use。なければ silver の更新時刻ベースで自動 trigger。

### Step 3: 結果検証

```bash
duckdb result/gold.duckdb -c "
SELECT COUNT(*) AS person_scores FROM person_scores;
SELECT MIN(theta_i), AVG(theta_i), MAX(theta_i), STDDEV(theta_i) FROM feat_person_scores
" > /tmp/akm_after.txt
diff /tmp/akm_before.txt /tmp/akm_after.txt
```

### Step 4: 妥当性チェック

主要 director の theta_i:
```sql
SELECT p.name_ja, fps.theta_i, fps.theta_se
FROM feat_person_scores fps
JOIN persons p ON p.id = fps.person_id
WHERE p.name_ja IN ('宮崎駿', '富野由悠季', '押井守', '今敏', '湯浅政明')
ORDER BY fps.theta_i DESC
```

### Step 5: bench 記録

```bash
pixi run bench 2>&1 | tee /tmp/akm_bench.log
```

`result/audit/akm_refresh_summary.md` に:
- 前後比較 (person_scores 数, theta_i 分布, top 10 director)
- AKM 計算時間
- limited mobility bias 統計
- bench 結果差分

---

## Verification

```bash
pixi run lint
pixi run test
duckdb result/gold.duckdb -c "SELECT COUNT(*) FROM person_scores"  # > 100,000
ls result/audit/akm_refresh_summary.md
```

---

## Stop-if conditions

- [ ] pipeline が SILVER 整合性問題で停止
- [ ] AKM 結果で theta_i mean が大きく 0 から外れる (> 0.1) → bug
- [ ] connected component が 1 個に偏る (limited mobility 過剰) → 警告のみ、Stop しない
- [ ] 既存テスト破壊 (新規)

---

## Rollback

```bash
cp result/silver.duckdb.bak.<timestamp> result/silver.duckdb
cp result/gold.duckdb.bak.<timestamp> result/gold.duckdb
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] feat_person_scores 更新確認
- [ ] `akm_refresh_summary.md` 生成
- [ ] DONE: `21_silver_enrichment/01_akm_refresh`
