# Task: SILVER → GOLD refresh (feat_* + scores 全再生成)

**ID**: `19_silver_postprocess/01_silver_to_gold_refresh`
**Priority**: 🟠
**Estimated changes**: 約 +200 / -50 lines, 2-3 files
**Requires senior judgment**: yes (gold pipeline 整合性)
**Blocks**: なし
**Blocked by**: なし

---

## Goal

SILVER 28 表が完備された現状を反映し、gold.duckdb の `feat_*` テーブル + `scores` + `score_history` を全再生成する。失敗箇所の root cause 修正含む。

---

## Hard constraints

- **H1**: scoring 経路に `anime.score` / `popularity` / `favourites` 流入禁止 (`display_*` のみ display 用に保持)
- **H5**: 既存テスト破壊禁止
- **H8**: 行番号信頼禁止
- 既存 pipeline (`src/pipeline_phases/`) の Phase 順序は不変

---

## Pre-conditions

- [ ] `git status` clean
- [ ] SILVER 28 表 row 確認 (credits 530 万, anime 56 万, persons 27 万)
- [ ] `pixi run test` baseline pass
- [ ] gold.duckdb backup (cleanup 前)

---

## 設計

### 対象 GOLD 表

```bash
duckdb result/gold.duckdb -c "SELECT table_name FROM information_schema.tables WHERE table_schema='main' ORDER BY 1"
```

期待される表:
- `feat_career_annual`
- `feat_person_scores`
- `feat_studio_affiliation`
- `scores`
- `score_history`
- `meta_*` (lineage / entity_resolution_audit)

### 再生成手順

```bash
pixi run pipeline  # 全 Phase 1-10 実行
# or
pixi run pipeline-resume  # checkpoint から再開
```

実行中エラーは log + 報告。

### 失敗箇所

過去のエラー: `tests/test_gold_writer.py` 2 件 pre-existing failure (atomic_swap 関連、既に対応済 commit `9436d09`)。`test_atomic_swap_replaces_stale_file` / `test_exception_preserves_old_file` の現状確認。

### Performance baseline

```bash
pixi run bench  # パフォーマンスベンチ
```

実行前/後の数値比較を `result/audit/gold_refresh_bench.md` に記録。

---

## Files to modify (potential)

| File | 変更内容 |
|------|---------|
| `src/pipeline_phases/*.py` | エラー箇所の修正 (発見時のみ) |
| `src/analysis/io/gold_writer.py` | 必要なら追加修正 |
| `tests/test_gold_writer.py` | pre-existing 失敗 2 件の修正 (atomic_swap 撤去後の整合) |
| `result/audit/gold_refresh_summary.md` | 結果記録 |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/etl/silver_loaders/*` | 18 で完成済 |
| `src/analysis/structural_estimation.py` | AKM ロジック不変 |

---

## Steps

### Step 1: gold.duckdb 現状把握

```bash
duckdb result/gold.duckdb -c "
SELECT table_name, (SELECT COUNT(*) FROM information_schema.columns WHERE table_name=t.table_name) AS cols
FROM information_schema.tables t WHERE table_schema='main' ORDER BY 1
"
```

各表の row count 取得。

### Step 2: gold backup

```bash
cp result/gold.duckdb result/gold.duckdb.bak.$(date +%Y%m%d-%H%M%S)
```

### Step 3: pipeline 実行 (差分 mode 推奨)

```bash
pixi run pipeline-inc 2>&1 | tee /tmp/gold_refresh.log
```

エラー発生時は full mode:

```bash
pixi run pipeline 2>&1 | tee /tmp/gold_refresh.log
```

### Step 4: pre-existing failure 修正

```bash
pixi run test-scoped tests/test_gold_writer.py
```

`test_atomic_swap_replaces_stale_file` / `test_exception_preserves_old_file` が atomic_swap 撤去後の整合性に対応するよう書き換え。

### Step 5: 結果検証

```bash
duckdb result/gold.duckdb -c "
SELECT 'feat_career_annual' AS t, COUNT(*) FROM feat_career_annual
UNION ALL SELECT 'feat_person_scores', COUNT(*) FROM feat_person_scores
UNION ALL SELECT 'scores', COUNT(*) FROM scores
"
```

全表 row > 0 確認。

### Step 6: bench 結果記録

`result/audit/gold_refresh_summary.md` に:
- gold 再生成 runtime
- 各 feat_* row count
- bench 結果 (pixi run bench)
- 修正内容

---

## Verification

```bash
pixi run lint
pixi run test
duckdb result/gold.duckdb -c "SELECT COUNT(*) FROM scores"
ls result/audit/gold_refresh_summary.md
```

---

## Stop-if conditions

- [ ] pipeline が SILVER 整合性問題で停止 → SILVER 側 fix が先 (本カード Stop)
- [ ] 既存テストの**新たな** 失敗 (pre-existing 2 件以外)
- [ ] gold.duckdb 容量爆発 (> 5 GB)

---

## Rollback

```bash
mv result/gold.duckdb.bak.<timestamp> result/gold.duckdb
git checkout src/pipeline_phases/ tests/test_gold_writer.py
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] gold 再生成完了、scores row > 100,000 期待
- [ ] tests/test_gold_writer.py の pre-existing 2 件含めて全 green
- [ ] DONE: `19_silver_postprocess/01_silver_to_gold_refresh`
