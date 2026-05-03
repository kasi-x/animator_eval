# Task: anime_studios 残取込率改善 (22/01 follow-up)

**ID**: `22_silver_coverage/02_anime_studios_residual`
**Priority**: 🟠
**Estimated changes**: 約 +250 / -30 lines, 4-5 files
**Requires senior judgment**: yes (filter 緩和判断)
**Blocks**: AKM 精度向上
**Blocked by**: 22/01 完了済

---

## Goal

22/01 で 2.5% → 7.7% まで改善した anime_studios 覆い率を更に向上。残取りこぼし source:

- **bangumi**: 0% (anime_studios INSERT 経路自体が無い可能性)
- **seesaawiki**: 状況未確認 (生 BRONZE には studios / gross_studios 表あり)
- **mediaarts**: 37% (`is_main=True` filter 厳しすぎ)
- **keyframe**: 67% (filter 確認要)

---

## Hard constraints

- H1, H3, H4 (`source` 列維持), H5, H8

---

## Pre-conditions

- [ ] `git status` clean
- [ ] 22/01 統合済確認: `git log --oneline | head -3`
- [ ] coverage 現状: `pixi run python -m src.etl.audit.anime_studios_coverage`

---

## 調査 + 修正対象

### bangumi
- BRONZE `subject_persons` で `position` (例: 動画制作 / 製作) や `subject_companies` 表 (もし存在) 確認
- `subject_relations` の relation_type で「制作」関連を抽出
- 該当源があれば silver_loaders/bangumi.py に anime_studios INSERT 追加

### seesaawiki
- BRONZE `studios` / `gross_studios` / `anime_studios` 表 (10.1 で 18,968 / 56,860 行確認済)
- silver_loaders/seesaawiki.py の現 INSERT 文を確認 + 補強

### mediaarts (37% → ?)
- `is_main=True` filter 緩和 (協力 studio も含める、ただし `role` 列で識別)
- 22/01 で `アニメーション制作 / is_main=True` のみ取込 → `is_main=False` も取込み role='support' で記録

### keyframe (67% → ?)
- silver_loaders/keyframe.py の filter 確認
- BRONZE `anime_studios` 全数チェック

---

## Files to modify

| File | 内容 |
|------|------|
| `src/etl/silver_loaders/bangumi.py` | anime_studios INSERT 経路追加 |
| `src/etl/silver_loaders/seesaawiki.py` | studios / gross_studios → anime_studios 取込補強 |
| `src/etl/silver_loaders/madb.py` | `is_main=False` も取込み (role='support') |
| `src/etl/silver_loaders/keyframe.py` | filter 緩和 |
| `tests/test_etl/test_silver_*.py` | 各 loader 回帰テスト追加 |

## Files to NOT touch

- `src/analysis/io/silver_reader.py` (21/01 修正済)
- `src/analysis/scoring/akm.py`

---

## Steps

### Step 1: 各 source の BRONZE 構造調査

```bash
duckdb -c "
SELECT 'bangumi/sp position' AS what, COUNT(DISTINCT position)
FROM read_parquet('result/bronze/source=bangumi/table=subject_persons/date=*/*.parquet', union_by_name=true);
SELECT 'seesaawiki/studios' AS what, COUNT(*)
FROM read_parquet('result/bronze/source=seesaawiki/table=studios/date=*/*.parquet', union_by_name=true)
"
```

### Step 2: 各 loader 修正 + テスト

### Step 3: 再 ETL + coverage 再計測

```bash
cp result/silver.duckdb result/silver.duckdb.bak.$(date +%Y%m%d-%H%M%S)
pixi run python -m src.etl.integrate_duckdb
pixi run python -m src.etl.audit.anime_studios_coverage
```

期待: 7.7% → 25%+

---

## Verification

```bash
pixi run lint
pixi run test-scoped tests/test_etl/test_silver_bangumi.py tests/test_etl/test_silver_seesaawiki.py tests/test_etl/test_silver_madb.py tests/test_etl/test_silver_keyframe.py tests/test_etl/test_anime_studios_coverage.py
duckdb result/silver.duckdb -c "
SELECT source, COUNT(DISTINCT anime_id) FROM anime_studios GROUP BY 1 ORDER BY 2 DESC
"
```

---

## Stop-if conditions

- [ ] BRONZE 側に該当 source の studio 情報が真に無い → scrape タスク化
- [ ] 既存テスト破壊

---

## Rollback

```bash
cp result/silver.duckdb.bak.<timestamp> result/silver.duckdb
git checkout src/etl/silver_loaders/ tests/test_etl/
```

---

## Completion signal

- [ ] coverage 7.7% → 20%+ 改善
- [ ] DONE: `22_silver_coverage/02_anime_studios_residual`
