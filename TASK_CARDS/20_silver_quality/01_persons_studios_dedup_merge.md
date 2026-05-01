# Task: persons / studios 高信頼度 dedup merge

**ID**: `20_silver_quality/01_persons_studios_dedup_merge`
**Priority**: 🟠
**Estimated changes**: 約 +400 / -50 lines, 3 files
**Requires senior judgment**: yes (H3 配慮、merge 基準)
**Blocks**: なし
**Blocked by**: なし

---

## Goal

18/01 dedup audit で検出された persons (1,260 候補) / studios (602 候補) のうち、**高信頼度ペア (similarity > 0.99 + 構造的属性一致)** のみを安全に merge し、`meta_entity_resolution_audit` に記録する。

---

## Hard constraints

- **H3**: `src/analysis/entity_resolution.py` のロジック自体は**触らない**。本タスクは audit 結果ベースの「特例 merge ETL」を別経路で実施
- **H1**: merge 判定に `anime.score` 不使用
- **H4**: merge 後も `credits.evidence_source` 維持 (各 source が並存)
- **H5**: 既存テスト破壊禁止
- **H8**: 行番号信頼禁止

---

## Pre-conditions

- [ ] `git status` clean
- [ ] `result/audit/silver_dedup_persons.csv` / `silver_dedup_studios.csv` 存在
- [ ] `pixi run test` baseline pass
- [ ] silver.duckdb backup

---

## High-confidence merge 基準

### persons
- **同 name_ja exact** + **birth_date exact** (差 0 日) → automatic merge
- **同 name_ja exact** + **birth_date 差 ≤ 1 日** + **同 cohort 役職分布 (Jaccard > 0.7)** → automatic merge
- それ以外は merge せず audit 記録のみ

### studios
- **同 name (NFKC + lowercase + 句読点除去)** + **country exact** → automatic merge
- それ以外は audit 記録のみ

### merge ロジック (簡易、ID 統合方式)

最若いレコード (= 最初に登録された ID) を canonical とし、他を redirect:
```
canonical_id ← min(候補 IDs)
他の ids → meta_entity_resolution_audit に redirect 記録
credits.person_id を canonical_id に UPDATE
persons から非 canonical 行を DELETE
```

ただし同一 person が複数 source で並存することは **許容** (各 source の identity は credits.evidence_source で保持)。merge は SILVER `persons` の重複行のみ。

---

## Files to create

| File | 内容 |
|------|------|
| `src/etl/dedup/__init__.py` | 空 |
| `src/etl/dedup/safe_merge.py` | `merge_persons(conn, audit_csv) -> dict` / `merge_studios(conn, audit_csv) -> dict` |
| `tests/test_etl/test_dedup_safe_merge.py` | 合成 fixture で merge ロジック検証 |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/analysis/entity_resolution.py` | H3 |
| `src/etl/silver_loaders/*` | 既存 ETL 不変 |

---

## Steps

### Step 1: audit CSV 読込 + 高信頼度フィルタ

```python
def filter_high_confidence_persons(audit_df: DataFrame) -> DataFrame:
    """returns rows where similarity > 0.99 + birth_date matches"""
```

### Step 2: merge 関数実装

`safe_merge.py`:
- `merge_persons(conn, audit_csv) -> dict[merge_count, audit_logged]`
- `merge_studios(conn, audit_csv) -> dict[merge_count, audit_logged]`

各 merge は transaction 内で:
1. `meta_entity_resolution_audit` に redirect ペア記録
2. `credits.person_id` (or `anime_studios.studio_id`) を canonical に UPDATE
3. 非 canonical 行を `persons` (or `studios`) から DELETE

### Step 3: Dry-run mode

```python
def merge_persons(conn, audit_csv, dry_run=False) -> dict:
    """if dry_run, returns counts without DB writes"""
```

最初は dry-run で件数確認、その後 actual merge。

### Step 4: Audit table 拡張

`meta_entity_resolution_audit` に列確認:
- `(redirect_from_id, redirect_to_id, table_name, similarity, merged_at, merge_reason)`

不足列があれば末尾追記で ALTER TABLE。

### Step 5: 実行 + ロールバック確認

```bash
# Dry-run
pixi run python -c "
import duckdb
from src.etl.dedup.safe_merge import merge_persons, merge_studios
conn = duckdb.connect('result/silver.duckdb')
print('persons:', merge_persons(conn, 'result/audit/silver_dedup_persons.csv', dry_run=True))
print('studios:', merge_studios(conn, 'result/audit/silver_dedup_studios.csv', dry_run=True))
"
# Actual run (with backup)
cp result/silver.duckdb result/silver.duckdb.bak.$(date +%Y%m%d-%H%M%S)
pixi run python -c "...(dry_run=False)..."
```

---

## Verification

```bash
pixi run lint
pixi run test-scoped tests/test_etl/test_dedup_safe_merge.py
duckdb result/silver.duckdb -c "SELECT COUNT(*) FROM meta_entity_resolution_audit WHERE merged_at >= '2026-05-02'"
duckdb result/silver.duckdb -c "SELECT COUNT(*) FROM persons"  # 微減確認
```

期待:
- persons audit row > 0 (高信頼度 merge 実績)
- credits row 不変 (person_id だけ更新、行数 invariant)
- studios row 微減

---

## Stop-if conditions

- [ ] audit CSV 不在 (18/01 未実行)
- [ ] merge 件数が persons 全体の 5% 超 → 基準厳格化要、本カード Stop
- [ ] credits row 数が変化 → INSERT/DELETE 漏れ、Rollback

---

## Rollback

```bash
cp result/silver.duckdb.bak.<timestamp> result/silver.duckdb
git checkout src/etl/dedup/ tests/test_etl/test_dedup_safe_merge.py
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] DONE: `20_silver_quality/01_persons_studios_dedup_merge`
