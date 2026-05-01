# Task: BRONZE → SILVER 取込率計測

**ID**: `18_data_integrity/02_silver_completeness_check`
**Priority**: 🟠
**Estimated changes**: 約 +400 / -0 lines, 3 files
**Requires senior judgment**: no
**Blocks**: なし
**Blocked by**: なし

---

## Goal

各 BRONZE 表 (9 source 80+ 表) について SILVER 統合後の row 取込率を計測し、欠損カバレッジを可視化する。

---

## Hard constraints

- **H5**: 既存テスト破壊禁止
- **H8**: 行番号信頼禁止
- read-only クエリのみ (BRONZE / SILVER 改変なし)

---

## Pre-conditions

- [ ] `git status` clean
- [ ] BRONZE parquet 存在確認: `result/bronze/source=*/table=*/date=*/`
- [ ] SILVER duckdb 存在確認: `result/silver.duckdb`
- [ ] `pixi run test` baseline pass

---

## 計測戦略

### per-source per-table

```
bronze_rows = SELECT COUNT(*) FROM read_parquet('result/bronze/source=<s>/table=<t>/date=*/*.parquet', union_by_name=true)
silver_rows = SELECT COUNT(*) FROM <silver_table> WHERE evidence_source='<s>' OR id LIKE '<prefix>:%'
coverage = silver_rows / bronze_rows
```

### サマリ表

```
| source | bronze_table | bronze_rows | silver_target | silver_rows | coverage | unmapped_rows |
|--------|--------------|-------------|---------------|-------------|----------|---------------|
```

### 欠損調査

各 BRONZE 表で SILVER に取込まれなかった行のサンプル (10 行) を抽出 → 欠損理由推定 (NULL ID / 不正型 / dedup 競合 / 未対応列)。

---

## Files to create

| File | 内容 |
|------|------|
| `src/etl/audit/silver_completeness.py` | `check(bronze_root, silver_db) -> DataFrame` |
| `tests/test_etl/test_silver_completeness.py` | smoke + 計算ロジック unit test |
| `result/audit/silver_completeness.md` | 取込率サマリ (本タスク実行で生成) |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/etl/silver_loaders/*` | 既存 ETL 不変 |
| BRONZE parquet | 改変禁止 |

---

## Steps

### Step 1: BRONZE 表一覧自動列挙

```python
def list_bronze_tables(bronze_root: Path) -> list[tuple[str, str]]:
    """Returns [(source, table_name), ...] from result/bronze/source=*/table=*/."""
```

### Step 2: source × table → silver_target マッピング

`SOURCE_TABLE_TO_SILVER` dict で各 BRONZE 表が SILVER のどの表に対応するか宣言:

```python
SOURCE_TABLE_TO_SILVER = {
    ("anilist", "anime"): ("anime", "id LIKE 'anilist:a%'"),
    ("anilist", "persons"): ("persons", "id LIKE 'anilist:p%'"),
    ...
}
```

未マップな BRONZE 表は `unmapped: true` で報告。

### Step 3: check() 関数実装

各 (source, table) に対し coverage 計算、結果 DataFrame 返却。

### Step 4: 欠損サンプル抽出

```python
def sample_missing_rows(conn, bronze_glob, silver_table, silver_filter, n=10) -> DataFrame:
    """Returns BRONZE rows not in SILVER (anti-join sample)."""
```

### Step 5: Markdown レポート生成

`result/audit/silver_completeness.md`:
- 全体集計 (covered / unmapped / partial / failed)
- per-source サマリ表
- 欠損サンプル top 5 (各 source × 各 table)

### Step 6: テスト

合成 BRONZE parquet + 合成 SILVER duckdb で coverage 計算検証。

---

## Verification

```bash
pixi run lint
pixi run test-scoped tests/test_etl/test_silver_completeness.py
pixi run python -c "
from pathlib import Path
import duckdb
from src.etl.audit.silver_completeness import check
print(check(Path('result/bronze'), 'result/silver.duckdb'))
"
ls result/audit/silver_completeness.md
```

---

## Stop-if conditions

- [ ] BRONZE parquet が読めない (パス不正)
- [ ] `pixi run test` 既存テスト失敗

---

## Rollback

```bash
rm -f src/etl/audit/silver_completeness.py tests/test_etl/test_silver_completeness.py
rm -f result/audit/silver_completeness.md
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] レポート生成
- [ ] DONE: `18_data_integrity/02_silver_completeness_check`
