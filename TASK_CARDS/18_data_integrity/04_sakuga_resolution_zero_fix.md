# Task: sakuga_work_title_resolution 0 行原因究明 + 修正

**ID**: `18_data_integrity/04_sakuga_resolution_zero_fix`
**Priority**: 🟠
**Estimated changes**: 約 +50 / -10 lines, 1-2 files
**Requires senior judgment**: no
**Blocks**: なし
**Blocked by**: なし

---

## Goal

`sakuga_work_title_resolution` 表が 0 行な原因を究明し、根本修正する。

---

## Hard constraints

- **H3**: entity_resolution ロジック不変
- **H5**: 既存テスト破壊禁止
- **H8**: 行番号信頼禁止

---

## Pre-conditions

- [ ] `git status` clean
- [ ] BRONZE sakuga_atwiki 表確認:
```bash
ls result/bronze/source=sakuga_atwiki/table=*/date=*/*.parquet
duckdb result/silver.duckdb -c "SELECT COUNT(*) FROM read_parquet('result/bronze/source=sakuga_atwiki/table=credits/date=*/*.parquet', union_by_name=true) WHERE work_title IS NOT NULL"
```
- [ ] `pixi run test` baseline pass

---

## 候補原因

1. **BRONZE 側 row 0**: scrape 未実行 / parser 失敗
2. **integrate_duckdb dispatcher で sakuga loader 呼出スキップ**: try/except で silent fail
3. **matcher が常に unresolved 返す**: title 正規化バグ / 年ガード過厳
4. **schema 不整合**: 列型ミスマッチで INSERT 失敗
5. **BRONZE work_title カラム名不一致**: parser 出力 vs loader 期待

---

## Files to investigate

| File | 確認内容 |
|------|---------|
| `result/bronze/source=sakuga_atwiki/table=credits/date=*/` | parquet 存在 + work_title カラム |
| `src/etl/silver_loaders/sakuga_atwiki.py` | integrate() 中で title resolution 行数を log 出力するか |
| `src/etl/sakuga_title_matcher.py` | match_title() の戻り値分布 |
| `src/etl/integrate_duckdb.py` | sakuga_atwiki dispatcher 呼出箇所、エラー suppression あるか |

---

## Steps

### Step 1: BRONZE 側 row 確認

```bash
duckdb result/silver.duckdb -c "
SELECT COUNT(*) AS rows,
       COUNT(work_title) AS non_null_titles,
       COUNT(DISTINCT work_title) AS distinct_titles
FROM read_parquet('result/bronze/source=sakuga_atwiki/table=credits/date=*/*.parquet', union_by_name=true)
"
```

### Step 2: integrate_duckdb 実行ログ確認

```bash
pixi run python -m src.etl.integrate_duckdb --rebuild 2>&1 | grep -i "sakuga\|resolution"
```

エラーメッセージ確認。

### Step 3: matcher 単体テスト

```python
from src.etl.sakuga_title_matcher import match_title
import duckdb
conn = duckdb.connect('result/silver.duckdb', read_only=True)
sample = conn.execute("""
    SELECT DISTINCT work_title FROM read_parquet('result/bronze/source=sakuga_atwiki/table=credits/date=*/*.parquet', union_by_name=true)
    WHERE work_title IS NOT NULL LIMIT 20
""").fetchall()
for (t,) in sample:
    print(t, "→", match_title(conn, t, year=None))
```

### Step 4: 根本修正

原因に応じて:
- BRONZE 側 row 0 → scrape 起動 (本カード範囲外、別カード or 手動)
- dispatcher silent fail → log message 追加 + 例外伝播
- matcher バグ → match 関数修正
- schema 不整合 → schema 修正

---

## Verification

修正後:
```bash
pixi run python -m src.etl.integrate_duckdb --rebuild
duckdb result/silver.duckdb -c "SELECT COUNT(*) FROM sakuga_work_title_resolution"
# 期待: > 0 (BRONZE work_title 行数の少なくとも 1%)
pixi run lint
pixi run test-scoped tests/test_etl/test_silver_sakuga_atwiki.py
```

---

## Stop-if conditions

- [ ] BRONZE 側 row 0 (scrape 未実行) → scrape 系タスク化 (別カード)、本カード Stop
- [ ] integrate ETL が他の理由で全体破綻 → broader fix が必要、本カード Stop

---

## Rollback

```bash
git checkout src/etl/silver_loaders/sakuga_atwiki.py src/etl/sakuga_title_matcher.py src/etl/integrate_duckdb.py
```

---

## Completion signal

- [ ] `sakuga_work_title_resolution` row count > 0
- [ ] 原因 + 修正内容を `docs/troubleshooting/sakuga_resolution_zero.md` に記録
- [ ] DONE: `18_data_integrity/04_sakuga_resolution_zero_fix`
