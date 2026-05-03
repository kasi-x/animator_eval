# Task: Phase 4 — mart schema PK 復元 + pipeline 再実行

**ID**: `23_5tier_migration/02_phase4_mart_pk_restore`
**Priority**: 🟠
**Status**: pending
**Blocks**: Resolved 層効果の AKM 結果検証
**Blocked by**: なし

---

## 問題

Phase 1c の `CREATE TABLE mart.<table> AS SELECT * FROM gld.main.<table>` で **PRIMARY KEY 制約が落ちた** (DuckDB の CTAS 仕様)。

Phase 4 (pipeline 全実行 with resolved 経路) が `mart_writer.write_person_scores` の `INSERT ... ON CONFLICT (person_id) DO UPDATE SET ...` で `BinderException` failure。

```
_duckdb.BinderException: Binder Error: The specified columns as conflict target 
are not referenced by a UNIQUE/PRIMARY KEY CONSTRAINT or INDEX
```

mart PK 検出: 0 件 (期待: person_scores.person_id / score_history / etc. 全 27 table)

---

## ゴール

mart schema の全 table に元 DDL の PK 制約を復元 + pipeline 全実行で AKM 結果を更新。

---

## Files to modify

| File | 変更 |
|------|------|
| `src/analysis/io/mart_writer.py` | `_DDL` の PK 制約を `ALTER TABLE ... ADD PRIMARY KEY (...)` で適用 (CTAS 後 backfill) |
| `src/etl/integrate_duckdb.py` (or 新規 migration script) | mart schema 既存 table に PK 制約復元 |

または:
- 新規 `src/etl/migrations/v62_mart_pk_restore.py` で 1-shot migration

---

## Steps

1. mart 全 table の元 DDL を `_DDL` から抽出
2. 各 table で `ALTER TABLE mart.<t> ADD PRIMARY KEY (<col>)` 実行 (DuckDB syntax)
3. 既存 mart data に重複行あれば DELETE で dedup 後に PK 適用
4. pipeline 全実行 (`pixi run pipeline`)
5. AKM 結果検証 (期待: r² 改善、cross-source studio で n_observations 増加)

---

## 完了条件

- mart 全 27 table に PK 制約復元
- pipeline 完走、`mart.person_scores` row 更新
- AKM 結果検証 (`result/json/akm_diagnostics.json` で r² / n_observations / connected_set_size 確認)
- 期待: 現 r²=0.5993 → 改善 (resolved cross-source 効果)
- commit + push
