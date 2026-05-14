# Task: source_mat fallback で source 名混入バグ

**ID**: `18_data_integrity/06_source_mat_fallback_bug`
**Priority**: 🟠 High
**Created**: 2026-05-15
**Estimated changes**: ~+5 / -3 lines, 1 file + integrate 再走
**Blocks**: source_mat 集計の正確性 / anime metadata 信頼性

---

## Goal

`src/etl/integrate_duckdb.py:538` の `source_mat` fallback が `original_work_type`
NULL 時に `source` (source 名) を入れているバグを修正する.

---

## 発見経緯

`scripts/maintenance/keyframe_diff_analysis.py` (2026-05-15) で
`keyframe_vs_others_diff.csv` を分類した結果, `anime.source_mat` 差分 2,438 件の
**100% が `kf_bug_source_name_in_field`** — kf 側の source_mat に `keyframe`
が入っていた.

```
anime.source_mat 2438 rows
  kf_bug_source_name_in_field 2438 (100.0%)
```

---

## Root cause

`integrate_duckdb.py:538`

```python
source_mat="COALESCE(TRY_CAST(original_work_type AS VARCHAR), TRY_CAST(source AS VARCHAR))"
if "original_work_type" in cols
else "'unknown'::VARCHAR",
```

`original_work_type` カラムが存在するが NULL の場合、`source` カラム (source 名 =
`keyframe` / `anilist` / `mal` 等) を fallback で source_mat に入れている.

本来 source_mat は **「原作素材」(MANGA / NOVEL / ORIGINAL / VISUAL_NOVEL 等)** を入れる列で、
source 名は不適切.

---

## Hard constraints

- **H1**: anime.score 系列は触らない (source_mat は構造的属性なので H1 抵触なし)
- **修正後 integrate 再実行が必要**: 既存 conformed.anime の source_mat 列は再
  ingest で正される

---

## Files to modify

| File | 変更内容 |
|------|----------|
| `src/etl/integrate_duckdb.py:538` | `COALESCE(...TRY_CAST(source...))` の fallback を NULL に変更 |

---

## 修正案

```python
# Before
source_mat="COALESCE(TRY_CAST(original_work_type AS VARCHAR), TRY_CAST(source AS VARCHAR))"

# After
source_mat="TRY_CAST(original_work_type AS VARCHAR)"
```

→ `original_work_type` 欠損時は NULL を入れる. `source` への fallback は意味的に誤り.

---

## Acceptance Criteria

- [ ] L538 修正済
- [ ] `pixi run lint` clean
- [ ] integrate_duckdb 再走 → conformed.anime.source_mat の `keyframe` 値が消える
- [ ] `keyframe_diff_analysis.py` 再実行で `kf_bug_source_name_in_field` 件数 0
- [ ] 既存 source_mat 関連 test pass (test_integrate_duckdb.py)

---

## Out of Scope

- 他 source (mal/seesaa 等) の original_work_type 欠損時の補完戦略 — 別タスク
- BRONZE parser 修正 (keyframe scraper が `original_work_type` を抽出するか)
