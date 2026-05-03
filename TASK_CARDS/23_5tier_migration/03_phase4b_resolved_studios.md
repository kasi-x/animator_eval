# Task: Phase 4b — Resolved 層 anime.studios 復元 (AKM 再計算)

**ID**: `23_5tier_migration/03_phase4b_resolved_studios`
**Priority**: 🟠
**Blocks**: 5層 architecture の AKM 効果検証

---

## 問題

Phase 4 で AKM 結果が **regression**:
- 21/01 で修正した silent fail と同じ「`anime_studios_not_in_silver` warning」が **resolved 経路で再発**
- akm_diagnostics: r²=0, n_observations=0, n_person_fe=0

原因候補:
1. `resolved.duckdb` の anime table に `studios` 列が無い (Phase 2a/2b 設計で欠落)
2. `load_anime_resolved(conformed_path=...)` の `_build_studios_map_from_conformed` SQL バグ
3. Hamilton DAG の `load_pipeline_data_resolved` 呼出時に `conformed_path` 不渡し

## ゴール

resolved 経路でも `infer_studio_assignment` が機能、AKM 結果復活 (r² > 0.5)。

## 範囲

- 調査: `src/etl/resolved/_ddl.py` の anime table 列定義
- 調査: `src/analysis/io/resolved_reader.py` の `_build_studios_map_from_conformed`
- 調査: `src/pipeline_phases/data_loading.py` の `load_pipeline_data_resolved` で conformed_path 渡し
- 修正: 該当箇所
- 実行: pipeline 再実行 + AKM 結果検証

## 完了条件

- AKM r² > 0.5、n_observations > 100K (21/01 復活時の数値)
- pipeline 完走、`mart.person_scores.person_fe` non-zero > 50K
- commit + push
