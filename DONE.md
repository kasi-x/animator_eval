# DONE.md — 完了済みサマリー

作成日: 2026-04-22

完了済み作業の参照用メモ。詳細履歴は git log と commit message に委ねる。未完了作業は `TODO.md`。

---

## 完了カテゴリ

### anime.score 汚染除去
- 16 pathways を全て除去 (akm, graph, skill, patronage, IV, temporal_pagerank, individual_contribution 他)
- SILVER 層 100% score-free
- 検証: `rg 'anime\.score\b' src/analysis/ src/pipeline_phases/` → 0 件

### 計算ロジック監査
- 設計疑念 D01-D27: 27 件全て対処済み (感度分析 / データ駆動推定 / 文書化)
- 実装バグ B01-B16 + A1-A3 + 2次10件: 合計 20 件修正

### Phase 1: データ層再構築
- v53 migration: anime テーブル slim 化 (16 columns、score/popularity 除去)
- v54 migration: `credits.source` 削除 (`evidence_source` のみ)
- BRONZE 層分離 (`src_anilist_anime`, `src_mal_anime`)
- `src/utils/display_lookup.py` helper (UI metadata 専用経路)

### Phase 2: Gold 層
- `meta_lineage` テーブル新設 (v54)
- Method Notes auto-generation
- 3 Audience Brief Indices (Policy / HR / Biz)
- Report inventory mapping (37 active + 12 archived)

### Phase 3: レポート再編
- `docs/REPORT_INVENTORY.md` / `docs/CALCULATION_COMPENDIUM.md`
- Vocabulary enforcement (39 files, 0 violations)
- 2161 tests pass

### Phase 4 基盤
- Vocabulary lint (word boundary、false positive 防止済み)
- Pre-commit hook integration
- SectionBuilder.validate()
- Taskfile 13 タスク追加

### コード品質
- `generate_all_reports.py` 分割 (23,904 → 22,777 行、-1,127 行)
- Lint エラー 9 件修正

### Phase 4 残務 (2026-04-23)
- **§1.4** `anime_display` 廃止: DDL コメントアウト済、v55 migration に DROP TABLE、分析コードの参照 0 件
- **§2.1** meta_lineage population: 5 briefs 全て実装済み確認 (policy_attrition / policy_monopsony / policy_gender_bottleneck / mgmt_studio_benchmark / biz_genre_whitespace)
- **§2.4** ci_check_lineage.py: bronze leak detection + lineage quality validation (semver / hex hash / staleness ≤30d) 完全実装済み確認
- **§2.5** vocabulary audit: `lint_vocab.py` に definitional filter + exceptions YAML (16 entries) 追加。56 files 0 violations
- **§3.3** ops_entity_resolution_audit 書き込み: `pipeline_phases/entity_resolution.py:422-471` で生成・upsert 済み確認
- **§6.4** report helpers 単体テスト: `tests/test_report_helpers.py` 46 tests — fmt_num / name_clusters_* / adaptive_height / insert_lineage / subsample_for_scatter / capped_categories / safe_nested / data_driven_badges / badge_class / add_distribution_stats

### lint 整理 (2026-04-23)
- `scripts/lint_report_vocabulary.py` 削除 (外部呼び出しなし、`scripts/report_generators/lint_vocab.py` に完全移行)
- `scripts/analyze_credit_intervals.py` DISCLAIMER 免除を exceptions YAML に登録

### テストカバレッジ追加 (2026-04-23)
- **T02** `tests/test_patronage_dormancy_direct.py`: 12 tests — dormancy 指数減衰/猶予期間/最新クレジット/単調性 + patronage premium 検証
- **T03 VA モジュール** `tests/test_va_modules.py`: +20 tests (TestVaAkm 4件・TestVaGraph 4件・TestEnsembleSynergy 3件 追加、計38件)
- **T03 VA パイプライン** `tests/test_va_pipeline_phases.py`: 新規10 tests — graph_construction / core_scoring / supplementary_metrics / result_assembly 各 smoke test

### DuckDB カード状態確認 (2026-04-23)
- カード 01-04 完了確認: bronze_writer / 全6 scraper 移行済み / integrate_duckdb.py / gold_writer.py 存在確認
- カード 05 (analysis cutover): data_loading.py 等が SQLite 継続使用 → 未完了
- カード 06 (SQLite decommission): カード05依存 → 未着手

---

## スキーマ進化

| Version | 状態 | 概要 |
|---------|------|------|
| v50 | 実装済み | canonical silver 確立 (anime 統合、sources lookup、evidence_source rename) |
| v51-v53 | 実装済み | anime テーブル slim 化 |
| v54 | 実装済み | credits.source 完全削除、meta_lineage 新設 |
| v55 | 🟡 未登録 | `TODO.md §1` で修復予定 |
| v56 | 🟡 保留 | ジャンル正規化 (実行コスト高で別途スケジュール) |

---

## 却下済みフレームワーク (再提案禁止)

- **OpenTelemetry**: 単一プロセスに過剰
- **Hydra / Pydantic Settings**: 方法論パラメータは固定宣言
- **Polars**: DuckDB で冗長
- **GPU (cuGraph / cuDF)**: Rust 比較データ不在、投資合わない

詳細: `~/.claude/projects/-home-user-dev-animetor-eval/memory/feedback_framework_rejections.md`
