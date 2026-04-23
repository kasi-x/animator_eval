# TODO.md — 未完了作業の一元管理

作成日: 2026-04-22 / 最終更新: 2026-04-24

本書はプロジェクト内のすべての**未完了**項目を一元管理するファイルです。完了済みは `DONE.md`、設計原則は `CLAUDE.md`。

---

## 優先度マトリクス

| 優先度 | カテゴリ | 内容 |
|--------|---------|------|
| 🟡 Minor | テストカバレッジ | analysis_modules ユニットテスト、tests/ ディレクトリ整理 |
| 🟡 Minor | スクレイパー強化 | 差分更新、retry refactor |
| 🟡 Maintenance | スキーマ後続 | v56 多言語・v57 構造的メタデータのフォローアップ |
| 🟢 Future | DuckDB 残務 | studio_affiliation 移植、entity resolution 書き込み経路 |
| 🟢 Future | Hamilton H-7 | PipelineContext 完全削除 (ctx → typed inputs) |
| 🟢 Future | feat_* 層別分離 | L2/L3 分割 |

**完了済み大項目**: DuckDB §4 全フェーズ ✅、Hamilton H-1〜H-6 ✅、レポート統廃合 §8 ✅、アーキテクチャ §9/11 ✅、ドキュメント §12 ✅

---

## SECTION 1: スキーマ後続タスク

### v56 多言語名対応

- [ ] 既存データ再スクレイプ: `hometown` 取得後に韓国・中国名の `name_ja` 誤入りを修正
- [ ] ANN / allcinema スクレイパーの `name_ko`/`name_zh` 対応 (+ タイ/ベトナム等は `names_alt` へ)

### v57 構造的メタデータ

- [ ] `title.native` を `country_of_origin` 分岐で `titles_alt` JSON へ格納 (v58 予定、names_alt 同パターン)

---

## SECTION 3: コード一貫性 残務

- [x] 他 scraper クエリ・パース関数を `src/scrapers/queries/` / `src/scrapers/parsers/` に分離 (2026-04-24)
- [ ] 既存 JVMG-source の credits を再スクレイプ or 再マップ (WIKIDATA_ROLE_MAP 修正後)

---

## SECTION 4: DuckDB 残務

§4.1〜§4.5 完了 (詳細: DONE.md)。

- [x] `compute_feat_studio_affiliation` DuckDB 移植 — silver に studios/anime_studios ETL 追加、feat_precompute.py に compute_feat_studio_affiliation_ddb() 追加、pipeline Phase 1.5 組み込み (2026-04-24)
- [x] Entity resolution 書き込み経路 DuckDB 化 — gold_writer.py に ops_entity_resolution_audit DDL + write_entity_resolution_audit_ddb() 追加 (2026-04-24)
- [x] Atlas migration DuckDB 環境再生成 — atlas.hcl に env "duckdb" 追加、migrations/legacy_sqlite/ へ旧 SQL 退避、migrations/duckdb/v1_initial.sql 生成 (2026-04-24)

---

## SECTION 5: Hamilton 残務

H-1〜H-6 完了 (詳細: DONE.md)。実装計画: `docs/ARCHITECTURE_CLEANUP.md` Phase C

### H-7: PipelineContext 完全削除

**ブロック要因**: VA pipeline が ctx を直接使用、export_and_viz.py に 71 箇所 ctx 参照。DuckDB §4 完了後が前提。

**進捗** (2026-04-24): Phase 1-4 pure function 化完了。`pipeline_types.py` + 3 phase ファイル変換済み。

- [x] `pipeline_types.py` 作成 (LoadedData, EntityResolutionResult, GraphsResult, CoreScoresResult, SupplementaryMetricsResult, VAScoresResult)
- [x] `data_loading.py` → pure function (→ LoadedData)
- [x] `entity_resolution.py` → pure function (LoadedData → EntityResolutionResult)
- [x] `graph_construction.py` → pure function (EntityResolutionResult → GraphsResult)
- [x] Hamilton adapter 更新: loading.py, resolution.py
- [x] metrics.py Phase 6 単純 8 nodes → typed inputs 変換 (engagement_decay, role_classification, career_analysis, director_circles, versatility_computed, network_density_computed, growth_trends_precomputed, career_tracks_inferred) (2026-04-24)
- [ ] metrics.py centrality_metrics + betweenness_cache ノード分離
- [ ] scoring.py Phase 5 の 8 nodes → typed inputs 変換
- [ ] metrics.py 残り 8 nodes → typed inputs 変換
- [ ] `assembly.py` Hamilton node + `result_assembly.py` / `post_processing.py` 変換
- [ ] VA pipeline を Hamilton module 化 (or ctx を受け取らない形に refactor)
- [ ] `export_and_viz.py` を ExportContext 置き換え + pure function 群に分解
- [ ] `src/pipeline_phases/context.py` を削除

---

## SECTION 6: テストカバレッジ

- [x] Phase 9 `analysis_modules.py` の並列実行テスト (2026-04-24): `tests/unit/test_analysis_modules.py` 16 tests — AnalysisTask・_execute_analysis_task・_run_task_batch スレッド安全性・失敗分離・ANALYSIS_TASKS 不変条件
- [x] fixture を `tests/conftest.py` に集約 (2026-04-24): 9 fixtures をconftest に統一、19 ファイルから 6532 bytes の重複削除（自動スクリプト）
- [x] `tests/unit/` / `tests/integration/` の最低限分離 (2026-04-24): unit→7ファイル (name_utils/models/protocols/episode_parser/parse_role/role_groups/normalize)、integration→6ファイル (integration/pipeline/pipeline_v55_smoke/statistical_invariants/hamilton_phase1_4/hamilton_phase5_8) を git mv で移動

---

## SECTION 7: スクレイパー強化残務

### 7.1 差分更新 — ブロック中 (スキーマ変更待ち)

- [ ] `src_*_anime` テーブルに `fetched_at` / `content_hash` カラム追加
- [ ] upsert 時に hash 比較して変更時のみ update
- [ ] scraper 側に `--since YYYY-MM-DD` mode 実装

### 7.3 anilist_scraper retry refactor (任意)

- [ ] 共通部分を `RetryingHttpClient` に委譲、X-RateLimit-* 専用 callback hook を追加

---

## SECTION 9: アーキテクチャ整理 残務

- [ ] `similarity.py` と `recommendation.py` の機能重複確認 (低優先度)

---

## SECTION 13: 将来タスク

- [ ] `agg_person_career` (L2) / `feat_career_scores` (L3) 分割
- [ ] `agg_person_network` (L2) / `feat_network_scores` (L3) 分割
- [ ] `corrections_*` テーブル: クレジット年補正・ロール正規化の修正差分追跡

---

## 実施順序

```
次 (任意・並行可):
  §1    v56/v57 スキーマ後続タスク
  §6    テスト整理

中期:
  §4    DuckDB 残務 (studio_affiliation 等)
  §7.1  差分更新 (スキーマ変更後)

長期:
  §5 H-7   PipelineContext 完全削除
  §13      feat_* 分離、corrections テーブル
```

---

## 禁止事項 (再提案しない)

- **OpenTelemetry / 分散トレーシング**: 単一プロセス分析に過剰
- **Hydra / Pydantic Settings**: method gate で固定宣言
- **Polars**: DuckDB 移行後は冗長
- **GPU (cuGraph / cuDF)**: Rust 比較データ不在、投資正当化困難

詳細: `~/.claude/projects/-home-user-dev-animetor-eval/memory/feedback_framework_rejections.md`
