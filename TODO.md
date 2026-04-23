# TODO.md — 未完了作業の一元管理

作成日: 2026-04-22 / 最終更新: 2026-04-24

本書はプロジェクト内のすべての**未完了**項目を一元管理するファイルです。完了済みサマリーは `DONE.md`、設計原則は `CLAUDE.md`。

---

## 優先度マトリクス

| 優先度 | カテゴリ | 内容 |
|--------|---------|------|
| 🟠 Major | レポートシステム残務 | `generate_reports_v2.py` → `generate_reports.py` リネーム、v1 shim 撤去 |
| 🟡 Minor | テストカバレッジ | analysis_modules ユニットテスト、tests/ ディレクトリ整理 |
| 🟡 Minor | スクレイパー強化残務 | 差分更新、retry refactor |
| 🟡 Maintenance | スキーマ後続タスク | v56 多言語・v57 構造的メタデータのフォローアップ |
| 🟡 Maintenance | アーキテクチャ整理 | src/ 平置き解消、analysis/ subpackage 化 |
| 🟢 Future | Hamilton H-7 | PipelineContext 完全削除 (ctx → typed inputs) |
| 🟢 Future | feat_* 層別分離 | L2/L3 分割 |

**完了済み大項目**: DuckDB §4 全フェーズ ✅、Hamilton H-1〜H-6 ✅、レポート統廃合 §8 ✅、アーキテクチャ §9 ✅、ドキュメント §12 ✅

---

## SECTION 1: スキーマ後続タスク

### v56 多言語名対応

- [ ] 既存データ再スクレイプ: `hometown` 取得後に韓国・中国名の `name_ja` 誤入りを修正
- [ ] `nationality` JSON カラムを使ったサンプルクエリを `docs/` に追加
- [ ] ANN / allcinema スクレイパーの `name_ko`/`name_zh` 対応

### v57 構造的メタデータ

- [ ] `anime.country_of_origin` 多数決で `studios.country_of_origin` を埋めるバッチ SQL
- [ ] `title.native` を `country_of_origin` 分岐で `title_zh`/`title_ko` へ格納 (v58 予定)
- [ ] `years_active` 活用: クレジットデータが薄い人物の活動期間推定クエリ

### src/db/ 後続 ✅ 完了

- [x] ✅ `init_db_v2` 抽出: `database_v2.py` → `src/db/schema.py` (1291 行 DDL + helper)
- [x] ✅ `database_v2.py` 廃止: 121 行の薄いラッパーに (使用箇所なし)
- [x] ✅ `generate_dbml.py` 移行: SQLAlchemy 除去、SQL 直接解析 (regex-based)
- [x] ✅ `models_v2.py` 廃止: コード内での参照なし (完全に unused)

---

## SECTION 3: コード一貫性 残務

### 3.1 Scraper 残務 (任意)

- [ ] 他 scraper クエリ・パース関数を `src/scrapers/queries/` / `src/scrapers/parsers/` に分離

### 3.7 JVMG 再スクレイプ

- [ ] 既存 JVMG-source の credits を再スクレイプ or 再マップ (WIKIDATA_ROLE_MAP 修正後)

---

## SECTION 4: DuckDB 後続タスク

§4.1〜§4.5 はすべて完了 (詳細: DONE.md)。残務のみ:

- [ ] `compute_feat_studio_affiliation` の DuckDB 移植 (anime_studios が silver に移ったとき)
- [ ] Entity resolution の書き込み経路を DuckDB に切替
- [ ] Atlas migration を DuckDB 環境で再生成
- [ ] `scripts/` 報告書生成・メンテスクリプトの DuckDB 移行 (低優先度)
- [ ] `CLAUDE.md` testing patterns: monkeypatch を `src.db.init.DEFAULT_DB_PATH` に更新

---

## SECTION 5: Hamilton 残務

H-1〜H-6 はすべて完了 (詳細: DONE.md)。

### H-7: PipelineContext 完全削除

**ブロック要因**: VA pipeline が ctx を直接使用、export_and_viz.py に 71 箇所 ctx 参照。DuckDB §4 完了後が前提。

- [ ] 全 Hamilton node を `ctx: PipelineContext` → 明示的 typed inputs に変換
- [ ] VA pipeline を Hamilton module 化 (or ctx を受け取らない形に refactor)
- [ ] `export_and_viz.py` を ExportSpec registry 経由の pure function 群に分解
- [ ] `src/pipeline_phases/context.py` を削除

---

## SECTION 6: テストカバレッジ

### 6.1 analysis_modules ユニットテスト

- [ ] Phase 9 `analysis_modules.py` の並列実行テスト

### 6.4 テストファイル整理

- [ ] fixture を `tests/conftest.py` + `tests/fixtures/` に集約
- [ ] `tests/unit/` / `tests/integration/` の最低限分離

---

## SECTION 7: スクレイパー強化残務

### 7.1 差分更新 — ブロック中 (スキーマ変更待ち)

- [ ] `src_*_anime` テーブルに `fetched_at` / `content_hash` カラム追加
- [ ] upsert 時に hash 比較して変更時のみ update
- [ ] scraper 側に `--since YYYY-MM-DD` mode 実装

### 7.3 anilist_scraper retry refactor (任意)

- [ ] 共通部分を `RetryingHttpClient` に委譲、X-RateLimit-* 専用 callback hook を追加

---

## SECTION 8: レポートシステム

§8.1〜§8.3 完了 (詳細: DONE.md)。残務:

- [ ] `scripts/generate_reports_v2.py` → `scripts/generate_reports.py` リネーム (v1 shim 撤去後)

---

## SECTION 9: アーキテクチャ整理 残務

- [ ] `similarity.py` と `recommendation.py` の機能重複確認 (低優先度)

---

## SECTION 11: レイアウト・命名整理

### 11.1 `src/` 直下の平置き解消

```
src/db/        ← 完了 (etl.py, scraper.py, init.py)
src/runtime/   ← api/, cli.py, pipeline.py
src/infra/     ← log.py, websocket_manager.py, freshness.py
src/testing/   ← synthetic.py → fixtures.py
```

- [ ] 残存の src/ 平置きファイルを上記レイアウトに移動

### 11.2 `src/analysis/` 69 本平置きの統合

- [ ] `analysis/graph/`, `analysis/career/`, `analysis/entity/` 等に整理 (`__init__.py` で後方互換)

### 11.3 命名ゆらぎの解消

- [ ] `_v2` suffix 廃止: `database_v2` / `models_v2` は破棄 or 正式名昇格
- [ ] `generate_reports_v2.py` → `generate_reports.py`
- [ ] `src/log.py` → `src/infra/logging.py`

---

## SECTION 12: ドキュメント整理

### 12.4 CLAUDE.md ドリフト修正 (随時)

- [ ] Testing patterns を `src.db.init.DEFAULT_DB_PATH` monkeypatch パターンに更新

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
  §8    generate_reports.py リネーム
  §6.4  テスト整理

中期:
  §4    DuckDB 後続 (studio_affiliation 等)
  §7.1  差分更新 (スキーマ変更後)
  §11   レイアウト整理

長期:
  §5 H-7   PipelineContext 完全削除
  §11-13   命名・feat_* 分離
```

---

## 禁止事項 (再提案しない)

- **OpenTelemetry / 分散トレーシング**: 単一プロセス分析に過剰
- **Hydra / Pydantic Settings**: method gate で固定宣言
- **Polars**: DuckDB 移行後は冗長
- **GPU (cuGraph / cuDF)**: Rust 比較データ不在、投資正当化困難

詳細: `~/.claude/projects/-home-user-dev-animetor-eval/memory/feedback_framework_rejections.md`
