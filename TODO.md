# TODO.md — 未完了作業の一元管理

作成日: 2026-04-22 / 最終更新: 2026-04-24

本書はプロジェクト内のすべての**未完了**項目を一元管理するファイルです。完了済みは `DONE.md`、設計原則は `CLAUDE.md`。

---

## 優先度マトリクス

| 優先度 | カテゴリ | 内容 |
|--------|---------|------|
| 🟡 Maintenance | スキーマ後続 | v56 既存データ再スクレイプ (name_ja 誤入り修正)、v57 title.native |
| 🟢 Future | データ修正 | WIKIDATA_ROLE_MAP 修正後の JVMG credits 再マップ |
| ✅ Done | DuckDB | Card 05 era_fe / era_deflated_iv / opportunity_residual 実装 — era_fe + era_deflated_iv 完了 (export_and_viz.py)、opportunity_residual も individual_profiles から読み込み済み (2026-04-24) |
| 🟡 Medium | Report methodology | Temporal foresight Section 3.3 holdout validation 実装 (Option A、feat_career_annual / feat_person_scores データ投入後)。Option B (記述的分析rename) 完了 2026-04-24 |
| ✅ Done | Test coverage | scraper E2E: anilist_scraper / seesaawiki_scraper / ann_scraper integration tests (18 cases, 6 each) — 2026-04-24 |
| ✅ Done | Test coverage | llm_pipeline.py (574 lines): 50 unit + integration tests (eb3837a) — 2026-04-24 |
| ✅ Done | Test coverage | seesaawiki parser unit tests: 82 cases (_split_names_paren_aware, _is_company_name, _clean_name, parse_credit_line, _parse_episode_ranges, parse_series_staff, parse_episodes) — 2c77147 — 2026-04-24 |
| ✅ Done | Test coverage | structural_estimation.py (AKM / causal): 19 unit tests (FE recovery, SE scaling, order invariance, DID contract, parallel trends, placebo, edge cases) — 2026-04-24 |

**完了済み大項目** (→ `DONE.md`): anime.score 汚染除去、Phase 1-4 基盤、DuckDB §4 全フェーズ、Hamilton H-1〜H-7 (PipelineContext 完全削除)、レポート統廃合 §8、アーキテクチャ §9/11、ドキュメント §12、テストカバレッジ §6、feat_* 層別分離 §13、scraper queries/parsers 分離 §3、§7.1 差分更新 (hash比較フィルタ + E2E)、§7.3 retry refactor、§9 similarity/recommendation スタブ化

---

## SECTION 1: スキーマ後続タスク

### v56 多言語名対応

- [x] ANN / allcinema スクレイパーの `name_ko`/`name_zh` 対応 (3c45ab6 + bdba63f)
- [x] `backfill_anilist_hometown.py` script + tests 実装 (573fed0, 13 tests pass)
- [x] dry-run 実行確認 (2026-04-24): persons テーブル空 → 対象ゼロ、将来データ入投入時用 script として待機

### v57 構造的メタデータ

- [x] `title.native` を `country_of_origin` 分岐で `titles_alt` JSON へ格納 (4ec1003 実装完了、assign_native_title_fields + parser フィールド追加)

---

## SECTION 3: データ修正 残務 ✅

- [x] WIKIDATA_ROLE_MAP 修正済 (813d684)
- ~~既存 JVMG credits 再マップ~~: **不要** (JVMG データは SILVER 未統合 → 旧マッピング汚染なし)
- [x] **オプション**: JVMG 初回統合試行 → 見送り
      - 理由: Wikidata SPARQL エンドポイント持続的 rate limit (429/504)
      - 再試行: Wikidata API quota 解放時に `jvmg_fetcher` を再実行可能 (scraper_cache は整備済)

---

## SECTION 7: スクレイパー強化残務

### 7.1 差分更新 — Parquet + DuckDB ベース ✅
- [x] `hash_utils.py`, anilist/ann/allcinema/seesaawiki hash 計算
- [x] integrate_duckdb.py REPLACE upsert, anilist `--since YYYY-MM-DD` mode
- [x] hash 比較フィルタリング (UPDATE skip) — ecd6477
- [x] E2E テスト (hash差分検出) — 1a8dfcd

### 7.3 anilist_scraper retry refactor ✅ (3cf8ad1)

### 7.4 ANN scraper 再実行 (NO-OP: 07_json_to_parquet/04)
- [ ] ANN scraper 再実行: `data/ann/anime_checkpoint.json` の `all_ids` を使い、新 bronze_writer 経路で parquet 出力
  - **理由**: 既存 `data/ann/` には checkpoint (`{all_ids, completed_ids}` dict) のみで、実データ JSON がない
  - **実施方法**: 既存 HTTP skip は effective、未完了 ID のみ fetch (差分更新)
  - **期限**: スクレイプ安定化後 (allcinema 統合完了時点で優先度上昇)

---

## SECTION 9: アーキテクチャ整理 ✅

similarity.py / recommendation.py はスタブ化済 (2行)、重複整理完了。

---

## SECTION 13: bangumi.tv BRONZE 統合 (Card 08)

TASK_CARDS/08_bangumi_scraper/ 参照。方針: 公式 `bangumi/Archive` dump (週次 jsonlines) を一次ソース、日次差分のみ `/v0` API。

- 13.1 Card 01: Archive dump DL + 展開 (`src/scrapers/bangumi_dump.py`, `scripts/fetch_bangumi_dump.py`) ✅ 実装完了
- 13.2 Card 02: subject.jsonlines → `src_bangumi_subjects` parquet (type=2 anime filter)
- 13.3 Card 03: subject × persons/characters/person-characters 関係 → 3 parquet (position code は raw 保存)
- 13.4 Card 04: person.jsonlines → `src_bangumi_persons` (relation 参照 id のみ filter)
- 13.5 Card 05: character.jsonlines → `src_bangumi_characters` (最後回し、優先度🟢)
- 13.6 Card 06: 日次差分 API cron (`src/scrapers/bangumi_scraper.py`, 1req/s 厳守、dump 運用安定後)

role label (中文「导演」等) の正規化 mapping は SILVER 移行タスクで別起票。

---

## 実施順序

```
短期:
  §1    v56 既存データ再スクレイプ (backfill_anilist_hometown.py)

中期:
  §3    JVMG credits 再マップ (WIKIDATA_ROLE_MAP 確定後)

長期:
  §13   bangumi.tv BRONZE 統合 (Card 08、dump 方式)
  §1 v57 title.native → titles_alt (v58 実施時)
```

---

## 禁止事項 (再提案しない)

- **OpenTelemetry / 分散トレーシング**: 単一プロセス分析に過剰
- **Hydra / Pydantic Settings**: method gate で固定宣言
- **Polars**: DuckDB 移行後は冗長
- **GPU (cuGraph / cuDF)**: Rust 比較データ不在、投資正当化困難

詳細: `~/.claude/projects/-home-user-dev-animetor-eval/memory/feedback_framework_rejections.md`
