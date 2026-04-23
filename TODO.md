# TODO.md — 未完了作業の一元管理

作成日: 2026-04-22 / 最終更新: 2026-04-24

本書はプロジェクト内のすべての**未完了**項目を一元管理するファイルです。完了済みは `DONE.md`、設計原則は `CLAUDE.md`。

---

## 優先度マトリクス

| 優先度 | カテゴリ | 内容 |
|--------|---------|------|
| 🟡 Maintenance | スキーマ後続 | v56 既存データ再スクレイプ (name_ja 誤入り修正)、v57 title.native |
| 🟢 Future | データ修正 | WIKIDATA_ROLE_MAP 修正後の JVMG credits 再マップ |

**完了済み大項目** (→ `DONE.md`): anime.score 汚染除去、Phase 1-4 基盤、DuckDB §4 全フェーズ、Hamilton H-1〜H-7 (PipelineContext 完全削除)、レポート統廃合 §8、アーキテクチャ §9/11、ドキュメント §12、テストカバレッジ §6、feat_* 層別分離 §13、scraper queries/parsers 分離 §3、§7.1 差分更新 (hash比較フィルタ + E2E)、§7.3 retry refactor、§9 similarity/recommendation スタブ化

---

## SECTION 1: スキーマ後続タスク

### v56 多言語名対応

- [ ] 既存データ再スクレイプ: `hometown` 取得後に韓国・中国名の `name_ja` 誤入りを修正
- [ ] ANN / allcinema スクレイパーの `name_ko`/`name_zh` 対応 (+ タイ/ベトナム等は `names_alt` へ)

### v57 構造的メタデータ

- [ ] `title.native` を `country_of_origin` 分岐で `titles_alt` JSON へ格納 (v58 予定、names_alt 同パターン)

---

## SECTION 3: データ修正 残務

- [ ] 既存 JVMG-source の credits を再スクレイプ or 再マップ (WIKIDATA_ROLE_MAP 修正後)

---

## SECTION 7: スクレイパー強化残務 (すべて完了 2026-04-24)

### 7.1 差分更新 — Parquet + DuckDB ベース ✅
- [x] `hash_utils.py`, anilist/ann/allcinema/seesaawiki hash 計算
- [x] integrate_duckdb.py REPLACE upsert, anilist `--since YYYY-MM-DD` mode
- [x] hash 比較フィルタリング (UPDATE skip) — ecd6477
- [x] E2E テスト (hash差分検出) — 1a8dfcd

### 7.3 anilist_scraper retry refactor ✅ (3cf8ad1)

---

## SECTION 9: アーキテクチャ整理 ✅

similarity.py / recommendation.py はスタブ化済 (2行)、重複整理完了。

---

## 実施順序

```
短期:
  §1    v56 既存データ再スクレイプ (backfill_anilist_hometown.py)

中期:
  §3    JVMG credits 再マップ (WIKIDATA_ROLE_MAP 確定後)

長期:
  §1 v57 title.native → titles_alt (v58 実施時)
```

---

## 禁止事項 (再提案しない)

- **OpenTelemetry / 分散トレーシング**: 単一プロセス分析に過剰
- **Hydra / Pydantic Settings**: method gate で固定宣言
- **Polars**: DuckDB 移行後は冗長
- **GPU (cuGraph / cuDF)**: Rust 比較データ不在、投資正当化困難

詳細: `~/.claude/projects/-home-user-dev-animetor-eval/memory/feedback_framework_rejections.md`
