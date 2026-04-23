# TODO.md — 未完了作業の一元管理

作成日: 2026-04-22 / 最終更新: 2026-04-24

本書はプロジェクト内のすべての**未完了**項目を一元管理するファイルです。完了済みは `DONE.md`、設計原則は `CLAUDE.md`。

---

## 優先度マトリクス

| 優先度 | カテゴリ | 内容 |
|--------|---------|------|
| 🟡 Maintenance | スキーマ後続 | v56 多言語・v57 構造的メタデータのフォローアップ |
| 🟡 Minor | スクレイパー強化 | §7.1 差分更新の残務 (hash 計算 / フィルタ / E2E)、retry refactor |
| 🟢 Future | データ修正 | WIKIDATA_ROLE_MAP 修正後の JVMG credits 再マップ |
| 🟢 Future | アーキテクチャ | similarity.py / recommendation.py 重複確認 |

**完了済み大項目** (→ `DONE.md`): anime.score 汚染除去、Phase 1-4 基盤、DuckDB §4 全フェーズ、Hamilton H-1〜H-7 (PipelineContext 完全削除)、レポート統廃合 §8、アーキテクチャ §9/11、ドキュメント §12、テストカバレッジ §6、feat_* 層別分離 §13、scraper queries/parsers 分離 §3

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

## SECTION 7: スクレイパー強化残務

### 7.1 差分更新 — Parquet + DuckDB ベース (進行中)

✅ **実装完了 (2026-04-24)**:
- [x] `hash_utils.py` 作成
- [x] anilist_scraper fetched_at/content_hash 追加
- [x] ann / allcinema / seesaawiki scraper に hash 計算追加
- [x] integrate_duckdb.py REPLACE upsert
- [x] anilist `--since YYYY-MM-DD` mode

**残務 (パフォーマンス tuning)**:
- [ ] hash 比較フィルタリング (WHERE content_hash != ?) で UPDATE skip
- [ ] E2E テスト (--since mode で真の差分検出確認)

### 7.3 anilist_scraper retry refactor (完了 2026-04-24)

- [x] 共通部分を `RetryingHttpClient` に委譲、X-RateLimit-* 専用 callback hook を追加

---

## SECTION 9: アーキテクチャ整理 残務

- [ ] `similarity.py` と `recommendation.py` の機能重複確認 (低優先度)

---

## 実施順序

```
短期 (独立・並行可):
  §1    v56/v57 スキーマ後続タスク
  §7.1  差分更新の hash / フィルタ / E2E

中期:
  §3    JVMG credits 再マップ (スキーマ確定後)
  §7.3  retry refactor

長期:
  §9    similarity / recommendation 重複整理
```

---

## 禁止事項 (再提案しない)

- **OpenTelemetry / 分散トレーシング**: 単一プロセス分析に過剰
- **Hydra / Pydantic Settings**: method gate で固定宣言
- **Polars**: DuckDB 移行後は冗長
- **GPU (cuGraph / cuDF)**: Rust 比較データ不在、投資正当化困難

詳細: `~/.claude/projects/-home-user-dev-animetor-eval/memory/feedback_framework_rejections.md`
