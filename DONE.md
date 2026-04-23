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
