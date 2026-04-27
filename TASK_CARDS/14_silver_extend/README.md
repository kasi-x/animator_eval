# 14 SILVER 統合拡張 (BRONZE → SILVER 多属性回収)

**Objective**: BRONZE parquet (9 source / 80 table) のうち、SILVER 未統合な属性を漏れなく SILVER duckdb に取り込む。

**前提** (ユーザ確定 2026-04-27):
1. **scrape は手動**: 本カード群は **BRONZE → SILVER 統合のみ**。scraper / parser は触らない。
2. **多属性最大化**: BRONZE の各列をできる限り SILVER 側に保持。display 系列は `display_*` prefix で scoring 経路から隔離 (H1)。
3. **並列実装可**: Card 01-08 は互いに独立、Sonnet 並列実行可。

## 並列衝突回避ルール

| 競合点 | 対処 |
|--------|------|
| `src/etl/integrate_duckdb.py` 編集 | **触らない**。各 Card は新規ファイル `src/etl/silver_loaders/<source>.py` を作成 |
| `src/db/schema.py` DDL 追加 | 各 Card は **末尾に source 別セクション** (`-- ===== <source> extension =====` コメント) を追加。中央部編集禁止 |
| Loader 単体テスト | 各 Card 専用 test ファイル `tests/test_etl/test_silver_<source>.py` |
| `integrate()` への dispatcher 統合 | **本カード群では行わない**。完了後にユーザが個別 PR で統合 |

## カード構成

| ID | source | 内容 | 並列? | Priority |
|----|--------|------|-------|---------|
| [01_anilist_extend](01_anilist_extend.md) | anilist | characters / CVA loader、anime 拡張列 | ✅ | 🟠 |
| [02_madb_silver](02_madb_silver.md) | mediaarts | 6 新表 (broadcasters / broadcast_schedule / production_committee / production_companies / video_releases / original_work_links) | ✅ | 🟠 |
| [03_ann_extend](03_ann_extend.md) | ann | anime 拡張列、cast → CVA、company / episodes / releases / news / related → SILVER 新表 | ✅ | 🟠 |
| [04_seesaawiki_extend](04_seesaawiki_extend.md) | seesaawiki | studios / anime_studios 統合、theme_songs / episode_titles / gross_studios / production_committee / original_work_info / persons | ✅ | 🟠 |
| [05_bangumi_silver](05_bangumi_silver.md) | bangumi | subjects / persons / characters / person_characters / subject_persons / subject_characters | ✅ | 🟠 |
| [06_keyframe_extend](06_keyframe_extend.md) | keyframe | person_studios / person_jobs / studios_master / settings_categories / preview | ✅ | 🟡 |
| [07_sakuga_atwiki_resolution](07_sakuga_atwiki_resolution.md) | sakuga_atwiki | work_title → silver anime_id title-matching ETL | ✅ | 🟡 |
| [08_mal_silver](08_mal_silver.md) | mal | 28 BRONZE → SILVER (anime / persons / credits / VA / characters / genres / themes / relations / 他) | ✅ | 🟡 |

## 実施手順

```
並列起動:
  Card 01-08 を並列で Sonnet に投入。各カードは独立。

完了後 (本カード群外):
  - 各 loader を integrate_duckdb.py から呼ぶ dispatcher 統合
  - End-to-end: pixi run python -m src.etl.integrate_duckdb で SILVER 構築確認
```

## Hard Rule リマインダ (全カード共通)

- **H1**: `score` / `popularity` / `favourites` / `mean_score` / `popularity_rank` を SILVER の scoring 関連列に入れない。display 系は `display_*` prefix で隔離
- **H3**: entity_resolution ロジック不変
- **H4**: `credits.evidence_source` 維持、新 source は `evidence_source = '<source>'` で挿入
- **H5**: 既存テスト破壊禁止
- **H8**: 行番号信頼禁止、シンボル grep して編集

## 完了判定

- 各 Card の Verification 全 pass
- 新 SILVER テーブル群 row count > 0
- `pixi run lint` clean
- 既存 2161+ tests green

## 関連

- `TODO.md §10.2 / §12.1`: 旧記述。本カード群完了時に「→ TASK_CARDS/14」へ書き換え
- `src/etl/integrate_duckdb.py`: 既存 SILVER (anime / persons / credits / studios / anime_studios) loader、本カード群は **触らない**
