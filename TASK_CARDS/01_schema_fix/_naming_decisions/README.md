# _naming_decisions — 新 schema 設計の材料

本ディレクトリは**実行対象ではない**。`00_target_schema.md` で新 schema を設計する時の参考資料。

各ファイルは「旧 schema の命名のどこに問題があるか、どう直すか」を個別論点ごとに記述したもの。新 schema を書き下ろす時はこれらの判断を**全て反映**する。

## 命名決定一覧

| 資料 | 要約 |
|---|---|
| 07_fix_animelist_brand_typo | `SrcAnimelist*` クラス名 → `SrcAnilist*` |
| 08_va_scores_rename | `va_scores` テーブル → `voice_actor_scores` |
| 09_data_sources_rename | `data_sources` テーブル → `source_scrape_status` |
| 10_anime_source_rename | `anime.source` 列 → `anime.original_work_type` |
| 11_model_consolidation | `Anime`/`AnimeAnalysis`/`AnimeDisplay`/`BronzeAnime` → `Anime` 1 つに |
| 12_meta_prefix_split | `meta_*` audience 用と運用監査用を `ops_*` で分離 |
| 13_display_lookup_naming | `DisplayLookup` クラス/テーブル同名衝突を解消 |
| 14_bronze_silver_gold_vocab | ドキュメント語彙: BRONZE/SILVER/GOLD → Source/Canonical/Feature/Aggregation/Report |

## 運用ルール

- 新 schema 設計 (`00_target_schema.md`) を書く時は**全資料を読み反映**
- 旧 schema との移行ロジック (`01_one_shot_copy.md`) を書く時も参照
- 個別資料の "段階的 migration" 指示は**無視してよい**(新方針では一発書き換え)
- 資料内の Stop-if / Rollback / Verification は参考程度(新方針では smoke 2 本で十分)
