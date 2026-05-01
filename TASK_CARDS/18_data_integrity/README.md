# 18 データ整備系 (silver 後処理)

**Objective**: SILVER 28 表が一通り構築済みの今、品質改善・整合性監査・欠損補填・lineage 充実を行う。

## 前提

- silver_loaders 8 source 完了済 (`integrate_duckdb.py` で dispatcher 統合済)
- SILVER 28 表 row 充実 (credits 530 万、anime 56 万、persons 27 万)
- `sakuga_work_title_resolution = 0` のみ要調査

## カード構成

| ID | 内容 | Priority | 並列? |
|----|------|---------|-------|
| [01_silver_dedup_audit](01_silver_dedup_audit.md) | credits / persons / anime / studios の cross-source 重複検出 + audit レポート | 🟠 | ✅ |
| [02_silver_completeness_check](02_silver_completeness_check.md) | BRONZE → SILVER 取込率計測ツール | 🟠 | ✅ |
| [03_meta_lineage_extend](03_meta_lineage_extend.md) | 28 SILVER 表の出処トレース集約 (post-hoc) | 🟡 | ✅ |
| [04_sakuga_resolution_zero_fix](04_sakuga_resolution_zero_fix.md) | `sakuga_work_title_resolution=0` の原因究明 + 修正 | 🟠 | ✅ |
| [05_anilist_extras_silver](05_anilist_extras_silver.md) | TODO §12.1 残務 (external_links_json / airing_schedule_json / trailer_url) SILVER 取込 | 🟠 | ✅ |

## 並列衝突回避

- 各 Card は新規ファイル (`src/etl/audit/<name>.py` or 既存 loader 拡張) 中心
- 出力は `result/audit/<report>.{md,csv}`
- schema.py 末尾追記 (Card 05 のみ、他は触らない)
- `meta_lineage` 表は post-hoc 集約 (各 loader 触らない、Card 03 専属)

## Hard Rule リマインダ

- **H1**: scoring 経路に score / popularity 流入禁止 (今までと同じ)
- **H3**: entity_resolution ロジック不変 (Card 01 audit は **検出のみ**、解像度ロジック変更しない)
- **H4**: `evidence_source` 維持
- **H5**: 既存 2161+ tests green 維持

## 完了判定

- 各 Card の Verification 全 pass
- audit レポート (`result/audit/`) が生成される
- silver dedup / completeness の数値が docs に記録される

## 関連

- `TODO.md §12.1`: AniList 拡張列 SILVER (Card 05 で対応)
- `TODO.md §15`: gender enrichment (本カード群外、scrape 系)
- `TASK_CARDS/14_silver_extend/`: 前段 (silver_loaders 構築)
