# 19 SILVER 後処理 (Tier 2)

**Objective**: TASK_CARDS/18 で SILVER 整合性監査 + 重複検出を完了した後の、実体修正と GOLD 再生成。

## 前提

- 18 完了 (5/5 全 main 統合済)
- `silver_dedup_*` audit 結果 (`result/audit/`) を活用
- `silver_completeness.md` で LOW coverage 13 表特定済

## カード構成

| ID | 内容 | Priority | 並列? |
|----|------|---------|-------|
| [01_silver_to_gold_refresh](01_silver_to_gold_refresh.md) | gold.duckdb 全再生成 (feat_career_annual / feat_person_scores / scores) | 🟠 | ✅ |
| [02_low_coverage_fix](02_low_coverage_fix.md) | 18/02 で発見した LOW coverage 13 表の調査+修正 | 🟠 | ✅ |
| [03_relations_recommendations_extend](03_relations_recommendations_extend.md) | anime_relations / anime_recommendations の cross-source 拡充 (anilist + mal + bgm 統合) | 🟡 | ✅ |

## 並列衝突回避

- 各 Card は新規ファイルまたは独立した既存 loader 1 つを修正
- gold.duckdb 触るのは 01 のみ (排他)
- 02 / 03 は silver_loaders 修正だが互いに別 source

## Hard Rule リマインダ

- **H1**: scoring 経路に score / popularity 流入禁止 (gold 再生成も同じ)
- **H4**: `evidence_source` 維持
- **H5**: 既存テスト破壊禁止
- **H8**: 行番号信頼禁止

## 完了判定

- 各 Card の Verification 全 pass
- 01: `feat_*` テーブル row 充実、scores 表更新
- 02: LOW (<50%) → PARTIAL (≥50%) に改善
- 03: anime_relations / anime_recommendations row 拡大
