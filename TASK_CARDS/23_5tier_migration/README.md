# 23 5層 architecture 移行

**Objective**: `docs/ARCHITECTURE_5_TIER_PROPOSAL.md` Phase 1-5 の実装。

## 前提

- 設計確定 (a7c722a): Raw → Source → Conformed → Resolved → Mart
- 物理 file: `result/animetor.duckdb` に conformed + mart 同居 (schema 分離)、Resolved だけ別 (`result/resolved.duckdb`)
- 旧 silver.duckdb / gold.duckdb 削除

## カード構成

| ID | 内容 | Phase |
|----|------|-------|
| [01_phase1_conformed_mart_unify](01_phase1_conformed_mart_unify.md) | silver_loaders → conformed_loaders rename + animetor.duckdb 統合 file 移行 | 1 |

(Phase 2-5 は Phase 1 完了後に起票)
