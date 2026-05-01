# 21 SILVER 強化 + GOLD 再計算 (Tier 4)

**Objective**: 18-20 で SILVER 整合性確保 + 重複解消済の今、最新 credits (3.2M) で AKM 再計算、追加 dedup、構造的列拡充を進める。

## 前提

- 20 完了 (credits 3.2M、persons 271K、studios 14.7K)
- gold.duckdb pipeline 健全 (19/01)
- silver_dedup audit / completeness / lineage 完備 (18/01-03)

## カード構成

| ID | 内容 | Priority | 並列? |
|----|------|---------|-------|
| [01_akm_refresh](01_akm_refresh.md) | 最新 credits 3.2M で AKM 再計算 → feat_person_scores / scores 反映 | 🟠 | ✅ |
| [02_characters_dedup](02_characters_dedup.md) | 135K characters の cross-source 重複検出 + safe merge | 🟠 | ✅ |
| [03_persons_canonical_name](03_persons_canonical_name.md) | canonical_name 列追加 (NFKC + 旧字体→新字体) | 🟡 | ✅ |
| [04_relations_cluster_id](04_relations_cluster_id.md) | Union-Find SEQUEL/PREQUEL クラスタを SILVER 列化 (15/01 O3 ロジック流用) | 🟡 | ✅ |

## 並列衝突回避

- A: gold.duckdb 排他、silver/anime/persons/studios 触らない
- B: `src/etl/dedup/characters_dedup.py` 新規 + characters 表のみ
- C: `src/db/schema.py` 末尾 `persons` 拡張セクション
- D: `src/db/schema.py` 末尾 `anime_relations` 拡張セクション (C と衝突可能 — マージで吸収)

## Hard Rule

- **H1**: scoring 経路に score / popularity 流入禁止
- **H3**: entity_resolution ロジック不変、検出 + 安全 merge は audit ベース
- **H4**: evidence_source 維持
- **H5**: 既存 2161+ tests green 維持
- **H8**: 行番号信頼禁止
