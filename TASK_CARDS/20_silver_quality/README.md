# 20 SILVER 品質強化 (Tier 3)

**Objective**: 18 で監査結果取得、19 で計測ロジック修正 + GOLD 再生成済の今、SILVER 内部品質 (重複解消 / display 列充実 / 残 WIP 修正) を進める。

## 前提

- 18/01 audit 結果 (`result/audit/silver_dedup_*.csv`) を活用
- 19/02 で partition-aware coverage 確定済
- 19/01 で gold pipeline 健全性確認済

## カード構成

| ID | 内容 | Priority | 並列? |
|----|------|---------|-------|
| [01_persons_studios_dedup_merge](01_persons_studios_dedup_merge.md) | 18/01 で検出した persons / studios の高信頼度ペアを安全 merge (H3 配慮) | 🟠 | ✅ |
| [02_credits_within_source_dedup](02_credits_within_source_dedup.md) | within-source 重複 668,347 件 (12.6%) を SQL ベースで削除 | 🟠 | ✅ |
| [03_display_columns_extend](03_display_columns_extend.md) | bgm/ANN/MAL の rating / popularity を SILVER に display_* prefix で取込 | 🟡 | ✅ |
| [04_anime_studios_pk_fix](04_anime_studios_pk_fix.md) | 93edd3d で WIP 残存の anime_studios PK 衝突修正 | 🟠 | ✅ |

## 並列衝突回避

- 各 Card は独立した SILVER 表 / loader を扱う
- `src/db/schema.py` 末尾追記 (Card 03 / 04 のみ)
- `src/etl/audit/` の出力は各 Card 別ファイル

## Hard Rule リマインダ

- **H1**: scoring 経路に score / popularity 流入禁止 (display 系列は `display_*` prefix で隔離、本タスク重点)
- **H3**: entity_resolution ロジック不変 (Card 01 は監査結果ベースの**特例 merge** で audit table 記録、ロジック変更なし)
- **H4**: `evidence_source` 維持
- **H5**: 既存 2161+ tests green 維持

## 完了判定

- 各 Card の Verification 全 pass
- 01: persons/studios row count 微減 (重複解消)、`meta_entity_resolution_audit` に記録
- 02: credits within-source dup 668K → 0 確認
- 03: 各 SILVER `display_*_<source>` 列 row > 0
- 04: anime_studios INSERT 成功率 100% (PK 衝突解消)
