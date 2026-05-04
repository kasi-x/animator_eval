# Task: 複数 source の anime/persons/credits orphan 一括修正

**ID**: `14_silver_extend/12_orphan_fix_batch`
**Priority**: 🔴 (AKM 結果に直結する FK 不整合)

## 問題

各 source の conformed 統合に多数の orphan / 抜け:

| source | anime | persons | credits | 問題 |
|----|---:|---:|---:|----|
| mal | 14,739 | **0** | 444,384 | persons 抜け |
| keyframe | **0** | **0** | 429,889 | anime + persons 全抜け、credits orphan |
| seesaawiki | 8,778 | 137,014 | **0** | credits 抜け |
| sakuga_atwiki | **0** | 130 | 6,267 | anime 抜け、credits orphan |
| bangumi | 3,715 | 21,125 | 232,262 | anime 少なすぎ要確認 |

## ゴール

各 conformed_loaders を確認 + 抜けてる INSERT 文を復元:

### 14/12-A: mal persons
- Card 14/08 で 9 table 統合だったが persons が漏れた
- BRONZE `result/bronze/source=mal/table=persons/` から `mal:p<id>` で INSERT

### 14/12-B: keyframe anime + persons
- Card 14/06 で実装済のはずだが現状 0
- `kf:a<id>` / `kf:p<id>` で BRONZE から INSERT
- credits 429K の orphan を解消

### 14/12-C: seesaawiki credits
- Card 14/04 で credits 統合済のはずだが現状 0
- `seesaa:` prefix で credits INSERT 復元
- evidence_source='seesaawiki'

### 14/12-D: sakuga_atwiki anime
- Card 14/07 は title resolution のみだった
- anime も `sakuga:a<id>` で取込

### 14/12-E: bangumi anime 拡大調査
- 現 3,715 件のみ = 一部 type filter で絞り過ぎ可能性
- BRONZE `subjects` で type=2 (anime) 全件確認、不足分 INSERT

## 範囲

- 修正: `src/etl/conformed_loaders/{mal,keyframe,seesaawiki,sakuga_atwiki,bangumi}.py`
- 修正: tests
- ETL 再実行 or 個別 build

## 完了条件

- 全 source で anime / persons / credits 0 が解消 (適切な数値に)
- orphan credits 解消 (FK invariant: credits.anime_id ∈ anime.id 95%+ hit)
- AKM connected_set 拡大期待 (現 50K → 大幅増)
- commit + bundle
