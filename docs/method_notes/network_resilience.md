# Method Note — Network Resilience

## 目的

collaboration graph (person × co-credit) における **構造的脆弱性** を測定する。
hub / bridge の順次除去 simulation で global metric (LCC / pair_connectivity /
mean_eigenvector_authority) の劣化速度を観察。

「中堅 N 人が一斉離職」「複数 studio が同期的に縮小」等の counterfactual を
politically neutral な形で可視化する政策的観察ツール。

## Specification

### Removal strategy

| strategy | 順序 |
|----------|------|
| random | 無作為 (基準線) |
| degree | degree centrality 降順 (古典的 attack シナリオ) |
| bridge | bridge_score 降順 (`knowledge_spanners` 由来) |

各 strategy で順次 node 除去 → step ごとに metric snapshot。

### Metric

| metric | 説明 | 範囲 |
|--------|------|------|
| LCC | 最大連結成分 node 数 | 0 - N |
| pair_connectivity | Σ |C|×(|C|-1)/2 over components | 0 - N×(N-1)/2 |
| mean_authority | 残存 graph の eigenvector centrality 平均 | 0 - 1 |

baseline (step=0) で正規化した ratio curve を主視覚化。

### AUC summary

Removal curve の trapezoidal AUC (0 - 1) を robustness index として算出:
- 高 AUC = node 除去耐性が高い (robust)
- 低 AUC = 少数除去で大幅劣化 (fragile)

### Strategy comparator

```
fragility_ratio = 1 - degree_auc / random_auc
```

| range | interpretation |
|-------|---------------|
| < 0.1 | robust (random と targeted がほぼ同等) |
| 0.1 - 0.3 | moderate fragility |
| > 0.3 | high fragility (hub 集中型構造) |

## H1/H2 制約

- H1: anime.score 不参入 (構造的指標のみ)
- H2: 「能力」「リーダー的存在」frame NG → "structural position" のみ
- critical persons は単独除去で global drop が大きい person。**個人評価ではない**。

## Caveats

- collaboration graph 構築時点で entity resolution の信頼性に依存。
  35/01 nationality / 19/01 cluster_fix 完了後の Resolved 層を入力とすること。
- per-anime cap (default 80 persons) で長期 series の O(n²) 爆発を回避。
  cap を超える anime は co-credit edge から除外。報告書に明記。
- bridge_score 属性は node attribute としてあらかじめ付与。
  外部で `detect_bridges` を走らせて attribute set してから resilience 呼出。

## 関連

- `src/analysis/network/resilience.py` (実装、32 tests pass)
- `src/analysis/network/bridges.py` (bridge_score 算出元)
- `scripts/report_generators/reports/network_resilience.py` (v2 report)
- Policy brief 構造的脆弱性 section
