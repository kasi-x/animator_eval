# 26_industry_structure — 業界構造の観察

**目的**: Resolved 層完成と新ソース統合を活用し、anime industry の構造変化を可視化。
publication / business / 政策の 3 経路すべてに供給可能な「構造観察」レポート群。

## カード一覧

| ID | 内容 | Priority | 経路 |
|----|------|---------|------|
| `01_committee_influence` | 制作委員会 influence centrality (bipartite) | 🟡 | publication / 政策 |
| `02_international_collab` | 中韓・東南アジアスタジオとの edge 構造 | 🟡 | business / publication |
| `03_studio_pipeline_strength` | スタジオ別 若手育成パイプライン強度 (bus factor) | 🟠 | business (HR / 投資家) |

## 共通

- データ可用性: Resolved 層 + 制作委員会データ (新規 source 候補) + AniList relations
- すべて temporal: 5 年 / 10 年窓を併記、structural break 検出
