# Task: 制作委員会 influence centrality (bipartite network)

**ID**: `26_industry_structure/01_committee_influence`
**Priority**: 🟡
**Estimated changes**: +500 / -0 lines, 5 files
**Requires senior judgment**: yes (committee data source / null model)
**Blocks**: Policy brief 「権力集中」section
**Blocked by**: 制作委員会 source 統合 (新規、現状未取得)

---

## Goal

制作委員会 (production committee) と studio / staff の bipartite network を構築し、
配信時代 (Netflix 2017+) 前後で committee の influence centrality がどう変化したか観察。

---

## 経路別価値

| 経路 | 用途 |
|------|------|
| **Publication** | メディア産業・組織研究 (e.g. Caves "Creative Industries") 系譜 |
| **政策** | 「権力集中」「資金流動」の構造証拠 → コンテンツ産業政策 |
| **Business** | 投資家向け: 委員会主導 vs 単独出資の構造比較 |

---

## Hard constraints

- **H1**: anime.score 不可。weight = 制作話数 / production_scale のみ
- 「支配」「独占」frame 禁止 → 「集中度 (HHI)」「中心性」と表記

---

## Method

### Bipartite

```
Nodes: committee[c], studio[s], staff[i]
Edges:
  committee[c] -- studio[s] : co-produced anime count, weighted by episodes
  studio[s] -- staff[i]     : credit count, weighted by role_weight × episodes
```

### Centrality

- committee の eigenvector centrality on full graph
- HHI (Herfindahl) by year: top 10 committees の market share
- structural break detection (PELT) on yearly HHI

### Comparison: pre/post Netflix wave

2017 を境界に DiD-like 比較 (但し causal 主張は弱める、observational)。

---

## Files

| File | 内容 |
|------|------|
| `src/db/schema.py` | committee table 追加 |
| `src/etl/committee.py` | source → Resolved layer ETL |
| `src/analysis/network/committee_centrality.py` | bipartite centrality |
| `scripts/report_generators/reports/structure_committee.py` | report |
| `tests/analysis/network/test_committee_centrality.py` | toy bipartite で検証 |

---

## Pre-conditions

- [ ] 制作委員会データ source 確定 (候補: ANN producers / Wikidata / allcinema 製作)
- [ ] Resolved 層の anime / studio 安定

---

## Stop-if

- committee data 取得不能 → 「producer credits」に縮退、scope 縮小
