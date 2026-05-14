# Task: 国際共同制作の edge 構造 (中韓・東南アジア)

**ID**: `26_industry_structure/02_international_collab`
**Priority**: 🟡
**Estimated changes**: +400 / -0 lines, 3 files
**Requires senior judgment**: yes (entity resolution 国際対応)
**Blocks**: O6 (`15/05_o6_cross_border` の上位拡張)
**Blocked by**: `19_resolved_cluster_fix` (CJK 名寄せ精度)

---

## Goal

JP 国内 staff/studio と CJK・東南アジア staff/studio の協業 edge 構造を時系列で観察。
動画 / 第二原画 / 仕上げ の海外比率変化を構造データから推定。

---

## 経路別価値

| 経路 | 用途 |
|------|------|
| **Business** | スタジオ調達戦略 / 投資家向け supply chain 分析 |
| **Publication** | global production network / cultural industries 国際分業 |
| **政策** | 雇用空洞化 vs 海外連携の trade-off データ |

---

## Hard constraints

- **H3**: entity resolution 不変。CJK 名寄せの誤マッチ警告は強化 (LAN/李豪凌/Haoling 例)
- 「空洞化」「下請け」frame 禁止 → 「海外協業比率」「役職別海外配分」

---

## Method

### Country tag

person.hometown / studio.country を Resolved 層から取得。null は集計から除外し量を明示。

### Metrics

- 役職別 海外比率 by year (動画 / 第二原画 / 仕上げ / 撮影 / etc)
- JP-CN / JP-KR / JP-SE-Asia の協業密度 (edges per anime)
- 海外側 staff の role progression: 動画 → 原画 transition rate
- community detection (Louvain) で国際クラスター検出

### Honest gaps

- credits 漏れバイアス (海外スタジオは ANN/AniList で under-credited)
- 表記ゆれによる過小推定リスク

---

## Files

| File | 内容 |
|------|------|
| `src/analysis/network/international_collab.py` | 集計 + community |
| `scripts/report_generators/reports/structure_international.py` | report |
| `tests/analysis/network/test_international_collab.py` | toy 国別 graph |

---

## Pre-conditions

- [ ] cluster fix Card 19 完了 (CJK 名寄せ信頼性)
- [ ] studio.country 充足率 ≥ 70%
- [ ] hometown 充足率 ≥ 50%

---

## Stop-if

- studio.country null > 50% → 報告見送り、source 拡張先行
