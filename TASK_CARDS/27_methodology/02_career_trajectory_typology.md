# Task: キャリア軌跡 typology (Markov / sequence cluster)

**ID**: `27_methodology/02_career_trajectory_typology`
**Priority**: 🟡
**Estimated changes**: +500 / -0 lines, 4 files
**Requires senior judgment**: yes (cluster 数決定 / 解釈)
**Blocks**: 個人向けキャリア説明 / publication
**Blocked by**: なし

---

## Goal

個人の役職遷移 sequence (動画 → 第二原画 → 原画 → 作画監督 → ...) を sequence analysis で
クラスタリングし、典型キャリア軌跡 (typology) を抽出。

---

## 経路別価値

| 経路 | 用途 |
|------|------|
| **Publication** | sequence analysis (Abbott 系譜) のキャリア研究応用 |
| **Business** | 個人向け「あなたの軌跡はクラスター X、類似軌跡 N 名」説明 |
| **政策** | 「直線的キャリア」が消失している実態の構造把握 |

---

## Hard constraints

- **H2**: 「成功軌跡」「失敗軌跡」frame 禁止 → 「軌跡 A」「軌跡 B」とラベルのみ
- cluster 解釈は別 section、Findings は cluster 形状の記述のみ

---

## Method

### Sequence

person ごとに年次 primary_role の sequence (例: `[動画, 動画, 第二原画, 原画, 原画, 作画監督]`)。

### Distance

Optimal Matching (OM)、substitution cost = role_groups 距離。
TraMineR (R) 相当の Python 実装は `seqlearn` or 自前。

### Clustering

Ward 法 hierarchical → silhouette / gap statistic で k 決定 (3-7 想定)。

### Output

- 各 cluster の typical sequence + 周辺特徴 (gender / cohort / studio tier 分布)
- transition matrix (Markov) を別途出力、cluster 内の遷移確率比較

---

## Files

| File | 内容 |
|------|------|
| `src/analysis/career/trajectory_typology.py` | sequence build + cluster |
| `scripts/report_generators/reports/career_typology.py` | report |
| `tests/analysis/career/test_trajectory_typology.py` | toy sequence |
| `docs/method_notes/sequence_analysis.md` | OM 仮定・代替手法 |

---

## Pre-conditions

- [ ] credits panel 過去 10 年カバー
- [ ] role_groups single source

---

## Stop-if

- silhouette < 0.2 で全 k → cluster 構造希薄、典型化見送り
