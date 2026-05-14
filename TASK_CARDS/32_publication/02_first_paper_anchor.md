# Task: 最初の論文の anchor 選定

**ID**: `32_publication/02_first_paper_anchor`
**Priority**: 🟠
**Estimated changes**: doc-only
**Requires senior judgment**: yes
**Blocks**: 最初の publication
**Blocked by**: `32_publication/00_paper_strategy`

---

## Goal

publication 第 1 弾を「どのカードの分析を anchor にするか」確定。
キャリアデザイン学会 (2026-09) は別 (1,600 字、進行中)。
ここで決めるのは **その後の RIETI DP / 査読誌 第 1 弾**。

---

## 候補

| anchor card | 内容 | 経済 venue 適合 | 情報 venue 適合 | 着手難度 |
|-------------|------|:---:|:---:|---------|
| `25/01_did_studio_transfer` | DiD スタジオ移籍 → theta 変化 | ◎ Labour Economics | △ | 高 (parallel trends 検定 / treatment 定義) |
| `25/02_opportunity_residual_null` | OLS 残差 + null model | ◎ J. Cultural Economics | ○ | 中 |
| `25/04_pay_equity_decomp` | Oaxaca-Blinder gender 分解 | ◎ Labour Economics | △ | 中 (gender 充足必要) |
| `26/01_committee_influence` | 制作委員会 bipartite | ○ Cultural Econ | ◎ NetSci | 高 (新規 source 統合必要) |
| `27/01_missingness_disclosure` | source × role × year coverage | ○ | ○ | 低 (基盤、論文化弱い) |
| `27/02_career_trajectory_typology` | sequence cluster | ○ | ◎ NetSci / Sequence Analysis 系 | 中 |

---

## 推奨

### 経済主軸 (Labour Economics 系) → `25/01` または `25/04`

- `25/01 DiD スタジオ移籍`: causal evidence 強い、政策提言と直結。ただし parallel trends 検定が reviewer に厳しく見られる
- `25/04 Oaxaca gender`: gender 機会格差 = labor-first と完全整合、政策インパクト大、ただし gender 充足 (現 19%) が前提

→ **gender 充足が短期で進む見込みなら `25/04`**、進まないなら `25/01` 先行。

### 情報副軸 (NetSci / Sequence Analysis) → `27/02`

- sequence cluster は NetSci / Sequence Analysis 系で workshop 採択早い
- 経済論文と data 共通、別 cut で論文化可能

---

## Steps

### Step 1: gender 充足進捗確認 (`§15` 参照)

```bash
# 現状: persons.gender null 80.9% (2026-05-05)
# MAL Card 05 + AniList orphan backfill 後に再評価
```

### Step 2: anchor 確定 → `docs/papers/<anchor>/` 作成

論文 draft 開始: outline → method → result → discussion 順。

### Step 3: STANCE.md §5.2 paper_strategy に anchor を反映

---

## Pre-conditions

- [ ] paper_strategy 確定 (`32/00`)
- [ ] gender 充足進捗確認 (`§15` 参照)

---

## Stop-if

- 全候補で reviewer 致命的問題 → preprint 経路 (SSRN / arXiv) のみで先行
