# Task: 機会格差の Oaxaca-Blinder 分解 (gender / cohort / 所属)

**ID**: `25_compensation_fairness/04_pay_equity_decomp`
**Priority**: 🟠
**Estimated changes**: +400 / -0 lines, 3 files
**Requires senior judgment**: yes (group 定義 / 解釈)
**Blocks**: Policy brief gender section
**Blocked by**: `25/01_did_studio_transfer` (causal 解釈強化)、`§15` gender 充足

---

## Goal

同等 theta_i / psi_j 条件下で観測される credit 機会の group 差を **Oaxaca-Blinder 分解** で
"endowment 差" (構造的位置の差) と "structural 差" (同位置でも処遇が異なる) に分離。
structural 差 = 機会格差の核 = 政策介入の対象。

---

## 経路別価値

| 経路 | 用途 |
|------|------|
| **Publication** | 労働経済学 (Oaxaca 1973 系譜) ストレート |
| **政策** | 経産省・厚労省・文化庁の gender 報告 / 補助金根拠 |
| **Business** | スタジオ単位の格差ベンチ → ESG 開示 |

---

## Hard constraints

- **H2**: 「能力差」frame 全否定。「endowment = 構造的位置の差」「structural = 同位置の処遇差」と命名
- gender が null の person は分解から除外し、その量を Findings に明示
- group 定義の任意性を openly 開示 (透明な遠近法)

---

## Method

### Spec

```
y = log(credit_count) or log(production_scale_sum)
group A vs B (例: female vs male)

ΔY = (X_A - X_B)·β   +   X_B·(β_A - β_B)
       ↑ endowment       ↑ structural
```

X = theta_i, tenure, role_diversity, primary_studio FE。

### CI

bootstrap 1000 回 (cluster = person)、percentile CI。

### Subgroup

- gender × cohort (debut decade)
- gender × studio tier
- 所属 (大手 / 中堅 / 個人)

---

## Files

| File | 内容 |
|------|------|
| `src/analysis/equity/oaxaca_decomp.py` | 推定 + bootstrap |
| `scripts/report_generators/reports/equity_oaxaca.py` | Policy brief セクション |
| `tests/analysis/equity/test_oaxaca.py` | 既知 toy データで分解一致 |

---

## Pre-conditions

- [ ] gender null 率 < 30% (現状 80.9% → §15 完了必須)
- [ ] AKM theta_i 安定

---

## Stop-if

- subgroup n < 100 → 推定不安定、報告見送り
