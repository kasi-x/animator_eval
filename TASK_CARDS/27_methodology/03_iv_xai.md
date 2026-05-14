# Task: IV transparent 分解 + 個人向け XAI

**ID**: `27_methodology/03_iv_xai`
**Priority**: 🟡
**Estimated changes**: +400 / -0 lines, 3 files
**Requires senior judgment**: medium
**Blocks**: 個人向け SaaS 候補機能
**Blocked by**: なし

---

## Goal

Integrated Value (IV) を 5 成分 (theta / birank / studio_exp / awcc / patronage) + dormancy に
透明分解し、個人ごとに「あなたの IV 構成、類似コホート分布」を提示できる出力を作る。

---

## 経路別価値

| 経路 | 用途 |
|------|------|
| **Business** | 個人向け SaaS (アニメーターが自分のスコア確認) → 報酬交渉支援 |
| **Publication** | XAI / explainable composite index の応用例 |
| **政策** | 補助金支給時の説明責任 (構成根拠) |

---

## Hard constraints

- **H2**: 「あなたは劣る / 優れている」frame 禁止 → 「コホート X 内で位置 Y」
- IV 構成式と λ 重みは method note で全公開 (透明な遠近法)
- 個人向け表示は同意ベース、第三者が他人の IV を覗く UI は禁止

---

## Method

### Decomposition

```
IV_i = (λ1·theta_i + λ2·birank_i + λ3·studio_exp_i + λ4·awcc_i + λ5·patronage_i) × D_i

成分 contribution:
contrib_k[i] = λ_k · component_k[i] / Σ_j λ_j · component_j[i]
```

### Cohort comparison

debut decade × primary_role group でコホート定義、コホート内 percentile + 成分別分布。

### Output

person_id 受け取り → JSON で:

```json
{
  "iv": 0.62,
  "cohort": "2010s_key_animator",
  "cohort_size": 1245,
  "percentile_in_cohort": 73,
  "components": {
    "theta": {"value": 0.4, "contrib_pct": 38, "cohort_pctl": 65},
    ...
  },
  "dormancy": {"D": 0.85, "last_credit_year": 2024}
}
```

---

## Files

| File | 内容 |
|------|------|
| `src/analysis/scoring/iv_decomposition.py` | 分解 + cohort |
| `src/routers/persons.py` | API endpoint `/persons/{id}/iv` |
| `tests/analysis/scoring/test_iv_decomposition.py` | identity check (Σ contrib = 100%) |

---

## Pre-conditions

- [ ] IV 全成分の安定出力
- [ ] cohort 定義の合意 (decade × role group が現実的か検討)

---

## Stop-if

- IV 成分間の高相関 (r > 0.9) → 加法分解の解釈困難、Shapley 価値ベースに切替検討
