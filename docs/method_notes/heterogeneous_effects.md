# Method Note — Heterogeneous Treatment Effect (HTE)

## 目的

既存 DiD (`did_studio_transfer`) は ATE (Average Treatment Effect) のみ報告。
本モジュールは「treatment 効果が誰に対して大きいか / 小さいか」を分解:

1. **Interaction-term DiD**: treatment × subgroup の交互作用係数で
   per-subgroup CATE を回帰式 1 本で同時推定 (parsimonious、CI 容易)。
2. **T-learner causal forest** (Künzel 2019): 個体レベル CATE を Random Forest で
   non-parametric 推定。subgroup definition を先験的に持たない探索的分析。

## Spec — interaction-term

```
y = α + β·treated + Σ_s γ_s·subgroup_s + Σ_s δ_s·(treated × subgroup_s) + ε
```

base subgroup omit:
- CATE_base = β
- CATE_s = β + δ_s

SE: HC0 (heteroskedasticity-consistent), CI = ±1.96 × SE_combined。

## Spec — T-learner

```
μ_1(X) := RF.fit(X_treated, y_treated)
μ_0(X) := RF.fit(X_control, y_control)
CATE_i := μ_1(X_i) - μ_0(X_i)
```

T-learner は最も単純な metalearner。S-learner / X-learner / DR-learner も可。

## Homogeneity F-test

```
H0: δ_2 = δ_3 = ... = δ_K = 0  (CATE が全 subgroup で同じ)
F = ((SSR_restricted - SSR_full) / q) / (SSR_full / (n - p))
```

`scipy.stats.f` で p-value 算出。p < 0.05 → CATE が subgroup 間で有意に異なる。

## H1/H2 制約

- H1: 結果変数は theta_i / opportunity_residual / log_credit のみ。anime.score NG。
- H2: 「treatment 効果が大きい層」表現可、「成長余地が大きい層」frame NG。

## Caveats

- DiD 仮定 (parallel trends) は HTE でも引き継ぐ。subgroup × time の交差で
  parallel trends を別途検定推奨。
- T-learner: treated と control の確率密度が乖離する subgroup では推定精度低下。
  IPW (inverse propensity weighting) で前処理推奨 (将来拡張)。
- subgroup n < 30 では interaction CI が広く解釈困難。報告に明示。

## 代替 metalearner (拡張候補)

- **X-learner**: small-sample で T-learner より頑健。
- **DR-learner**: doubly robust、propensity score も必要。
- **Causal Forest** (econml): split rule が CATE heterogeneity に最適化。

本実装は **T-learner Tier 1**。

## 関連

- `src/analysis/causal/heterogeneous_effects.py` (15 tests pass)
- `src/analysis/causal/did_studio_transfer.py` (ATE base)
- Künzel et al. (2019) "Metalearners for estimating HTE using ML"
- Athey & Imbens (2016) "Recursive partitioning for HTE"
