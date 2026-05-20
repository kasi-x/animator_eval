# Method Note — Statistical Power Analysis Dashboard

## 目的

各 analysis / report の **検出可能 effect size** を一覧化。「データはあるが
power 不足」を openly に開示する。method gate 強化の柱:

- CI 計算 (横断必須)
- null model (group claim 用)
- holdout (予測 claim 用)
- **power audit (本 module: 「nullable 結果」が真に nullable か区別)**

## カバー test family

| family | input | output |
|--------|-------|--------|
| t-test | n1, n2, d (Cohen's d) | power, can_detect, MDE_d |
| regression | n, β, SE_β | power, can_detect |
| correlation | n, r | power, MDE_r |

## 主要 formula

### t-test power (two-sample, two-sided)

```
n_eff = 2 / (1/n1 + 1/n2)
δ = d × √(n_eff / 2)
power = 1 - Φ(z_{α/2} - δ) + Φ(-z_{α/2} - δ)
```

### regression power (coefficient ≠ 0)

```
t-stat = β / SE
large-n z approximation:
power = 1 - Φ(z_{α/2} - |β|/SE) + Φ(-z_{α/2} - |β|/SE)
```

### correlation power (Fisher z)

```
z_r = atanh(r)
SE = 1 / √(n - 3)
power = 1 - Φ(z_{α/2} - z_r/SE) + Φ(-z_{α/2} - z_r/SE)
```

### MDE (inverse problem)

target_power と α 固定で、検出可能な最小 effect size を逆算:

```
d_MDE = (z_{α/2} + z_{β}) × √(2 / n_eff)
r_MDE = tanh((z_{α/2} + z_{β}) / √(n - 3))
```

## Verdict classification

| power | verdict |
|-------|---------|
| ≥ target_power (default 0.8) | ok |
| ≥ target_power - 0.1 | borderline |
| < target_power - 0.1 | underpowered |

## H1 制約

- 統計手法であり anime.score 不参入。
- report の主要 test に対し audit を **必須化** (本 method_note を CI に強制可能)。

## Caveats

- normal-approximation を一貫使用 (z-test ベース)。t-distribution の self correction
  は n > 30 で誤差 < 5% なので採用。小サンプル (n < 30) では power をやや過大評価。
- effect size の "観測値" は estimate 経由なので **observed power = post-hoc**。
  pre-registration 用には **想定 effect size** を投入すべき。post-hoc observed power
  は批判される (Hoenig & Heisey 2001) → MDE を主に報告する pattern を推奨。

## audit 推奨 input

各 v2 report の主要 test を以下 dict で audit_report_power() に渡す:

```python
specs = [
  dict(report_name="equity_oaxaca",
       test_label="gender raw_gap",
       test_family="t_test",
       n1=N_female, n2=N_male,
       observed_effect=Cohen_d),
  dict(report_name="did_studio_transfer",
       test_label="ATE",
       test_family="regression",
       n=N_panel, beta=ate, se_beta=ate_se),
  ...
]
audit_table = audit_report_power(specs)
```

→ 各 row が ok / borderline / underpowered。underpowered は report の Findings に
"low statistical power" caveat を必須挿入。

## 関連

- `src/analysis/quality/power_analysis.py` (21 tests pass)
- Cohen (1988) "Statistical Power Analysis for the Behavioral Sciences"
- Hoenig & Heisey (2001) "The abuse of power: The pervasive fallacy of power calculations"
