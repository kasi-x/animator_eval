# Method Note — Cox Proportional Hazards (visibility loss)

## 目的

既存 `career_visibility_warning` の **LightGBM + isotonic calibration** に対する
**統計推論的補完**。Cox PH モデルで:

- per-feature HR (hazard ratio) + 95% CI
- Schoenfeld residual test (PH 仮定検定)
- temporal holdout concordance (debut_year split)

を提供。LightGBM = 予測精度、Cox = 解釈性 (HR で「θ が 1σ 増加すると hazard が
何倍」が直接読める)。academic venue 受けに必須。

## Spec

```
Cox PH: λ(t | X) = λ_0(t) · exp(X · β)
HR_k = exp(β_k)  # k 番目 feature の hazard ratio
```

features (default candidate): θ_i, tenure_years, role_diversity, cohort_decade dummy。

## PH assumption check

`check_ph_assumption()` (旧名 `test_ph_assumption` → pytest collision 回避で改名):
Schoenfeld residual test (`lifelines.statistics.proportional_hazard_test`)。

H0: proportional hazards holds。p < 0.05 で reject → 時間変化する β。
violators 列挙 → 時間相互作用項を追加した拡張 spec を別途検討。

## Temporal holdout

`evaluate_temporal_holdout(split_year=2010)`:
- train: debut_year < split_year
- test: debut_year >= split_year
- concordance: train vs test の Harrell's C-index 並列。

drift signal: test_C < train_C - 0.05 で重大な temporal drift。

## H1/H2 制約

- H1: anime.score 非依存。
- H2: "離職" "キャリア終了" frame NG → "visibility loss" / "credit 出現の途絶"。

## Caveats

- censoring: 観測期間末尾で event 未観測 = censored。観測尺の短い person は censoring 過多。
- left truncation: debut 前の credit 不在を仮定。1990s 前の credit データ薄さに留意。
- competing risks: visibility_loss は引退 / 死亡 / 別職移行 / data attrition の **複合**。
  単一 hazard で扱う単純化を明示。
- PH 仮定: 違反時は AFT (accelerated failure time) や Aalen 加法モデルを別 cut で検討。

## 関連

- `src/analysis/career/cox_visibility.py` (13 tests pass)
- `src/analysis/career/visibility_loss.py` (LightGBM 既存)
- `scripts/report_generators/reports/career_visibility_warning.py` (統合 candidate)
- lifelines ドキュメント: https://lifelines.readthedocs.io/
