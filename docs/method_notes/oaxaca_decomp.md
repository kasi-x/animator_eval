# Method Note — Oaxaca-Blinder 分解 (機会格差)

## 目的

person × y panel で観測される group A vs B (e.g. female vs male) の y 差を
"endowment" (構造的位置の差) と "structural" (同位置の処遇差) に分離する。
**「能力差」とは無関係に、構造的に同位置な person 間の機会差を測る** 指標。

労働経済学 (Oaxaca 1973 / Blinder 1973) の系譜。

## Specification

```
y_A = X_A · β̂_A + ε_A    # group A 回帰
y_B = X_B · β̂_B + ε_B    # group B 回帰

ΔY = ȳ_A - ȳ_B
   = (X̄_A - X̄_B) · β̂_B   +   X̄_A · (β̂_A - β̂_B)
     ↑ endowment           ↑ structural
```

- 結果変数 y: `log(1 + total_credits)` (機会量 proxy)
- features X: `theta_i` (person FE), `tenure_years`, `role_diversity (=active_years proxy)`
- 基準 group: group_B (= male in default gender cut)

切片差 (β̂_A[0] - β̂_B[0]) は structural に統合。

## CI

bootstrap 1000 回 (group A / B それぞれを置換抽出) + 95% percentile CI。
cluster = person 想定 (1-row-per-person panel)。

## H2 厳格制約

- 「能力差」/「優劣」NG。endowment = "構造的位置の差"、structural = "同位置の処遇差" のみ。
- group 定義 (female / male 二値) は **openly な単純化**。non-binary 等は別 cut。
- gender が null の person は **必ず除外** + 除外量を Findings に開示。
- subgroup n < 100 → 推定不安定、Findings に明示。

## Pre-conditions

- gender null < 30% (`§15` gender enrichment 完了後に動作)
- `feat_career` (mart) + `person_scores` (mart) が pipeline 出力済み

## 解釈ガイド

| pattern | 解釈 |
|---------|------|
| raw_gap ≈ 0 | 機会量の group 平均差なし |
| endowment 大 / structural ≈ 0 | 構造的位置の差で raw_gap 完全説明、structural gap なし |
| endowment ≈ 0 / structural 負 | 同位置でも group A が小さい credit を得ている (機会の structural gap) |
| 両方非ゼロ | 構造的位置 + 処遇の両方に group 差 |

structural が CI 0 を含まず負 = 政策介入 / 監査の対象。

## 代替 spec (拡張カード候補)

- Cotton (1988): pooled β̂ を referent に使用
- Neumark (1988): non-discriminatory β̂ 推定
- Reimers (1983): 平均 β̂ を referent に使用 (50/50 重み)

本実装は **Blinder (1973) 基準型** (基準 = group B の β̂)。先験的に female が group A、male が group B。

## Subgroup 拡張

- gender × cohort_decade (debut decade)
- gender × studio tier
- 所属 (大手 / 中堅 / 個人)

cohort × gender の交差では subgroup n が薄くなりやすい → boot_failures に注意。

## 関連

- `src/analysis/equity/oaxaca_decomp.py` (実装)
- `scripts/report_generators/reports/equity_oaxaca.py` (レポート)
- `tests/analysis/equity/test_oaxaca.py` (14 tests)
- `TASK_CARDS/25_compensation_fairness/04_pay_equity_decomp.md` (起票カード)
- `TODO.md §15` (gender enrichment 前提)
