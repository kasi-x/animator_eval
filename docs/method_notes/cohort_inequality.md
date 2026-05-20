# Method Note — Cohort Inequality

## 目的

同年代 cohort (5 年 bin) 内の credit 機会量分布の不平等を 3 指標で測定し、
世代間の構造的格差変化を時系列で観察する。

## 指標

| 指標 | 範囲 | 解釈 |
|------|------|------|
| Gini | 0 - 1 | 0 = 完全均等、1 = 独占 |
| Theil-T | 0 - ∞ | entropy-based、decomposable |
| Atkinson(ε=0.5) | 0 - 1 | welfare-loss 解釈、ε 大 = inequality aversion 大 |

3 指標併設の理由: Gini は中央値付近に敏感、Theil は両端尾に敏感、Atkinson は
welfare 解釈可能。同方向の動きが揃えば robust。

## Spec

```
records = [(debut_year, log(1 + total_credits))]
cohort_bin = (debut_year // bin_width) * bin_width  # default bin_width=5
cohort 内で各指標を計算 → 時系列 row 生成
```

min_cohort_n = 30 (default) 未満の cohort は除外。

## CI

`bootstrap_inequality()` で各 cohort × 指標の percentile CI (1000 回)。
`compare_cohorts()` で 2 cohort 間 CI 重複から有意差判定。

## H1/H2 制約

- H1: anime.score 不参入。結果変数は `log(1 + total_credits)`。
- H2: 主観的評価 frame NG。"structural position inequality" のみ。

## Caveats (報告書に必須開示)

1. **生存者バイアス**: 短寿命 person は credit 少 → Gini を **下方** に押し下げる。
   最近 cohort が「より平等」に見えるバイアス。
2. **累積途上**: 近年 cohort は活動年数が浅く total_credits が累積途上 →
   時間軸比較に右への構造的減衰。
3. **role weight 捨象**: credit 1 件 = 同じ重み。動画 1 件と監督 1 件を区別しない単純化。
4. **anime scale 捨象**: 1 話のクレジットも 50 話のクレジットも同じ重み。

## 代替 spec (拡張候補)

- 重み付き credit (role weight × episode 数)
- production_scale_sum で credit を normalize
- gender × cohort 交差不平等
- inter-cohort decomposition (within / between)

本実装は **Tier 1** (重み無し credit count + log 変換)。

## 関連

- `src/analysis/equity/cohort_inequality.py` (23 tests pass)
- `scripts/report_generators/reports/cohort_inequality.py` (v2 report、hr brief)
- Atkinson (1970) "On the measurement of inequality"
- Cowell (2011) "Measuring Inequality" 3rd ed.
