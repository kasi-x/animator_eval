# Task: opportunity_residual 厳格化 (null model + analytical CI)

**ID**: `25_compensation_fairness/02_opportunity_residual_null`
**Priority**: 🟠
**Estimated changes**: +400 / -100 lines, 3 files
**Requires senior judgment**: yes (null model 設計)
**Blocks**: 補償根拠系 report (HR / Business brief)
**Blocked by**: なし

---

## Goal

現状ヒューリスティックな `opportunity_residual` を OLS 残差 + analytical CI + permutation null model に置換。
H4 (補償根拠は analytical CI 必須) の正面対応。

---

## 経路別価値

| 経路 | 用途 |
|------|------|
| **Business** | スタジオ HR が「同等位置の個人と比較して機会過少」を法的に主張可能なレベルへ |
| **Publication** | 労働市場 mismatch の定量化 (構造空席の検出) |
| **政策** | 業界全体の機会偏在度 → 補助金・税優遇の根拠 |

---

## Hard constraints

- **H1**: 説明変数に anime.score 不可。`role_weight × episodes × duration_mult` のみ
- **H2**: 「機会過少」「構造的不利」表現可。「実力に見合わない」frame 禁止
- compensation basis として出すなら CI 必須 (CLAUDE.md ハードルール 4)

---

## Method

### Spec

```
log(credit_count[i, year]) = β0 + β1·theta_i + β2·tenure_i + β3·role_diversity_i
                           + α_studio[i] + γ_year + ε[i, year]

opportunity_residual[i] = mean over years (ε[i, year])
SE = σ_ε / √n_years[i]
CI95 = ±1.96 · SE
```

### Permutation null

H0: opportunity is independent of person identity.
person id を 1000 回 permute → 各 individual の null 分布 → empirical p-value。
analytical CI と null 95% interval が両立するなら頑健。

### 既存実装の置換

```bash
grep -rn "opportunity_residual" src/ scripts/
```

該当箇所:
- `src/analysis/scoring/individual.py` (推定)
- `scripts/report_generators/reports/*.py` 複数

新実装は `src/analysis/scoring/opportunity.py` に分離、既存は thin wrapper で呼出。

---

## Files

| File | 変更 |
|------|------|
| `src/analysis/scoring/opportunity.py` (新規) | OLS 残差 + analytical CI + permutation null |
| `src/analysis/scoring/individual.py` | 旧ヒューリスティック → 新実装呼出 |
| `tests/analysis/scoring/test_opportunity.py` | CI カバレッジ検証 (95% 名目で実 95±2%) |
| `docs/method_notes/opportunity_residual.md` | 仮定・代替 spec |

---

## Pre-conditions

- [ ] AKM theta_i 安定出力
- [ ] persons × year panel (空年含む) が組める
- [ ] 既存 opportunity_residual 利用箇所の grep 一覧化

---

## Verification

```bash
pixi run test-scoped tests/analysis/scoring/test_opportunity.py
pixi run python scripts/lint_report_vocabulary.py
# CI カバレッジ
pixi run python -m src.analysis.scoring.opportunity --calibration-check
```

---

## Stop-if

- residual が systematic に skewed (Q-Q plot で正規逸脱大) → log link 再検討
- permutation null 1000 回が 24h 超 → 100 回 + 分散縮小法
