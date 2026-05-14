# Task: 翌年クレジット可視性喪失 早期警告 (holdout 必須)

**ID**: `25_compensation_fairness/03_visibility_loss_holdout`
**Priority**: 🟠
**Estimated changes**: +500 / -0 lines, 4 files
**Requires senior judgment**: yes (label 定義 / leakage 防止)
**Blocks**: HR brief 早期警告セクション
**Blocked by**: なし

---

## Goal

person の「翌年 credit 可視性喪失」を temporal holdout で予測。
naming は狭く: 「離職率」ではなく **"翌年クレジット可視性喪失率"** (REPORT_PHILOSOPHY 準拠)。

---

## 経路別価値

| 経路 | 用途 |
|------|------|
| **Business** | スタジオ HR 介入 (面談・契約調整) のトリガー |
| **政策** | 業界全体の流出率時系列 → 産業政策の根拠 |
| **Publication** | 労働市場 attrition の構造要因分析 |

---

## Hard constraints

- **H2**: 「離職」「離脱」予測ではなく「クレジット可視性喪失」(可視性 ≠ 業界離脱)
- 個人レベル予測は **CI** + holdout AUC + calibration plot 必須
- データ source bias の honest 開示 (in-between animator 等は credit 可視性低い)

---

## Method

### Label

```
visibility_loss[i, t+1] = 1 if credit_count[i, t+1] == 0
                              AND credit_count[i, max(t-2, t)] >= 1
```

### Features (構造的のみ)

- theta_i (AKM)、PageRank、betweenness、role_diversity
- 直近 3 年 credit count 軌跡 (slope / variance)
- 直近スタジオ多様性 (Shannon entropy)
- 役職進行 stall (同一 role 連続年数)
- 共クレジット相手の離脱率 (peer effect)
- cohort 年齢 (debut 年からの経過)

### Model

LightGBM + isotonic calibration。比較ベースライン = logistic + last-3-year mean。

### Validation (leakage 防止)

```
Train: <= year T-1
Holdout: year T (label = year T+1 の可視性)
```

person split ではなく **year split**。同一 person の過去 feature は train、未来は holdout。

### Honest reporting

- AUC + Brier score + calibration plot
- false positive cost (誤って警告 → 不要な介入) を明示
- subgroup performance (gender / role group / cohort) で fairness check

---

## Files

| File | 内容 |
|------|------|
| `src/analysis/career/visibility_loss.py` | label / feature / train / predict |
| `scripts/report_generators/reports/career_visibility_warning.py` | HR brief セクション |
| `tests/analysis/career/test_visibility_loss.py` | leakage 検出テスト含む |
| `docs/method_notes/visibility_loss.md` | 仮定・誤分類コスト |

---

## Pre-conditions

- [ ] credits panel 過去 10 年以上カバー
- [ ] AKM theta 安定
- [ ] AniList orphan backfill 完了 (label 定義の信頼性向上)

---

## Stop-if

- holdout AUC < 0.65 → 構造的予測限界、report 化見送り
- subgroup AUC 差 > 0.10 → bias 顕著、fairness 修正先行

---

## Verification

```bash
pixi run test-scoped tests/analysis/career/test_visibility_loss.py
pixi run python -m src.analysis.career.visibility_loss --leakage-check
```
