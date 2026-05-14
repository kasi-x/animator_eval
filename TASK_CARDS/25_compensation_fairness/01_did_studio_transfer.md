# Task: スタジオ移籍 DiD — theta_i / opportunity 因果効果

**ID**: `25_compensation_fairness/01_did_studio_transfer`
**Priority**: 🟠
**Estimated changes**: +600 / -0 lines, 4 files (analysis / report / test / inventory)
**Requires senior judgment**: yes (parallel trends / treatment 定義)
**Blocks**: `25/04_pay_equity_decomp` (decomp の causal 解釈強化)
**Blocked by**: なし

---

## Goal

person × year panel で「スタジオ移籍 (treatment)」の theta_i / opportunity_residual / credit count に対する **因果効果** を DiD で推定。
parallel trends 検定 + analytical CI を満たし、政策・publication 双方に出せる evidence にする。

---

## 経路別価値

| 経路 | 用途 |
|------|------|
| **Publication** | キャリアデザイン学会 → 労働経済学系学会 (RIETI / 日本経済学会) フル論文 |
| **政策** | 労働流動性の効果データ → 経産省コンテンツ産業課・文化庁の政策 brief |
| **Business** | スタジオ間移籍が個人の構造的位置に及ぼす効果 → 報酬交渉根拠 |

---

## Hard constraints

- **H1**: 結果変数は `theta_i` (AKM) / opportunity_residual / credit_count のみ。anime.score 不可
- **H2**: 「移籍効果」表現可、「能力向上」「成長」frame 禁止 → 「構造的位置の変化」
- **H3**: entity resolution は Resolved 層成果をそのまま使用、自前ロジック禁止

---

## Method

### Treatment 定義

```
transfer[i, t] = 1 if (
    primary_studio[i, t-1] != primary_studio[i, t]
    AND credits_at_new_studio[i, t] >= 3   # 一時参加除外
    AND credits_at_old_studio[i, t-1] >= 3
)
```

primary_studio = 直近 3 年で credits 最多のスタジオ (tie-break: 最新)。

### Specification

```
y[i, t] = α_i + γ_t + β · post[i, t] · treated[i] + X[i, t]·δ + ε[i, t]
```

- `α_i`: person FE、`γ_t`: year FE
- `treated[i]`: i が観測期間中に移籍したか
- `post[i, t]`: 移籍 (event_year) 以後 = 1
- `X`: 経験年数 / role_diversity / cohort_size

### Parallel trends 検定

event-study spec で leads (-3, -2, -1) が 0 と区別不能か検定:

```
y[i, t] = α_i + γ_t + Σ_{k=-3}^{+5} β_k · 1[t - event_year[i] = k] + ε
```

baseline = k = -1。leads の β が 0 と区別不能なら parallel trends 成立。

### CI

cluster-robust SE (cluster = person)、`statsmodels` PanelOLS。

---

## Files to create

| File | 内容 |
|------|------|
| `src/analysis/causal/did_studio_transfer.py` | panel build + estimation + event-study |
| `scripts/report_generators/reports/causal_studio_transfer.py` | report (Findings / Interpretation 分離) |
| `tests/analysis/causal/test_did_studio_transfer.py` | 合成データで β 推定値検証 |
| `docs/method_notes/did_studio_transfer.md` | 仮定・代替 spec 併記 |

---

## Pre-conditions

- [ ] Resolved 層の persons / credits / studios が安定
- [ ] AKM theta_i / psi_j 既存出力利用可能
- [ ] treated person 数 ≥ 500 (power 確保)
- [ ] `pixi run test` baseline pass

---

## Stop-if

- treated < 200 (power 不足) → 別 treatment (COVID shock 等) に切替検討
- parallel trends 顕著違反 → DiD 不適、synthetic control 検討
- credit attribution 誤りで多数の偽 transfer 発生 → entity resolution 監査 (28/01) 先行

---

## Verification

```bash
pixi run test-scoped tests/analysis/causal/test_did_studio_transfer.py
pixi run python -m src.analysis.causal.did_studio_transfer --dry-run
rg 'anime\.score|capability|talent|能力' src/analysis/causal/did_studio_transfer.py   # 0
```

---

## 拡張候補 (別カード)

- 監督との初協業 effect on next theta
- 受賞 (アニメアワード) effect on subsequent collaboration breadth
- 制作委員会変化 effect on staff retention
