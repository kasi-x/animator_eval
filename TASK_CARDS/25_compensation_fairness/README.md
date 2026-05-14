# 25_compensation_fairness — 補償公正の核

**目的**: プロジェクト目的「個人の貢献を可視化して公正な報酬につなげる」の核。
analytical CI / null model / holdout の method gate を満たした **causal evidence** を提供。

## カード一覧

| ID | 内容 | Priority | 経路 |
|----|------|---------|------|
| `01_did_studio_transfer` | スタジオ移籍 DiD: theta_i / opportunity への因果効果 | 🟠 | publication / 政策 |
| `02_opportunity_residual_null` | opportunity_residual を null model + analytical CI に厳格化 | 🟠 | business / 政策 |
| `03_visibility_loss_holdout` | 翌年クレジット可視性喪失 早期警告 (holdout 必須) | 🟠 | business (HR) |
| `04_pay_equity_decomp` | 同等 theta / psi 条件下の credit 機会差分解 (gender / cohort / 所属) | 🟠 | publication / 政策 |

## 横断原則

- 「能力差」frame 禁止 (H2)。常に「機会・位置の差」
- すべて method gate (CI / null / holdout) 表示
- Findings (評価形容詞なし) / Interpretation (一人称) 分離 (`docs/REPORT_PHILOSOPHY.md`)
- treatment 候補: COVID (2020-2022) / Netflix 大量発注 (2017-) / 京アニ事件 (2019)
