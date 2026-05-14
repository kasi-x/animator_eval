# Task: 個別アニメーター向け B2C SaaS 設計

**ID**: `31_business/02_b2c_design`
**Priority**: 🟡
**Estimated changes**: 設計のみ (実装は派生カード)
**Requires senior judgment**: medium
**Blocks**: B2C launch
**Blocked by**: `27_methodology/03_iv_xai`、`29_legal/01_data_protection_review`、`29_legal/03_optout_mechanism`

---

## Goal

個別アニメーター本人が「自分の構造的位置を確認する」B2C SaaS の MVP 設計。
labor-first スタンス (`STANCE.md §1`) の最終形。

---

## 経路別価値

| 経路 | 用途 |
|------|------|
| **Business** | 直接収益 (subscription / 報酬交渉サポート) |
| **政策** | 「労働者寄り tool」の存在自体が政策提言の材料 |
| **Publication** | 「実装した社会的効果」の事例として論文添付可能 |

---

## 想定機能 (MVP)

### Tier 0 (無料・誰でも)

- 自分の portfolio ページ (公開クレジット集約)
- 構造的位置 (theta_i / network 中心性) の percentile 表示
- 業界平均との比較 (cohort 内)
- opt-out ボタン

### Tier 1 (本人認証後 = 無料)

- IV 5 成分 + dormancy の透明分解 (`27_methodology/03_iv_xai` 連動)
- 類似軌跡の anonymized cohort 表示
- 報酬交渉時に出力できる PDF report (1-2 ページ)

### Tier 2 (subscription 想定)

- 詳細な分析 (counterfactual シミュレーション: スタジオ移籍 / 役職変化時の予測)
- 同役職の credit 公表率比較 (会社にクレジット公表を依頼する根拠)
- カスタム alert (他社が同役職で公開しているか)

---

## 価格設定 (検討)

- Tier 0-1: 無料 (labor-first スタンスの中核)
- Tier 2: 月 ¥500-1500 程度 (アニメーター給与帯を考慮)
- 業界団体 (JAniCA) 経由の bulk discount

---

## 本人認証

- SNS 紐付け (X / pixiv account)
- 業界内コラボ歴の照合
- 追加: メアド + 自筆 portfolio との照合

---

## Files (実装時、派生カード)

- `src/routers/persons_b2c.py`
- `static/persons/{id}/index.html`
- `src/analysis/scoring/iv_decomposition.py` (既存、`27/03` で実装)
- `src/auth/` (新規)

---

## Pre-conditions

- [ ] `27_methodology/03_iv_xai` 実装済
- [ ] `29_legal/01_data_protection_review` 完了
- [ ] `29_legal/03_optout_mechanism` 実装済
- [ ] `31_business/01_startup_form` 確定 (運営主体)

---

## Stop-if

- 法務 review で「個人 score を本人以外も閲覧する」設計が NG → Tier 0 を本人のみに縮退
