# 29_legal — 法的・倫理的設計

**目的**: full public + opt-out 運用の法務 review、publication ethics、最低保護 policy。
labor-first スタンス (`docs/STANCE.md`) を法的にも整合させる。

## カード一覧

| ID | 内容 | Priority | トリガー |
|----|------|---------|---------|
| `01_data_protection_review` | full public + on-request opt-out 設計の法務 review | 🟠 | business 化前 (B2B 顧客接触前) 必須 |
| `02_ethics_review_for_publication` | 査読誌投稿時の ethics statement / IRB 相当の自主審査 | 🟠 | 最初の査読誌投稿前 |
| `03_optout_mechanism` | 削除フォーム + 7 日 SLA の実装 | 🟡 | public 化と同時 (DOB 露出なしなら緩い) |

## 不要と判断したカード

- `~~04_minor_protection~~` — エンドクレジットに年齢情報なし、年齢推論しない方針 (`STANCE.md §3.2`) のため不要
- `~~05_deceased_special_handling~~` — display 層の「故人」表示は将来検討、urgent ではない
