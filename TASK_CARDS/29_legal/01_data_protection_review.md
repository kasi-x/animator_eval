# Task: full public + on-request opt-out 設計の法務 review

**ID**: `29_legal/01_data_protection_review`
**Priority**: 🟠
**Estimated changes**: doc-only (法務 engagement + STANCE.md §3 改訂)
**Requires senior judgment**: yes (法律家 engagement)
**Blocks**: B2B / B2C 顧客への有償サービス提供
**Blocked by**: なし

---

## Goal

公開クレジット集約 + 個人スコア full public + on-request opt-out 運用の法的整理を、
個人情報保護法 + GDPR (将来の海外展開時) の観点で法律家レビューを通す。
review 後、`docs/STANCE.md §3` を改訂し、不可避な保護機構があれば実装カード化。

---

## 経路別価値

| 経路 | 用途 |
|------|------|
| **Business** | B2B / B2C 契約時の法務照会への耐性 |
| **政策** | 政策担当・議員アプローチ時の自プロジェクト整合性 |
| **Publication** | 査読 ethics 審査の前提整理 |

---

## 想定論点

### 個人情報保護法

- 公開クレジット = 既公開個人情報の集約 → 「個人関連情報の取得」該当性
- 第 3 者提供 (B2B 顧客への閲覧提供) の本人同意要件
- 利用目的の特定・通知義務 (cookie / 利用規約での開示)
- 漏洩時の報告義務 (個人情報保護委員会)

### GDPR (将来の海外展開時)

- legitimate interest による処理の正当化可能性
- データ主体の権利 (access / rectification / erasure / objection)
- public interest と balancing test

### 名誉毀損

- 低スコア表示・順位下位表示が defamation に該当しうるか
- labor-first framing (能力評価でなく構造的位置) の defamation 防御効果
- disclaimers の実効性

### opt-out

- on-request の SLA (7 日 / 30 日) の業界 standard
- 削除後の集計データへの取り扱い (匿名化 / 完全削除)

---

## Steps

### Step 1: 法律家選定

- 個人情報保護法 + IT 法務に強い弁護士事務所を 2-3 contact
- 「集約データを公開する事業」の前例があるか確認
- 初回相談 (1-2h) で論点絞込

### Step 2: 論点書面の準備

- 現状の data flow 図 (Raw → Source → Conformed → Resolved → Mart → public display)
- DOB / hometown / gender 等の特に注意要するフィールドリスト
- 想定 user flow (誰が何を見られるか)
- `docs/STANCE.md` を添付

### Step 3: review 実施 + 結果反映

- 不可避な実装事項があれば `29_legal/03_optout_mechanism` 等のカードに反映
- STANCE.md §3 を法務確認済の文言に改訂
- disclaimer 文言を法務 OK のものに更新

---

## Pre-conditions

- [ ] `docs/STANCE.md` 初版完成 (済、2026-05-06)
- [ ] business 経路 MVP 設計開始前

---

## Stop-if

- 法律家が「full public は無理」と判定 → STANCE.md §3 を full public → public_aggregate 等に変更、再設計

---

## Verification

- [ ] 法務 review 完了書面の保管
- [ ] STANCE.md §3 が法務確認済文言に更新
- [ ] 必要なら 03_optout_mechanism / 03_disclaimers 等の派生カード作成
