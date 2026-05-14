# Task: 議連 / 政策研究会経由のアクセス設計

**ID**: `33_policy/02_giin_renkei_protocol`
**Priority**: 🟡
**Estimated changes**: doc-only
**Requires senior judgment**: yes (政治的判断)
**Blocks**: 議員アプローチ
**Blocked by**: `33_policy/01_short_form_brief`

---

## Goal

アニメ関連議連 / 文化議連 / コンテンツ産業政策研究会 経由で議員秘書 → 議員 にアクセスする protocol。
labor-first スタンスを維持しつつ、政治的中立性を保つ設計。

---

## 経路別価値

政策提言の実装経路。

---

## 想定議連 / 研究会

- アニメ・マンガ議員連盟 (超党派)
- 文化議員連盟
- フリーランス保護議員連盟
- コンテンツ産業政策議員連盟
- 知的財産戦略推進事務局 (内閣府)

---

## アクセス protocol

### Step 1: 議連の動向 monitor

- 公開資料 / 議事録から関心領域確認
- 直近の議員 (会長 / 事務局長) を identify

### Step 2: 議員秘書経由

- 議員 HP の問合せフォーム → 秘書宛に brief 送付
- 「アニメ業界の労働実態に関するデータ提供を行っており、議連の議論に資する可能性があるためご紹介させていただきたい」
- 2 ページ brief 添付

### Step 3: 政治的中立性

- 特定政党のみへの提供は避ける
- 全議連 (超党派) に同時 / 段階的に提供
- 個別議員の政治活動への利用は拒否

### Step 4: フォローアップ

- 議連勉強会への参加打診 (賓客スピーカーとして)
- 必要に応じて custom 集計提供

---

## Pre-conditions

- [ ] `33_policy/01_short_form_brief` 完成
- [ ] STANCE.md §4.5 確定 (済)

---

## Stop-if

- 議連経由で特定政党のみへの利用要求 → 拒否、ministry 経路に切替
