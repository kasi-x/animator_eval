# Task: 2 ページ短縮 policy brief テンプレート

**ID**: `33_policy/01_short_form_brief`
**Priority**: 🟡
**Estimated changes**: +200 / -0 lines (template + 既存 Policy brief 抜粋ロジック)
**Requires senior judgment**: medium
**Blocks**: 政策担当アクセス
**Blocked by**: なし

## 完了 (2026-05-06)

- ✅ `scripts/report_generators/briefs/policy_brief_short.py` 新規実装 (curated 抜粋方式)
- ✅ `scripts/generate_briefs_v2.py` orchestrator に統合 (`policy_short` 出力)
- ✅ `tests/reports/test_policy_brief_short.py` (14 tests) すべて pass
- ✅ HTML (5.4 KB / A4 print CSS / 2 page-break sections) + Markdown 出力
- ✅ lint_vocab pass (negation context 検出強化済)
- ✅ STANCE 整合 (labor-first 立場 line + 政策推奨 5 件 + opt-out 言及 + 序列化禁止)

出力先:
- `result/html/policy_brief_short.html`
- `result/md/policy_brief_short.md`

---

## Goal

政策担当者・議員秘書向けの 2 ページ短縮 brief。
現状の長文 Policy brief は学術寄りで政策担当者には長過ぎる。

---

## 経路別価値

政策提言経路の最初の deliverable。

---

## 構成 (2 ページ厳守)

### Page 1: Findings + Key figures

- 1-2 行: プロジェクト概要 (labor-first 含む)
- 3-5 個の主要数値 (例: 「アニメーター credit 公表率は業界平均 X%、ジェンダー間で Y ポイント差」)
- figure 1 枚 (時系列 or 比較棒グラフ)
- 数値の出所と method 1 行

### Page 2: 政策推奨 + 連絡先

- 推奨事項 3-5 個 (labor-first 方向)
  - 例: 「クレジット記載のガイドライン化」
  - 例: 「ジェンダー機会格差是正の補助金」
  - 例: 「中堅枯渇対策の人材育成助成」
- 各推奨の根拠 = Page 1 の数値
- 連絡先 + プロジェクト URL + 詳細 brief への参照
- 免責 (短縮版でも必須)

---

## Files

| File | 内容 |
|------|------|
| `docs/templates/policy_brief_short.md` | テンプレート |
| `scripts/report_generators/reports/policy_brief_short.py` | 自動生成 (長文 Policy brief から抜粋) |
| `tests/reports/test_policy_brief_short.py` | 2 ページ制約 + 必須要素確認 |

---

## Pre-conditions

- [ ] STANCE.md §4.5 / §5 完成 (済)
- [ ] 既存 Policy brief 第 1 弾出力済

---

## Stop-if

- 2 ページに収まらない (主要数値が多すぎる) → claim 1 つに絞った single-issue brief 化
