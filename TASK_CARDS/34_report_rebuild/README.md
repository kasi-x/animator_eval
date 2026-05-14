# 34_report_rebuild — レポート全体の labor-first 再構築

**目的**: `docs/STANCE.md` (2026-05-06 確定) に基づき、既存 37 v3 reports + 3 brief を labor-first スタンスに合わせて再構築。

## 既に実装済 (2026-05-06)

- ✅ `docs/STANCE.md` 起草 (labor-first 宣言、目的経路重み、JAniCA 関係、DOB 扱い)
- ✅ `scripts/report_generators/html_templates.py`: `STANCE_BLOCK` 追加、`wrap_html_v2` で全 v2 report に自動注入。`DISCLAIMER` 文言を STANCE 整合に改訂
- ✅ `scripts/report_generators/helpers.py`: `build_stance_block(lang)` / `build_disclaimer(lang)` 追加
- ✅ `scripts/report_generators/export.py`: brief HTML renderer の disclaimer を STANCE 整合に改訂
- ✅ `scripts/report_generators/forbidden_vocab.yaml`: `ranking_framing` / `hiring_framing` カテゴリ追加 (序列化・雇用判断 frame 禁止)

## カード一覧

| ID | 内容 | Priority | 自動化可否 |
|----|------|---------|----------|
| `01_audit_existing_reports` | 全 37 v3 report に lint_vocab + 新カテゴリ実行、違反洗い出し | 🟠 | 半自動 |
| `02_brief_restructure` | 3 brief (Policy / HR / Business) の section 構成を labor-first に改訂 | 🟠 | 手動 |
| `03_sns_outreach_layer` | SNS 発信用の short-form report 自動生成 (existing report からダイジェスト) | 🟡 | 半自動 |
| `04_b2c_individual_view` | 個別 person view の labor-first 設計 (`27/03_iv_xai` + `31/02_b2c_design` 連動) | 🟡 | 手動 |
| `05_policy_brief_short` | 2 ページ短縮 policy brief (33_policy/01 と統合) | 🟡 | 手動 |

## 残作業の規模感

- 37 v3 report 個別 audit: 各 5-15 分 → 計 ~5h
- 3 brief restructure: 各 1-2h → 計 ~5h
- 新規 short-form / SNS-friendly cut: 設計含め ~10h

## 共通方針

1. **labor-first frame 違反検出**: `lint_vocab.py` を新カテゴリ込みで全 report に実行
2. **Findings / Interpretation 分離強化**: ranking 表現は Findings から除去、Interpretation でも CI 必須
3. **データソース明示**: 各 report 末尾に missingness caveat を追加 (`27_methodology/01_missingness_disclosure` 連動)
4. **stance 整合 link**: 全 report 末尾に `docs/STANCE.md` への link
