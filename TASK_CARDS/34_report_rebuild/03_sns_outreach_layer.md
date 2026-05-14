# Task: SNS 発信用 short-form 自動生成

**ID**: `34_report_rebuild/03_sns_outreach_layer`
**Priority**: 🟡
**Estimated changes**: +400 / -0 lines, 3-4 files
**Requires senior judgment**: medium
**Blocks**: SNS 運営の効率化
**Blocked by**: `30_stakeholder/02_individual_outreach_sns`

---

## Goal

既存 37 v3 report のうち SNS 配信向きのものから、X / note 用の short-form を自動生成。
- X: 280 字以内 + figure 1 枚
- note: 1500-3000 字 long-form

---

## Method

### X 用テンプレート

```
[Finding 1 行] (例: "アニメーター credit 公表率は業界平均 X%")
[根拠 1 行] (例: "5 年平均、source = AniList/MAL/ANN cross-validated")
[figure 1 枚]
[詳細 link]
[hashtag: #アニメ業界 #労働環境]
```

### note 用テンプレート

```
[タイトル]
[STANCE.md 引用 1 段落]
[Finding 説明 (詳細)]
[Method note]
[Caveat block (missingness)]
[Interpretation (一人称)]
[詳細レポート link]
[免責 footer]
```

### 自動生成

各 report.py に `to_sns_post()` / `to_note_post()` メソッド追加:

```python
class MyReport(BaseReportGenerator):
    def to_sns_post(self) -> dict:
        return {
            "platform": "x",
            "text": "...",
            "figure_path": "...",
            "url": "..."
        }
```

---

## Files

| File | 内容 |
|------|------|
| `scripts/report_generators/sns_export.py` | SNS post 生成器 |
| `scripts/report_generators/reports/_base.py` | `to_sns_post()` / `to_note_post()` 抽象メソッド追加 |
| `docs/templates/x_post.md` | X テンプレート |
| `docs/templates/note_post.md` | note テンプレート |

---

## Pre-conditions

- [ ] STANCE.md 公開 (済)
- [ ] forbidden_vocab 新カテゴリ反映 (済)
- [ ] `30_stakeholder/02_individual_outreach_sns` 設計確定

---

## Stop-if

- 自動生成テキストの quality が低く手動修正コスト > 自動化価値 → テンプレートのみ提供 (auto-generation 廃止)
