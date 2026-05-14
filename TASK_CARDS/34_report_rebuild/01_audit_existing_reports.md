# Task: 既存 37 v3 report の labor-first audit

**ID**: `34_report_rebuild/01_audit_existing_reports`
**Priority**: 🟠
**Estimated changes**: +200 / -50 lines (lint runner + report 個別修正は派生)
**Requires senior judgment**: medium
**Blocks**: brief restructure
**Blocked by**: forbidden_vocab.yaml 拡張 (済)

---

## Goal

`scripts/report_generators/lint_vocab.py` に新カテゴリ (ranking_framing / hiring_framing) を反映し、
全 37 v3 report に対して実行 → 違反洗い出し → 個別修正カード化。

---

## 経路別価値

publication / 政策 brief 共通の品質基盤。

---

## Steps

### Step 1: lint_vocab.py に新カテゴリ追加対応

```bash
grep -n "ability_framing\|causal_verbs\|evaluative_adjectives" scripts/report_generators/lint_vocab.py
# 新カテゴリ ranking_framing / hiring_framing を同じ列挙に追加
```

### Step 2: 全 report に対して実行

```bash
pixi run python scripts/report_generators/lint_vocab.py --all-reports --json > /tmp/audit.json
```

### Step 3: 違反集計

```bash
jq '.[] | select(.violations | length > 0) | {report: .file, violations: .violations | length}' /tmp/audit.json
```

### Step 4: 修正カード化

各 report の違反数で priority 付け:
- 5+ 件: `34_report_rebuild/01a_<report_name>` カード
- 1-4 件: lint 通過まで一括修正 (1 PR)
- 0 件: skip

---

## Files

| File | 変更 |
|------|------|
| `scripts/report_generators/lint_vocab.py` | ranking_framing / hiring_framing カテゴリ enabled |
| `scripts/report_generators/reports/<各report>.py` | 違反箇所修正 (派生カード) |

---

## Pre-conditions

- [ ] forbidden_vocab.yaml 新カテゴリ追加済 (済)
- [ ] STANCE.md 公開 (済)

---

## Verification

```bash
pixi run python scripts/report_generators/lint_vocab.py --all-reports
# 全 report が ranking_framing / hiring_framing で 0 violations
```

---

## Stop-if

- 違反数が 100 件超 → 段階的修正 (severity error → warning に一時降格 + roadmap 化)
