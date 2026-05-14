# Task: entity resolution drift monitoring

**ID**: `28_monitoring/01_entity_resolution_drift`
**Priority**: 🟡
**Estimated changes**: +300 / -0 lines, 3 files
**Requires senior judgment**: medium
**Blocks**: 全分析の信頼性 (基盤)
**Blocked by**: なし

---

## Goal

Conformed 層の cross-source disagreement (同 person を異なる source が異なる属性で記述する rate)
を週次で snapshot し、drift を検出 → 再 scrape / human review トリガー。

---

## 経路別価値

| 経路 | 用途 |
|------|------|
| **基盤 (全経路)** | データ品質保証、long-term 分析の信頼性 |
| **Business** | SLA 級の品質指標 (顧客に提示可能) |

---

## Hard constraints

- **H3**: entity resolution ロジック不変。本タスクは **監査のみ**
- drift 検出時の自動マージ修正は禁止 (false positive リスク)

---

## Method

### Disagreement metrics

```
gender_disagreement_rate = N(person で source A vs B で gender 不一致) / N(両方 non-null)
hometown_disagreement_rate = ...
birthday_disagreement_rate = ...
role_label_disagreement_rate = ...
```

### Drift detection

週次 snapshot → CUSUM で trend 変化点検出。閾値超過 → alert (issue 化候補)。

### Snapshot storage

`mart.meta_resolution_audit_weekly` (week, source_pair, attribute, disagreement_rate, n)。

---

## Files

| File | 内容 |
|------|------|
| `src/analysis/quality/resolution_drift.py` | metric + CUSUM |
| `scripts/monitoring/weekly_resolution_snapshot.py` | cron |
| `tests/analysis/quality/test_resolution_drift.py` | toy CUSUM |

---

## Pre-conditions

- [ ] Resolved 層安定
- [ ] Conformed 層に source pair 別属性が読める

---

## Stop-if

- weekly run > 1h → sample-based 監視に切替
