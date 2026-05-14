# Task: opt-out 機構実装 (削除フォーム + 7 日 SLA)

**ID**: `29_legal/03_optout_mechanism`
**Priority**: 🟡
**Estimated changes**: +400 / -0 lines, 5 files (router + form + 削除 ETL + audit table + test)
**Requires senior judgment**: medium
**Blocks**: public 化 / B2B / B2C
**Blocked by**: `29_legal/01_data_protection_review` (法務 review で SLA / 削除範囲確定)

---

## Goal

person 削除リクエストを受け付け、7 日以内に display 層から除外、集計層 (Mart) から再生成不能な形で削除する機構。

---

## 経路別価値

全経路の前提条件。

---

## Method

### 受付経路

- 個人ページ右下「このページを削除」ボタン → 確認画面 → メール認証 → 受付
- 直接メール窓口 (delete@example.com) → 手動受付 → 認証
- フォーム入力 (本人確認: 業界内コラボ歴 / SNS account / etc) → 受付

### 削除範囲

- **必須**: display 層 (portfolio.html / explorer / API) から該当 person_id を非表示
- **必須**: Resolved 層から該当 person 行を削除
- **方針 (法務確認次第)**: Conformed / Source 層は historical record として残すか、完全削除するか
- **必須**: 集計 (Mart) は次回 pipeline 時に再生成 (削除済 person を含まない)

### Audit

`mart.meta_optout_audit` テーブル:

```sql
person_id_removed, requested_at, verified_at, removed_at, requester_method, sla_met
```

### SLA

7 日以内 (法務 review で延長可能性あり)。

---

## Files

| File | 内容 |
|------|------|
| `src/routers/optout.py` | 受付 API endpoint |
| `static/optout.html` | フォーム UI |
| `src/db/optout.py` | 削除 ETL |
| `src/db/schema.py` | meta_optout_audit table 追加 |
| `tests/routers/test_optout.py` | E2E test (合成 person 削除 → display 非表示確認) |

---

## Pre-conditions

- [ ] `29_legal/01_data_protection_review` 完了 (削除範囲確定)
- [ ] STANCE.md §3.5 確定

---

## Stop-if

- 法務 review で「Conformed / Source 完全削除必須」 → 削除 ETL 大幅再設計

---

## Verification

```bash
pixi run test-scoped tests/routers/test_optout.py
# 合成 person を opt-out → display API で 404 確認
```
