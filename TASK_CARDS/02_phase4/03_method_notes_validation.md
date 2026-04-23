# Task: Method Notes validation gate を CI に追加

**ID**: `02_phase4/03_method_notes_validation`
**Priority**: 🟠 Major
**Estimated changes**: 約 +60 / -0 lines, 2 files
**Requires senior judgment**: no
**Blocks**: (なし)
**Blocked by**: `02_phase4/01_meta_lineage_population` (method notes が lineage 参照できる前提)

---

## Goal

生成されたレポート HTML 内で Method Note セクションが `meta_lineage` に対応するエントリを持つことを CI で検査する。不備があれば CI fail。

---

## Hard constraints

- H5 既存テスト green 維持

**本タスク固有**: レポート本文や `meta_lineage` の中身は変えない。検査スクリプトの追加のみ。

---

## Pre-conditions

- [ ] `02_phase4/01_meta_lineage_population` 完了 (5 レポートに lineage 投入済み)
- [ ] `pixi run test` pass

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `scripts/report_generators/ci_check_method_notes.py` (新規 or 拡張) | Method Note ↔ meta_lineage 整合性を検査するスクリプト |
| `.github/workflows/report-validation.yml` or `Taskfile.yml` | 上記スクリプトを CI で実行 |

---

## Files to NOT touch

- レポート本体 (`scripts/report_generators/reports/*`)
- `method_notes.py` のヘルパー本体

---

## Steps

### Step 0: 既存 CI スクリプトの確認

```bash
ls scripts/report_generators/ci_check*.py 2>/dev/null
cat .github/workflows/report-validation.yml 2>/dev/null | head -40
```

### Step 1: チェックスクリプト作成

`scripts/report_generators/ci_check_method_notes.py`:

```python
"""CI gate: every generated report's Method Note must be backed by meta_lineage.

Fails with non-zero exit code if a published report lacks a corresponding
meta_lineage row, or if mandatory fields (formula_version, ci_method,
null_model, inputs_hash) are empty.
"""
from __future__ import annotations

import sys
import sqlite3
from pathlib import Path

# Reports whose lineage is mandatory (others may be exempted as "technical appendix")
REQUIRED_REPORTS = [
    "policy_attrition",
    "policy_monopsony",
    "policy_gender_bottleneck",
    "mgmt_studio_benchmark",
    "biz_genre_whitespace",
]

REQUIRED_FIELDS = [
    "formula_version", "ci_method", "null_model", "inputs_hash",
]


def main() -> int:
    db_path = Path("result/animetor.db")
    if not db_path.exists():
        print(f"[ci_check_method_notes] DB not found: {db_path}", file=sys.stderr)
        return 2

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        errors: list[str] = []
        for report_id in REQUIRED_REPORTS:
            row = conn.execute(
                "SELECT * FROM meta_lineage WHERE report_id = ?", (report_id,)
            ).fetchone()
            if row is None:
                errors.append(f"missing lineage: {report_id}")
                continue
            for field in REQUIRED_FIELDS:
                value = row[field] if field in row.keys() else None
                if not value:
                    errors.append(f"{report_id}.{field} is empty/null")

        if errors:
            print("[ci_check_method_notes] FAIL", file=sys.stderr)
            for e in errors:
                print(f"  - {e}", file=sys.stderr)
            return 1

        print(f"[ci_check_method_notes] OK — {len(REQUIRED_REPORTS)} report(s) validated")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    raise SystemExit(main())
```

### Step 2: Taskfile or CI に組込

既存 `Taskfile.yml` にタスク追加 (既に類似タスクがあれば同じ形式で):

```yaml
tasks:
  ci-check-method-notes:
    desc: "Validate Method Notes are backed by meta_lineage rows"
    cmds:
      - pixi run python scripts/report_generators/ci_check_method_notes.py
```

`.github/workflows/report-validation.yml` の末尾 (既存の validate ジョブの一部として) に以下を追加:

```yaml
      - name: Method Notes validation gate
        run: pixi run task ci-check-method-notes
```

### Step 3: ローカルで pass 確認

```bash
pixi run python scripts/report_generators/ci_check_method_notes.py
# 期待: "OK — 5 report(s) validated" (exit 0)
```

### Step 4: 失敗ケースも確認

一時的に `REQUIRED_REPORTS` に存在しない `"policy_fake"` を追加して fail することを確認:

```bash
# (手動テスト: REQUIRED_REPORTS に "policy_fake" を追加)
pixi run python scripts/report_generators/ci_check_method_notes.py
# 期待: "FAIL", "missing lineage: policy_fake", exit 1
# 確認後、元に戻す
```

---

## Verification

**テスト Tier 指針 (本カード固有)**:
- **T1 (Step 中)**: `pixi run test-impact` — testmon が影響テストのみ選択
- **T2 (失敗直後)**: `pixi run test-quick` — 前回失敗のみ再実行
- **T3 (カード完了時)**: `pixi run test-scoped tests/ -k "method_notes or lineage"` — 下記参照
- **T4 (commit 直前 1 回)**: `pixi run test` — 全 2161 件

```bash
# 1. 構文
python -m py_compile scripts/report_generators/ci_check_method_notes.py

# 2. 実行して pass
pixi run python scripts/report_generators/ci_check_method_notes.py
# 期待: OK, exit 0

# 3. Taskfile 経由でも動く
pixi run task ci-check-method-notes
# 期待: OK

# 4. 既存テスト
pixi run test-scoped tests/ -k "method_notes or lineage"

# 5. Lint
pixi run lint
```

---

## Stop-if conditions

- [ ] Step 3 で "missing lineage" が出る → `02_phase4/01` が未完了の可能性
- [ ] Taskfile 構文エラー
- [ ] CI workflow yml の syntax が壊れる (`yamllint .github/workflows/`)

---

## Rollback

```bash
rm scripts/report_generators/ci_check_method_notes.py
git checkout Taskfile.yml .github/workflows/report-validation.yml
pixi run test-scoped tests/ -k "method_notes or lineage"
```

---

## Completion signal

- [ ] スクリプトが pass し、Taskfile から呼べる
- [ ] CI workflow ファイルに組込まれている
- [ ] `git commit`:
  ```
  Add Method Notes validation gate

  CI script ci_check_method_notes.py verifies that each of the
  5 required reports has a meta_lineage row with non-empty
  formula_version / ci_method / null_model / inputs_hash.
  ```
