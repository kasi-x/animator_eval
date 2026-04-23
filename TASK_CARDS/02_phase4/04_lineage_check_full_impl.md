# Task: Full lineage check の完全実装

**ID**: `02_phase4/04_lineage_check_full_impl`
**Priority**: 🟠 Major
**Estimated changes**: 約 +120 / -30 lines, 1-2 files
**Requires senior judgment**: **yes** (検査ロジックの設計が必要)
**Blocks**: (なし)
**Blocked by**: `02_phase4/01_meta_lineage_population`

---

## Goal

`scripts/report_generators/ci_check_lineage.py` の骨格実装を完全実装に昇格。以下を全レポートに対して検査:

1. `meta_lineage` row が存在する
2. formula_version が semver-like (`v1.0`, `v1.2.3`, etc.) フォーマット
3. inputs_hash が 16 文字以上の hex
4. description が 50 文字以上 (空内容を防ぐ)
5. generated_at が過去 30 日以内 (stale 検知)

失敗時は詳細レポートを出力し exit 1。

---

## Hard constraints

- H5 既存テスト green 維持
- `02_phase4/03` の `ci_check_method_notes.py` とは **別の観点** で検査 (被り可だが別ファイル)

---

## Pre-conditions

- [ ] `02_phase4/01_meta_lineage_population` 完了
- [ ] `scripts/report_generators/ci_check_lineage.py` の骨格が既に存在 (確認: `ls scripts/report_generators/ci_check_lineage.py`)

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `scripts/report_generators/ci_check_lineage.py` | 骨格 → 完全実装 |
| `Taskfile.yml` | 既存タスク `ci-check-lineage` を更新 (必要なら) |

---

## Files to NOT touch

- レポート本体
- `meta_lineage` テーブル中身 (DB 更新は別タスク)

---

## Steps

### Step 0: 既存スクリプトを読む

```bash
cat scripts/report_generators/ci_check_lineage.py
```

骨格で何が実装済みかを把握。既存のチェックはそのまま残す or 整理する。

### Step 1: 検査関数を追加

スクリプトに以下の検査関数群を追加:

```python
import re
from datetime import datetime, timedelta

_SEMVER_RE = re.compile(r"^v\d+\.\d+(\.\d+)?(-[a-z0-9]+)?$")
_HEX_RE = re.compile(r"^[0-9a-f]{16,}$", re.IGNORECASE)
_STALE_DAYS = 30
_MIN_DESC_LEN = 50


def _check_formula_version(value: str | None, report_id: str) -> list[str]:
    if not value:
        return [f"{report_id}: formula_version missing"]
    if not _SEMVER_RE.match(value):
        return [f"{report_id}: formula_version '{value}' not semver-like (expected vX.Y[.Z])"]
    return []


def _check_inputs_hash(value: str | None, report_id: str) -> list[str]:
    if not value:
        return [f"{report_id}: inputs_hash missing"]
    if not _HEX_RE.match(value):
        return [f"{report_id}: inputs_hash '{value}' not hex (expected >=16 hex chars)"]
    return []


def _check_description(value: str | None, report_id: str) -> list[str]:
    if not value or len(value.strip()) < _MIN_DESC_LEN:
        return [f"{report_id}: description <{_MIN_DESC_LEN} chars (got {len(value or '')})"]
    return []


def _check_staleness(generated_at: str | None, report_id: str) -> list[str]:
    if not generated_at:
        return [f"{report_id}: generated_at missing"]
    try:
        ts = datetime.fromisoformat(generated_at.replace("Z", "+00:00"))
    except ValueError:
        return [f"{report_id}: generated_at '{generated_at}' not ISO-8601"]
    if datetime.now().astimezone() - ts > timedelta(days=_STALE_DAYS):
        return [f"{report_id}: generated_at is stale (>{_STALE_DAYS} days old)"]
    return []
```

### Step 2: main 関数の構造化

```python
def validate_all(conn) -> list[str]:
    """Run all lineage checks; return list of human-readable error messages."""
    errors: list[str] = []
    rows = conn.execute(
        "SELECT report_id, formula_version, ci_method, null_model, "
        "holdout_validation, inputs_hash, generated_at, description "
        "FROM meta_lineage"
    ).fetchall()

    if not rows:
        return ["meta_lineage is empty — no reports registered"]

    for row in rows:
        rid = row["report_id"] or "<unknown>"
        errors.extend(_check_formula_version(row["formula_version"], rid))
        errors.extend(_check_inputs_hash(row["inputs_hash"], rid))
        errors.extend(_check_description(row["description"], rid))
        errors.extend(_check_staleness(row["generated_at"], rid))
        if not row["ci_method"]:
            errors.append(f"{rid}: ci_method missing")
        if not row["null_model"]:
            errors.append(f"{rid}: null_model missing")
    return errors


def main() -> int:
    import sqlite3
    from pathlib import Path

    db_path = Path("result/animetor.db")
    if not db_path.exists():
        print(f"[ci_check_lineage] DB not found: {db_path}", file=__import__("sys").stderr)
        return 2

    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    try:
        errors = validate_all(conn)
    finally:
        conn.close()

    if errors:
        import sys
        print("[ci_check_lineage] FAIL", file=sys.stderr)
        for e in errors:
            print(f"  - {e}", file=sys.stderr)
        return 1

    count_row = sqlite3.connect(db_path).execute("SELECT COUNT(*) FROM meta_lineage").fetchone()
    print(f"[ci_check_lineage] OK — {count_row[0]} row(s) validated")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
```

### Step 3: ローカル実行

```bash
pixi run python scripts/report_generators/ci_check_lineage.py
# 期待: OK, exit 0
```

もし失敗する場合、02_phase4/01 で投入した lineage の description が短すぎる可能性。description を充実させて再投入。

### Step 4: Taskfile に接続 (既存 `ci-check-lineage` タスクがあれば更新、なければ追加)

```yaml
  ci-check-lineage:
    desc: "Validate meta_lineage rows (formula, hash, description, staleness)"
    cmds:
      - pixi run python scripts/report_generators/ci_check_lineage.py
```

---

## Verification

**テスト Tier 指針 (本カード固有)**:
- **T1 (Step 中)**: `pixi run test-impact` — testmon が影響テストのみ選択
- **T2 (失敗直後)**: `pixi run test-quick` — 前回失敗のみ再実行
- **T3 (カード完了時)**: `pixi run test-scoped tests/ -k "lineage or ci_check"` — 下記参照
- **T4 (commit 直前 1 回)**: `pixi run test` — 全 2161 件

```bash
# 1. 構文
python -m py_compile scripts/report_generators/ci_check_lineage.py

# 2. 実行 pass
pixi run python scripts/report_generators/ci_check_lineage.py
# 期待: "OK — N row(s) validated"

# 3. Taskfile 経由
pixi run task ci-check-lineage
# 期待: OK

# 4. 失敗ケースを意図的に作って FAIL 確認
sqlite3 result/animetor.db "UPDATE meta_lineage SET description='' WHERE report_id='policy_attrition';"
pixi run python scripts/report_generators/ci_check_lineage.py
# 期待: FAIL, "policy_attrition: description <50 chars", exit 1
# その後、description を復元

# 5. 既存テスト
pixi run test-scoped tests/ -k "lineage or ci_check"

# 6. Lint
pixi run lint
```

---

## Stop-if conditions

- [ ] `meta_lineage` が empty (`02_phase4/01` 未完了)
- [ ] lineage rows が全て validation fail (description が 50 文字未満など)
  - → `02_phase4/01` に戻って description を拡充
- [ ] Taskfile の構文エラー

---

## Rollback

```bash
git checkout scripts/report_generators/ci_check_lineage.py Taskfile.yml
pixi run test-scoped tests/ -k "lineage or ci_check"
```

---

## Completion signal

- [ ] pass / fail 両ケースが期待通り動く
- [ ] Taskfile から呼べる
- [ ] `git commit`:
  ```
  Complete lineage validation (ci_check_lineage)

  Adds formula_version semver, inputs_hash hex, description length,
  and generated_at staleness checks.
  ```
