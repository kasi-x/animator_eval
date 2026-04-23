# Task: フレッシュ init の smoke 確認

**ID**: `01_schema_fix/03_fresh_init_smoke`
**Priority**: 🔴 Critical (成功判定の最終 gate 1/2)
**Estimated changes**: 約 +40 lines (新テスト 1 本)
**Blocks**: なし
**Blocked by**: `02_legacy_cleanup`

---

## Goal

「**ゼロから DB を作ると正しい新 schema になる**」を確認する smoke テスト。本セクションの成功判定 2 つのうち 1 つ目。

---

## 方針

- テストは 1 本、30 行くらいで十分
- 詳細 parity test (旧 `07_schema_baseline/01`) は**不要**。migration 履歴をもう保持しないので意味がない
- assert は「期待テーブルが存在」「廃止テーブルが存在しない」の 2 点で十分

---

## Files to create

| File | 内容 |
|---|---|
| `tests/test_fresh_init.py` | fresh init smoke |

---

## 実装

```python
"""Smoke test: fresh DB init produces the target schema."""
from __future__ import annotations
import sqlite3
from pathlib import Path

import pytest

from src.database import init_db

EXPECTED = {
    "anime", "persons", "credits",
    "sources", "roles",
    "anime_studios", "anime_genres", "anime_tags",
    "anime_external_ids", "person_external_ids",
    "person_scores", "voice_actor_scores",
    "ops_source_scrape_status", "ops_lineage",
    "schema_meta",
}

FORBIDDEN = {
    "anime_display", "anime_analysis",
    "va_scores", "scores",
    "data_sources",
    "meta_lineage",  # moved to ops_lineage
}


def test_fresh_init_creates_target_schema(tmp_path: Path):
    db_path = tmp_path / "fresh.db"
    conn = sqlite3.connect(db_path)
    try:
        init_db(conn)
        rows = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' "
            "AND name NOT LIKE 'sqlite_%'"
        ).fetchall()
        tables = {r[0] for r in rows}
    finally:
        conn.close()

    missing = EXPECTED - tables
    leaked = tables & FORBIDDEN
    assert not missing, f"Missing canonical tables: {sorted(missing)}"
    assert not leaked, f"Deprecated tables leaked: {sorted(leaked)}"
```

---

## Steps

1. `tests/test_fresh_init.py` を書く
2. `pixi run pytest tests/test_fresh_init.py -v`
3. 失敗したら → 新 `database.py` の DDL 修正 → 再実行

---

## Verification

```bash
pixi run pytest tests/test_fresh_init.py -v
# 期待: 1 passed
```

---

## Completion signal

- [ ] `test_fresh_init_creates_target_schema` が pass
- [ ] コミット: `Add fresh init smoke test`
