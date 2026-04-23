# Task: v55 フレッシュ schema での pipeline smoke test

**ID**: `02_phase4/02_pipeline_smoke_test`
**Priority**: 🟠 Major
**Estimated changes**: 約 +80 / -0 lines, 1-2 files
**Requires senior judgment**: no (典型的なテスト追加)
**Blocks**: (なし)
**Blocked by**: `01_schema_fix/` 全完了

---

## Goal

新規 DB (empty SQLite ファイル) に対し `init_db()` + `run_migrations()` を走らせ、SCHEMA_VERSION が 55 になり、pipeline の最小フローが動くことを確認するテストを追加する。

---

## Hard constraints

- H5 既存テスト green 維持
- H8 行番号を信じない

**本タスク固有**: **既存の pipeline コードやプロダクション DB は変更しない**。テストのみ追加。

---

## Pre-conditions

- [ ] `01_schema_fix/` 全カード完了
- [ ] `pixi run test` pass (2161+)
- [ ] `SCHEMA_VERSION = 55`

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `tests/test_pipeline_v55_smoke.py` (新規) | フレッシュ schema での pipeline smoke test |

---

## Files to NOT touch

- `src/pipeline.py`
- `src/pipeline_phases/*`
- `src/database.py`
- プロダクション DB (`result/animetor.db`)

---

## Steps

### Step 0: 既存テストのパターン確認

```bash
# 既存の pipeline 系テストを参考にする
ls tests/test_*pipeline*.py tests/test_integration*.py
grep -l 'synthetic\|fixture' tests/test_*.py | head -5
```

既存 fixture (`conftest.py` など) を活用する。

### Step 1: テストファイル新規作成

`tests/test_pipeline_v55_smoke.py`:

```python
"""Smoke test: fresh v55 schema can host a minimal pipeline run.

Ensures the schema migrations land cleanly on an empty DB and the
pipeline's initial phases execute without error.
"""
from __future__ import annotations

import tempfile
from pathlib import Path

import pytest

from src.database import get_connection, get_schema_version, init_db, run_migrations, SCHEMA_VERSION


def test_fresh_init_reaches_v55(tmp_path: Path):
    """Fresh DB init must reach the latest SCHEMA_VERSION."""
    db_path = tmp_path / "fresh.db"
    conn = get_connection(db_path)
    try:
        init_db(conn)
        run_migrations(conn)
        version = get_schema_version(conn)
    finally:
        conn.close()

    assert version == SCHEMA_VERSION, (
        f"expected version {SCHEMA_VERSION}, got {version}"
    )
    assert version >= 55, "v55 migration must be applied"


def test_fresh_init_has_canonical_tables(tmp_path: Path):
    """Fresh DB must have canonical silver/bronze/gold tables after migration."""
    db_path = tmp_path / "fresh.db"
    conn = get_connection(db_path)
    try:
        init_db(conn)
        run_migrations(conn)

        # Silver (canonical)
        assert _table_exists(conn, "anime"), "canonical anime table missing"
        assert _table_exists(conn, "persons"), "persons missing"
        assert _table_exists(conn, "credits"), "credits missing"

        # Lookup
        assert _table_exists(conn, "sources"), "sources lookup missing"

        # Gold
        assert _table_exists(conn, "person_scores"), "person_scores (renamed from scores) missing"
        assert _table_exists(conn, "meta_lineage"), "meta_lineage missing"

        # NOT expected (deprecated)
        assert not _table_exists(conn, "anime_display"), (
            "anime_display should have been dropped in v55"
        )
        assert not _table_exists(conn, "anime_analysis"), (
            "anime_analysis should be renamed to anime"
        )
        assert not _view_exists(conn, "person_scores"), (
            "person_scores should be a TABLE, not a VIEW"
        )

        # credits.evidence_source column
        cols = {r[1] for r in conn.execute("PRAGMA table_info(credits)")}
        assert "evidence_source" in cols, "credits.evidence_source missing"
        assert "source" not in cols, "credits.source should have been removed in v54"
    finally:
        conn.close()


def test_fresh_init_seeds_sources(tmp_path: Path):
    """Sources lookup must contain the canonical 5+ seeds after v55."""
    db_path = tmp_path / "fresh.db"
    conn = get_connection(db_path)
    try:
        init_db(conn)
        run_migrations(conn)
        rows = conn.execute("SELECT code FROM sources ORDER BY code").fetchall()
    finally:
        conn.close()

    codes = {r[0] for r in rows}
    expected = {"anilist", "ann", "allcinema", "seesaawiki", "keyframe"}
    missing = expected - codes
    assert not missing, f"sources seeds missing: {missing}"


# --- helpers ---

def _table_exists(conn, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
    ).fetchone()
    return row is not None


def _view_exists(conn, name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='view' AND name=?", (name,)
    ).fetchone()
    return row is not None
```

### Step 2: 実行して pass 確認

```bash
pixi run test -- tests/test_pipeline_v55_smoke.py -v
```

**期待**: 3 tests passed。

### Step 3: 全テストでも pass 確認

```bash
pixi run test-scoped tests/ -k "smoke or v55"
# 期待: 2164 passed (2161 + 3 new), 4 skipped
```

---

## Verification

**テスト Tier 指針 (本カード固有)**:
- **T1 (Step 中)**: `pixi run test-impact` — testmon が影響テストのみ選択
- **T2 (失敗直後)**: `pixi run test-quick` — 前回失敗のみ再実行
- **T3 (カード完了時)**: `pixi run test-scoped tests/ -k "smoke or v55"` — 下記参照
- **T4 (commit 直前 1 回)**: `pixi run test` — 全 2161 件

```bash
# 1. 新規ファイルが作られた
ls tests/test_pipeline_v55_smoke.py

# 2. テスト実行
pixi run test -- tests/test_pipeline_v55_smoke.py -v
# 期待: 3 passed

# 3. 全テスト
pixi run test-scoped tests/ -k "smoke or v55"

# 4. Lint
pixi run lint

# 5. invariant
rg 'anime\.score\b' src/analysis/ src/pipeline_phases/
```

---

## Stop-if conditions

- [ ] 3 つのテストのいずれかが fail
  - `test_fresh_init_reaches_v55` fail → `01_schema_fix/02` が未完了の可能性
  - `test_fresh_init_has_canonical_tables` で `anime_display exists: True` → `01_schema_fix/04` 未完了
  - `test_fresh_init_has_canonical_tables` で `person_scores is a view` → `01_schema_fix/03` 未完了
- [ ] 既存 2161 テストが影響を受ける (本来無関係)

---

## Rollback

```bash
rm tests/test_pipeline_v55_smoke.py
pixi run test-scoped tests/ -k "smoke or v55"   # baseline 復元
```

---

## Completion signal

- [ ] 3 test pass + 既存 2161 pass
- [ ] `git diff --stat` が 1 new file (+80 lines)
- [ ] `git commit`:
  ```
  Add v55 schema smoke test

  Verifies fresh init + migrations reach SCHEMA_VERSION 55 and
  produce the expected canonical tables (anime, person_scores as TABLE,
  sources with seeds, no anime_display/anime_analysis).
  ```
