# Task: T01b — data_loading.py ユニットテスト

**ID**: `06_tests/T01b_data_loading`
**Priority**: 🟡 Minor
**Estimated changes**: +150 / -0 lines (`tests/test_data_loading_phase.py` 新規)
**Requires senior judgment**: no
**Blocks**: なし
**Blocked by**: `01_schema_fix/` 全完了

---

## Goal

`src/pipeline_phases/data_loading.py:run_data_loading_phase()` を
**インメモリ SQLite DB** を使って直接テストする。

現状: `test_pipeline.py` の統合テストでしか通らない。Phase 1 単独のユニットテストがない。

---

## Hard constraints

(`_hard_constraints.md` を事前に読むこと)

- H5 既存テスト green 維持
- **本タスク固有**: SILVER 層のみ使用 (anime.score を使わない)

---

## Pre-conditions

- [ ] `pixi run test` pass (baseline)
- [ ] `git status` clean

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `tests/test_data_loading_phase.py` | **新規作成** |

---

## Test Cases

### 1. 空 DB でエラーにならない

```python
def test_empty_db_loads_without_error(empty_silver_db):
    """run_data_loading_phase() on empty DB must not raise."""
    from src.pipeline_phases.data_loading import run_data_loading_phase
    ctx = make_empty_context(empty_silver_db)
    run_data_loading_phase(ctx)
    assert ctx.persons == []
    assert ctx.credits == []
```

### 2. persons と credits が正しく読み込まれる

```python
def test_loads_persons_and_credits(seeded_silver_db):
    """Persons and credits in DB are reflected in ctx after loading."""
    from src.pipeline_phases.data_loading import run_data_loading_phase
    ctx = make_empty_context(seeded_silver_db)
    run_data_loading_phase(ctx)
    assert len(ctx.persons) == 5    # seeded 5 persons
    assert len(ctx.credits) >= 10   # seeded 10+ credits
```

### 3. NON_PRODUCTION_ROLES が除外される

```python
def test_non_production_roles_excluded(db_with_non_production_credits):
    """Credits with NON_PRODUCTION_ROLES (e.g., ORIGINAL_CREATOR) are filtered out."""
    from src.pipeline_phases.data_loading import run_data_loading_phase
    from src.utils.role_groups import NON_PRODUCTION_ROLES
    ctx = make_empty_context(db_with_non_production_credits)
    run_data_loading_phase(ctx)
    for credit in ctx.credits:
        assert credit.role not in NON_PRODUCTION_ROLES
```

### 4. anime_map の整合性

```python
def test_anime_map_keyed_by_anime_id(seeded_silver_db):
    """ctx.anime_map must be a dict keyed by anime_id."""
    from src.pipeline_phases.data_loading import run_data_loading_phase
    ctx = make_empty_context(seeded_silver_db)
    run_data_loading_phase(ctx)
    assert isinstance(ctx.anime_map, dict)
    for anime_id, anime in ctx.anime_map.items():
        assert anime.anime_id == anime_id
```

### 5. person_id がクレジットに存在しない場合のフィルタリング

```python
def test_orphan_credits_filtered(db_with_orphan_credits):
    """Credits referencing non-existent person_id are dropped."""
    from src.pipeline_phases.data_loading import run_data_loading_phase
    ctx = make_empty_context(db_with_orphan_credits)
    run_data_loading_phase(ctx)
    valid_person_ids = {p.person_id for p in ctx.persons}
    for credit in ctx.credits:
        assert credit.person_id in valid_person_ids
```

---

## Fixtures

```python
import sqlite3
import pytest
from pathlib import Path

@pytest.fixture
def empty_silver_db(tmp_path: Path) -> Path:
    """Minimal SILVER schema, no data."""
    db = tmp_path / "test.db"
    conn = sqlite3.connect(str(db))
    from src.database import init_db
    init_db(conn)
    conn.close()
    return db

@pytest.fixture
def seeded_silver_db(tmp_path: Path) -> Path:
    """5 persons, 3 anime, 15 credits in SILVER schema."""
    db = tmp_path / "test.db"
    conn = sqlite3.connect(str(db))
    from src.database import init_db
    init_db(conn)
    # seed persons
    conn.executemany(
        "INSERT INTO persons (person_id, name_ja) VALUES (?,?)",
        [(f"p{i}", f"Person{i}") for i in range(5)],
    )
    # seed anime
    conn.executemany(
        "INSERT INTO anime (anime_id, title, year, episodes) VALUES (?,?,?,?)",
        [(f"a{i}", f"Anime{i}", 2020 + i, 12) for i in range(3)],
    )
    # seed credits (key_animator)
    conn.executemany(
        "INSERT INTO credits (person_id, anime_id, role) VALUES (?,?,?)",
        [(f"p{i%5}", f"a{i%3}", "key_animator") for i in range(15)],
    )
    conn.commit()
    conn.close()
    return db
```

---

## Steps

```bash
touch tests/test_data_loading_phase.py
pixi run python -m pytest tests/test_data_loading_phase.py -v
pixi run test
pixi run lint
```

---

## Verification

```bash
pixi run python -m pytest tests/test_data_loading_phase.py -v
# 期待: 5+ passed

rg 'anime\.score\b' tests/test_data_loading_phase.py   # 0 件
```

---

## Completion signal

- [ ] `tests/test_data_loading_phase.py` が 5 件以上 pass
- [ ] 既存テストに影響なし
- [ ] commit:
  ```
  T01b: Add data_loading phase unit tests (empty DB, filter, orphan credits)
  ```
