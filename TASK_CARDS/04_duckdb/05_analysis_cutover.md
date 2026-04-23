# Task: src/analysis/ の SQLite 経路を silver.duckdb に切替

**ID**: `04_duckdb/05_analysis_cutover`
**Priority**: 🟠 Major
**Estimated changes**: 約 +200 / -300 lines, 15-25 files
**Requires senior judgment**: **yes** (各 analysis モジュールのクエリパターン)
**Blocks**: `04_duckdb/06_sqlite_decommission`
**Blocked by**: `04_duckdb/03_integrate_etl`, `04_duckdb/04_gold_atomic_swap`

---

## Goal

`src/analysis/` と `src/pipeline_phases/` の SQLite 経由のデータ取得をすべて silver.duckdb 直読に切替える。`get_connection()` (sqlite3) への依存をこのレイヤから削除。

API / report も `GoldReader` (既に DuckDB) と同様に silver.duckdb 直接 connect に切替える。

---

## Hard constraints

- H1 anime.score を analysis に流さない (silver には score がないので構造的に保証されるが、import パスに display_lookup が漏れていないか確認)
- H3 entity resolution ロジック不変 (DB アクセスパスの差し替えのみ)
- H5 既存テスト green 維持
- H8 行番号を信じない

**本タスク固有**:
- **memory_limit を必ず明示**: 各 connection 開設時に `PRAGMA memory_limit='2GB'`
- **per-query open/close を基本**: long-lived connection を保持しない (atomic swap で古い inode に張り付くのを避ける)
- **`src/analysis/duckdb_io.py` の ATTACH パターンは廃止**: silver が DuckDB native になったので ATTACH 不要、`duckdb.connect(silver_path)` で直接 query
- **`PipelineContext` のフィールドは触らない** (Hamilton 化は §5 で別途)

---

## Pre-conditions

- [ ] `04_duckdb/03_integrate_etl` 完了 (silver.duckdb が integrate で生成可能)
- [ ] `04_duckdb/04_gold_atomic_swap` 完了
- [ ] silver.duckdb と gold.duckdb が手元で生成済み
  ```bash
  pixi run integrate
  pixi run pipeline
  ls -la result/silver.duckdb result/gold.duckdb
  ```

---

## Files to modify

### 中核 (helper)
| File | 変更内容 |
|------|---------|
| `src/analysis/silver_reader.py` | **新規作成** — `silver_connect()` ヘルパ + よく使う query 関数 |
| `src/analysis/duckdb_io.py` | ATTACH パターン廃止、`silver_connect()` 経由に書き換え |

### Pipeline phases (ロード経路)
| File | 変更内容 |
|------|---------|
| `src/pipeline_phases/data_loading.py` | sqlite → silver.duckdb |
| `src/pipeline_phases/validation.py` | 同上 |
| `src/pipeline_phases/graph_construction.py` | 同上 (`load_credits_ddb` の利用箇所も整理) |
| `src/pipeline_phases/result_assembly.py` | persons 取得経路 |

### Analysis (中規模)
| File | 概要 |
|------|---------|
| `src/analysis/scoring/akm.py` | credits/anime ロード経路 |
| `src/analysis/scoring/birank.py` | 同上 |
| `src/analysis/scoring/iv.py` | 同上 |
| `src/analysis/network/*.py` | persons/credits ロード |
| `src/analysis/career/*.py` | 同上 |
| (他 10+ ファイル — `grep -l "get_connection\|sqlite3" src/analysis/` で網羅) | 同上 |

### API / CLI
| File | 変更内容 |
|------|---------|
| `src/api.py` | sqlite クエリを silver/gold connect に置換 |
| `src/cli.py` | 同上 |

---

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/database.py` (まだ削除しない) | `06` で扱う |
| `src/etl/integrate.py` (旧 SQLite 版) | `06` で削除 |
| `src/scrapers/*` | `02` 完了済み |
| `src/utils/display_lookup.py` | report 用 (analysis から呼ばない invariant 維持) |
| `src/analysis/entity_resolution.py` のロジック | H3 |

---

## Steps

### Step 0: 影響範囲の網羅

```bash
# get_connection / sqlite3 の analysis 内利用を全列挙
rg -l 'get_connection|import sqlite3' src/analysis/ src/pipeline_phases/ \
  > /tmp/sqlite_callers.txt
wc -l /tmp/sqlite_callers.txt
cat /tmp/sqlite_callers.txt
```

→ **件数を見て senior が分割粒度を決める**。20 ファイル以上なら本カードを「helper + pipeline_phases」「analysis モジュール群」「API/CLI」の 3 サブタスクに分けることを検討。

### Step 1: `silver_reader.py` 新設

```python
# src/analysis/silver_reader.py
"""DuckDB-native readers for SILVER layer (replaces SQLite get_connection).

All analysis modules import from here instead of src.database.
Per-query open/close to avoid pinning to a stale inode after atomic swap.
"""
from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import Iterator

import duckdb

DEFAULT_SILVER_PATH = Path(
    os.environ.get("ANIMETOR_SILVER_PATH", "result/silver.duckdb")
)


@contextmanager
def silver_connect(
    path: Path | str | None = None,
    *,
    memory_limit: str = "2GB",
    read_only: bool = True,
) -> Iterator[duckdb.DuckDBPyConnection]:
    """Open silver.duckdb (read-only by default, per-query lifecycle)."""
    p = str(path or DEFAULT_SILVER_PATH)
    conn = duckdb.connect(p, read_only=read_only)
    try:
        conn.execute(f"PRAGMA memory_limit='{memory_limit}'")
        conn.execute("PRAGMA temp_directory='/tmp/duckdb_spill'")
        yield conn
    finally:
        conn.close()


def load_credits(path: Path | str | None = None) -> list[dict]:
    """Replacement for the SQLite credits scan."""
    with silver_connect(path) as conn:
        rel = conn.execute("SELECT * FROM credits")
        cols = [d[0] for d in rel.description]
        return [dict(zip(cols, row)) for row in rel.fetchall()]


def load_anime(path: Path | str | None = None) -> list[dict]:
    with silver_connect(path) as conn:
        rel = conn.execute("SELECT * FROM anime")
        cols = [d[0] for d in rel.description]
        return [dict(zip(cols, row)) for row in rel.fetchall()]


def load_persons(path: Path | str | None = None) -> list[dict]:
    with silver_connect(path) as conn:
        rel = conn.execute("SELECT * FROM persons")
        cols = [d[0] for d in rel.description]
        return [dict(zip(cols, row)) for row in rel.fetchall()]


# 他のよく使われる loader を必要に応じて追加 (anime_studios, anime_genres, ...)
```

### Step 2: `duckdb_io.py` 整理

ATTACH パターン (`ATTACH '...' AS sl (TYPE SQLITE, READ_ONLY TRUE)`) を全廃。silver が DuckDB native なので:

**Before** (`src/analysis/duckdb_io.py:39-52`):

```python
def load_credits_ddb(...):
    conn = _duck(db_path)  # in-memory + ATTACH SQLite
    rel = conn.execute("SELECT * FROM sl.credits")
    ...
```

**After**:

```python
def load_credits_ddb(silver_path=None):
    # silver は既に DuckDB なので ATTACH 不要、直接 connect
    from src.analysis.silver_reader import silver_connect
    with silver_connect(silver_path) as conn:
        rel = conn.execute("SELECT * FROM credits")
        cols = [d[0] for d in rel.description]
        return [dict(zip(cols, row)) for row in rel.fetchall()]
```

→ もしくは `silver_reader.load_credits()` に統合して `duckdb_io.py` 自体を deprecate する。判断は senior。

### Step 3: pipeline_phases を順次切替

`src/pipeline_phases/data_loading.py` から開始。各ファイル:

1. `from src.database import get_connection` 削除
2. `from src.analysis.silver_reader import silver_connect, load_credits, load_anime, load_persons` 追加
3. `with get_connection() as conn: rows = conn.execute("SELECT * FROM ...")` を `with silver_connect() as conn: rel = conn.execute("...")` に
4. row tuple アクセス (`row[0]`) はそのまま動く (DuckDB も positional tuple を返す)
5. テスト: `pixi run test-scoped tests/test_pipeline_phases.py -v`

### Step 4: analysis モジュールを順次切替

各モジュール (`scoring/akm.py`, `network/*.py`, `career/*.py`, ...) を Step 3 と同じパターンで。**1 モジュール完了 → test → commit** のサイクル。

### Step 5: API / CLI 切替

`src/api.py` / `src/cli.py` の SQLite 経路を silver/gold direct に置換。Gold は `GoldReader` 既存利用。

### Step 6: monkeypatch ポイント変更

既存テストは `monkeypatch.setattr(src.database, "DEFAULT_DB_PATH", ...)` を使っている (CLAUDE.md にも明記)。これを以下に切替:

```python
# Before
monkeypatch.setattr(src.database, "DEFAULT_DB_PATH", tmp_path / "test.db")

# After
monkeypatch.setattr(
    "src.analysis.silver_reader.DEFAULT_SILVER_PATH",
    tmp_path / "silver.duckdb",
)
monkeypatch.setattr(
    "src.analysis.gold_writer.DEFAULT_GOLD_DB_PATH",
    tmp_path / "gold.duckdb",
)
```

CLAUDE.md の "Critical Testing Patterns" セクションを更新する (本カードの最後で commit に含める)。

---

## Verification

```bash
# 1. analysis から sqlite3 import が消えた
rg 'import sqlite3|from sqlite3' src/analysis/ src/pipeline_phases/
# 期待: 0 件

# 2. analysis から get_connection の利用が消えた
rg 'get_connection' src/analysis/ src/pipeline_phases/
# 期待: 0 件

# 3. invariant
rg 'anime\.score\b' src/analysis/ src/pipeline_phases/
rg 'display_lookup' src/analysis/ src/pipeline_phases/

# 4. 全テスト
pixi run test
# 期待: 2161+ tests pass

# 5. lint
pixi run lint

# 6. 実 pipeline 走行
pixi run pipeline
# 期待: silver.duckdb 読み + gold.duckdb 書きで完走

# 7. atomic swap 同時実行確認
# ターミナル A: pixi run serve  (API 起動)
# ターミナル B: curl localhost:8000/api/persons/p:1/profile  (連続)
# ターミナル C: pixi run pipeline  (gold 再生成)
# A の応答が一切 error なく続くこと
```

**T4** (commit 直前): `pixi run test` 1 回。

---

## Stop-if conditions

- [ ] sqlite3 row → DuckDB row で型が違ってテスト失敗 (e.g., `Row` vs `tuple`、Decimal vs float)
- [ ] DuckDB に存在しない SQLite 関数 (e.g., `julianday()`) を analysis が使っている → 個別書き換え
- [ ] 切替したモジュールが大幅に slower (期待は equal or faster)
- [ ] monkeypatch が効かず integration test で本番 silver.duckdb を読みに行ってしまう

---

## Rollback

ファイル単位で:

```bash
git checkout src/analysis/{name}.py tests/test_{name}.py
pixi run test-scoped tests/ -k "{name}"
```

全部:

```bash
git checkout src/analysis/ src/pipeline_phases/ src/api.py src/cli.py
rm src/analysis/silver_reader.py
pixi run test
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] CLAUDE.md の "Critical Testing Patterns" を更新済み (silver_reader monkeypatch パターンに)
- [ ] commit messages: モジュール群ごとに分けて 5-10 commits 程度
