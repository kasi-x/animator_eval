# Task: integrate ETL (parquet → silver.duckdb) + atomic swap

**ID**: `04_duckdb/03_integrate_etl`
**Priority**: 🟠 Major
**Estimated changes**: 約 +250 / -0 lines, 4 files
**Requires senior judgment**: **yes** (各 BRONZE 表 → SILVER 表のマッピング)
**Blocks**: `04_duckdb/04_gold_atomic_swap`, `04_duckdb/05_analysis_cutover`
**Blocked by**: `04_duckdb/02_scraper_migration`

---

## Goal

`bronze/source=*/table=*/date=*/*.parquet` を読み取り、SILVER 層 (`silver.duckdb`) に dedup + 正規化して書き込む ETL を新設。

**書き込みは atomic swap で行う**: 一時ファイル `silver.duckdb.new` に build → `os.replace()` で本番ファイルに置換。analysis 側の long-running query をブロックしない。

---

## Hard constraints

- H1 anime.score を SILVER に流さない (`anime` テーブルに score / popularity / favourites カラムを足さない)
- H4 `credits.evidence_source` カラムを保持
- H5 既存テスト green 維持

**本タスク固有**:
- **memory_limit を必ず明示**: `PRAGMA memory_limit='2GB'` を connection 開設直後に実行 (RAM 暴走防止)
- **temp_directory を明示**: `PRAGMA temp_directory='/tmp/duckdb_spill'`
- **atomic swap は `os.replace()` を使う** (`shutil.move` ではない、cross-device の保証がない)
- **新ファイルへの書き込み完了確認後にのみ swap** (途中エラーなら新ファイルを削除して abort)
- BRONZE 側の dedup は `(source, id)` キーで `arg_max(updated_at)` 的に最新を採用 (具体ロジックはソースごとに senior 判断)

---

## Pre-conditions

- [ ] `04_duckdb/02_scraper_migration` 完了
- [ ] BRONZE parquet が手元に少なくとも 1 source 分ある (テスト用)
  ```bash
  ls result/bronze/source=*/table=*/date=*/*.parquet | head
  ```
- [ ] `pixi run test` pass

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/etl/integrate_duckdb.py` | **新規作成** (~150 行) — parquet → silver.duckdb |
| `src/etl/atomic_swap.py` | **新規作成** (~50 行) — `with atomic_duckdb_swap(path) as new_path:` |
| `tests/test_integrate_duckdb.py` | **新規作成** (~100 行) |
| `tests/test_atomic_swap.py` | **新規作成** (~80 行) |
| `pixi.toml` | `pixi run integrate` task 追加 |

---

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/etl/integrate.py` (既存 SQLite 版) | `06` で削除予定。現段階では並存 |
| `src/database.py` | `06` で扱う |
| `src/scrapers/bronze_writer.py` | `01` で確定 |
| `src/analysis/` | `05` で扱う |

---

## Steps

### Step 1: `atomic_swap.py` 実装

```python
# src/etl/atomic_swap.py
"""Atomic file swap for DuckDB output (no writer block on readers)."""
from __future__ import annotations

import contextlib
import os
from pathlib import Path
from typing import Iterator

import structlog

logger = structlog.get_logger()


@contextlib.contextmanager
def atomic_duckdb_swap(target: Path | str) -> Iterator[Path]:
    """Yield a temporary path for building a new DuckDB file.

    On successful exit, atomically replaces `target` with the new file.
    On exception, deletes the temporary file and re-raises (target unchanged).

    Readers holding `target` open keep the old inode (POSIX); they continue
    to see the pre-swap data until they close the connection.

    Usage:
        with atomic_duckdb_swap("silver.duckdb") as new_path:
            conn = duckdb.connect(str(new_path))
            conn.execute("PRAGMA memory_limit='2GB'")
            ...
            conn.close()
        # silver.duckdb is now the new file
    """
    target = Path(target)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".new")
    if tmp.exists():
        tmp.unlink()  # leftover from prior crashed run
    try:
        yield tmp
    except Exception:
        if tmp.exists():
            tmp.unlink()
        raise
    if not tmp.exists():
        raise RuntimeError(f"atomic_duckdb_swap: {tmp} was not created")
    os.replace(tmp, target)  # POSIX atomic rename
    logger.info("duckdb_atomic_swap", target=str(target))
```

### Step 2: `integrate_duckdb.py` 実装 (骨格)

```python
# src/etl/integrate_duckdb.py
"""ETL: BRONZE parquet → SILVER duckdb (with atomic swap)."""
from __future__ import annotations

import os
from pathlib import Path

import duckdb
import structlog

from src.etl.atomic_swap import atomic_duckdb_swap

logger = structlog.get_logger()

DEFAULT_SILVER_PATH = Path(
    os.environ.get("ANIMETOR_SILVER_PATH", "result/silver.duckdb")
)
DEFAULT_BRONZE_ROOT = Path(
    os.environ.get("ANIMETOR_BRONZE_ROOT", "result/bronze")
)

# SILVER schema (subset; full DDL lives in src/database.py / models_v2.py
# until 06_sqlite_decommission moves it here)
_SILVER_DDL = """
CREATE TABLE IF NOT EXISTS anime (
    id              VARCHAR PRIMARY KEY,
    title_ja        VARCHAR,
    title_en        VARCHAR,
    media_type      VARCHAR,
    episodes        INTEGER,
    duration_min    INTEGER,
    start_year      INTEGER,
    end_year        INTEGER
    -- NO score / popularity / favourites (H1)
);

CREATE TABLE IF NOT EXISTS persons (
    id          VARCHAR PRIMARY KEY,
    name_ja     VARCHAR,
    name_en     VARCHAR
);

CREATE TABLE IF NOT EXISTS credits (
    anime_id        VARCHAR,
    person_id       VARCHAR,
    role            VARCHAR,
    episode         INTEGER,
    credit_year     INTEGER,
    evidence_source VARCHAR NOT NULL,
    PRIMARY KEY (anime_id, person_id, role, COALESCE(episode, -1), evidence_source)
);
-- 他の SILVER 表 (studios, anime_studios, anime_genres, anime_tags, ...) も同様
"""


def integrate(
    bronze_root: Path | str | None = None,
    silver_path: Path | str | None = None,
    *,
    memory_limit: str = "2GB",
) -> None:
    """Build a fresh SILVER duckdb from BRONZE parquet glob, then atomic swap."""
    bronze_root = Path(bronze_root or DEFAULT_BRONZE_ROOT)
    silver_path = Path(silver_path or DEFAULT_SILVER_PATH)

    glob_anime = str(bronze_root / "source=*" / "table=anime" / "date=*" / "*.parquet")
    glob_credits = str(bronze_root / "source=*" / "table=credits" / "date=*" / "*.parquet")
    glob_persons = str(bronze_root / "source=*" / "table=persons" / "date=*" / "*.parquet")

    with atomic_duckdb_swap(silver_path) as new_path:
        conn = duckdb.connect(str(new_path))
        try:
            conn.execute(f"PRAGMA memory_limit='{memory_limit}'")
            conn.execute("PRAGMA temp_directory='/tmp/duckdb_spill'")
            conn.execute(_SILVER_DDL)

            # anime: dedup by (source, id), prefer newest scrape
            conn.execute(f"""
                INSERT INTO anime
                SELECT id, title_ja, title_en, media_type,
                       episodes, duration_min, start_year, end_year
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS rn
                    FROM read_parquet('{glob_anime}', hive_partitioning=true)
                )
                WHERE rn = 1
                ON CONFLICT (id) DO NOTHING
            """)
            logger.info("silver_anime_loaded", count=conn.execute(
                "SELECT COUNT(*) FROM anime").fetchone()[0])

            # persons
            conn.execute(f"""
                INSERT INTO persons
                SELECT id, name_ja, name_en
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (PARTITION BY id ORDER BY date DESC) AS rn
                    FROM read_parquet('{glob_persons}', hive_partitioning=true)
                )
                WHERE rn = 1
                ON CONFLICT (id) DO NOTHING
            """)

            # credits — append all evidence (H4)
            conn.execute(f"""
                INSERT INTO credits
                SELECT DISTINCT anime_id, person_id, role, episode,
                       credit_year, source AS evidence_source
                FROM read_parquet('{glob_credits}', hive_partitioning=true)
                ON CONFLICT DO NOTHING
            """)

            # 他テーブル (studios, anime_studios, anime_genres, anime_tags など)
            # → senior judgment が必要 (元 ETL ロジックを移植)
        finally:
            conn.close()
```

**senior 判断ポイント**:
- どこまでを 1 query で済ませて、どこからは複数 query (entity resolution など) に分けるか
- BRONZE → SILVER の dedup ルール (どの updated_at を採用するか、各ソースで異なる)
- v55 schema の全 SILVER テーブル (`anime_studios`, `anime_genres`, `anime_tags`, `studios`, `roles`, `sources`, `person_aliases`, ...) を全部マッピング

→ **本カードは骨格 (anime/persons/credits) のみ**。残りテーブルは別カード or 本カード内で senior が拡張。

### Step 3: pixi task 追加

`pixi.toml` に追記:

```toml
[tasks]
integrate = "python -m src.etl.integrate_duckdb"
```

### Step 4: テスト

`tests/test_atomic_swap.py`:

```python
import os
from pathlib import Path
import pytest
from src.etl.atomic_swap import atomic_duckdb_swap


def test_swap_replaces_target(tmp_path: Path) -> None:
    target = tmp_path / "db.duckdb"
    target.write_bytes(b"OLD")
    with atomic_duckdb_swap(target) as new_path:
        new_path.write_bytes(b"NEW")
    assert target.read_bytes() == b"NEW"


def test_swap_creates_new_target(tmp_path: Path) -> None:
    target = tmp_path / "db.duckdb"
    assert not target.exists()
    with atomic_duckdb_swap(target) as new_path:
        new_path.write_bytes(b"NEW")
    assert target.read_bytes() == b"NEW"


def test_exception_preserves_old(tmp_path: Path) -> None:
    target = tmp_path / "db.duckdb"
    target.write_bytes(b"OLD")
    with pytest.raises(RuntimeError, match="boom"):
        with atomic_duckdb_swap(target) as new_path:
            new_path.write_bytes(b"PARTIAL")
            raise RuntimeError("boom")
    assert target.read_bytes() == b"OLD"
    assert not (tmp_path / "db.duckdb.new").exists()


def test_no_file_created_raises(tmp_path: Path) -> None:
    target = tmp_path / "db.duckdb"
    with pytest.raises(RuntimeError, match="not created"):
        with atomic_duckdb_swap(target):
            pass  # never write to new_path
```

`tests/test_integrate_duckdb.py`:

```python
import datetime as _dt
from pathlib import Path
import duckdb
import pytest
from src.scrapers.bronze_writer import BronzeWriter
from src.etl.integrate_duckdb import integrate


@pytest.fixture
def bronze_dir(tmp_path: Path) -> Path:
    root = tmp_path / "bronze"
    with BronzeWriter("anilist", table="anime", root=root) as bw:
        bw.append({
            "id": "ani:1", "title_ja": "X", "title_en": None,
            "media_type": "TV", "episodes": 12, "duration_min": 24,
            "start_year": 2024, "end_year": 2024,
        })
    with BronzeWriter("anilist", table="persons", root=root) as bw:
        bw.append({"id": "p:1", "name_ja": "山田太郎", "name_en": None})
    with BronzeWriter("anilist", table="credits", root=root) as bw:
        bw.append({
            "anime_id": "ani:1", "person_id": "p:1", "role": "director",
            "episode": None, "credit_year": 2024, "source": "anilist",
        })
    return root


def test_integrate_creates_silver(bronze_dir: Path, tmp_path: Path) -> None:
    silver = tmp_path / "silver.duckdb"
    integrate(bronze_root=bronze_dir, silver_path=silver)
    assert silver.exists()
    conn = duckdb.connect(str(silver))
    assert conn.execute("SELECT COUNT(*) FROM anime").fetchone()[0] == 1
    assert conn.execute("SELECT COUNT(*) FROM credits").fetchone()[0] == 1
    conn.close()


def test_integrate_atomic_replaces(bronze_dir: Path, tmp_path: Path) -> None:
    silver = tmp_path / "silver.duckdb"
    silver.write_bytes(b"OLD")  # simulate prior file
    integrate(bronze_root=bronze_dir, silver_path=silver)
    # File replaced with valid duckdb (not the old garbage)
    conn = duckdb.connect(str(silver))
    assert conn.execute("SELECT COUNT(*) FROM anime").fetchone()[0] == 1
    conn.close()


def test_integrate_dedup_keeps_latest(tmp_path: Path) -> None:
    root = tmp_path / "bronze"
    # 2 scrapes of same anime, different dates
    with BronzeWriter(
        "anilist", table="anime", root=root, date=_dt.date(2026, 4, 22)
    ) as bw:
        bw.append({"id": "ani:1", "title_ja": "OLD_TITLE", "title_en": None,
                   "media_type": "TV", "episodes": 12, "duration_min": 24,
                   "start_year": 2024, "end_year": 2024})
    with BronzeWriter(
        "anilist", table="anime", root=root, date=_dt.date(2026, 4, 23)
    ) as bw:
        bw.append({"id": "ani:1", "title_ja": "NEW_TITLE", "title_en": None,
                   "media_type": "TV", "episodes": 12, "duration_min": 24,
                   "start_year": 2024, "end_year": 2024})
    silver = tmp_path / "silver.duckdb"
    integrate(bronze_root=root, silver_path=silver)
    conn = duckdb.connect(str(silver))
    title = conn.execute("SELECT title_ja FROM anime WHERE id='ani:1'").fetchone()[0]
    assert title == "NEW_TITLE"
    conn.close()
```

### Step 5: 検証

```bash
pixi run test-scoped tests/test_atomic_swap.py tests/test_integrate_duckdb.py -v
pixi run lint
```

### Step 6: 実 BRONZE で smoke

```bash
pixi run integrate
ls -la result/silver.duckdb result/silver.duckdb.new 2>&1
# 期待: silver.duckdb 存在、.new 不在 (swap 完了)
duckdb result/silver.duckdb "SELECT COUNT(*) FROM anime"
```

---

## Verification

```bash
# 1. 単体テスト
pixi run test-scoped tests/test_atomic_swap.py tests/test_integrate_duckdb.py -v

# 2. atomic swap が機能していることを確認 (analysis 中の swap)
# 別ターミナル A:
#   duckdb result/silver.duckdb "SELECT COUNT(*) FROM anime"  # 開いたまま
# ターミナル B:
#   pixi run integrate
# A で再 query → 古い結果が見える (POSIX inode 仕様)、エラーなし

# 3. memory_limit が効いている (PRAGMA 確認)
duckdb result/silver.duckdb "SELECT current_setting('memory_limit')"

# 4. 全テスト
pixi run test

# 5. lint
pixi run lint

# 6. invariant
rg 'anime\.score\b' src/analysis/ src/pipeline_phases/
```

---

## Stop-if conditions

- [ ] `os.replace()` が EXDEV (cross-device) で失敗 → silver.duckdb と /tmp が別 mount のケース。`atomic_duckdb_swap` の tmp 配置を `target.parent` に固定 (実装は既にそうなっているはず)
- [ ] BRONZE → SILVER のテーブルマッピングで意図しないデータ欠損が出る → senior 確認
- [ ] memory_limit を設定しても OOM → bronze parquet の volume が想定超え。dedup ロジックを見直し
- [ ] 既存の `src/etl/integrate.py` (SQLite 版) が削除されてしまった → `06` まで保持必要

---

## Rollback

```bash
rm src/etl/integrate_duckdb.py src/etl/atomic_swap.py \
   tests/test_integrate_duckdb.py tests/test_atomic_swap.py
git checkout pixi.toml
pixi run test
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] `pixi run integrate` が手元の BRONZE に対して動作
- [ ] atomic swap 動作確認済み (analysis 中の integrate でブロックなし)
- [ ] commit message:
  ```
  Add integrate ETL (parquet → silver.duckdb) with atomic swap

  Reads bronze/**/*.parquet, dedups, writes a new silver.duckdb file,
  then os.replace() into target. Analysis processes holding the old
  inode are not blocked. memory_limit pragma is set explicitly to
  prevent OOM when integrate + analysis run concurrently.
  ```
