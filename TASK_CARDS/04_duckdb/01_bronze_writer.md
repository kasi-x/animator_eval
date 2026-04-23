# Task: BronzeWriter (parquet 出力ヘルパ) 新設

**ID**: `04_duckdb/01_bronze_writer`
**Priority**: 🟠 Major
**Estimated changes**: 約 +120 / -0 lines, 2 files (1 src + 1 test)
**Requires senior judgment**: no
**Blocks**: `04_duckdb/02_scraper_migration`
**Blocked by**: `01_schema_fix/` 全完了

---

## Goal

`src/scrapers/bronze_writer.py` を新設し、scraper が **append-only Parquet ファイル** に行を書けるヘルパクラス `BronzeWriter` を提供する。本カードでは **新ファイル追加と単体テストのみ**。scraper の置き換えは次カード (`02`) で行う。

---

## Hard constraints

(`_hard_constraints.md` を事前に読むこと)

- H1 anime.score を scoring に使わない (BRONZE は score 保持して OK、SILVER に流さない設計)
- H5 既存テスト green 維持
- H8 行番号を信じない

**本タスク固有**:
- **既存 scraper を一切変更しない** (本カードは新ファイル追加のみ、影響範囲ゼロ)
- Parquet 出力先は `bronze/source={src}/date={YYYYMMDD}/` の hive partition 形式に固定 (DuckDB の `read_parquet` の自動 partition 認識に合わせる)
- `pyarrow` を依存追加する (`pixi add pyarrow`)

---

## Pre-conditions

- [ ] `01_schema_fix/` 全完了
- [ ] `pixi run test` pass (baseline)
- [ ] `git status` clean

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `pixi.toml` | `pyarrow` を `[dependencies]` に追加 |
| `src/scrapers/bronze_writer.py` | **新規作成** (~80 行) |
| `tests/test_bronze_writer.py` | **新規作成** (~80 行) |

---

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/scrapers/*.py` (既存 scraper) | 本カードは新ファイル追加のみ。置換は `02` |
| `src/database.py` | DuckDB 切替は `03`/`05` で扱う |
| `src/etl/integrate.py` | parquet 読み取り側は `03` で扱う |

---

## Steps

### Step 1: pyarrow 依存追加

```bash
pixi add pyarrow
pixi run python -c "import pyarrow; print(pyarrow.__version__)"
```

### Step 2: `BronzeWriter` 実装

`src/scrapers/bronze_writer.py` を新規作成:

```python
"""Append-only Parquet writer for BRONZE layer (scraper output).

Each scraper instantiates BronzeWriter(source) and calls .append(row)
during scraping. .flush() (or context manager exit) writes a single
parquet file under bronze/source={src}/date={YYYYMMDD}/{uuid}.parquet.

Files are immutable once written. Re-running a scraper produces a new
file under the same partition. integrate ETL (04_duckdb/03) reads the
glob and dedups in SILVER.
"""
from __future__ import annotations

import datetime as _dt
import os
import uuid
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import structlog

logger = structlog.get_logger()

DEFAULT_BRONZE_ROOT: Path = Path(
    os.environ.get(
        "ANIMETOR_BRONZE_ROOT",
        str(Path(__file__).resolve().parent.parent.parent / "result" / "bronze"),
    )
)

ALLOWED_SOURCES = {
    "anilist", "ann", "allcinema", "seesaawiki", "keyframe", "mal", "mediaarts", "jvmg",
}


class BronzeWriter:
    """Append-only parquet writer scoped to one (source, date) partition.

    Usage:
        with BronzeWriter("anilist", table="anime") as bw:
            for row in scraped_rows:
                bw.append(row)
        # parquet file flushed on exit

    Multiple scrapers can run in parallel — each writes its own file
    under its own partition, no contention.
    """

    def __init__(
        self,
        source: str,
        *,
        table: str,
        root: Path | str | None = None,
        date: _dt.date | None = None,
    ) -> None:
        if source not in ALLOWED_SOURCES:
            raise ValueError(f"Unknown source: {source!r} (allowed: {ALLOWED_SOURCES})")
        self.source = source
        self.table = table
        self._root = Path(root or DEFAULT_BRONZE_ROOT)
        self._date = date or _dt.date.today()
        self._buffer: list[dict[str, Any]] = []
        self._partition: Path = (
            self._root
            / f"source={source}"
            / f"table={table}"
            / f"date={self._date.isoformat()}"
        )

    def __enter__(self) -> "BronzeWriter":
        return self

    def __exit__(self, exc_type, *_: object) -> None:
        if exc_type is None:
            self.flush()
        # On exception, drop buffer — caller will retry the whole scrape

    def append(self, row: dict[str, Any]) -> None:
        self._buffer.append(row)

    def extend(self, rows: list[dict[str, Any]]) -> None:
        self._buffer.extend(rows)

    def flush(self) -> Path | None:
        """Write buffered rows to a new parquet file. No-op if buffer empty."""
        if not self._buffer:
            return None
        self._partition.mkdir(parents=True, exist_ok=True)
        path = self._partition / f"{uuid.uuid4().hex}.parquet"
        table = pa.Table.from_pylist(self._buffer)
        pq.write_table(table, path, compression="zstd")
        logger.info(
            "bronze_parquet_written",
            source=self.source,
            table=self.table,
            rows=len(self._buffer),
            path=str(path),
        )
        self._buffer.clear()
        return path
```

### Step 3: テスト追加

`tests/test_bronze_writer.py` を新規作成:

```python
"""Tests for BronzeWriter (parquet append-only writer)."""
from __future__ import annotations

import datetime as _dt
from pathlib import Path

import pyarrow.parquet as pq
import pytest

from src.scrapers.bronze_writer import ALLOWED_SOURCES, BronzeWriter


def test_unknown_source_rejected(tmp_path: Path) -> None:
    with pytest.raises(ValueError, match="Unknown source"):
        BronzeWriter("nonexistent", table="anime", root=tmp_path)


def test_partition_layout(tmp_path: Path) -> None:
    bw = BronzeWriter(
        "anilist", table="anime", root=tmp_path, date=_dt.date(2026, 4, 23)
    )
    bw.append({"id": "ani:1", "title": "X", "score": 8.5})
    out = bw.flush()
    assert out is not None
    assert out.parent == (
        tmp_path / "source=anilist" / "table=anime" / "date=2026-04-23"
    )
    assert out.suffix == ".parquet"


def test_roundtrip_single_file(tmp_path: Path) -> None:
    rows = [{"id": f"ani:{i}", "title": f"T{i}", "score": float(i)} for i in range(5)]
    with BronzeWriter("anilist", table="anime", root=tmp_path) as bw:
        bw.extend(rows)
    files = list(tmp_path.rglob("*.parquet"))
    assert len(files) == 1
    table = pq.read_table(files[0])
    assert table.num_rows == 5
    assert set(table.column_names) == {"id", "title", "score"}


def test_empty_buffer_flush_is_noop(tmp_path: Path) -> None:
    bw = BronzeWriter("ann", table="credits", root=tmp_path)
    assert bw.flush() is None
    assert list(tmp_path.rglob("*.parquet")) == []


def test_multiple_flushes_produce_multiple_files(tmp_path: Path) -> None:
    bw = BronzeWriter("mal", table="anime", root=tmp_path)
    bw.append({"id": "mal:1"})
    bw.flush()
    bw.append({"id": "mal:2"})
    bw.flush()
    files = list(tmp_path.rglob("*.parquet"))
    assert len(files) == 2


def test_exception_drops_buffer(tmp_path: Path) -> None:
    """Exception inside context must NOT flush partial buffer."""
    with pytest.raises(RuntimeError):
        with BronzeWriter("anilist", table="anime", root=tmp_path) as bw:
            bw.append({"id": "ani:1"})
            raise RuntimeError("scraper crashed")
    assert list(tmp_path.rglob("*.parquet")) == []


def test_all_allowed_sources(tmp_path: Path) -> None:
    for src in ALLOWED_SOURCES:
        bw = BronzeWriter(src, table="anime", root=tmp_path)
        bw.append({"id": f"{src}:1"})
        bw.flush()
    assert len(list(tmp_path.rglob("*.parquet"))) == len(ALLOWED_SOURCES)
```

### Step 4: 検証

```bash
pixi run test-scoped tests/test_bronze_writer.py -v
pixi run lint
```

---

## Verification

```bash
# 1. 新ファイルが存在し import 可能
pixi run python -c "from src.scrapers.bronze_writer import BronzeWriter, ALLOWED_SOURCES; print(sorted(ALLOWED_SOURCES))"

# 2. 単体テスト pass
pixi run test-scoped tests/test_bronze_writer.py -v
# 期待: 7 passed

# 3. lint clean
pixi run lint

# 4. 既存 scraper に影響がないこと
pixi run test-scoped tests/ -k "scraper" -v
# 期待: baseline と同じ pass 数

# 5. invariant
rg 'anime\.score\b' src/analysis/ src/pipeline_phases/   # 0 件
rg 'display_lookup' src/analysis/ src/pipeline_phases/   # 0 件
```

**T4** (commit 直前): `pixi run test`

---

## Stop-if conditions

- [ ] `pyarrow` install 失敗 → ユーザに報告
- [ ] `pixi run test-scoped tests/test_bronze_writer.py` 失敗
- [ ] `pixi run test -k scraper` で baseline からの regression
- [ ] `git diff --stat` が想定 (3 ファイル, ~+200/-0 行) を大きく超える

---

## Rollback

```bash
rm src/scrapers/bronze_writer.py tests/test_bronze_writer.py
git checkout pixi.toml
pixi install
pixi run test
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] `git diff --stat`: 3 files, +200 / -0 程度
- [ ] commit message:
  ```
  Add BronzeWriter (append-only parquet helper for scrapers)

  New module with no caller yet — scraper migration follows in 04_duckdb/02.
  Per-source partition (source=X/table=Y/date=Z) matches DuckDB hive layout.
  ```
