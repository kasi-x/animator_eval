# Task: `BronzeSink` パターン化 (asdict + hash 自動付与)

**ID**: `13_scraper_runner_refactor/03_bronze_sink`
**Priority**: 🟠
**Estimated changes**: 約 +120 / -30 lines, 2 files (新規 `sinks.py` + テスト)
**Requires senior judgment**: no
**Blocks**: `04_runner_abstraction`
**Blocked by**: なし (`01` `02` と並行可)

---

## Goal

各 scraper で **bronze writer 操作が手書き重複**:

```python
# allcinema_scraper.py の save_anime_record
anime_row = dataclasses.asdict(rec)
anime_row.pop("staff", None)
anime_row.pop("cast", None)
anime_row["fetched_at"] = datetime.now(timezone.utc).isoformat()
anime_row["content_hash"] = hash_anime_data(anime_row)
anime_bw.append(anime_row)
for credit_entry in all_credits:
    credit_row = dataclasses.asdict(credit_entry)
    credit_row["cinema_id"] = rec.cinema_id
    credits_bw.append(credit_row)
```

これと同型コードが ann_scraper.py の `save_anime_parse_result`、anilist_scraper.py の `save_anime_batch_to_bronze`、keyframe にも存在。

`BronzeSink` クラスで `mapper: Record → {table: [row_dict]}` 形式に統一し、hash + fetched_at 自動付与する。

---

## Hard constraints

- **`content_hash` 計算ロジックを変えない**: `hash_anime_data()` の入力から `fetched_at` を除外する既存挙動を維持 (hash 安定性のため)
- **`fetched_at` フォーマット維持**: `datetime.now(timezone.utc).isoformat()` 形式
- **既存 BRONZE スキーマを変えない**: 出力 row dict の keys は同一
- **`hash_utils.hash_anime_data()` 既存関数を流用**

---

## Pre-conditions

- [ ] `git status` clean
- [ ] `pixi run test-scoped tests/scrapers/` baseline pass
- [ ] `src/scrapers/hash_utils.py` の `hash_anime_data()` API 確認

---

## Files to create / modify

| File | 変更内容 |
|------|---------|
| `src/scrapers/sinks.py` | **新規**: `BronzeSink` クラス |
| `tests/scrapers/test_sinks.py` | **新規**: 単体テスト |

scraper 本体への適用は `04_runner_abstraction` で行う。本 CARD は **Sink クラスとテストの新設のみ**。

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/scrapers/bronze_writer.py` | `BronzeWriter` / `BronzeWriterGroup` は維持 |
| `src/scrapers/hash_utils.py` | `hash_anime_data()` を流用、変更しない |
| 各 scraper の `save_*_record` | `04` で削除/移行 |

---

## Steps

### Step 1: `src/scrapers/sinks.py` 新規作成

```python
"""BronzeSink: dataclass record → BronzeWriterGroup.

Wraps the common pattern:
    1. dataclass.asdict()
    2. attach fetched_at (UTC ISO) + content_hash (on the primary table)
    3. append to BronzeWriter for the table

Usage:
    from src.scrapers.bronze_writer import BronzeWriterGroup
    from src.scrapers.sinks import BronzeSink

    def map_anime(rec: AllcinemaAnimeRecord) -> dict[str, list[dict]]:
        anime_row = dataclasses.asdict(rec)
        credits = anime_row.pop("staff", []) + anime_row.pop("cast", [])
        return {
            "anime": [anime_row],
            "credits": [{**c, "cinema_id": rec.cinema_id} for c in credits],
        }

    with BronzeWriterGroup("allcinema", tables=["anime", "credits"]) as g:
        sink = BronzeSink(g, mapper=map_anime, hash_table="anime")
        for rec in records:
            n_rows = sink(rec)  # writes all tables, returns total row count
"""
from __future__ import annotations

import dataclasses
from datetime import datetime, timezone
from typing import Any, Callable, Generic, TypeVar

from src.scrapers.bronze_writer import BronzeWriterGroup
from src.scrapers.hash_utils import hash_anime_data

Rec = TypeVar("Rec")

# mapper: Record → {table_name: [row_dict, ...]}
Mapper = Callable[[Rec], dict[str, list[dict[str, Any]]]]


class BronzeSink(Generic[Rec]):
    """Writes one Record across multiple BRONZE tables.

    On each __call__:
      - mapper(rec) yields {table: [rows]}
      - if hash_table is set, the first row of that table gets fetched_at +
        content_hash injected (content_hash computed BEFORE fetched_at is added,
        so hash is stable across re-runs)
      - all rows appended to the corresponding BronzeWriter
      - returns total row count written
    """

    def __init__(
        self,
        group: BronzeWriterGroup,
        *,
        mapper: Mapper[Rec],
        hash_table: str | None = "anime",
    ) -> None:
        self._group = group
        self._mapper = mapper
        self._hash_table = hash_table

    def __call__(self, rec: Rec) -> int:
        tables = self._mapper(rec)
        total = 0
        for table_name, rows in tables.items():
            if not rows:
                continue
            writer = self._group[table_name]
            if table_name == self._hash_table and rows:
                # hash 計算は fetched_at 抜きで安定させる
                row = rows[0]
                row["content_hash"] = hash_anime_data(row)
                row["fetched_at"] = datetime.now(timezone.utc).isoformat()
            for row in rows:
                writer.append(row)
                total += 1
        return total


def asdict_record(rec: Any, *, drop: tuple[str, ...] = ()) -> dict[str, Any]:
    """Convenience: dataclasses.asdict() with field removal.

    Use in mapper functions to extract the primary row from a dataclass that
    embeds nested lists (e.g. AllcinemaAnimeRecord with .staff / .cast).
    """
    if not dataclasses.is_dataclass(rec):
        raise TypeError(f"asdict_record: not a dataclass: {type(rec).__name__}")
    d = dataclasses.asdict(rec)
    for key in drop:
        d.pop(key, None)
    return d
```

### Step 2: `tests/scrapers/test_sinks.py` 新規作成

```python
"""BronzeSink tests."""
from __future__ import annotations

import dataclasses

import pytest

from src.scrapers.bronze_writer import BronzeWriterGroup
from src.scrapers.sinks import BronzeSink, asdict_record


@dataclasses.dataclass
class FakeAnime:
    cinema_id: int
    title_ja: str
    extras: list[dict] = dataclasses.field(default_factory=list)


def test_sink_writes_primary_and_secondary_tables(tmp_path):
    def mapper(rec: FakeAnime) -> dict[str, list[dict]]:
        primary = asdict_record(rec, drop=("extras",))
        secondary = [{**e, "cinema_id": rec.cinema_id} for e in rec.extras]
        return {"anime": [primary], "credits": secondary}

    with BronzeWriterGroup("allcinema", tables=["anime", "credits"], root=tmp_path) as g:
        sink = BronzeSink(g, mapper=mapper, hash_table="anime")
        rec = FakeAnime(cinema_id=42, title_ja="テスト", extras=[{"name": "X"}, {"name": "Y"}])
        n = sink(rec)
        assert n == 3  # 1 anime + 2 credits


def test_sink_injects_hash_and_fetched_at(tmp_path):
    captured: dict[str, list[dict]] = {}

    def mapper(rec: FakeAnime) -> dict[str, list[dict]]:
        d = asdict_record(rec, drop=("extras",))
        captured["row"] = [d]
        return {"anime": [d]}

    with BronzeWriterGroup("allcinema", tables=["anime"], root=tmp_path) as g:
        sink = BronzeSink(g, mapper=mapper, hash_table="anime")
        sink(FakeAnime(cinema_id=1, title_ja="A"))

    row = captured["row"][0]
    assert "fetched_at" in row
    assert "content_hash" in row
    # fetched_at が hash に含まれていない (hash 安定性)
    # → 別の rec で同じ payload を投げると hash が一致する想定だが、本テストは存在確認のみ


def test_sink_no_hash_when_disabled(tmp_path):
    def mapper(rec: FakeAnime) -> dict[str, list[dict]]:
        return {"anime": [asdict_record(rec, drop=("extras",))]}

    with BronzeWriterGroup("allcinema", tables=["anime"], root=tmp_path) as g:
        sink = BronzeSink(g, mapper=mapper, hash_table=None)
        sink(FakeAnime(cinema_id=1, title_ja="A"))

    # 確認: hash/fetched_at 注入なし (parquet を読み戻して assert)
    import pyarrow.dataset as ds
    paths = list((tmp_path / "source=allcinema" / "table=anime").rglob("*.parquet"))
    assert paths
    tbl = ds.dataset(paths[0]).to_table()
    cols = tbl.column_names
    assert "content_hash" not in cols
    assert "fetched_at" not in cols
```

### Step 3: hash 安定性の確認

`hash_anime_data()` が `content_hash` / `fetched_at` を入力から除外しているか再確認:

```bash
grep -A 20 "def hash_anime_data" src/scrapers/hash_utils.py
```

除外していなければ、本 CARD で `BronzeSink` 側で対応 (hash 計算前に `content_hash` / `fetched_at` を pop)。既存挙動は anilist_scraper.py / allcinema_scraper.py の `save_anime_*` を参照。

---

## Verification

```bash
# 1. import OK
pixi run python -c "
from src.scrapers.sinks import BronzeSink, asdict_record
print('OK')
"

# 2. 新規テスト pass
pixi run test-scoped tests/scrapers/test_sinks.py

# 3. bronze_writer 既存テスト
pixi run test-scoped -k bronze_writer

# 4. Lint
pixi run lint
```

---

## Stop-if conditions

- 新規テスト fail
- bronze_writer 既存テスト fail
- `git diff --stat` が +250/-50 を超える

---

## Rollback

```bash
rm -f src/scrapers/sinks.py tests/scrapers/test_sinks.py
pixi run test-scoped tests/scrapers/
```

---

## Completion signal

- [ ] `src/scrapers/sinks.py` 存在、`BronzeSink` + `asdict_record` 定義
- [ ] `tests/scrapers/test_sinks.py` pass (3+ tests)
- [ ] git log message: `feat(scraper): add BronzeSink for asdict+hash automation (13_scraper_runner_refactor/03)`
