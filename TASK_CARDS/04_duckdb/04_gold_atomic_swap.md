# Task: gold.duckdb の atomic swap 化 (pipeline 出力)

**ID**: `04_duckdb/04_gold_atomic_swap`
**Priority**: 🟠 Major
**Estimated changes**: 約 +50 / -30 lines, 2 files
**Requires senior judgment**: no
**Blocks**: `04_duckdb/05_analysis_cutover` (gold reader 切替の前提)
**Blocked by**: `04_duckdb/03_integrate_etl` (`atomic_swap` モジュールが必要)

---

## Goal

`src/analysis/gold_writer.py` の `GoldWriter` を atomic swap 経由に書き換える。pipeline 走行中に API/report が gold.duckdb を読んでもブロックされない構成にする。

---

## Hard constraints

- H1 anime.score を gold に流さない (現状の DDL は OK、追加変更しない)
- H5 既存テスト green 維持
- H8 行番号を信じない

**本タスク固有**:
- **memory_limit を必ず明示**: pipeline はメモリを大量消費する。`PRAGMA memory_limit='4GB'` を connection 開設直後に
- **既存の `GoldReader` は変更しない** (atomic swap は writer 側の話、reader は per-query open/close で OK)
- **増分書き込み**ではなく **fresh build + swap** モデル: pipeline は GOLD を 1 run = 1 ファイル全置換で再構築する。これにより部分整合性問題を排除

---

## Pre-conditions

- [ ] `04_duckdb/03_integrate_etl` 完了 (`src/etl/atomic_swap.py` 利用可能)
- [ ] `pixi run test` pass
- [ ] `git status` clean

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/analysis/gold_writer.py` | `GoldWriter.__init__/__enter__/__exit__` を atomic swap で再実装 |
| `tests/test_gold_writer.py` | atomic swap 動作テスト追加 |

---

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/analysis/gold_writer.py` の `GoldReader` クラス | reader 側は per-query open。変更不要 |
| `src/etl/atomic_swap.py` | `03` で確定 |
| `src/pipeline_phases/export_and_viz.py` | 呼び出し側が変更不要なよう writer の API を保つ |

---

## Steps

### Step 1: `GoldWriter` を atomic swap 化

**Before** (`src/analysis/gold_writer.py:79-99`):

```python
class GoldWriter:
    def __init__(self, db_path: Path | str | None = None) -> None:
        self._path = db_path
        self._conn: duckdb.DuckDBPyConnection | None = None

    def __enter__(self) -> "GoldWriter":
        self._conn = _open(self._path)
        self._conn.execute(_DDL)
        return self

    def __exit__(self, *_: object) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None
```

**After**:

```python
from src.etl.atomic_swap import atomic_duckdb_swap


class GoldWriter:
    """Atomic-swap writer for gold.duckdb.

    Builds a fresh DB file at gold.duckdb.new, then os.replace() into
    target on context exit. Readers holding the old inode are not blocked.

    Pipeline writes once per run, so fresh-build (vs incremental) is fine.
    """

    def __init__(
        self,
        db_path: Path | str | None = None,
        *,
        memory_limit: str = "4GB",
    ) -> None:
        self._path = Path(db_path or DEFAULT_GOLD_DB_PATH)
        self._memory_limit = memory_limit
        self._conn: duckdb.DuckDBPyConnection | None = None
        self._swap_ctx = None
        self._tmp_path: Path | None = None

    def __enter__(self) -> "GoldWriter":
        self._swap_ctx = atomic_duckdb_swap(self._path)
        self._tmp_path = self._swap_ctx.__enter__()
        self._tmp_path.parent.mkdir(parents=True, exist_ok=True)
        self._conn = duckdb.connect(str(self._tmp_path))
        self._conn.execute(f"PRAGMA memory_limit='{self._memory_limit}'")
        self._conn.execute("PRAGMA temp_directory='/tmp/duckdb_spill'")
        self._conn.execute(_DDL)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        try:
            if self._conn:
                self._conn.close()
                self._conn = None
        finally:
            if self._swap_ctx is not None:
                self._swap_ctx.__exit__(exc_type, exc_val, exc_tb)
                self._swap_ctx = None
                self._tmp_path = None
```

**API は不変**: `write_person_scores()` / `write_score_history()` の署名と挙動は変えない。pipeline 側は何も変更しなくて良い。

### Step 2: テスト追加

`tests/test_gold_writer.py` に追記:

```python
def test_gold_writer_atomic_swap_replaces(tmp_path: Path, monkeypatch) -> None:
    """Existing gold.duckdb is replaced atomically."""
    target = tmp_path / "gold.duckdb"
    target.write_bytes(b"OLD")
    with GoldWriter(target) as gw:
        gw.write_person_scores([
            ("p:1", 1.0, 0.5, 0.8, 0.3, 0.0, 0.7, 2.0)
        ])
    assert target.exists()
    # Not the old garbage — must be a valid duckdb
    conn = duckdb.connect(str(target))
    rows = conn.execute("SELECT * FROM person_scores").fetchall()
    conn.close()
    assert len(rows) == 1


def test_gold_writer_exception_preserves_old(tmp_path: Path) -> None:
    target = tmp_path / "gold.duckdb"
    # Build initial valid gold
    with GoldWriter(target) as gw:
        gw.write_person_scores([
            ("p:1", 1.0, 0.5, 0.8, 0.3, 0.0, 0.7, 2.0)
        ])
    # Attempt a write that crashes mid-flight
    with pytest.raises(RuntimeError, match="boom"):
        with GoldWriter(target) as gw:
            gw.write_person_scores([
                ("p:2", 2.0, 0.5, 0.8, 0.3, 0.0, 0.7, 3.0)
            ])
            raise RuntimeError("boom")
    # Old file preserved
    conn = duckdb.connect(str(target))
    ids = [r[0] for r in conn.execute(
        "SELECT person_id FROM person_scores"
    ).fetchall()]
    conn.close()
    assert ids == ["p:1"]
    assert not target.with_suffix(".duckdb.new").exists()


def test_gold_writer_memory_limit_set(tmp_path: Path) -> None:
    target = tmp_path / "gold.duckdb"
    with GoldWriter(target, memory_limit="1GB") as gw:
        # Inspect via attached connection
        limit = gw._conn.execute(
            "SELECT current_setting('memory_limit')"
        ).fetchone()[0]
    # DuckDB normalizes "1GB" → "1.0 GB" or similar; just check non-empty
    assert limit and "GB" in limit.upper().replace("IB", "B")
```

### Step 3: pipeline 側で確認

`src/pipeline_phases/export_and_viz.py` で `GoldWriter` を context manager で使っているはず。**変更不要** だが念のため:

```bash
grep -n 'GoldWriter' src/pipeline_phases/export_and_viz.py
# 期待: with GoldWriter() as gw: のパターンになっている
```

---

## Verification

```bash
# 1. 単体テスト
pixi run test-scoped tests/test_gold_writer.py -v

# 2. atomic swap が機能していることを確認
# 別ターミナル A:
#   python -c "from src.analysis.gold_writer import GoldReader; \
#              import time; r = GoldReader(); \
#              while True: print(len(r.person_scores())); time.sleep(1)"
# ターミナル B:
#   pixi run pipeline    # gold.duckdb を再生成
# A は連続実行してエラーなし、件数が swap 後に新値に切り替わる

# 3. pipeline で実 GOLD 書き込み
pixi run pipeline-inc
ls -la result/gold.duckdb result/gold.duckdb.new 2>&1
# 期待: gold.duckdb 存在、.new 不在

# 4. 全テスト
pixi run test

# 5. lint
pixi run lint
```

---

## Stop-if conditions

- [ ] `GoldWriter` の API 互換性を壊した (pipeline 側で test 失敗)
- [ ] memory_limit を設定しても pipeline が OOM → 大規模データで実測。limit を上げる or spill を確認
- [ ] swap 中に reader が openエラーを起こす (POSIX 仕様外の挙動) → ファイル system を確認 (NFS / FUSE は不可)

---

## Rollback

```bash
git checkout src/analysis/gold_writer.py tests/test_gold_writer.py
pixi run test-scoped tests/test_gold_writer.py
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] pipeline 実行中に GoldReader が連続呼び出しできる (block されない)
- [ ] commit message:
  ```
  Make GoldWriter use atomic swap (no reader block during pipeline)

  GoldWriter now builds gold.duckdb.new and os.replace() into target
  on context exit. memory_limit pragma is set explicitly. Public API
  (write_person_scores / write_score_history) unchanged — callers do
  not need updates.
  ```
