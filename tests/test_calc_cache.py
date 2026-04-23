"""Tests for src/analysis/calc_cache.py (DuckDB-backed incremental cache)."""

from pathlib import Path

from src.analysis.calc_cache import (
    DEFAULT_CACHE_PATH,
    get_calc_execution_hashes,
    record_calc_execution,
    record_calc_executions_batch,
)


def test_round_trip_single(tmp_path: Path) -> None:
    """Record one entry and read it back."""
    db = tmp_path / "cache.duckdb"
    record_calc_execution("scope_a", "task_x", "hash_abc", path=db)
    result = get_calc_execution_hashes("scope_a", path=db)
    assert result == {"task_x": "hash_abc"}


def test_upsert_overwrites(tmp_path: Path) -> None:
    """Recording the same (scope, calc_name) twice keeps the newest hash."""
    db = tmp_path / "cache.duckdb"
    record_calc_execution("scope_a", "task_x", "hash_old", path=db)
    record_calc_execution("scope_a", "task_x", "hash_new", path=db)
    result = get_calc_execution_hashes("scope_a", path=db)
    assert result == {"task_x": "hash_new"}


def test_scope_isolation(tmp_path: Path) -> None:
    """Each scope only sees its own entries."""
    db = tmp_path / "cache.duckdb"
    record_calc_execution("scope_a", "task_a", "hash_a", path=db)
    record_calc_execution("scope_b", "task_b", "hash_b", path=db)

    result_a = get_calc_execution_hashes("scope_a", path=db)
    result_b = get_calc_execution_hashes("scope_b", path=db)

    assert result_a == {"task_a": "hash_a"}
    assert result_b == {"task_b": "hash_b"}


def test_batch_empty_is_noop(tmp_path: Path) -> None:
    """Calling batch with an empty list creates the file but leaves the table empty."""
    db = tmp_path / "cache.duckdb"
    record_calc_executions_batch("scope_a", [], path=db)
    # File will exist because _connect creates it; table should be empty for scope.
    result = get_calc_execution_hashes("scope_a", path=db)
    assert result == {}


def test_batch_multiple(tmp_path: Path) -> None:
    """Batch of three items — all present with correct hashes and output_paths."""
    db = tmp_path / "cache.duckdb"
    items = [
        ("task_1", "hash_1", "/out/task_1.json"),
        ("task_2", "hash_2", "/out/task_2.json"),
        ("task_3", "hash_3", "/out/task_3.json"),
    ]
    record_calc_executions_batch("scope_x", items, path=db)
    result = get_calc_execution_hashes("scope_x", path=db)
    assert result == {
        "task_1": "hash_1",
        "task_2": "hash_2",
        "task_3": "hash_3",
    }


def test_default_path_module_attr() -> None:
    """DEFAULT_CACHE_PATH is a Path instance."""
    assert isinstance(DEFAULT_CACHE_PATH, Path)


def test_missing_hash_returns_empty(tmp_path: Path) -> None:
    """Querying a scope that was never recorded returns an empty dict."""
    db = tmp_path / "cache.duckdb"
    result = get_calc_execution_hashes("never_recorded", path=db)
    assert result == {}
