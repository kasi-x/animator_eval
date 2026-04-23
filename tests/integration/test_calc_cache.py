"""Tests for src/analysis/calc_cache.py (DuckDB-backed incremental cache)."""

from pathlib import Path

from src.analysis.calc_cache import (
    DEFAULT_CACHE_PATH,
    get_all_llm_decisions_bulk,
    get_calc_execution_hashes,
    get_llm_decision,
    record_calc_execution,
    record_calc_executions_batch,
    upsert_llm_decision,
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


# --- LLM decision cache ---


def test_llm_upsert_and_read(tmp_path: Path) -> None:
    """Record one LLM decision and read it back."""
    p = tmp_path / "cache.duckdb"
    upsert_llm_decision("田中", "org_classification", {"type": "person"}, path=p)
    result = get_llm_decision("田中", "org_classification", path=p)
    assert result == {"type": "person"}


def test_llm_upsert_overwrites(tmp_path: Path) -> None:
    """Recording the same (name, task) twice keeps the newest result."""
    p = tmp_path / "cache.duckdb"
    upsert_llm_decision("A", "t", {"v": 1}, path=p)
    upsert_llm_decision("A", "t", {"v": 2}, path=p)
    assert get_llm_decision("A", "t", path=p) == {"v": 2}


def test_llm_bulk_fetch(tmp_path: Path) -> None:
    """Fetch all decisions for a given task."""
    p = tmp_path / "cache.duckdb"
    upsert_llm_decision("X", "org_classification", {"type": "org"}, path=p)
    upsert_llm_decision("Y", "org_classification", {"type": "person"}, path=p)
    upsert_llm_decision("Z", "name_normalization", {"names": ["Z"]}, path=p)
    bulk = get_all_llm_decisions_bulk("org_classification", path=p)
    assert set(bulk.keys()) == {"X", "Y"}
    assert bulk["X"] == {"type": "org"}


def test_llm_missing_returns_none(tmp_path: Path) -> None:
    """get_llm_decision returns None for missing entry."""
    p = tmp_path / "cache.duckdb"
    assert get_llm_decision("nobody", "unknown_task", path=p) is None
