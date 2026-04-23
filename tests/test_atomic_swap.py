"""Tests for src/etl/atomic_swap.py."""
from pathlib import Path

import pytest

from src.etl.atomic_swap import atomic_duckdb_swap


def test_swap_replaces_existing_target(tmp_path: Path) -> None:
    target = tmp_path / "db.duckdb"
    target.write_bytes(b"OLD")
    with atomic_duckdb_swap(target) as new_path:
        new_path.write_bytes(b"NEW")
    assert target.read_bytes() == b"NEW"
    assert not (tmp_path / "db.duckdb.new").exists()


def test_swap_creates_new_target(tmp_path: Path) -> None:
    target = tmp_path / "db.duckdb"
    assert not target.exists()
    with atomic_duckdb_swap(target) as new_path:
        new_path.write_bytes(b"NEW")
    assert target.read_bytes() == b"NEW"


def test_exception_preserves_old_target(tmp_path: Path) -> None:
    target = tmp_path / "db.duckdb"
    target.write_bytes(b"OLD")
    with pytest.raises(RuntimeError, match="boom"):
        with atomic_duckdb_swap(target) as new_path:
            new_path.write_bytes(b"PARTIAL")
            raise RuntimeError("boom")
    assert target.read_bytes() == b"OLD"
    assert not (tmp_path / "db.duckdb.new").exists()


def test_exception_no_prior_target(tmp_path: Path) -> None:
    target = tmp_path / "db.duckdb"
    with pytest.raises(ValueError):
        with atomic_duckdb_swap(target) as _:
            raise ValueError("oops")
    assert not target.exists()
    assert not (tmp_path / "db.duckdb.new").exists()


def test_no_file_created_raises(tmp_path: Path) -> None:
    target = tmp_path / "db.duckdb"
    with pytest.raises(RuntimeError, match="not created"):
        with atomic_duckdb_swap(target):
            pass  # intentionally do not write new_path


def test_stale_tmp_cleaned_up(tmp_path: Path) -> None:
    """Leftover .new file from a prior crash is removed at entry."""
    target = tmp_path / "db.duckdb"
    stale = tmp_path / "db.duckdb.new"
    stale.write_bytes(b"STALE")
    with atomic_duckdb_swap(target) as new_path:
        new_path.write_bytes(b"FRESH")
    assert target.read_bytes() == b"FRESH"


def test_creates_parent_dirs(tmp_path: Path) -> None:
    target = tmp_path / "subdir" / "nested" / "db.duckdb"
    with atomic_duckdb_swap(target) as new_path:
        new_path.write_bytes(b"DATA")
    assert target.exists()
    assert target.read_bytes() == b"DATA"
