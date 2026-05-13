"""Tests for scripts/publication/snapshot.py and write_score_frozen.

Covers:
- meta_score_frozen DDL creation
- write_score_frozen idempotence and SHA-256 correctness
- build_snapshot staging directory layout
- build_snapshot tarball contents
- MANIFEST.json structure
- README.txt disclaimer presence
- dry_run flag (no DB write)
"""

from __future__ import annotations

import hashlib
import json
import tarfile
from pathlib import Path

import duckdb


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _sha256(s: str) -> str:
    return hashlib.sha256(s.encode()).hexdigest()


def _make_gold_db(path: Path) -> None:
    """Create a minimal gold.duckdb with person_scores rows."""
    from src.analysis.io.mart_writer import GoldWriter

    rows = [
        ("p1", 0.8, 0.5, 0.9, 0.3, 0.95, 0.6, 0.85),
        ("p2", 0.4, 0.3, 0.5, 0.2, 0.80, 0.4, 0.42),
    ]
    with GoldWriter(path) as gw:
        gw.write_person_scores(rows)


def _make_resolved_db(path: Path) -> None:
    """Create a minimal resolved.duckdb (empty schema)."""
    conn = duckdb.connect(str(path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS resolved")
    conn.close()


# ---------------------------------------------------------------------------
# Tests: DDL — meta_score_frozen table
# ---------------------------------------------------------------------------


class TestMetaScoreFrozenDDL:
    def test_table_created_by_ddl(self, tmp_path):
        """DDL string creates meta_score_frozen in mart schema."""
        from src.analysis.io.mart_writer import _DDL, gold_connect_write

        gold = tmp_path / "gold.duckdb"
        with gold_connect_write(gold) as conn:
            conn.execute(_DDL)
            result = conn.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema='mart' AND table_name='meta_score_frozen'"
            ).fetchone()
        assert result is not None, "meta_score_frozen table not created"

    def test_table_columns_present(self, tmp_path):
        """meta_score_frozen has all expected columns."""
        from src.analysis.io.mart_writer import _DDL, gold_connect_write

        gold = tmp_path / "gold.duckdb"
        with gold_connect_write(gold) as conn:
            conn.execute(_DDL)
            cols = {
                row[0]
                for row in conn.execute(
                    "SELECT column_name FROM information_schema.columns "
                    "WHERE table_schema='mart' AND table_name='meta_score_frozen'"
                ).fetchall()
            }
        required = {
            "snapshot_id", "paper_anchor", "venue", "spec_hash", "score_hash",
            "lambda_json", "pipeline_version", "git_sha", "pixi_lock_hash",
            "resolved_db_hash", "score_rows_json", "frozen_at", "notes",
        }
        assert required.issubset(cols), f"Missing columns: {required - cols}"


# ---------------------------------------------------------------------------
# Tests: write_score_frozen
# ---------------------------------------------------------------------------


class TestWriteScoreFrozen:
    def test_returns_score_hash(self, tmp_path):
        """write_score_frozen returns SHA-256 of serialised score rows."""
        from src.analysis.io.mart_writer import write_score_frozen

        gold = tmp_path / "gold.duckdb"
        score_rows = [{"person_id": "p1", "iv_score": 0.85}]
        returned_hash = write_score_frozen(
            snapshot_id="test_20260101",
            paper_anchor="test_paper",
            venue="JASSS",
            spec_hash="deadbeef" * 8,
            score_rows=score_rows,
            gold_path=gold,
        )
        expected = hashlib.sha256(
            json.dumps(score_rows, sort_keys=True).encode()
        ).hexdigest()
        assert returned_hash == expected

    def test_row_written_to_db(self, tmp_path):
        """A snapshot row is readable after write_score_frozen."""
        from src.analysis.io.mart_writer import write_score_frozen, gold_connect

        gold = tmp_path / "gold.duckdb"
        score_rows = [{"person_id": "p1", "iv_score": 0.85}]
        write_score_frozen(
            snapshot_id="snap_001",
            paper_anchor="paper_a",
            venue="JASSS",
            spec_hash="abc" * 21 + "d",
            score_rows=score_rows,
            gold_path=gold,
        )
        with gold_connect(gold) as conn:
            row = conn.execute(
                "SELECT snapshot_id, venue, paper_anchor FROM mart.meta_score_frozen "
                "WHERE snapshot_id='snap_001'"
            ).fetchone()
        assert row is not None
        assert row[0] == "snap_001"
        assert row[1] == "JASSS"

    def test_idempotent_same_content(self, tmp_path):
        """Calling write_score_frozen twice with identical args produces one row."""
        from src.analysis.io.mart_writer import write_score_frozen, gold_connect

        gold = tmp_path / "gold.duckdb"
        kwargs: dict = dict(
            snapshot_id="snap_idem",
            paper_anchor="paper_b",
            venue="JASSS",
            spec_hash="0" * 64,
            score_rows=[{"person_id": "p1", "iv_score": 0.5}],
            gold_path=gold,
        )
        write_score_frozen(**kwargs)
        write_score_frozen(**kwargs)

        with gold_connect(gold) as conn:
            count = conn.execute(
                "SELECT COUNT(*) FROM mart.meta_score_frozen WHERE snapshot_id='snap_idem'"
            ).fetchone()[0]
        assert count == 1

    def test_upsert_updates_score_hash(self, tmp_path):
        """Re-inserting with different score_rows updates the score_hash."""
        from src.analysis.io.mart_writer import write_score_frozen, gold_connect

        gold = tmp_path / "gold.duckdb"
        rows_v1 = [{"person_id": "p1", "iv_score": 0.5}]
        rows_v2 = [{"person_id": "p1", "iv_score": 0.9}]

        write_score_frozen(
            snapshot_id="snap_upd",
            paper_anchor="paper_c",
            venue="JASSS",
            spec_hash="0" * 64,
            score_rows=rows_v1,
            gold_path=gold,
        )
        h2 = write_score_frozen(
            snapshot_id="snap_upd",
            paper_anchor="paper_c",
            venue="JASSS",
            spec_hash="0" * 64,
            score_rows=rows_v2,
            gold_path=gold,
        )
        with gold_connect(gold) as conn:
            stored_hash = conn.execute(
                "SELECT score_hash FROM mart.meta_score_frozen WHERE snapshot_id='snap_upd'"
            ).fetchone()[0]
        assert stored_hash == h2

    def test_empty_score_rows_allowed(self, tmp_path):
        """write_score_frozen with empty score_rows writes a valid row."""
        from src.analysis.io.mart_writer import write_score_frozen, gold_connect

        gold = tmp_path / "gold.duckdb"
        write_score_frozen(
            snapshot_id="snap_empty",
            paper_anchor="paper_d",
            venue="TestVenue",
            spec_hash="0" * 64,
            score_rows=[],
            gold_path=gold,
        )
        with gold_connect(gold) as conn:
            row = conn.execute(
                "SELECT score_rows_json FROM mart.meta_score_frozen "
                "WHERE snapshot_id='snap_empty'"
            ).fetchone()
        assert row is not None
        assert json.loads(row[0]) == []


# ---------------------------------------------------------------------------
# Tests: build_snapshot staging
# ---------------------------------------------------------------------------


class TestBuildSnapshotStaging:
    def test_tarball_created(self, tmp_path):
        """build_snapshot returns a .tar.gz that exists."""
        from scripts.publication.snapshot import build_snapshot

        gold = tmp_path / "gold.duckdb"
        resolved = tmp_path / "resolved.duckdb"
        lock = tmp_path / "pixi.lock"
        lock.write_text("# pixi lock stub")
        _make_gold_db(gold)
        _make_resolved_db(resolved)

        tarball = build_snapshot(
            venue="TestVenue",
            paper_anchor="test_paper",
            output_dir=tmp_path / "out",
            resolved_db=resolved,
            gold_db=gold,
            pixi_lock=lock,
            method_notes_dir=tmp_path / "nonexistent_notes",
            repo_root=tmp_path,
            dry_run=False,
        )
        assert tarball.exists()
        assert tarball.suffix == ".gz"

    def test_tarball_contains_expected_files(self, tmp_path):
        """Tarball contains resolved.duckdb, gold.duckdb, pixi.lock, README."""
        from scripts.publication.snapshot import build_snapshot

        gold = tmp_path / "gold.duckdb"
        resolved = tmp_path / "resolved.duckdb"
        lock = tmp_path / "pixi.lock"
        lock.write_text("# pixi lock stub")
        _make_gold_db(gold)
        _make_resolved_db(resolved)

        tarball = build_snapshot(
            venue="TestVenue",
            paper_anchor="test_paper",
            output_dir=tmp_path / "out",
            resolved_db=resolved,
            gold_db=gold,
            pixi_lock=lock,
            method_notes_dir=tmp_path / "nonexistent_notes",
            repo_root=tmp_path,
            dry_run=True,
        )
        with tarfile.open(tarball, "r:gz") as tf:
            names = {m.name for m in tf.getmembers()}

        expected_suffixes = {
            "resolved.duckdb",
            "gold.duckdb",
            "pixi.lock",
            "README.txt",
            "MANIFEST.json",
        }
        for suffix in expected_suffixes:
            assert any(n.endswith(suffix) for n in names), (
                f"{suffix} not found in tarball: {names}"
            )

    def test_manifest_json_structure(self, tmp_path):
        """MANIFEST.json contains snapshot_id, spec_hash, score_hash, files."""
        from scripts.publication.snapshot import build_snapshot

        gold = tmp_path / "gold.duckdb"
        resolved = tmp_path / "resolved.duckdb"
        lock = tmp_path / "pixi.lock"
        lock.write_text("# pixi lock stub")
        _make_gold_db(gold)
        _make_resolved_db(resolved)

        tarball = build_snapshot(
            venue="JASSS",
            paper_anchor="manifest_test",
            output_dir=tmp_path / "out",
            resolved_db=resolved,
            gold_db=gold,
            pixi_lock=lock,
            method_notes_dir=tmp_path / "nonexistent_notes",
            repo_root=tmp_path,
            dry_run=True,
        )

        with tarfile.open(tarball, "r:gz") as tf:
            manifest_member = next(
                m for m in tf.getmembers() if m.name.endswith("MANIFEST.json")
            )
            manifest = json.loads(tf.extractfile(manifest_member).read())

        assert "snapshot_id" in manifest
        assert "spec_hash" in manifest
        assert "score_hash" in manifest
        assert "files" in manifest
        assert isinstance(manifest["files"], dict)

    def test_readme_contains_disclaimer(self, tmp_path):
        """README.txt includes both JA and EN disclaimers."""
        from scripts.publication.snapshot import build_snapshot

        gold = tmp_path / "gold.duckdb"
        resolved = tmp_path / "resolved.duckdb"
        lock = tmp_path / "pixi.lock"
        lock.write_text("# pixi lock stub")
        _make_gold_db(gold)
        _make_resolved_db(resolved)

        tarball = build_snapshot(
            venue="JASSS",
            paper_anchor="readme_test",
            output_dir=tmp_path / "out",
            resolved_db=resolved,
            gold_db=gold,
            pixi_lock=lock,
            method_notes_dir=tmp_path / "nonexistent_notes",
            repo_root=tmp_path,
            dry_run=True,
        )

        with tarfile.open(tarball, "r:gz") as tf:
            readme_member = next(
                m for m in tf.getmembers() if m.name.endswith("README.txt")
            )
            readme = tf.extractfile(readme_member).read().decode("utf-8")

        # EN disclaimer
        assert "No claim about individual" in readme
        # JA disclaimer
        assert "免責事項" in readme
        # Must not use ability-framing in *claims* (outside the disclaimer block).
        # Split off the disclaimer section before checking.
        pre_disclaimer = readme.split("Disclaimer")[0].lower()
        forbidden_in_claims = ["skill", "talent"]
        for word in forbidden_in_claims:
            assert word not in pre_disclaimer, (
                f"Forbidden word '{word}' found in README.txt claim section"
            )

    def test_method_notes_copied(self, tmp_path):
        """Method notes .md files appear inside the tarball."""
        from scripts.publication.snapshot import build_snapshot

        gold = tmp_path / "gold.duckdb"
        resolved = tmp_path / "resolved.duckdb"
        lock = tmp_path / "pixi.lock"
        lock.write_text("# pixi lock stub")
        _make_gold_db(gold)
        _make_resolved_db(resolved)

        notes_dir = tmp_path / "method_notes"
        notes_dir.mkdir()
        (notes_dir / "opportunity_residual.md").write_text("# test note")

        tarball = build_snapshot(
            venue="JASSS",
            paper_anchor="notes_test",
            output_dir=tmp_path / "out",
            resolved_db=resolved,
            gold_db=gold,
            pixi_lock=lock,
            method_notes_dir=notes_dir,
            repo_root=tmp_path,
            dry_run=True,
        )

        with tarfile.open(tarball, "r:gz") as tf:
            names = {m.name for m in tf.getmembers()}

        assert any("opportunity_residual.md" in n for n in names)

    def test_dry_run_no_db_write(self, tmp_path):
        """dry_run=True does not write to meta_score_frozen."""
        from scripts.publication.snapshot import build_snapshot
        from src.analysis.io.mart_writer import gold_connect_write, _DDL

        gold = tmp_path / "gold.duckdb"
        resolved = tmp_path / "resolved.duckdb"
        lock = tmp_path / "pixi.lock"
        lock.write_text("# pixi lock stub")
        _make_gold_db(gold)
        _make_resolved_db(resolved)

        build_snapshot(
            venue="JASSS",
            paper_anchor="dry_run_test",
            output_dir=tmp_path / "out",
            resolved_db=resolved,
            gold_db=gold,
            pixi_lock=lock,
            method_notes_dir=tmp_path / "nonexistent_notes",
            repo_root=tmp_path,
            dry_run=True,
        )

        with gold_connect_write(gold) as conn:
            conn.execute(_DDL)
            count = conn.execute(
                "SELECT COUNT(*) FROM mart.meta_score_frozen "
                "WHERE paper_anchor='dry_run_test'"
            ).fetchone()[0]
        assert count == 0

    def test_snapshot_id_uses_paper_anchor(self, tmp_path):
        """Tarball filename and snapshot_id contain the paper_anchor."""
        from scripts.publication.snapshot import build_snapshot

        gold = tmp_path / "gold.duckdb"
        resolved = tmp_path / "resolved.duckdb"
        lock = tmp_path / "pixi.lock"
        lock.write_text("# pixi lock stub")
        _make_gold_db(gold)
        _make_resolved_db(resolved)

        tarball = build_snapshot(
            venue="JASSS",
            paper_anchor="my_special_paper",
            output_dir=tmp_path / "out",
            resolved_db=resolved,
            gold_db=gold,
            pixi_lock=lock,
            method_notes_dir=tmp_path / "nonexistent_notes",
            repo_root=tmp_path,
            dry_run=True,
        )

        assert "my_special_paper" in tarball.name

    def test_missing_source_db_still_completes(self, tmp_path):
        """build_snapshot completes even if resolved.duckdb is absent."""
        from scripts.publication.snapshot import build_snapshot

        gold = tmp_path / "gold.duckdb"
        lock = tmp_path / "pixi.lock"
        lock.write_text("# pixi lock stub")
        _make_gold_db(gold)

        tarball = build_snapshot(
            venue="JASSS",
            paper_anchor="missing_resolved",
            output_dir=tmp_path / "out",
            resolved_db=tmp_path / "does_not_exist.duckdb",
            gold_db=gold,
            pixi_lock=lock,
            method_notes_dir=tmp_path / "nonexistent_notes",
            repo_root=tmp_path,
            dry_run=True,
        )
        assert tarball.exists()
