"""Tests for mart.meta_report_spec DDL and write_report_specs()."""

from __future__ import annotations

import dataclasses
import hashlib
import json

import duckdb
import pytest

from scripts.report_generators._spec import (
    CIMethod,
    DataLineage,
    InterpretationGuard,
    MethodGate,
    ReportSpec,
    SensitivityAxis,
)
from src.analysis.io.mart_writer import _DDL, write_report_specs


# ---------------------------------------------------------------------------
# Minimal ReportSpec fixture
# ---------------------------------------------------------------------------

def _make_spec(name: str = "test_report") -> ReportSpec:
    return ReportSpec(
        name=name,
        audience="policy",
        claim="claim text",
        identifying_assumption="assumption text",
        null_model=["N3"],
        method_gate=MethodGate(
            name="KM",
            estimator="kaplan_meier",
            ci=CIMethod(estimator="greenwood"),
            rng_seed=42,
            null=["N3"],
            limitations=["limitation A", "limitation B", "limitation C"],
        ),
        sensitivity_grid=[
            SensitivityAxis(name="window", values=["1y", "3y"]),
        ],
        interpretation_guard=InterpretationGuard(
            forbidden_framing=["ability"],
            required_alternatives=1,
        ),
        data_lineage=DataLineage(
            sources=["credits", "persons"],
            meta_table="meta_policy_attrition",
            snapshot_date="2026-05-05",
            pipeline_version="v55",
        ),
    )


@pytest.fixture()
def db_conn(tmp_path):
    """In-memory DuckDB with the mart schema applied."""
    conn = duckdb.connect(str(tmp_path / "test.duckdb"))
    conn.execute("CREATE SCHEMA IF NOT EXISTS mart")
    conn.execute("SET schema='mart'")
    conn.execute(_DDL)
    return conn


# ---------------------------------------------------------------------------
# DDL tests
# ---------------------------------------------------------------------------


class TestMetaReportSpecDDL:
    def test_table_exists(self, db_conn):
        count = db_conn.execute(
            "SELECT COUNT(*) FROM information_schema.tables "
            "WHERE table_schema='mart' AND table_name='meta_report_spec'"
        ).fetchone()[0]
        assert count == 1

    def test_expected_columns(self, db_conn):
        cols = {
            row[0]
            for row in db_conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_schema='mart' AND table_name='meta_report_spec'"
            ).fetchall()
        }
        expected = {
            "report_id", "audience", "claim", "identifying_assumption",
            "null_model_ids", "method_gate_json", "sensitivity_grid_json",
            "interpretation_guard_json", "data_lineage_json",
            "spec_hash", "curated_at",
        }
        assert expected.issubset(cols)


# ---------------------------------------------------------------------------
# write_report_specs() integration tests
# ---------------------------------------------------------------------------


class TestWriteReportSpecs:
    def test_upsert_single_spec_returns_one(self, gold_path):
        spec = _make_spec()
        n = write_report_specs([spec], gold_path)
        assert n == 1

    def test_spec_round_trips_fields(self, gold_path):
        spec = _make_spec("round_trip_report")
        write_report_specs([spec], gold_path)

        conn = duckdb.connect(str(gold_path), read_only=True)
        try:
            row = conn.execute(
                "SELECT * FROM mart.meta_report_spec WHERE report_id=?",
                ("round_trip_report",),
            ).fetchone()
        finally:
            conn.close()

        assert row is not None
        col_names = [
            "report_id", "audience", "claim", "identifying_assumption",
            "null_model_ids", "method_gate_json", "sensitivity_grid_json",
            "interpretation_guard_json", "data_lineage_json",
            "spec_hash", "curated_at",
        ]
        record = dict(zip(col_names, row))

        assert record["report_id"] == "round_trip_report"
        assert record["audience"] == "policy"
        assert record["claim"] == "claim text"
        assert record["identifying_assumption"] == "assumption text"
        assert json.loads(record["null_model_ids"]) == ["N3"]
        assert isinstance(json.loads(record["method_gate_json"]), dict)
        assert isinstance(json.loads(record["sensitivity_grid_json"]), list)
        assert isinstance(json.loads(record["interpretation_guard_json"]), dict)
        assert isinstance(json.loads(record["data_lineage_json"]), dict)

    def test_idempotent_upsert_no_duplicate_rows(self, gold_path):
        spec = _make_spec("idempotent_report")
        write_report_specs([spec], gold_path)
        write_report_specs([spec], gold_path)

        conn = duckdb.connect(str(gold_path), read_only=True)
        try:
            count = conn.execute(
                "SELECT COUNT(*) FROM mart.meta_report_spec "
                "WHERE report_id='idempotent_report'"
            ).fetchone()[0]
        finally:
            conn.close()

        assert count == 1

    def test_spec_hash_is_deterministic(self, gold_path):
        """Same spec content → same sha256 hash on every run."""
        spec = _make_spec("hash_determinism")
        d = dataclasses.asdict(spec)
        canonical = json.dumps(d, sort_keys=True)
        expected_hash = hashlib.sha256(canonical.encode()).hexdigest()

        write_report_specs([spec], gold_path)

        conn = duckdb.connect(str(gold_path), read_only=True)
        try:
            stored_hash = conn.execute(
                "SELECT spec_hash FROM mart.meta_report_spec WHERE report_id=?",
                ("hash_determinism",),
            ).fetchone()[0]
        finally:
            conn.close()

        assert stored_hash == expected_hash

    def test_updated_spec_changes_hash(self, gold_path):
        """Upsert with changed claim updates spec_hash."""
        spec_v1 = _make_spec("changing_report")
        write_report_specs([spec_v1], gold_path)

        spec_v2 = dataclasses.replace(spec_v1, claim="different claim")
        write_report_specs([spec_v2], gold_path)

        d = dataclasses.asdict(spec_v2)
        expected_hash = hashlib.sha256(json.dumps(d, sort_keys=True).encode()).hexdigest()

        conn = duckdb.connect(str(gold_path), read_only=True)
        try:
            stored_hash = conn.execute(
                "SELECT spec_hash FROM mart.meta_report_spec WHERE report_id=?",
                ("changing_report",),
            ).fetchone()[0]
        finally:
            conn.close()

        assert stored_hash == expected_hash

    def test_empty_list_is_noop(self, gold_path):
        n = write_report_specs([], gold_path)
        assert n == 0

    def test_multiple_specs_all_upserted(self, gold_path):
        specs = [_make_spec(f"report_{i}") for i in range(5)]
        n = write_report_specs(specs, gold_path)
        assert n == 5

        conn = duckdb.connect(str(gold_path), read_only=True)
        try:
            count = conn.execute(
                "SELECT COUNT(*) FROM mart.meta_report_spec"
            ).fetchone()[0]
        finally:
            conn.close()

        assert count == 5


# ---------------------------------------------------------------------------
# V2_REPORT_CLASSES SPEC coverage smoke test
# ---------------------------------------------------------------------------


class TestV2ReportClassesSpecCoverage:
    def test_at_least_one_spec_in_v2_classes(self):
        """Smoke test: V2_REPORT_CLASSES modules expose at least one SPEC."""
        import inspect
        from scripts.report_generators.reports import V2_REPORT_CLASSES

        specs_found = []
        for cls in V2_REPORT_CLASSES:
            mod = inspect.getmodule(cls)
            if getattr(mod, "SPEC", None) is not None:
                specs_found.append(cls)
        assert len(specs_found) > 0, "No module-level SPEC found in any V2_REPORT_CLASSES module"

    def test_upsert_report_specs_pipeline_step(self, gold_path, monkeypatch):
        """upsert_report_specs() writes > 0 rows using real V2_REPORT_CLASSES."""
        monkeypatch.setattr(
            "src.analysis.io.mart_writer.DEFAULT_GOLD_DB_PATH", gold_path
        )
        from src.pipeline_phases.post_processing import upsert_report_specs

        n = upsert_report_specs()
        assert n > 0
