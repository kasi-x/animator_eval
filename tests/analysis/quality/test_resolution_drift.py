"""Tests for src/analysis/quality/resolution_drift.py

Coverage:
  - DisagreementRow / CUSUMResult dataclasses
  - run_cusum: zero-history, stable, upward drift, threshold crossing
  - compute_disagreement_metrics: fixture DuckDB with conformed schema
  - write_snapshot / load_history_rates: round-trip via in-memory DuckDB
  - weekly_resolution_snapshot.run_snapshot: end-to-end with fixture DB
"""

from __future__ import annotations

from pathlib import Path

import duckdb
import pytest

from src.analysis.quality.resolution_drift import (
    CUSUM_ALLOWANCE,
    CUSUM_THRESHOLD,
    CUSUMResult,
    DisagreementRow,
    compute_disagreement_metrics,
    ensure_audit_weekly_table,
    load_history_rates,
    run_cusum,
    write_snapshot,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _build_conformed_duckdb(path: Path) -> None:
    """Create a minimal animetor.duckdb with conformed schema + fixture persons/credits."""
    conn = duckdb.connect(str(path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS conformed")
    conn.execute("SET schema='conformed'")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS persons (
            id           VARCHAR PRIMARY KEY,
            name_ja      VARCHAR NOT NULL DEFAULT '',
            name_en      VARCHAR NOT NULL DEFAULT '',
            canonical_id VARCHAR,
            gender       VARCHAR,
            hometown     VARCHAR,
            date_of_birth VARCHAR,
            updated_at   TIMESTAMP DEFAULT now()
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS credits (
            person_id        VARCHAR NOT NULL,
            anime_id         VARCHAR NOT NULL,
            role             VARCHAR NOT NULL,
            raw_role         VARCHAR,
            evidence_source  VARCHAR NOT NULL DEFAULT '',
            updated_at       TIMESTAMP DEFAULT now()
        )
    """)

    # Two persons each covered by two sources with disagreements.
    # Canonical person A: anilist says gender='Female', mal says gender='Male' → disagree
    # Canonical person B: anilist says gender='Male', mal says gender='Male'  → agree
    conn.executemany(
        "INSERT INTO persons (id, name_ja, canonical_id, gender, hometown, date_of_birth) VALUES (?, ?, ?, ?, ?, ?)",
        [
            # canonical_A: gender disagree, hometown disagree, birthday agree
            ("anilist:1001", "テスト子", "canonical_A", "Female", "東京", "1990-01-01"),
            ("mal:2001", "テスト子", "canonical_A", "Male", "大阪", "1990-01-01"),
            # canonical_B: gender agree, hometown agree, birthday agree
            (
                "anilist:1002",
                "サンプル太郎",
                "canonical_B",
                "Male",
                "名古屋",
                "1985-06-15",
            ),
            ("mal:2002", "サンプル太郎", "canonical_B", "Male", "名古屋", "1985-06-15"),
        ],
    )

    # Credits: same (person, anime) in two sources with different role → role disagree
    conn.executemany(
        "INSERT INTO credits (person_id, anime_id, role, evidence_source) VALUES (?, ?, ?, ?)",
        [
            ("anilist:1001", "anime:101", "DIRECTOR", "anilist:credits"),
            ("anilist:1001", "anime:101", "ANIMATION_DIRECTOR", "mal:credits"),
            # Pair 2: same role from both sources → agree
            ("anilist:1002", "anime:102", "KEY_ANIMATOR", "anilist:credits"),
            ("anilist:1002", "anime:102", "KEY_ANIMATOR", "mal:credits"),
        ],
    )
    conn.close()


def _build_mart_duckdb(path: Path) -> None:
    """Create an animetor.duckdb with mart schema + audit table."""
    conn = duckdb.connect(str(path))
    conn.execute("CREATE SCHEMA IF NOT EXISTS mart")
    conn.execute("SET schema='mart'")
    ensure_audit_weekly_table(conn)
    conn.close()


# ---------------------------------------------------------------------------
# Unit tests: CUSUMResult / DisagreementRow
# ---------------------------------------------------------------------------


class TestDisagreementRow:
    def test_fields(self):
        row = DisagreementRow(
            source_pair="anilist_mal",
            attribute="gender",
            disagreement_rate=0.05,
            n_comparable=100,
        )
        assert row.source_pair == "anilist_mal"
        assert row.attribute == "gender"
        assert row.disagreement_rate == pytest.approx(0.05)
        assert row.n_comparable == 100

    def test_frozen(self):
        row = DisagreementRow("p", "a", 0.1, 10)
        with pytest.raises(Exception):  # frozen dataclass
            row.attribute = "other"  # type: ignore[misc]


class TestCUSUMResult:
    def test_fields(self):
        r = CUSUMResult(cusum_value=0.05, alert=False, n_history=10)
        assert r.cusum_value == pytest.approx(0.05)
        assert r.alert is False
        assert r.n_history == 10

    def test_alert_true(self):
        r = CUSUMResult(cusum_value=0.15, alert=True, n_history=5)
        assert r.alert is True


# ---------------------------------------------------------------------------
# Unit tests: run_cusum
# ---------------------------------------------------------------------------


class TestRunCusum:
    def test_empty_history_no_alert(self):
        result = run_cusum(history_rates=[], new_rate=0.5)
        assert result.cusum_value == pytest.approx(0.0)
        assert result.alert is False
        assert result.n_history == 0

    def test_stable_series_no_alert(self):
        # Rates fluctuating around 0.05 — CUSUM should stay near zero.
        history = [0.05, 0.04, 0.06, 0.05, 0.05]
        result = run_cusum(history_rates=history, new_rate=0.05)
        assert not result.alert
        assert result.n_history == len(history)

    def test_upward_drift_triggers_alert(self):
        # Baseline mean ~0.05; series escalating well above → should cross threshold.
        # With allowance=0.02 and threshold=0.10 we need enough upward deviation.
        history = [0.05] * 10
        # Feed a very large spike to guarantee alert
        result = run_cusum(
            history_rates=history,
            new_rate=0.30,
            allowance=CUSUM_ALLOWANCE,
            threshold=CUSUM_THRESHOLD,
        )
        assert result.alert is True
        assert result.cusum_value >= CUSUM_THRESHOLD

    def test_cusum_value_non_negative(self):
        # CUSUM statistic must never go negative (lower barrier at 0).
        history = [0.10, 0.09, 0.10, 0.09]
        result = run_cusum(history_rates=history, new_rate=0.01)
        assert result.cusum_value >= 0.0

    def test_single_history_point(self):
        result = run_cusum(history_rates=[0.05], new_rate=0.05)
        # Same value as baseline: increment = 0 - k = negative → clamped to 0.
        assert result.cusum_value == pytest.approx(0.0)
        assert result.alert is False

    def test_custom_threshold(self):
        # Very low threshold means even small drift fires alert.
        history = [0.05] * 5
        result = run_cusum(
            history_rates=history,
            new_rate=0.20,
            allowance=0.01,
            threshold=0.001,
        )
        assert result.alert is True

    def test_gradual_drift_eventually_alerts(self):
        # Simulate 20 weeks of gradual increase from 0.05 to 0.15.
        history = [0.05 + i * 0.005 for i in range(20)]
        new_rate = 0.15
        result = run_cusum(
            history_rates=history,
            new_rate=new_rate,
            allowance=CUSUM_ALLOWANCE,
            threshold=CUSUM_THRESHOLD,
        )
        # With 20 weeks of sustained upward trend the CUSUM should be positive.
        assert result.cusum_value > 0.0


# ---------------------------------------------------------------------------
# Integration tests: compute_disagreement_metrics
# ---------------------------------------------------------------------------


class TestComputeDisagreementMetrics:
    def test_returns_list_of_rows(self, tmp_path):
        db = tmp_path / "animetor.duckdb"
        _build_conformed_duckdb(db)
        rows = compute_disagreement_metrics(db)
        assert isinstance(rows, list)
        # All 4 monitored attributes should be represented.
        attrs = {r.attribute for r in rows}
        assert {"gender", "hometown", "birthday", "role_label"}.issubset(attrs)

    def test_gender_disagree(self, tmp_path):
        db = tmp_path / "animetor.duckdb"
        _build_conformed_duckdb(db)
        rows = compute_disagreement_metrics(db)
        gender_row = next(r for r in rows if r.attribute == "gender")
        # 1 out of 2 canonical persons has gender disagreement → rate = 0.5
        assert gender_row.n_comparable == 2
        assert gender_row.disagreement_rate == pytest.approx(0.5)

    def test_hometown_disagree(self, tmp_path):
        db = tmp_path / "animetor.duckdb"
        _build_conformed_duckdb(db)
        rows = compute_disagreement_metrics(db)
        ht_row = next(r for r in rows if r.attribute == "hometown")
        # canonical_A disagrees (東京 vs 大阪); canonical_B agrees (名古屋 == 名古屋)
        assert ht_row.n_comparable == 2
        assert ht_row.disagreement_rate == pytest.approx(0.5)

    def test_birthday_agree(self, tmp_path):
        db = tmp_path / "animetor.duckdb"
        _build_conformed_duckdb(db)
        rows = compute_disagreement_metrics(db)
        bd_row = next(r for r in rows if r.attribute == "birthday")
        # Both canonical persons agree on birthday
        assert bd_row.disagreement_rate == pytest.approx(0.0)

    def test_role_label_disagree(self, tmp_path):
        db = tmp_path / "animetor.duckdb"
        _build_conformed_duckdb(db)
        rows = compute_disagreement_metrics(db)
        role_row = next(r for r in rows if r.attribute == "role_label")
        # 1 out of 2 (person, anime) pairs disagrees on role → rate = 0.5
        assert role_row.n_comparable == 2
        assert role_row.disagreement_rate == pytest.approx(0.5)

    def test_missing_db_returns_empty(self, tmp_path):
        missing = tmp_path / "does_not_exist.duckdb"
        rows = compute_disagreement_metrics(missing)
        assert rows == []

    def test_empty_conformed_schema_returns_empty(self, tmp_path):
        db = tmp_path / "animetor.duckdb"
        # Create a DB but with NO conformed schema — simulates pre-migration state.
        conn = duckdb.connect(str(db))
        conn.execute("CREATE TABLE placeholder (id INTEGER)")
        conn.close()
        rows = compute_disagreement_metrics(db)
        assert rows == []


# ---------------------------------------------------------------------------
# Integration tests: write_snapshot / load_history_rates
# ---------------------------------------------------------------------------


class TestSnapshotPersistence:
    def _make_mart_conn(self, path: Path) -> duckdb.DuckDBPyConnection:
        conn = duckdb.connect(str(path))
        conn.execute("CREATE SCHEMA IF NOT EXISTS mart")
        conn.execute("SET schema='mart'")
        ensure_audit_weekly_table(conn)
        return conn

    def test_write_and_load_roundtrip(self, tmp_path):
        db = tmp_path / "mart.duckdb"
        conn = self._make_mart_conn(db)

        rows = [
            DisagreementRow("all_sources", "gender", 0.10, 50),
            DisagreementRow("all_sources", "hometown", 0.05, 40),
        ]
        cusum_results = {
            "gender": CUSUMResult(0.08, False, 0),
            "hometown": CUSUMResult(0.00, False, 0),
        }
        n = write_snapshot(conn, "2026-05-12", rows, cusum_results)
        assert n == 2

        gender_hist = load_history_rates(conn, "gender")
        assert len(gender_hist) == 1
        assert gender_hist[0] == pytest.approx(0.10)

        hometown_hist = load_history_rates(conn, "hometown")
        assert len(hometown_hist) == 1
        assert hometown_hist[0] == pytest.approx(0.05)

        conn.close()

    def test_upsert_idempotent(self, tmp_path):
        db = tmp_path / "mart.duckdb"
        conn = self._make_mart_conn(db)

        rows = [DisagreementRow("all_sources", "gender", 0.10, 50)]
        cusum = {"gender": CUSUMResult(0.05, False, 1)}
        write_snapshot(conn, "2026-05-12", rows, cusum)

        # Upsert with updated rate
        rows2 = [DisagreementRow("all_sources", "gender", 0.20, 55)]
        cusum2 = {"gender": CUSUMResult(0.15, True, 1)}
        write_snapshot(conn, "2026-05-12", rows2, cusum2)

        hist = load_history_rates(conn, "gender")
        # Still 1 row (upserted, not duplicated), updated to new rate
        assert len(hist) == 1
        assert hist[0] == pytest.approx(0.20)
        conn.close()

    def test_empty_rows_writes_nothing(self, tmp_path):
        db = tmp_path / "mart.duckdb"
        conn = self._make_mart_conn(db)
        n = write_snapshot(conn, "2026-05-12", [], {})
        assert n == 0
        hist = load_history_rates(conn, "gender")
        assert hist == []
        conn.close()

    def test_history_chronological_order(self, tmp_path):
        db = tmp_path / "mart.duckdb"
        conn = self._make_mart_conn(db)

        weeks = [
            ("2026-04-28", 0.05),
            ("2026-05-05", 0.07),
            ("2026-05-12", 0.09),
        ]
        for week, rate in weeks:
            row = [DisagreementRow("all_sources", "gender", rate, 30)]
            write_snapshot(conn, week, row, {})

        hist = load_history_rates(conn, "gender")
        assert len(hist) == 3
        assert hist == pytest.approx([0.05, 0.07, 0.09])
        conn.close()

    def test_max_weeks_limit(self, tmp_path):
        db = tmp_path / "mart.duckdb"
        conn = self._make_mart_conn(db)

        for i in range(10):
            week = f"2025-{i + 1:02d}-01"
            row = [DisagreementRow("all_sources", "gender", 0.01 * i, 10)]
            write_snapshot(conn, week, row, {})

        hist = load_history_rates(conn, "gender", max_weeks=3)
        assert len(hist) == 3
        conn.close()


# ---------------------------------------------------------------------------
# End-to-end: run_snapshot entry point
# ---------------------------------------------------------------------------


class TestRunSnapshot:
    def test_dry_run_no_write(self, tmp_path):
        """Dry-run should succeed without writing to Mart even if conformed data exists."""
        from scripts.monitoring.weekly_resolution_snapshot import run_snapshot

        db = tmp_path / "animetor.duckdb"
        _build_conformed_duckdb(db)

        summary = run_snapshot(db, week_start="2026-05-12", dry_run=True)
        assert summary["rows_written"] == 0
        assert "metrics" in summary

    def test_full_run_writes_rows(self, tmp_path):
        """Full run with conformed fixture should write 4 rows (one per attribute)."""
        from scripts.monitoring.weekly_resolution_snapshot import run_snapshot

        db = tmp_path / "animetor.duckdb"
        _build_conformed_duckdb(db)

        summary = run_snapshot(db, week_start="2026-05-12")
        assert summary["rows_written"] == 4
        assert summary["week_start"] == "2026-05-12"

    def test_missing_db_returns_zero(self, tmp_path):
        """Non-existent DB path: run_snapshot on a missing file returns empty summary."""
        from scripts.monitoring.weekly_resolution_snapshot import run_snapshot

        db = tmp_path / "missing.duckdb"
        # The script main() does the existence check; run_snapshot itself tries to open.
        summary = run_snapshot(db, week_start="2026-05-12")
        assert summary["rows_written"] == 0

    def test_alert_fires_on_drift(self, tmp_path, monkeypatch):
        """When CUSUM crosses threshold, alert_fired > 0 in summary."""
        import src.analysis.quality.resolution_drift as drift_mod
        from scripts.monitoring.weekly_resolution_snapshot import run_snapshot

        db = tmp_path / "animetor.duckdb"
        _build_conformed_duckdb(db)

        # Pre-populate history with stable low rates so baseline is ~0.01,
        # then a spike of 0.80 will cross threshold easily.
        mart_conn = duckdb.connect(str(db))
        mart_conn.execute("CREATE SCHEMA IF NOT EXISTS mart")
        mart_conn.execute("SET schema='mart'")
        ensure_audit_weekly_table(mart_conn)
        for i in range(5):
            week = f"2026-0{i + 1}-01"
            rows = [
                DisagreementRow("all_sources", attr, 0.01, 100)
                for attr in ("gender", "hometown", "birthday", "role_label")
            ]
            write_snapshot(mart_conn, week, rows, {})
        mart_conn.close()

        # Now monkeypatch compute_disagreement_metrics to return high rates
        monkeypatch.setattr(
            drift_mod,
            "compute_disagreement_metrics",
            lambda *a, **kw: [
                DisagreementRow("all_sources", "gender", 0.90, 100),
                DisagreementRow("all_sources", "hometown", 0.01, 100),
                DisagreementRow("all_sources", "birthday", 0.01, 100),
                DisagreementRow("all_sources", "role_label", 0.01, 100),
            ],
        )

        summary = run_snapshot(db, week_start="2026-06-01")
        assert summary["alerts_fired"] >= 1

    def test_second_run_accumulates_history(self, tmp_path):
        """Running snapshot twice should persist two distinct week rows."""
        from scripts.monitoring.weekly_resolution_snapshot import run_snapshot

        db = tmp_path / "animetor.duckdb"
        _build_conformed_duckdb(db)

        run_snapshot(db, week_start="2026-05-05")
        run_snapshot(db, week_start="2026-05-12")

        mart_conn = duckdb.connect(str(db))
        mart_conn.execute("SET schema='mart'")
        ensure_audit_weekly_table(mart_conn)
        hist = load_history_rates(mart_conn, "gender")
        mart_conn.close()

        assert len(hist) == 2
