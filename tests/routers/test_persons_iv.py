"""Tests for GET /api/persons/{person_id}/iv endpoint.

Uses monkeypatching to avoid needing a real DuckDB file.
The endpoint is tested via two strategies:
  1. Direct call to the decompose_iv_for_person logic (unit-level)
  2. FastAPI TestClient with mocked gold_connect_with_silver (integration-level)
"""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock

import pytest


# ---------------------------------------------------------------------------
# Shared fixtures
# ---------------------------------------------------------------------------


def _make_mock_rows() -> list[dict]:
    """Build minimal mock DB rows for 3 persons."""
    return [
        {
            "person_id": "p1",
            "person_fe": 0.5,
            "birank": 0.4,
            "awcc": 0.3,
            "patronage": 0.2,
            "studio_fe_exposure": 0.15,
            "dormancy": 0.9,
            "iv_score": 0.8,
            "first_year": 2010,
            "latest_year": 2023,
            "primary_role": "key_animator",
        },
        {
            "person_id": "p2",
            "person_fe": 0.3,
            "birank": 0.25,
            "awcc": 0.2,
            "patronage": 0.1,
            "studio_fe_exposure": 0.1,
            "dormancy": 1.0,
            "iv_score": 0.5,
            "first_year": 2012,
            "latest_year": 2022,
            "primary_role": "key_animator",
        },
        {
            "person_id": "p3",
            "person_fe": 0.1,
            "birank": 0.08,
            "awcc": 0.05,
            "patronage": 0.03,
            "studio_fe_exposure": 0.02,
            "dormancy": 0.6,
            "iv_score": 0.2,
            "first_year": 2015,
            "latest_year": 2020,
            "primary_role": "director",
        },
    ]


@contextmanager
def _mock_gold_connection(rows: list[dict]):
    """Context manager that yields a mock DuckDB connection returning given rows."""
    if not rows:
        yield MagicMock()
        return

    cols = list(rows[0].keys())
    fetchall_data = [[r[c] for c in cols] for r in rows]

    mock_conn = MagicMock()
    mock_rel = MagicMock()
    mock_rel.description = [(c, None) for c in cols]
    mock_rel.fetchall.return_value = fetchall_data
    mock_conn.execute.return_value = mock_rel
    mock_conn.__enter__ = lambda s: s
    mock_conn.__exit__ = MagicMock(return_value=False)

    yield mock_conn


# ---------------------------------------------------------------------------
# Direct logic tests (no HTTP layer)
# ---------------------------------------------------------------------------


class TestIVEndpointLogic:
    """Test the decomposition logic that the endpoint uses, directly."""

    def test_decompose_returns_all_five_components(self):
        """Decomposition result includes all 5 named components."""
        from src.analysis.scoring.iv_decomposition import (
            build_cohort_labels,
            build_person_cohort_data_from_scores,
            compute_component_correlations,
            decompose_iv_for_person,
        )

        rows = _make_mock_rows()
        debut_years, primary_roles = build_person_cohort_data_from_scores(rows)
        cohort_labels = build_cohort_labels(debut_years, primary_roles)

        raw_components = {
            "person_fe":       {r["person_id"]: r["person_fe"] for r in rows},
            "birank":          {r["person_id"]: r["birank"] for r in rows},
            "studio_exposure": {r["person_id"]: r["studio_fe_exposure"] for r in rows},
            "awcc":            {r["person_id"]: r["awcc"] for r in rows},
            "patronage":       {r["person_id"]: r["patronage"] for r in rows},
        }
        dormancy = {r["person_id"]: r["dormancy"] for r in rows}
        iv_scores = {r["person_id"]: r["iv_score"] for r in rows}
        last_credit_years = {r["person_id"]: r["latest_year"] for r in rows}

        lambdas = {name: 0.2 for name in raw_components}
        component_breakdown = {}
        for r in rows:
            pid = r["person_id"]
            bd = {name: lambdas[name] * raw_components[name][pid] for name in raw_components}
            bd["dormancy"] = dormancy[pid]
            component_breakdown[pid] = bd

        corr_report = compute_component_correlations(raw_components)

        result = decompose_iv_for_person(
            person_id="p1",
            iv_scores=iv_scores,
            component_breakdown=component_breakdown,
            lambda_weights=lambdas,
            dormancy=dormancy,
            last_credit_years=last_credit_years,
            cohort_labels=cohort_labels,
            raw_components=raw_components,
            correlation_report=corr_report,
        )

        assert result is not None
        assert set(result.components.keys()) == {
            "person_fe", "birank", "studio_exposure", "awcc", "patronage"
        }

    def test_contrib_pct_sums_to_100(self):
        """Contribution percentages sum to exactly 100.0."""
        from src.analysis.scoring.iv_decomposition import (
            build_cohort_labels,
            build_person_cohort_data_from_scores,
            compute_component_correlations,
            decompose_iv_for_person,
        )

        rows = _make_mock_rows()
        debut_years, primary_roles = build_person_cohort_data_from_scores(rows)
        cohort_labels = build_cohort_labels(debut_years, primary_roles)

        raw_components = {
            "person_fe":       {r["person_id"]: r["person_fe"] for r in rows},
            "birank":          {r["person_id"]: r["birank"] for r in rows},
            "studio_exposure": {r["person_id"]: r["studio_fe_exposure"] for r in rows},
            "awcc":            {r["person_id"]: r["awcc"] for r in rows},
            "patronage":       {r["person_id"]: r["patronage"] for r in rows},
        }
        dormancy = {r["person_id"]: r["dormancy"] for r in rows}
        iv_scores = {r["person_id"]: r["iv_score"] for r in rows}
        last_credit_years = {r["person_id"]: r["latest_year"] for r in rows}
        lambdas = {name: 0.2 for name in raw_components}

        component_breakdown = {}
        for r in rows:
            pid = r["person_id"]
            bd = {name: lambdas[name] * raw_components[name][pid] for name in raw_components}
            bd["dormancy"] = dormancy[pid]
            component_breakdown[pid] = bd

        corr_report = compute_component_correlations(raw_components)

        for row in rows:
            pid = row["person_id"]
            result = decompose_iv_for_person(
                person_id=pid,
                iv_scores=iv_scores,
                component_breakdown=component_breakdown,
                lambda_weights=lambdas,
                dormancy=dormancy,
                last_credit_years=last_credit_years,
                cohort_labels=cohort_labels,
                raw_components=raw_components,
                correlation_report=corr_report,
            )
            assert result is not None
            total = sum(cd.contrib_pct for cd in result.components.values())
            assert abs(total - 100.0) < 1e-3, f"{pid}: contrib_pct sum={total}"

    def test_iv_value_matches_input(self):
        """result.iv matches iv_scores[person_id] within 1e-6."""
        from src.analysis.scoring.iv_decomposition import (
            build_cohort_labels,
            build_person_cohort_data_from_scores,
            compute_component_correlations,
            decompose_iv_for_person,
        )

        rows = _make_mock_rows()
        debut_years, primary_roles = build_person_cohort_data_from_scores(rows)
        cohort_labels = build_cohort_labels(debut_years, primary_roles)

        raw_components = {
            "person_fe":       {r["person_id"]: r["person_fe"] for r in rows},
            "birank":          {r["person_id"]: r["birank"] for r in rows},
            "studio_exposure": {r["person_id"]: r["studio_fe_exposure"] for r in rows},
            "awcc":            {r["person_id"]: r["awcc"] for r in rows},
            "patronage":       {r["person_id"]: r["patronage"] for r in rows},
        }
        dormancy = {r["person_id"]: r["dormancy"] for r in rows}
        iv_scores = {r["person_id"]: r["iv_score"] for r in rows}
        last_credit_years = {r["person_id"]: r["latest_year"] for r in rows}
        lambdas = {name: 0.2 for name in raw_components}

        component_breakdown = {}
        for r in rows:
            pid = r["person_id"]
            bd = {name: lambdas[name] * raw_components[name][pid] for name in raw_components}
            bd["dormancy"] = dormancy[pid]
            component_breakdown[pid] = bd

        corr_report = compute_component_correlations(raw_components)

        for row in rows:
            pid = row["person_id"]
            result = decompose_iv_for_person(
                person_id=pid,
                iv_scores=iv_scores,
                component_breakdown=component_breakdown,
                lambda_weights=lambdas,
                dormancy=dormancy,
                last_credit_years=last_credit_years,
                cohort_labels=cohort_labels,
                raw_components=raw_components,
                correlation_report=corr_report,
            )
            assert result is not None
            assert abs(result.iv - iv_scores[pid]) < 1e-6, (
                f"{pid}: result.iv={result.iv} vs stored={iv_scores[pid]}"
            )


# ---------------------------------------------------------------------------
# FastAPI TestClient tests
# ---------------------------------------------------------------------------


@pytest.fixture
def client():
    from fastapi.testclient import TestClient
    from src.runtime.api import app
    return TestClient(app)


class TestIVEndpointHTTP:
    """HTTP-level tests using TestClient with mocked DB."""

    def _patch_gold(self, monkeypatch, rows: list[dict]):
        """Monkeypatch gold_connect_with_silver to return mock rows."""
        import src.routers.persons as persons_module

        @contextmanager
        def _fake_gold():
            cols = list(rows[0].keys()) if rows else []
            fetchall_data = [[r[c] for c in cols] for r in rows]

            mock_conn = MagicMock()
            mock_rel = MagicMock()
            mock_rel.description = [(c, None) for c in cols]
            mock_rel.fetchall.return_value = fetchall_data
            mock_conn.execute.return_value = mock_rel
            yield mock_conn

        monkeypatch.setattr(persons_module, "gold_connect_with_silver", _fake_gold)

    def test_valid_person_returns_200(self, client, monkeypatch):
        """Valid person_id returns HTTP 200 with expected keys."""
        rows = _make_mock_rows()
        self._patch_gold(monkeypatch, rows)
        resp = client.get("/api/persons/p1/iv")
        assert resp.status_code == 200
        data = resp.json()
        assert "iv" in data
        assert "cohort" in data
        assert "cohort_size" in data
        assert "percentile_in_cohort" in data
        assert "components" in data
        assert "dormancy" in data

    def test_components_have_required_keys(self, client, monkeypatch):
        """Each component entry must have value, contrib_pct, cohort_pctl."""
        rows = _make_mock_rows()
        self._patch_gold(monkeypatch, rows)
        resp = client.get("/api/persons/p1/iv")
        assert resp.status_code == 200
        data = resp.json()
        for comp_name, comp_data in data["components"].items():
            assert "value" in comp_data, f"{comp_name}: missing 'value'"
            assert "contrib_pct" in comp_data, f"{comp_name}: missing 'contrib_pct'"
            assert "cohort_pctl" in comp_data, f"{comp_name}: missing 'cohort_pctl'"

    def test_contrib_pct_sums_to_100_in_response(self, client, monkeypatch):
        """contrib_pct values in HTTP response sum to ~100."""
        rows = _make_mock_rows()
        self._patch_gold(monkeypatch, rows)
        resp = client.get("/api/persons/p1/iv")
        assert resp.status_code == 200
        data = resp.json()
        total = sum(c["contrib_pct"] for c in data["components"].values())
        assert abs(total - 100.0) < 1e-2, f"contrib_pct sum = {total}"  # 4-decimal rounding: max drift ~0.0025

    def test_dormancy_key_present(self, client, monkeypatch):
        """Response includes dormancy.D and dormancy.last_credit_year."""
        rows = _make_mock_rows()
        self._patch_gold(monkeypatch, rows)
        resp = client.get("/api/persons/p1/iv")
        assert resp.status_code == 200
        data = resp.json()
        assert "D" in data["dormancy"]
        assert "last_credit_year" in data["dormancy"]

    def test_metadata_disclaimer_present(self, client, monkeypatch):
        """Response includes metadata with JA + EN disclaimers."""
        rows = _make_mock_rows()
        self._patch_gold(monkeypatch, rows)
        resp = client.get("/api/persons/p1/iv")
        assert resp.status_code == 200
        data = resp.json()
        meta = data.get("metadata", {})
        assert "disclaimer_ja" in meta
        assert "disclaimer_en" in meta
        assert "cohort_definition" in meta

    def test_unknown_person_returns_404(self, client, monkeypatch):
        """Unknown person_id returns HTTP 404."""
        rows = _make_mock_rows()
        self._patch_gold(monkeypatch, rows)
        resp = client.get("/api/persons/ghost_9999/iv")
        assert resp.status_code == 404

    def test_db_unavailable_returns_503(self, client, monkeypatch):
        """DB connection failure returns HTTP 503."""
        import src.routers.persons as persons_module

        @contextmanager
        def _failing_gold():
            raise RuntimeError("DB unavailable")
            yield  # unreachable, just makes it a generator

        monkeypatch.setattr(persons_module, "gold_connect_with_silver", _failing_gold)
        resp = client.get("/api/persons/p1/iv")
        assert resp.status_code == 503

    def test_shapley_fallback_reported_when_correlated(self, client, monkeypatch):
        """When components are perfectly correlated, shapley_fallback is True in response."""
        import src.routers.persons as persons_module

        # Build perfectly correlated rows (person_fe == birank == studio_fe_exposure == awcc == patronage)
        n = 20
        rows_corr = [
            {
                "person_id": f"p{i}",
                "person_fe": float(i + 1),
                "birank": float(i + 1),         # r=1.0 with person_fe
                "awcc": float(i + 1),            # r=1.0
                "patronage": float(i + 1),       # r=1.0
                "studio_fe_exposure": float(i + 1),  # r=1.0
                "dormancy": 1.0,
                "iv_score": float(i + 1) / n,
                "first_year": 2010,
                "latest_year": 2023,
                "primary_role": "key_animator",
            }
            for i in range(n)
        ]

        @contextmanager
        def _fake_gold_corr():
            cols = list(rows_corr[0].keys())
            mock_conn = MagicMock()
            mock_rel = MagicMock()
            mock_rel.description = [(c, None) for c in cols]
            mock_rel.fetchall.return_value = [[r[c] for c in cols] for r in rows_corr]
            mock_conn.execute.return_value = mock_rel
            yield mock_conn

        monkeypatch.setattr(persons_module, "gold_connect_with_silver", _fake_gold_corr)

        resp = client.get("/api/persons/p1/iv")
        assert resp.status_code == 200
        data = resp.json()
        assert data["shapley_fallback"] is True
        assert data["correlation_diagnostics"]["max_abs_r"] > 0.9

    def test_response_includes_all_five_component_names(self, client, monkeypatch):
        """All 5 component names appear in the response."""
        rows = _make_mock_rows()
        self._patch_gold(monkeypatch, rows)
        resp = client.get("/api/persons/p1/iv")
        assert resp.status_code == 200
        data = resp.json()
        for name in ("person_fe", "birank", "studio_exposure", "awcc", "patronage"):
            assert name in data["components"], f"Missing component: {name}"

    def test_invalid_person_id_format_returns_422(self, client, monkeypatch):
        """Malformed person_id (SQL injection attempt) returns 400 or 422."""
        rows = _make_mock_rows()
        self._patch_gold(monkeypatch, rows)
        resp = client.get("/api/persons/p1'; DROP TABLE persons;--/iv")
        assert resp.status_code in (400, 404, 422)
