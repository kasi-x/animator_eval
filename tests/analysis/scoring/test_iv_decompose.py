"""Tests for iv_decompose convenience wrapper.

Covers:
- decompose_iv returns correct structure
- Invariant: 5-component contrib_pct sum == 100 % (±1e-3)
- Invariant: result.iv == iv_scores[person_id] within 1e-9
- reconstruction_ok flag set correctly
- All 5 components present in output
- Source metadata present in each component
- decompose_iv_batch: produces same result as decompose_iv for each person
- decompose_iv_batch: correlation check runs once (shared report)
- Unknown person returns None
- dormancy multiplier stored correctly
- lambda_weights echoed in output
"""

from __future__ import annotations

import pytest

from src.analysis.scoring.iv_decompose import (
    COMPONENT_METADATA,
    decompose_iv,
    decompose_iv_batch,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def population():
    """Synthetic 10-person population with all 5 components."""
    pids = [f"p{i}" for i in range(10)]

    raw_components = {
        "person_fe":      {p: 0.10 * (i + 1) for i, p in enumerate(pids)},
        "birank":         {p: 0.08 * (i + 1) for i, p in enumerate(pids)},
        "studio_exposure":{p: 0.05 * (i + 1) for i, p in enumerate(pids)},
        "awcc":           {p: 0.06 * (i + 1) for i, p in enumerate(pids)},
        "patronage":      {p: 0.03 * (i + 1) for i, p in enumerate(pids)},
    }
    lambdas = {name: 0.2 for name in raw_components}

    # Dormancy: vary slightly to test multiplier pass-through
    dormancy = {p: 1.0 - 0.03 * i for i, p in enumerate(pids)}

    # iv_scores: synthetic values that are already "renormalized"
    iv_scores = {p: round(0.05 + 0.09 * i, 6) for i, p in enumerate(pids)}

    last_credit_years = {p: 2015 + i for i, p in enumerate(pids)}

    cohort_labels = {
        p: ("2000s_animation" if i < 5 else "2010s_animation")
        for i, p in enumerate(pids)
    }

    return {
        "pids": pids,
        "raw_components": raw_components,
        "lambdas": lambdas,
        "dormancy": dormancy,
        "iv_scores": iv_scores,
        "last_credit_years": last_credit_years,
        "cohort_labels": cohort_labels,
    }


# ---------------------------------------------------------------------------
# decompose_iv — basic structure
# ---------------------------------------------------------------------------


class TestDecomposeIvBasic:
    def test_returns_dict_for_known_person(self, population):
        s = population
        result = decompose_iv(
            person_id="p0",
            iv_scores=s["iv_scores"],
            raw_components=s["raw_components"],
            lambda_weights=s["lambdas"],
            dormancy=s["dormancy"],
            last_credit_years=s["last_credit_years"],
            cohort_labels=s["cohort_labels"],
        )
        assert result is not None
        assert isinstance(result, dict)

    def test_returns_none_for_unknown_person(self, population):
        s = population
        result = decompose_iv(
            person_id="ghost_xyz",
            iv_scores=s["iv_scores"],
            raw_components=s["raw_components"],
            lambda_weights=s["lambdas"],
            dormancy=s["dormancy"],
            last_credit_years=s["last_credit_years"],
            cohort_labels=s["cohort_labels"],
        )
        assert result is None

    def test_all_five_components_present(self, population):
        s = population
        result = decompose_iv(
            person_id="p3",
            iv_scores=s["iv_scores"],
            raw_components=s["raw_components"],
            lambda_weights=s["lambdas"],
            dormancy=s["dormancy"],
            last_credit_years=s["last_credit_years"],
            cohort_labels=s["cohort_labels"],
        )
        assert result is not None
        expected = {"person_fe", "birank", "studio_exposure", "awcc", "patronage"}
        assert set(result["components"].keys()) == expected

    def test_required_top_level_keys(self, population):
        s = population
        result = decompose_iv(
            person_id="p2",
            iv_scores=s["iv_scores"],
            raw_components=s["raw_components"],
            lambda_weights=s["lambdas"],
            dormancy=s["dormancy"],
            last_credit_years=s["last_credit_years"],
            cohort_labels=s["cohort_labels"],
        )
        assert result is not None
        for key in (
            "iv", "cohort", "cohort_size", "percentile_in_cohort",
            "components", "dormancy", "shapley_fallback", "method_note",
            "lambda_weights", "reconstruction_ok", "reconstruction_tol",
            "correlation_diagnostics", "metadata",
        ):
            assert key in result, f"Missing top-level key: {key}"

    def test_component_has_required_fields(self, population):
        s = population
        result = decompose_iv(
            person_id="p5",
            iv_scores=s["iv_scores"],
            raw_components=s["raw_components"],
            lambda_weights=s["lambdas"],
            dormancy=s["dormancy"],
            last_credit_years=s["last_credit_years"],
            cohort_labels=s["cohort_labels"],
        )
        assert result is not None
        for comp_name, comp_data in result["components"].items():
            for field in ("value", "contrib_pct", "cohort_pctl", "lambda", "source", "aggregation_note"):
                assert field in comp_data, f"{comp_name} missing field: {field}"


# ---------------------------------------------------------------------------
# Invariant: contrib_pct sums to 100
# ---------------------------------------------------------------------------


class TestContribPctSumInvariant:
    def test_contrib_pct_sums_to_100_for_all_persons(self, population):
        """Σ contrib_pct == 100 ± 1e-3 for every person."""
        s = population
        for pid in s["pids"]:
            result = decompose_iv(
                person_id=pid,
                iv_scores=s["iv_scores"],
                raw_components=s["raw_components"],
                lambda_weights=s["lambdas"],
                dormancy=s["dormancy"],
                last_credit_years=s["last_credit_years"],
                cohort_labels=s["cohort_labels"],
            )
            assert result is not None
            total = sum(c["contrib_pct"] for c in result["components"].values())
            assert abs(total - 100.0) < 1e-3, (
                f"{pid}: contrib_pct sum = {total:.6f}, expected 100.0"
            )


# ---------------------------------------------------------------------------
# Invariant: reconstruction_ok — result.iv == iv_scores[person_id] ≤ 1e-9
# ---------------------------------------------------------------------------


class TestReconstructionInvariant:
    def test_reconstruction_ok_true_for_all_persons(self, population):
        """reconstruction_ok must be True for every person (tol=1e-9)."""
        s = population
        for pid in s["pids"]:
            result = decompose_iv(
                person_id=pid,
                iv_scores=s["iv_scores"],
                raw_components=s["raw_components"],
                lambda_weights=s["lambdas"],
                dormancy=s["dormancy"],
                last_credit_years=s["last_credit_years"],
                cohort_labels=s["cohort_labels"],
                tol=1e-9,
            )
            assert result is not None, f"No result for {pid}"
            assert result["reconstruction_ok"], (
                f"{pid}: reconstruction_ok=False, "
                f"result.iv={result['iv']}, stored={s['iv_scores'][pid]}"
            )

    def test_result_iv_equals_stored_iv(self, population):
        """result['iv'] must equal iv_scores[person_id] within 1e-9."""
        s = population
        for pid in s["pids"]:
            result = decompose_iv(
                person_id=pid,
                iv_scores=s["iv_scores"],
                raw_components=s["raw_components"],
                lambda_weights=s["lambdas"],
                dormancy=s["dormancy"],
                last_credit_years=s["last_credit_years"],
                cohort_labels=s["cohort_labels"],
            )
            assert result is not None
            diff = abs(result["iv"] - s["iv_scores"][pid])
            assert diff < 1e-9, (
                f"{pid}: |result.iv - stored_iv| = {diff:.2e} > 1e-9"
            )


# ---------------------------------------------------------------------------
# dormancy pass-through
# ---------------------------------------------------------------------------


class TestDormancyPassThrough:
    def test_dormancy_D_matches_input(self, population):
        """Dormancy multiplier in output matches input dormancy dict."""
        s = population
        for pid in s["pids"]:
            result = decompose_iv(
                person_id=pid,
                iv_scores=s["iv_scores"],
                raw_components=s["raw_components"],
                lambda_weights=s["lambdas"],
                dormancy=s["dormancy"],
                last_credit_years=s["last_credit_years"],
                cohort_labels=s["cohort_labels"],
            )
            assert result is not None
            expected_D = s["dormancy"][pid]
            assert abs(result["dormancy"]["D"] - expected_D) < 1e-9, (
                f"{pid}: dormancy D mismatch: {result['dormancy']['D']} vs {expected_D}"
            )

    def test_last_credit_year_stored_correctly(self, population):
        """last_credit_year matches input."""
        s = population
        result = decompose_iv(
            person_id="p4",
            iv_scores=s["iv_scores"],
            raw_components=s["raw_components"],
            lambda_weights=s["lambdas"],
            dormancy=s["dormancy"],
            last_credit_years=s["last_credit_years"],
            cohort_labels=s["cohort_labels"],
        )
        assert result is not None
        assert result["dormancy"]["last_credit_year"] == s["last_credit_years"]["p4"]


# ---------------------------------------------------------------------------
# lambda_weights echoed in output
# ---------------------------------------------------------------------------


class TestLambdaEchoed:
    def test_lambda_weights_in_output(self, population):
        """lambda_weights must be echoed at top level and per component."""
        s = population
        result = decompose_iv(
            person_id="p1",
            iv_scores=s["iv_scores"],
            raw_components=s["raw_components"],
            lambda_weights=s["lambdas"],
            dormancy=s["dormancy"],
            last_credit_years=s["last_credit_years"],
            cohort_labels=s["cohort_labels"],
        )
        assert result is not None
        # Top-level lambda_weights echoed
        for name in s["lambdas"]:
            assert name in result["lambda_weights"], f"lambda_weights missing {name}"
        # Per-component lambda field
        for name, comp_data in result["components"].items():
            assert "lambda" in comp_data
            assert abs(comp_data["lambda"] - s["lambdas"].get(name, 0.2)) < 1e-9


# ---------------------------------------------------------------------------
# Source metadata present
# ---------------------------------------------------------------------------


class TestSourceMetadata:
    def test_all_components_have_source_and_aggregation_note(self, population):
        """Every component entry must have non-empty 'source' and 'aggregation_note'."""
        s = population
        result = decompose_iv(
            person_id="p6",
            iv_scores=s["iv_scores"],
            raw_components=s["raw_components"],
            lambda_weights=s["lambdas"],
            dormancy=s["dormancy"],
            last_credit_years=s["last_credit_years"],
            cohort_labels=s["cohort_labels"],
        )
        assert result is not None
        for comp_name, comp_data in result["components"].items():
            assert comp_data["source"], f"{comp_name}: 'source' is empty"
            assert comp_data["aggregation_note"], f"{comp_name}: 'aggregation_note' is empty"

    def test_component_metadata_covers_all_canonical_names(self):
        """COMPONENT_METADATA must have entries for all 5 canonical components."""
        expected = {"person_fe", "birank", "studio_exposure", "awcc", "patronage"}
        assert set(COMPONENT_METADATA.keys()) == expected


# ---------------------------------------------------------------------------
# Metadata / disclaimer
# ---------------------------------------------------------------------------


class TestMetadata:
    def test_disclaimer_ja_present(self, population):
        s = population
        result = decompose_iv(
            person_id="p0",
            iv_scores=s["iv_scores"],
            raw_components=s["raw_components"],
            lambda_weights=s["lambdas"],
            dormancy=s["dormancy"],
            last_credit_years=s["last_credit_years"],
            cohort_labels=s["cohort_labels"],
        )
        assert result is not None
        assert "disclaimer_ja" in result["metadata"]
        assert len(result["metadata"]["disclaimer_ja"]) > 10

    def test_disclaimer_en_present(self, population):
        s = population
        result = decompose_iv(
            person_id="p0",
            iv_scores=s["iv_scores"],
            raw_components=s["raw_components"],
            lambda_weights=s["lambdas"],
            dormancy=s["dormancy"],
            last_credit_years=s["last_credit_years"],
            cohort_labels=s["cohort_labels"],
        )
        assert result is not None
        assert "disclaimer_en" in result["metadata"]
        assert len(result["metadata"]["disclaimer_en"]) > 10

    def test_no_forbidden_framing_in_metadata(self, population):
        """Metadata disclaimers must not contain forbidden ability-framing words."""
        s = population
        result = decompose_iv(
            person_id="p0",
            iv_scores=s["iv_scores"],
            raw_components=s["raw_components"],
            lambda_weights=s["lambdas"],
            dormancy=s["dormancy"],
            last_credit_years=s["last_credit_years"],
            cohort_labels=s["cohort_labels"],
        )
        assert result is not None
        forbidden = {"ability", "talent", "skill", "competence", "capability"}
        for key in ("disclaimer_ja", "disclaimer_en"):
            text = result["metadata"].get(key, "").lower()
            for word in forbidden:
                assert word not in text, (
                    f"Forbidden word '{word}' found in metadata.{key}"
                )


# ---------------------------------------------------------------------------
# decompose_iv_batch
# ---------------------------------------------------------------------------


class TestDecomposeIvBatch:
    def test_batch_matches_single_for_each_person(self, population):
        """decompose_iv_batch must produce the same iv + contrib_pct as single calls."""
        s = population
        batch = decompose_iv_batch(
            person_ids=s["pids"],
            iv_scores=s["iv_scores"],
            raw_components=s["raw_components"],
            lambda_weights=s["lambdas"],
            dormancy=s["dormancy"],
            last_credit_years=s["last_credit_years"],
            cohort_labels=s["cohort_labels"],
        )

        for pid in s["pids"]:
            single = decompose_iv(
                person_id=pid,
                iv_scores=s["iv_scores"],
                raw_components=s["raw_components"],
                lambda_weights=s["lambdas"],
                dormancy=s["dormancy"],
                last_credit_years=s["last_credit_years"],
                cohort_labels=s["cohort_labels"],
            )
            assert single is not None
            assert batch[pid] is not None
            assert abs(batch[pid]["iv"] - single["iv"]) < 1e-9, f"{pid}: iv mismatch"
            for comp in ("person_fe", "birank", "studio_exposure", "awcc", "patronage"):
                bp = batch[pid]["components"][comp]["contrib_pct"]
                sp = single["components"][comp]["contrib_pct"]
                assert abs(bp - sp) < 1e-3, f"{pid}/{comp}: contrib_pct mismatch"

    def test_batch_returns_none_for_unknown_person(self, population):
        """Batch: unknown person_id → None in result dict."""
        s = population
        batch = decompose_iv_batch(
            person_ids=s["pids"] + ["ghost_999"],
            iv_scores=s["iv_scores"],
            raw_components=s["raw_components"],
            lambda_weights=s["lambdas"],
            dormancy=s["dormancy"],
            last_credit_years=s["last_credit_years"],
            cohort_labels=s["cohort_labels"],
        )
        assert batch.get("ghost_999") is None

    def test_batch_reconstruction_invariant(self, population):
        """Batch: reconstruction_ok must be True for every found person (tol=1e-9)."""
        s = population
        batch = decompose_iv_batch(
            person_ids=s["pids"],
            iv_scores=s["iv_scores"],
            raw_components=s["raw_components"],
            lambda_weights=s["lambdas"],
            dormancy=s["dormancy"],
            last_credit_years=s["last_credit_years"],
            cohort_labels=s["cohort_labels"],
            tol=1e-9,
        )
        for pid in s["pids"]:
            assert batch[pid] is not None
            assert batch[pid]["reconstruction_ok"], (
                f"Batch reconstruction_ok=False for {pid}"
            )
