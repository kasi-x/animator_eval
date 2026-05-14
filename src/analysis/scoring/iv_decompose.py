"""Convenience wrapper for IV transparent decomposition.

Provides ``decompose_iv``, a pure-Python entry point that accepts pre-loaded
component dicts and returns a flat ``dict[str, float | int | str | dict]``
suitable for JSON serialisation or direct API use.

This module is a thin adapter over ``iv_decomposition``. All heavy logic
(cohort labelling, correlation check, percentile) lives there.

Formula (from CLAUDE.md):
    IV_i = (λ1·θ_i + λ2·birank_i + λ3·studio_exp_i
            + λ4·awcc_i + λ5·patronage_i) × D_i

Additive decomposition:
    contrib_k[i] = |λ_k·z_k(i)| / Σ_j |λ_j·z_j(i)|   (as %)

Each component entry in the returned dict exposes:
    - ``value``: λ_k · z_k (weighted-normalized value)
    - ``contrib_pct``: share of un-dormanted IV sum (0-100)
    - ``cohort_pctl``: within-cohort percentile for raw component (0-100)
    - ``lambda``: λ weight used
    - ``source``: data source description
    - ``aggregation_note``: how/when the component was aggregated
"""

from __future__ import annotations

from typing import Any

import structlog

from src.analysis.scoring.iv_decomposition import (
    CorrelationReport,
    IVDecompositionResult,
    compute_component_correlations,
    decompose_iv_for_person,
)

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Component metadata: source + aggregation context
# ---------------------------------------------------------------------------

#: For each canonical component, describes the data source and the
#: aggregation window used when computing the score. Displayed verbatim
#: in the API response and in the HTML report so users can trace values.
COMPONENT_METADATA: dict[str, dict[str, str]] = {
    "person_fe": {
        "source": "AKM (Abowd-Kramarz-Margolis) two-way fixed effects — credits table",
        "aggregation_note": (
            "Person fixed effect θ_i estimated via iterative OLS on the full credit log. "
            "Result variable: log(production_scale_ij) = staff_count × episodes × duration_mult. "
            "Estimated once per pipeline run over the full observed period."
        ),
    },
    "birank": {
        "source": "BiRank algorithm — co-credit bipartite graph (persons × anime)",
        "aggregation_note": (
            "BiRank score computed on the person–anime co-credit bipartite graph. "
            "Edge weight = role_weight × episode_coverage × duration_mult. "
            "Convergence criterion: L2 norm change < 1e-8. Estimated per pipeline run."
        ),
    },
    "studio_exposure": {
        "source": "AKM studio fixed effects — credits + studio_memberships tables",
        "aggregation_note": (
            "studio_exposure_i = Σ_j(I{i∈j} · ψ_j) — time-weighted sum of studio fixed effects "
            "for all studios the person has worked at. "
            "Missing = AKM could not estimate (non-mover); imputed to population mean (0 in z-space)."
        ),
    },
    "awcc": {
        "source": "AWCC (Attention-Weighted Credit Count) — credits × role_weight table",
        "aggregation_note": (
            "AWCC_i = Σ_j role_weight_j × attention_j. "
            "role_weight from src/utils/role_groups.py. "
            "attention = production_scale proxy. Aggregated over all observed credits."
        ),
    },
    "patronage": {
        "source": "Patronage premium — credits × director_tier × studio_prestige",
        "aggregation_note": (
            "patronage_i = mean studio prestige of directors the person has worked under, "
            "weighted by co-credit count. "
            "Reflects structural access to high-prestige production environments."
        ),
    },
}


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def decompose_iv(
    person_id: str,
    iv_scores: dict[str, float],
    raw_components: dict[str, dict[str, float]],
    lambda_weights: dict[str, float],
    dormancy: dict[str, float],
    last_credit_years: dict[str, int | None],
    cohort_labels: dict[str, str],
    *,
    tol: float = 1e-9,
) -> dict[str, Any] | None:
    """Decompose IV into 5 components + dormancy for a single person.

    Pure-Python entry point — no DB access. Accepts pre-loaded dicts from
    the pipeline or test fixtures.

    Invariant enforced: the weighted-component sum reconstructs
    ``iv_scores[person_id]`` to within ``tol`` (after renormalization check).

    Args:
        person_id: target person ID
        iv_scores: person_id → IV score (post-renormalization, [0, 1])
        raw_components: {component_name → {person_id → raw value}}
        lambda_weights: {component_name → λ weight}
        dormancy: person_id → dormancy multiplier (0-1)
        last_credit_years: person_id → most recent credit year (or None)
        cohort_labels: person_id → cohort label "decade_rolegroup"
        tol: absolute tolerance for the reconstruction check (default 1e-9)

    Returns:
        Flat dict suitable for JSON serialisation, or None if person not found.
        Keys: iv, cohort, cohort_size, percentile_in_cohort, components,
              dormancy, shapley_fallback, method_note, lambda_weights,
              reconstruction_ok, reconstruction_tol.

    Example::

        result = decompose_iv(
            person_id="p1",
            iv_scores={"p1": 0.62},
            raw_components={...},
            lambda_weights={"person_fe": 0.2, ...},
            dormancy={"p1": 0.85},
            last_credit_years={"p1": 2024},
            cohort_labels={"p1": "2010s_animation"},
        )
        assert result["components"]["person_fe"]["contrib_pct"] >= 0
    """
    if person_id not in iv_scores:
        return None

    # Build component breakdown (λ · z, before dormancy)
    component_breakdown: dict[str, dict[str, float]] = {}
    for pid in iv_scores:
        bd: dict[str, float] = {}
        for name, comp_dict in raw_components.items():
            lam = lambda_weights.get(name, 0.2)
            bd[name] = lam * comp_dict.get(pid, 0.0)
        bd["dormancy"] = dormancy.get(pid, 1.0)
        component_breakdown[pid] = bd

    # Correlation check across all persons
    corr_report = compute_component_correlations(raw_components)

    result: IVDecompositionResult | None = decompose_iv_for_person(
        person_id=person_id,
        iv_scores=iv_scores,
        component_breakdown=component_breakdown,
        lambda_weights=lambda_weights,
        dormancy=dormancy,
        last_credit_years=last_credit_years,
        cohort_labels=cohort_labels,
        raw_components=raw_components,
        correlation_report=corr_report,
    )

    if result is None:
        return None

    # Reconstruction invariant check (within tol)
    stored_iv = iv_scores[person_id]
    reconstruction_ok = abs(result.iv - stored_iv) < tol

    if not reconstruction_ok:
        logger.warning(
            "iv_decompose_reconstruction_mismatch",
            person_id=person_id,
            result_iv=result.iv,
            stored_iv=stored_iv,
            diff=abs(result.iv - stored_iv),
            tol=tol,
        )

    # Build component output with metadata attached
    components_out: dict[str, Any] = {}
    for name, cd in result.components.items():
        meta = COMPONENT_METADATA.get(name, {})
        components_out[name] = {
            "value": cd.value,
            "contrib_pct": cd.contrib_pct,
            "cohort_pctl": cd.cohort_pctl,
            "lambda": round(lambda_weights.get(name, 0.2), 6),
            "source": meta.get("source", ""),
            "aggregation_note": meta.get("aggregation_note", ""),
        }

    return {
        "iv": result.iv,
        "cohort": result.cohort,
        "cohort_size": result.cohort_size,
        "percentile_in_cohort": result.percentile_in_cohort,
        "components": components_out,
        "dormancy": {
            "D": result.dormancy.D,
            "last_credit_year": result.dormancy.last_credit_year,
        },
        "shapley_fallback": result.shapley_fallback,
        "method_note": result.method_note,
        "lambda_weights": {k: round(v, 6) for k, v in lambda_weights.items()},
        "reconstruction_ok": reconstruction_ok,
        "reconstruction_tol": tol,
        "correlation_diagnostics": {
            "max_abs_r": corr_report.max_abs_r,
            "high_corr_pairs": [
                {"a": a, "b": b, "r": r}
                for a, b, r in corr_report.high_corr_pairs
            ],
        },
        "metadata": {
            "disclaimer_ja": (
                "各成分はネットワーク上の位置・協業密度に基づく構造的指標です。"
                "個人の能力・技量を測定するものではありません。"
                "コホート百分位は同デビュー年代・同役職グループ内での位置を示します。"
            ),
            "disclaimer_en": (
                "Components are structural indicators based on network position and "
                "collaboration density. They do not measure individual merit or artistic quality. "
                "Cohort percentile reflects position within the same debut decade and role group."
            ),
            "cohort_definition": "debut_decade × primary_role_group",
            "percentile_scope": "within-cohort only — not a global rank",
        },
    }


def decompose_iv_batch(
    person_ids: list[str],
    iv_scores: dict[str, float],
    raw_components: dict[str, dict[str, float]],
    lambda_weights: dict[str, float],
    dormancy: dict[str, float],
    last_credit_years: dict[str, int | None],
    cohort_labels: dict[str, str],
    *,
    tol: float = 1e-9,
) -> dict[str, dict[str, Any] | None]:
    """Decompose IV for multiple persons in one correlation-check pass.

    Computes the correlation report once across the full population and
    reuses it for each person, avoiding O(n_persons × n_persons) work.

    Args:
        person_ids: list of target person IDs
        iv_scores: person_id → IV score (post-renormalization)
        raw_components: {component_name → {person_id → raw value}}
        lambda_weights: {component_name → λ weight}
        dormancy: person_id → dormancy multiplier
        last_credit_years: person_id → last credit year (or None)
        cohort_labels: person_id → cohort label
        tol: reconstruction tolerance

    Returns:
        {person_id → decompose_iv result dict (or None if not found)}
    """
    # Compute correlation once for the entire population
    corr_report: CorrelationReport = compute_component_correlations(raw_components)

    # Build component breakdown once for the entire population
    component_breakdown: dict[str, dict[str, float]] = {}
    for pid in iv_scores:
        bd: dict[str, float] = {}
        for name, comp_dict in raw_components.items():
            lam = lambda_weights.get(name, 0.2)
            bd[name] = lam * comp_dict.get(pid, 0.0)
        bd["dormancy"] = dormancy.get(pid, 1.0)
        component_breakdown[pid] = bd

    results: dict[str, dict[str, Any] | None] = {}
    for person_id in person_ids:
        if person_id not in iv_scores:
            results[person_id] = None
            continue

        iv_result = decompose_iv_for_person(
            person_id=person_id,
            iv_scores=iv_scores,
            component_breakdown=component_breakdown,
            lambda_weights=lambda_weights,
            dormancy=dormancy,
            last_credit_years=last_credit_years,
            cohort_labels=cohort_labels,
            raw_components=raw_components,
            correlation_report=corr_report,
        )

        if iv_result is None:
            results[person_id] = None
            continue

        stored_iv = iv_scores[person_id]
        reconstruction_ok = abs(iv_result.iv - stored_iv) < tol

        components_out: dict[str, Any] = {}
        for name, cd in iv_result.components.items():
            meta = COMPONENT_METADATA.get(name, {})
            components_out[name] = {
                "value": cd.value,
                "contrib_pct": cd.contrib_pct,
                "cohort_pctl": cd.cohort_pctl,
                "lambda": round(lambda_weights.get(name, 0.2), 6),
                "source": meta.get("source", ""),
                "aggregation_note": meta.get("aggregation_note", ""),
            }

        results[person_id] = {
            "iv": iv_result.iv,
            "cohort": iv_result.cohort,
            "cohort_size": iv_result.cohort_size,
            "percentile_in_cohort": iv_result.percentile_in_cohort,
            "components": components_out,
            "dormancy": {
                "D": iv_result.dormancy.D,
                "last_credit_year": last_credit_years.get(person_id),
            },
            "shapley_fallback": iv_result.shapley_fallback,
            "method_note": iv_result.method_note,
            "lambda_weights": {k: round(v, 6) for k, v in lambda_weights.items()},
            "reconstruction_ok": reconstruction_ok,
            "reconstruction_tol": tol,
        }

    return results
