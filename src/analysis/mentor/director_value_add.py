"""Director mentoring-power ranking — DML-based mentor effect estimation.

Input:
    mentorships_list: [{mentor_id, mentee_id, shared_works, confidence}]
    person_fe: {person_id: float}
    credits: list[Credit]
    anime_map: {anime_id: Anime}
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import numpy as np
import structlog

logger = structlog.get_logger()

# ─────────────────────────────────────────────────────────────────────────────
# data construction
# ─────────────────────────────────────────────────────────────────────────────


def build_mentee_outcome_dataset(
    mentorships: list[dict],
    person_fe: dict[str, float],
    credits: list[Any],
    anime_map: dict[str, Any],
    outcome_year: int = 5,
) -> tuple[list[dict], Any, float]:
    """Mentee outcome dataset: Y_p = person_fe_percentile at career_year=5.

    Covariates: initial_role, first_anime_scale_tier, debut_year.
    GBM baseline model for Ŷ_p.

    Returns (mentee_records, gbm_model, baseline_r2)
    """
    from sklearn.ensemble import GradientBoostingRegressor

    fe_values = np.array(list(person_fe.values()))
    fe_sorted = np.sort(fe_values)

    def _fe_pct(fe: float) -> float:
        idx = np.searchsorted(fe_sorted, fe)
        return float(idx / max(len(fe_sorted) - 1, 1) * 100)

    # Build credit metadata per person
    person_credits: dict[str, list] = defaultdict(list)
    for c in credits:
        if hasattr(c, "person_id"):
            pid = c.person_id
            yr = getattr(c, "credit_year", None) or getattr(c, "year", None)
            role = str(getattr(c, "role", "unknown"))
            aid = getattr(c, "anime_id", None)
        elif isinstance(c, dict):
            pid = c.get("person_id")
            yr = c.get("credit_year") or c.get("year")
            role = str(c.get("role", "unknown"))
            aid = c.get("anime_id")
        else:
            continue
        if pid:
            person_credits[pid].append({"year": yr, "role": role, "anime_id": aid})

    # Mentee set
    mentee_ids: set[str] = set()
    mentor_of: dict[str, str] = {}
    for m in mentorships:
        mentee = m.get("mentee_id")
        mentor = m.get("mentor_id")
        if mentee and mentor:
            mentee_ids.add(mentee)
            if mentee not in mentor_of:
                mentor_of[mentee] = mentor

    records: list[dict] = []
    for pid in mentee_ids:
        if pid not in person_fe:
            continue
        y_val = _fe_pct(person_fe[pid])

        # debut year + initial role
        p_credits = person_credits.get(pid, [])
        if not p_credits:
            debut_yr = 2010
            initial_role = "unknown"
            first_scale = 2
        else:
            p_credits_sorted = sorted(p_credits, key=lambda x: x.get("year") or 0)
            debut_yr = int(p_credits_sorted[0].get("year") or 2010)
            initial_role = p_credits_sorted[0]["role"]
            first_aid = p_credits_sorted[0].get("anime_id")
            anime_obj = anime_map.get(str(first_aid)) if first_aid else None
            if hasattr(anime_obj, "scale_tier"):
                first_scale = anime_obj.scale_tier or 2
            elif isinstance(anime_obj, dict):
                first_scale = anime_obj.get("scale_tier") or 2
            else:
                first_scale = 2

        records.append(
            {
                "person_id": pid,
                "mentor_id": mentor_of[pid],
                "Y": y_val,
                "debut_year": debut_yr,
                "first_scale_tier": first_scale,
                "initial_role_hash": hash(initial_role) % 100,
            }
        )

    if len(records) < 10:
        return records, None, 0.0

    import pandas as pd

    df = pd.DataFrame(records)
    X = df[["debut_year", "first_scale_tier", "initial_role_hash"]].values.astype(float)
    Y = df["Y"].values.astype(float)

    gbm = GradientBoostingRegressor(n_estimators=100, max_depth=3, random_state=42)
    gbm.fit(X, Y)
    Y_pred = gbm.predict(X)
    ss_res = np.sum((Y - Y_pred) ** 2)
    ss_tot = np.sum((Y - Y.mean()) ** 2)
    r2 = float(1 - ss_res / ss_tot) if ss_tot > 0 else 0.0

    # Add predicted values
    for i, rec in enumerate(records):
        rec["Y_hat"] = float(Y_pred[i])
        rec["residual"] = float(Y[i] - Y_pred[i])

    return records, gbm, r2


# ─────────────────────────────────────────────────────────────────────────────
# Mentor effect
# ─────────────────────────────────────────────────────────────────────────────


def compute_mentor_effect(
    mentee_dataset: list[dict],
    n_perm: int = 500,
) -> dict[str, Any]:
    """M_d = mean(Y_p - Ŷ_p) for mentees of director d.

    Empirical Bayes shrinkage.
    Permutation null model for significance.

    Returns {director_id: {m_raw, m_shrunk, ci, n_mentees, null_p_value}}
    """
    if not mentee_dataset or not any("residual" in d for d in mentee_dataset):
        return {"error": "no_residuals_computed"}

    # Group residuals by mentor
    mentor_residuals: dict[str, list[float]] = defaultdict(list)
    for d in mentee_dataset:
        if "residual" in d:
            mentor_residuals[d["mentor_id"]].append(d["residual"])

    all_residuals = np.array([d["residual"] for d in mentee_dataset if "residual" in d])
    global_var = float(np.var(all_residuals)) if len(all_residuals) > 1 else 0.01

    # Cross-director variance for EB prior
    mentor_means = np.array([np.mean(v) for v in mentor_residuals.values() if v])
    tau2 = max(
        float(np.var(mentor_means))
        - global_var / max(len(all_residuals) // max(len(mentor_residuals), 1), 1),
        1e-6,
    )

    # Permutation null: shuffle mentor labels n_perm times
    rng = np.random.default_rng(42)
    perm_max_effects: list[float] = []
    mentor_ids = [d["mentor_id"] for d in mentee_dataset if "residual" in d]
    residuals_arr = np.array([d["residual"] for d in mentee_dataset if "residual" in d])

    for _ in range(n_perm):
        perm_ids = rng.permutation(mentor_ids)
        perm_mentor_res: dict[str, list] = defaultdict(list)
        for mid, res in zip(perm_ids, residuals_arr):
            perm_mentor_res[mid].append(float(res))
        perm_effects = [np.mean(v) for v in perm_mentor_res.values() if len(v) >= 2]
        if perm_effects:
            perm_max_effects.append(float(max(abs(e) for e in perm_effects)))

    perm_max = (
        np.array(perm_max_effects) if perm_max_effects else np.array([float("inf")])
    )

    results: dict = {}
    for mentor_id, residuals in mentor_residuals.items():
        n = len(residuals)
        if n < 2:
            continue
        arr = np.array(residuals)
        m_raw = float(arr.mean())

        # EB shrinkage
        var_within = float(arr.var()) / n
        k = var_within / tau2
        m_shrunk = float(m_raw / (1 + k))

        # Bootstrap CI
        boots = [float(rng.choice(arr, n, replace=True).mean()) for _ in range(500)]
        ci = [
            round(float(np.percentile(boots, 2.5)), 4),
            round(float(np.percentile(boots, 97.5)), 4),
        ]

        # Permutation p-value
        null_p = float(np.mean(perm_max >= abs(m_raw)))

        results[mentor_id] = {
            "m_raw": round(m_raw, 4),
            "m_shrunk": round(m_shrunk, 4),
            "ci": ci,
            "n_mentees": n,
            "null_p_value": round(null_p, 4),
            "significant": null_p < 0.05,
        }

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────


def run_director_value_add(
    mentorships: list[dict],
    person_fe: dict[str, float],
    credits: list[Any],
    anime_map: dict[str, Any],
) -> dict[str, Any]:
    """Director mentoring-power ranking — main entry point."""
    if not mentorships or not person_fe:
        return {"error": "missing_inputs"}

    mentee_dataset, gbm_model, baseline_r2 = build_mentee_outcome_dataset(
        mentorships, person_fe, credits, anime_map
    )

    if not mentee_dataset:
        return {"error": "no_mentee_data"}

    mentor_effects = compute_mentor_effect(mentee_dataset)

    # Top 50 by |m_shrunk|
    ranked = sorted(
        [(mid, d) for mid, d in mentor_effects.items() if isinstance(d, dict)],
        key=lambda x: abs(x[1].get("m_shrunk", 0)),
        reverse=True,
    )[:50]

    return {
        "n_mentees_analyzed": len(mentee_dataset),
        "baseline_r2": round(baseline_r2, 4),
        "n_mentors_with_effect": len(mentor_effects),
        "top_50_by_effect": {mid: d for mid, d in ranked},
        "all_effects": mentor_effects,
        "method_notes": {
            "outcome": "person_fe percentile at evaluation time",
            "baseline": "GBM on debut_year, first_anime_tier, initial_role_hash",
            "effect": "mean(Y - Ŷ) per mentor, EB shrinkage",
            "significance": "permutation null (n_perm=500)",
        },
    }
