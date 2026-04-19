"""スタジオ・ベンチマーク・カード — R5 / Value-Add / 役割多様性.

5-axis percentile composite per studio.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import numpy as np
import structlog

logger = structlog.get_logger()

# ─────────────────────────────────────────────────────────────────────────────
# R5 retention
# ─────────────────────────────────────────────────────────────────────────────


def compute_r5_retention(
    studio_assignments: dict[str, dict[int, str]],
    first_year_range: tuple[int, int] = (2010, 2015),
) -> dict[str, Any]:
    """R_5_s = |active at t+5| / |debuted at studio s|.

    Wilson CI (binomial). Empirical Bayes shrinkage for small n.

    Returns {studio_id: {r5_raw, r5_shrunk, ci_lower, ci_upper, n}}
    """

    # Build debut + active-at-5 per studio
    studio_debuts: dict[str, int] = defaultdict(int)
    studio_active5: dict[str, int] = defaultdict(int)

    for pid, year_map in studio_assignments.items():
        years = sorted(year_map.keys())
        if not years:
            continue
        debut_yr = years[0]
        if not (first_year_range[0] <= debut_yr <= first_year_range[1]):
            continue

        debut_studio = year_map[debut_yr]
        studio_debuts[debut_studio] += 1

        # active 5 years later = has any credit at debut_yr+5
        target_yr = debut_yr + 5
        is_active = target_yr in year_map
        if is_active:
            studio_active5[debut_studio] += 1

    # Global prior for Empirical Bayes: overall retention rate
    total_debuts = sum(studio_debuts.values())
    total_active5 = sum(studio_active5.values())
    global_rate = total_active5 / total_debuts if total_debuts > 0 else 0.5
    prior_alpha = global_rate * 10
    prior_beta = (1 - global_rate) * 10

    results: dict = {}
    for studio_id, n in studio_debuts.items():
        k = studio_active5.get(studio_id, 0)
        r5_raw = k / n if n > 0 else 0.0

        # Wilson CI
        z = 1.96
        denom = 1 + z * z / n
        center = (r5_raw + z * z / (2 * n)) / denom
        margin = z * np.sqrt(r5_raw * (1 - r5_raw) / n + z * z / (4 * n * n)) / denom
        ci_lower = max(0.0, center - margin)
        ci_upper = min(1.0, center + margin)

        # EB shrinkage
        post_alpha = prior_alpha + k
        post_beta = prior_beta + (n - k)
        r5_shrunk = post_alpha / (post_alpha + post_beta)

        results[studio_id] = {
            "r5_raw": round(r5_raw, 4),
            "r5_shrunk": round(r5_shrunk, 4),
            "ci_lower": round(ci_lower, 4),
            "ci_upper": round(ci_upper, 4),
            "n": n,
            "n_active5": k,
        }

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Value-Add
# ─────────────────────────────────────────────────────────────────────────────


def compute_value_add(
    studio_assignments: dict[str, dict[int, str]],
    person_fe: dict[str, float],
    expected_ability_gap: dict[str, Any],
) -> dict[str, Any]:
    """VA_s = mean(ε_p) for alumni of studio s.

    ε_p = person_fe(post) - expected_fe.
    Bootstrap CI (n_boot=500). EB shrinkage.

    Returns {studio_id: {va, va_shrunk, ci_lower, ci_upper, n}}
    """
    fe_sorted = np.sort(np.array(list(person_fe.values())))
    def _fe_pct(fe: float) -> float:
        idx = np.searchsorted(fe_sorted, fe)
        return float(idx / max(len(fe_sorted) - 1, 1))

    # expected percentile from expected_ability_gap
    expected_pct: dict[str, float] = {}
    for pid, d in expected_ability_gap.items():
        if isinstance(d, dict):
            expected_pct[pid] = float(d.get("expected", 0.5) or 0.5)
        else:
            expected_pct[pid] = 0.5

    # Residuals per person
    residuals: dict[str, float] = {}
    for pid, fe in person_fe.items():
        fe_p = _fe_pct(fe)
        exp_p = expected_pct.get(pid, fe_p)  # default: no surprise
        residuals[pid] = fe_p - exp_p

    # Group by studio (use primary studio = most frequent)
    studio_residuals: dict[str, list[float]] = defaultdict(list)
    for pid, year_map in studio_assignments.items():
        if pid not in residuals:
            continue
        if not year_map:
            continue
        primary_studio = max(
            set(year_map.values()),
            key=lambda s: list(year_map.values()).count(s),
        )
        studio_residuals[primary_studio].append(residuals[pid])

    # Global mean for EB prior
    all_res = [v for vals in studio_residuals.values() for v in vals]
    global_mean = np.mean(all_res) if all_res else 0.0
    global_var = np.var(all_res) if all_res else 0.01

    results: dict = {}
    rng = np.random.default_rng(42)

    for studio_id, res_list in studio_residuals.items():
        n = len(res_list)
        if n < 3:
            continue
        arr = np.array(res_list)
        va = float(arr.mean())

        # Bootstrap CI
        boots = [float(rng.choice(arr, n, replace=True).mean()) for _ in range(500)]
        ci_lower = float(np.percentile(boots, 2.5))
        ci_upper = float(np.percentile(boots, 97.5))

        # EB shrinkage: k = var_within / tau²
        var_within = float(arr.var()) / n
        tau2 = max(global_var - var_within, 1e-6)
        k = var_within / tau2
        va_shrunk = float((va + k * global_mean) / (1 + k))

        results[studio_id] = {
            "va": round(va, 4),
            "va_shrunk": round(va_shrunk, 4),
            "ci_lower": round(ci_lower, 4),
            "ci_upper": round(ci_upper, 4),
            "n": n,
        }

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Role diversity
# ─────────────────────────────────────────────────────────────────────────────


def compute_role_diversity(
    studio_assignments: dict[str, dict[int, str]],
    credits: list[Any],
) -> dict[str, Any]:
    """Shannon entropy of role distribution per studio.

    Returns {studio_id: {entropy, percentile}}
    """
    from collections import Counter

    # Build role counts per studio
    studio_role_counts: dict[str, Counter] = defaultdict(Counter)

    for credit in credits:
        if hasattr(credit, "anime_id"):
            pid = getattr(credit, "person_id", None)
            role = getattr(credit, "role", "unknown")
            year = getattr(credit, "credit_year", None) or getattr(credit, "year", None)
        elif isinstance(credit, dict):
            pid = credit.get("person_id")
            role = credit.get("role", "unknown")
            year = credit.get("credit_year") or credit.get("year")
        else:
            continue

        if pid and year:
            studio = None
            yr_map = studio_assignments.get(pid, {})
            if isinstance(year, int):
                studio = yr_map.get(year)
            if studio:
                studio_role_counts[studio][str(role)] += 1

    results: dict = {}
    for studio_id, role_counts in studio_role_counts.items():
        total = sum(role_counts.values())
        if total == 0:
            continue
        probs = np.array(list(role_counts.values()), dtype=float) / total
        entropy = float(-np.sum(probs * np.log(probs + 1e-10)))
        results[studio_id] = {"entropy": round(entropy, 4)}

    # Percentile rank
    if results:
        entropies = np.array([d["entropy"] for d in results.values()])
        sorted_ents = np.sort(entropies)
        for studio_id, d in results.items():
            idx = np.searchsorted(sorted_ents, d["entropy"])
            d["percentile"] = round(float(idx / max(len(sorted_ents) - 1, 1) * 100), 1)

    return results


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────


def compute_studio_benchmark_cards(
    studio_assignments: dict[str, dict[int, str]],
    person_fe: dict[str, float],
    expected_ability_gap: dict[str, Any],
    credits: list[Any],
    studios_json: list[dict] | None = None,
) -> dict[str, Any]:
    """Aggregate all metrics into per-studio card with 5-axis percentile scores."""
    r5 = compute_r5_retention(studio_assignments)
    va = compute_value_add(studio_assignments, person_fe, expected_ability_gap)
    role_div = compute_role_diversity(studio_assignments, credits)

    # Attraction: studio size growth (from studios_json if available)
    attraction: dict[str, float] = {}
    if studios_json:
        for s in studios_json:
            sid = s.get("studio_id") or s.get("id", "")
            attr = s.get("talent_attraction_score") or s.get("growth_score") or 0.5
            attraction[str(sid)] = float(attr)

    all_studios = set(r5) | set(va) | set(role_div)

    # Build 5-axis percentile for each studio
    def _percentile_ranks(metric_dict: dict, key: str) -> dict[str, float]:
        values = [(sid, d.get(key, 0.0)) for sid, d in metric_dict.items()]
        if not values:
            return {}
        arr = np.array([v for _, v in values])
        sorted_arr = np.sort(arr)
        return {
            sid: round(float(np.searchsorted(sorted_arr, v) / max(len(sorted_arr) - 1, 1) * 100), 1)
            for sid, v in values
        }

    r5_pct = _percentile_ranks(r5, "r5_shrunk")
    va_pct = _percentile_ranks(va, "va_shrunk")
    ent_pct = _percentile_ranks(role_div, "entropy")

    cards: dict = {}
    for studio_id in all_studios:
        axes = {
            "retention": r5_pct.get(studio_id, 50.0),
            "value_add": va_pct.get(studio_id, 50.0),
            "role_diversity": ent_pct.get(studio_id, 50.0),
            "attraction": round(attraction.get(studio_id, 0.5) * 100, 1),
            "scale": 50.0,  # placeholder — studio scale from existing JSON
        }
        composite = round(np.mean(list(axes.values())), 1)

        cards[studio_id] = {
            "axes": axes,
            "composite_percentile": composite,
            "r5": r5.get(studio_id, {}),
            "value_add": va.get(studio_id, {}),
            "role_diversity": role_div.get(studio_id, {}),
            "n_staff": r5.get(studio_id, {}).get("n", 0),
        }

    return cards
