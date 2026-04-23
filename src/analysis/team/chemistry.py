"""Team chemistry — pair residuals / network / bridge persons.

Y_{a,ij} = log(production_scale_a) - expected(a)
Pair-level mean residual, BH-corrected p-values.
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import numpy as np
import structlog

logger = structlog.get_logger()


def compute_pair_residuals(
    credits: list[Any],
    anime_map: dict[str, Any],
    iv_scores: dict[str, float],
    min_shared_works: int = 3,
) -> dict[str, Any]:
    """Y_{a,ij} = log(production_scale_a) - mean(log(production_scale)).

    For pairs with n_ij >= min_shared_works: t-test + BH correction.

    Returns {pair_key: {mean_res, se, n, p_bh, significant, pid_a, pid_b}}
    """
    from scipy import stats as scipy_stats

    # Build work-level production scale
    work_scale: dict[str, float] = {}
    for aid, anime in anime_map.items():
        if hasattr(anime, "production_scale"):
            scale = anime.production_scale
        elif isinstance(anime, dict):
            scale = anime.get("production_scale") or anime.get("scale") or 1.0
        else:
            scale = 1.0
        work_scale[str(aid)] = max(float(scale or 1.0), 1.0)

    if not work_scale:
        return {}

    log_scales = np.log(np.array(list(work_scale.values())))
    mean_log_scale = float(log_scales.mean())

    # Work residual
    work_residual: dict[str, float] = {
        aid: float(np.log(scale)) - mean_log_scale for aid, scale in work_scale.items()
    }

    # Anime → persons mapping
    anime_persons: dict[str, set] = defaultdict(set)
    for c in credits:
        if hasattr(c, "anime_id"):
            aid = str(c.anime_id)
            pid = str(c.person_id)
        elif isinstance(c, dict):
            aid = str(c.get("anime_id", ""))
            pid = str(c.get("person_id", ""))
        else:
            continue
        if aid and pid:
            anime_persons[aid].add(pid)

    # Pair co-credits
    pair_residuals: dict[tuple, list[float]] = defaultdict(list)
    for aid, persons in anime_persons.items():
        plist = sorted(persons)
        res = work_residual.get(aid, 0.0)
        for i in range(len(plist)):
            for j in range(i + 1, len(plist)):
                key = (plist[i], plist[j])
                pair_residuals[key].append(res)

    # Filter and compute stats
    results: dict = {}
    p_values_raw: list[float] = []
    pair_keys: list[tuple] = []

    for pair_key, residuals in pair_residuals.items():
        if len(residuals) < min_shared_works:
            continue
        arr = np.array(residuals)
        mean_res = float(arr.mean())
        se = float(arr.std() / np.sqrt(len(arr))) if len(arr) > 1 else float("inf")

        if se > 0:
            t_stat, p_val = scipy_stats.ttest_1samp(arr, 0.0)
            p_values_raw.append(float(p_val))
        else:
            p_values_raw.append(1.0)

        pair_keys.append(pair_key)
        results[f"{pair_key[0]}:{pair_key[1]}"] = {
            "pid_a": pair_key[0],
            "pid_b": pair_key[1],
            "mean_res": round(mean_res, 4),
            "se": round(se, 4),
            "n": len(residuals),
            "p_raw": round(float(p_values_raw[-1]), 4),
        }

    if not results:
        return {}

    # BH correction
    n_tests = len(p_values_raw)
    p_arr = np.array(p_values_raw)
    sorted_idx = np.argsort(p_arr)
    rank = np.empty(n_tests)
    rank[sorted_idx] = np.arange(1, n_tests + 1)
    rank / n_tests * 0.05
    bh_corrected = p_arr * n_tests / rank

    for i, (pair_key, pk_str) in enumerate(zip(pair_keys, list(results.keys()))):
        pk = f"{pair_key[0]}:{pair_key[1]}"
        if pk in results:
            results[pk]["p_bh"] = round(float(bh_corrected[i]), 4)
            results[pk]["significant"] = bool(bh_corrected[i] < 0.05)

    return results


def run_team_chemistry(
    credits: list[Any],
    anime_map: dict[str, Any],
    iv_scores: dict[str, float],
) -> dict[str, Any]:
    """Team chemistry — main entry point."""
    pair_results = compute_pair_residuals(credits, anime_map, iv_scores)

    if not pair_results:
        return {"error": "insufficient_data_for_pair_analysis"}

    n_pairs = len(pair_results)
    n_significant = sum(1 for d in pair_results.values() if d.get("significant"))
    n_positive = sum(
        1
        for d in pair_results.values()
        if d.get("significant") and d.get("mean_res", 0) > 0
    )

    top_positive = sorted(
        [
            (k, d)
            for k, d in pair_results.items()
            if d.get("significant") and d.get("mean_res", 0) > 0
        ],
        key=lambda x: x[1]["mean_res"],
        reverse=True,
    )[:20]

    return {
        "n_pairs_analyzed": n_pairs,
        "n_significant_pairs": n_significant,
        "n_positive_chemistry": n_positive,
        "top_20_positive_pairs": dict(top_positive),
        "pair_residuals": pair_results,
        "method_notes": {
            "outcome": "log(production_scale) - global_mean",
            "test": "one-sample t-test vs 0, BH correction alpha=0.05",
            "min_shared_works": 3,
        },
    }
