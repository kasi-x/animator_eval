"""人材市場流動性・独占度分析 — HHI / 移籍率 / Lock-in 回帰.

studio_assignments: {person_id: {year: studio_id}}
person_fe: {person_id: float}  — AKM person fixed effect
"""

from __future__ import annotations

from collections import Counter, defaultdict
from typing import Any

import numpy as np
import structlog

logger = structlog.get_logger()

# ─────────────────────────────────────────────────────────────────────────────
# HHI 時系列
# ─────────────────────────────────────────────────────────────────────────────


def compute_hhi_timeseries(
    studio_assignments: dict[str, dict[int, str]],
    year_range: tuple[int, int] = (1990, 2025),
) -> dict[str, Any]:
    """年別 HHI (Herfindahl-Hirschman Index) を計算する.

    HHI_y = Σ share_{s,y}² × 10000
    normalized HHI* = (HHI - 1/N) / (1 - 1/N)

    Returns {year: {hhi, hhi_normalized, n_active_studios, n_active_persons}}
    """
    results: dict = {}

    for year in range(year_range[0], year_range[1] + 1):
        studio_counts: Counter = Counter()
        for pid, year_map in studio_assignments.items():
            studio = year_map.get(year)
            if studio:
                studio_counts[studio] += 1

        total = sum(studio_counts.values())
        if total == 0:
            continue

        n_studios = len(studio_counts)
        shares = [c / total for c in studio_counts.values()]
        hhi = sum(s * s for s in shares) * 10000
        hhi_norm = (
            (hhi / 10000 - 1.0 / n_studios) / (1.0 - 1.0 / n_studios)
            if n_studios > 1
            else 1.0
        )

        results[str(year)] = {
            "hhi": round(hhi, 2),
            "hhi_normalized": round(max(0.0, hhi_norm), 4),
            "n_active_studios": n_studios,
            "n_active_persons": total,
        }

    return results


# ─────────────────────────────────────────────────────────────────────────────
# 移籍率
# ─────────────────────────────────────────────────────────────────────────────


def compute_mobility_rates(
    studio_assignments: dict[str, dict[int, str]],
    window: int = 5,
) -> dict[str, Any]:
    """キャリアステージ別・時代別 移籍率を計算する.

    mobility_{p,5y}: 5年間に1度でもスタジオが変わった場合 1

    Returns {overall, by_era, by_career_window}
    """
    total_windows = 0
    mobile_windows = 0

    era_mobile: dict[str, list[int]] = defaultdict(list)
    window_mobile: dict[str, list[int]] = defaultdict(list)

    for pid, year_map in studio_assignments.items():
        years = sorted(year_map.keys())
        if len(years) < 2:
            continue

        for i, y0 in enumerate(years):
            y_end = y0 + window
            window_years = [y for y in years if y0 <= y < y_end]
            if len(window_years) < 2:
                continue

            studios_in_window = {year_map[y] for y in window_years}
            moved = int(len(studios_in_window) > 1)

            total_windows += 1
            mobile_windows += moved

            era = str((y0 // 10) * 10)  # decade
            era_mobile[era].append(moved)

            career_idx = years.index(y0)
            career_stage = (
                "early" if career_idx < 3 else "mid" if career_idx < 10 else "senior"
            )
            window_mobile[career_stage].append(moved)

    overall = mobile_windows / total_windows if total_windows > 0 else 0.0

    by_era = {
        era: round(sum(vals) / len(vals), 4)
        for era, vals in sorted(era_mobile.items())
        if vals
    }
    by_career_window = {
        stage: round(sum(vals) / len(vals), 4)
        for stage, vals in window_mobile.items()
        if vals
    }

    return {
        "overall": round(overall, 4),
        "n_windows": total_windows,
        "by_era": by_era,
        "by_career_stage": by_career_window,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Lock-in 回帰
# ─────────────────────────────────────────────────────────────────────────────


def compute_lockin_regression(
    studio_assignments: dict[str, dict[int, str]],
    person_fe: dict[str, float],
) -> dict[str, Any]:
    """Logit: P(same_studio_next_year) ~ log(fe_rank) + controls.

    Returns {coef_fe, se, or_, ci_lower, ci_upper, n, interpretation}
    """
    from sklearn.linear_model import LogisticRegression

    Y_list: list[int] = []
    fe_rank_list: list[float] = []
    active_years_list: list[float] = []

    # rank-transform person_fe
    fe_values = np.array(list(person_fe.values()))
    fe_rank_map = {
        pid: float(np.searchsorted(np.sort(fe_values), v)) / max(len(fe_values) - 1, 1)
        for pid, v in person_fe.items()
    }

    for pid, year_map in studio_assignments.items():
        years = sorted(year_map.keys())
        fe_rank = fe_rank_map.get(pid, 0.5)
        fe_log = float(np.log(max(fe_rank, 0.01)))

        for i, y in enumerate(years[:-1]):
            same = int(year_map[y] == year_map.get(y + 1))
            career_yr = float(i + 1)
            Y_list.append(same)
            fe_rank_list.append(fe_log)
            active_years_list.append(career_yr)

    if len(Y_list) < 50:
        return {"error": "insufficient_data", "n": len(Y_list)}

    X = np.column_stack([fe_rank_list, active_years_list])
    Y = np.array(Y_list)

    try:
        model = LogisticRegression(max_iter=500)
        model.fit(X, Y)
        coef_fe = float(model.coef_[0][0])
        or_ = float(np.exp(coef_fe))

        # bootstrap CI (n=200)
        rng = np.random.default_rng(42)
        boot_coefs: list[float] = []
        for _ in range(200):
            idx = rng.integers(0, len(Y), len(Y))
            m = LogisticRegression(max_iter=200)
            try:
                m.fit(X[idx], Y[idx])
                boot_coefs.append(float(m.coef_[0][0]))
            except Exception:
                pass

        se = float(np.std(boot_coefs)) if boot_coefs else 0.0
        ci_lower = float(np.exp(np.percentile(boot_coefs, 2.5))) if boot_coefs else 0.0
        ci_upper = float(np.exp(np.percentile(boot_coefs, 97.5))) if boot_coefs else 0.0

        direction = "正" if coef_fe > 0 else "負"
        interpretation = (
            f"AKM person FE 上位者はスタジオ継続率が{direction}の関連 (OR={or_:.2f})"
        )
    except Exception as e:
        logger.warning("lockin_regression_failed", error=str(e))
        return {"error": str(e)}

    return {
        "coef_fe": round(coef_fe, 4),
        "se": round(se, 4),
        "or_": round(or_, 4),
        "ci_lower": round(ci_lower, 4),
        "ci_upper": round(ci_upper, 4),
        "n": len(Y_list),
        "interpretation": interpretation,
    }


# ─────────────────────────────────────────────────────────────────────────────
# Main entry point
# ─────────────────────────────────────────────────────────────────────────────


def run_monopsony_analysis(
    studio_assignments: dict[str, dict[int, str]],
    person_fe: dict[str, float],
) -> dict[str, Any]:
    """人材市場流動性・独占度分析 — メインエントリポイント."""
    if not studio_assignments:
        return {"error": "no_studio_assignments"}

    hhi = compute_hhi_timeseries(studio_assignments)
    mobility = compute_mobility_rates(studio_assignments)
    lockin = compute_lockin_regression(studio_assignments, person_fe)

    return {
        "hhi_timeseries": hhi,
        "mobility": mobility,
        "lockin_regression": lockin,
        "method_notes": {
            "hhi": "Herfindahl-Hirschman Index (×10000), normalized per year",
            "mobility": "5-year window, binary same-studio indicator",
            "lockin": "Logistic regression, bootstrap CI n=200",
        },
    }
