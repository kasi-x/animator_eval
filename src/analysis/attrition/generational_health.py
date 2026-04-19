"""世代交代健全性指標 — コホート生存率 / 世代ピラミッド / フロー会計."""

from __future__ import annotations

import sqlite3
from collections import defaultdict
from typing import Any

import structlog

logger = structlog.get_logger()

_RELIABLE_MAX_YEAR = 2025
_EXIT_CUTOFF_YEAR = 2022  # 2023+ は離脱データを除外


def compute_cohort_survival_rates(conn: sqlite3.Connection) -> dict[str, Any]:
    """S(5), S(10), S(15), S(20) per debut_decade.

    S(t) = fraction of cohort still active (any credit) t years after debut.

    Returns {decade: {S5, S10, S15, S20, n_cohort}}
    """
    rows = conn.execute(
        """
        SELECT fc.person_id, fc.first_year, fc.latest_year, fc.active_years
        FROM feat_career fc
        WHERE fc.first_year IS NOT NULL
        """
    ).fetchall()

    if not rows:
        return {}

    # group by debut_decade
    decade_cohorts: dict[str, list[dict]] = defaultdict(list)
    for pid, first_yr, latest_yr, active_yrs in rows:
        if not first_yr:
            continue
        decade = str((first_yr // 10) * 10)
        decade_cohorts[decade].append(
            {
                "first_year": first_yr,
                "latest_year": latest_yr or first_yr,
                "active_years": active_yrs or 0,
            }
        )

    results: dict = {}
    for decade, cohort in decade_cohorts.items():
        n = len(cohort)
        start_year = int(decade)

        def _survival_rate(t_years: int) -> float:
            threshold_year = start_year + t_years
            if threshold_year > _RELIABLE_MAX_YEAR:
                return None  # type: ignore[return-value]
            active = sum(
                1 for p in cohort
                if (p["latest_year"] or p["first_year"]) >= threshold_year
            )
            return round(active / n, 4) if n > 0 else 0.0

        results[decade] = {
            "n_cohort": n,
            "S5": _survival_rate(5),
            "S10": _survival_rate(10),
            "S15": _survival_rate(15),
            "S20": _survival_rate(20),
        }

    return results


def compute_generation_pyramid(
    conn: sqlite3.Connection,
    year_range: tuple[int, int] = (2010, 2025),
) -> dict[str, Any]:
    """カレンダー年別 キャリア年数ビン別 人材在籍数.

    career_year = calendar_year - first_year

    Returns {year: {bin_0_5, bin_5_15, bin_15_plus, total_active}}
    """
    rows = conn.execute(
        """
        SELECT fc.person_id, fc.first_year, fc.latest_year
        FROM feat_career fc
        WHERE fc.first_year IS NOT NULL
        """
    ).fetchall()

    if not rows:
        return {}

    results: dict = {}
    for year in range(year_range[0], year_range[1] + 1):
        bin_0_5 = 0
        bin_5_15 = 0
        bin_15_plus = 0

        for pid, first_yr, latest_yr in rows:
            if not first_yr:
                continue
            last = latest_yr or first_yr
            if first_yr > year or last < year:
                continue
            career_yr = year - first_yr
            if career_yr < 5:
                bin_0_5 += 1
            elif career_yr < 15:
                bin_5_15 += 1
            else:
                bin_15_plus += 1

        total = bin_0_5 + bin_5_15 + bin_15_plus
        if total == 0:
            continue

        results[str(year)] = {
            "bin_0_5": bin_0_5,
            "bin_5_15": bin_5_15,
            "bin_15_plus": bin_15_plus,
            "total_active": total,
            "dependency_ratio": round(bin_15_plus / max(bin_0_5, 1), 3),
        }

    return results


def compute_flow_accounting(conn: sqlite3.Connection) -> dict[str, Any]:
    """年別 入職 / 離職 / 純フロー / 依存比率.

    entry = first_year == calendar_year
    exit  = latest_year == calendar_year AND calendar_year < EXIT_CUTOFF_YEAR

    Returns {year: {entry, exit_, net_flow, dependency_ratio}}
    """
    entry_rows = conn.execute(
        "SELECT first_year, COUNT(*) FROM feat_career GROUP BY first_year"
    ).fetchall()
    exit_rows = conn.execute(
        f"""
        SELECT latest_year, COUNT(*)
        FROM feat_career
        WHERE latest_year < {_EXIT_CUTOFF_YEAR}
        GROUP BY latest_year
        """
    ).fetchall()

    entry_by_year = {r[0]: r[1] for r in entry_rows if r[0]}
    exit_by_year = {r[0]: r[1] for r in exit_rows if r[0]}

    all_years = sorted(set(entry_by_year) | set(exit_by_year))
    results: dict = {}

    for year in all_years:
        entry = entry_by_year.get(year, 0)
        exit_ = exit_by_year.get(year, 0)
        results[str(year)] = {
            "entry": entry,
            "exit": exit_,
            "net_flow": entry - exit_,
        }

    return results


def run_generational_health(conn: sqlite3.Connection) -> dict[str, Any]:
    """世代交代健全性指標 — メインエントリポイント."""
    survival = compute_cohort_survival_rates(conn)
    pyramid = compute_generation_pyramid(conn)
    flow = compute_flow_accounting(conn)

    return {
        "cohort_survival": survival,
        "generation_pyramid": pyramid,
        "flow_accounting": flow,
        "method_notes": {
            "survival": f"latest_year >= debut_year+t で在籍判定, reliable_max_year={_RELIABLE_MAX_YEAR}",
            "exit_cutoff": f"exit データは {_EXIT_CUTOFF_YEAR} 未満のみ (それ以降は集計不完全)",
        },
    }
