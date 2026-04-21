"""後継計画マトリクス — 退職リスク / 後継候補.

RetireRisk = sigmoid(career_years - 25) × (1 - normalized_slope)
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import numpy as np
import structlog

logger = structlog.get_logger()

_CURRENT_YEAR = 2025


def _sigmoid(x: float) -> float:
    return 1.0 / (1.0 + np.exp(-x))


def compute_retirement_risk(
    person_fe: dict[str, float],
    credits: list[Any],
    current_year: int = _CURRENT_YEAR,
) -> dict[str, float]:
    """RetireRisk = sigmoid(career_years - 25) × (1 - normalized_slope).

    slope = linear trend of last 3y credit counts.

    Returns {person_id: retire_risk_score}
    """
    # Build credit counts per person per year
    person_year_credits: dict[str, dict[int, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    for c in credits:
        if hasattr(c, "person_id"):
            pid = c.person_id
            yr = getattr(c, "credit_year", None) or getattr(c, "year", None)
        elif isinstance(c, dict):
            pid = c.get("person_id")
            yr = c.get("credit_year") or c.get("year")
        else:
            continue
        if pid and yr:
            person_year_credits[pid][int(yr)] += 1

    results: dict[str, float] = {}
    for pid in person_fe:
        year_data = person_year_credits.get(pid, {})
        if not year_data:
            continue

        first_yr = min(year_data.keys())
        career_years = current_year - first_yr

        # slope from last 3 years
        recent_years = sorted([y for y in year_data if y >= current_year - 3])
        if len(recent_years) >= 2:
            yy = np.array(recent_years, dtype=float)
            cc = np.array([year_data[y] for y in recent_years], dtype=float)
            slope = np.polyfit(yy, cc, 1)[0] if len(yy) >= 2 else 0.0
            max_credits = max(cc) if len(cc) > 0 else 1.0
            norm_slope = float(np.clip(slope / max(max_credits, 1.0), -1.0, 1.0))
        else:
            norm_slope = 0.0

        retire_risk = _sigmoid(career_years - 25) * (1.0 - (norm_slope + 1.0) / 2.0)
        results[pid] = round(float(retire_risk), 4)

    return results


def find_succession_candidates(
    person_fe: dict[str, float],
    credits: list[Any],
    studio_assignments: dict[str, dict[int, str]],
    retire_risk: dict[str, float],
    top_k: int = 10,
    risk_threshold: float = 0.6,
) -> dict[str, list[dict]]:
    """For each high-risk veteran: find top-k candidates.

    Score = cosine_similarity × 0.5 + same_studio × 0.2 + co_credit_freq × 0.3

    Returns {veteran_id: [{candidate_id, score, components}]}
    """
    fe_values = np.array(list(person_fe.values()))
    fe_sorted = np.sort(fe_values)

    def _fe_pct(fe: float) -> float:
        idx = np.searchsorted(fe_sorted, fe)
        return float(idx / max(len(fe_sorted) - 1, 1) * 100)

    # co-credit frequency
    anime_persons: dict[str, set] = defaultdict(set)
    for c in credits:
        if hasattr(c, "anime_id"):
            aid = c.anime_id
            pid = c.person_id
        elif isinstance(c, dict):
            aid = c.get("anime_id")
            pid = c.get("person_id")
        else:
            continue
        if aid and pid:
            anime_persons[str(aid)].add(str(pid))

    co_credit_freq: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for aid, persons in anime_persons.items():
        plist = list(persons)
        for i in range(len(plist)):
            for j in range(i + 1, len(plist)):
                co_credit_freq[plist[i]][plist[j]] += 1
                co_credit_freq[plist[j]][plist[i]] += 1

    # High-risk veterans
    high_risk = {
        pid: risk
        for pid, risk in retire_risk.items()
        if risk >= risk_threshold and _fe_pct(person_fe.get(pid, 0)) >= 70
    }

    results: dict = {}
    all_candidates = [pid for pid in person_fe if _fe_pct(person_fe[pid]) >= 40]
    max_co = (
        max(
            (max(co_credit_freq[v].values()) if co_credit_freq.get(v) else 1)
            for v in high_risk
        )
        if high_risk
        else 1
    )

    for veteran_id in high_risk:
        vet_studio = None
        vet_yr_map = studio_assignments.get(veteran_id, {})
        if vet_yr_map:
            vet_studio = max(
                set(vet_yr_map.values()), key=list(vet_yr_map.values()).count
            )

        vet_fe_pct = _fe_pct(person_fe.get(veteran_id, 0))
        scored: list[tuple[str, float, dict]] = []

        for cand_id in all_candidates:
            if cand_id == veteran_id:
                continue

            cand_fe_pct = _fe_pct(person_fe.get(cand_id, 0))

            # cosine sim on fe_pct (single dimension → normalized diff)
            sim = 1.0 - abs(cand_fe_pct - vet_fe_pct) / 100.0

            # same studio
            cand_studio = None
            cand_yr_map = studio_assignments.get(cand_id, {})
            if cand_yr_map:
                cand_studio = max(
                    set(cand_yr_map.values()), key=list(cand_yr_map.values()).count
                )
            same_studio = 1.0 if cand_studio and cand_studio == vet_studio else 0.0

            # co-credit frequency
            co_n = co_credit_freq[veteran_id].get(cand_id, 0)
            co_score = np.log1p(co_n) / np.log1p(max_co)

            total = sim * 0.5 + same_studio * 0.2 + co_score * 0.3
            scored.append(
                (
                    cand_id,
                    total,
                    {
                        "similarity": round(sim, 3),
                        "same_studio": same_studio,
                        "co_credit": round(co_score, 3),
                    },
                )
            )

        top = sorted(scored, key=lambda x: x[1], reverse=True)[:top_k]
        results[veteran_id] = [
            {"candidate_id": cid, "score": round(s, 4), "components": comps}
            for cid, s, comps in top
        ]

    return results


def run_succession_matrix(
    person_fe: dict[str, float],
    credits: list[Any],
    studio_assignments: dict[str, dict[int, str]],
) -> dict[str, Any]:
    """後継計画マトリクス — メインエントリポイント."""
    if not person_fe:
        return {"error": "no_person_fe"}

    retire_risk = compute_retirement_risk(person_fe, credits)
    succession = find_succession_candidates(
        person_fe, credits, studio_assignments, retire_risk
    )

    high_risk_count = sum(1 for r in retire_risk.values() if r >= 0.6)

    return {
        "retire_risk_distribution": {
            "p25": round(float(np.percentile(list(retire_risk.values()), 25)), 4),
            "p50": round(float(np.percentile(list(retire_risk.values()), 50)), 4),
            "p75": round(float(np.percentile(list(retire_risk.values()), 75)), 4),
            "n_high_risk": high_risk_count,
        },
        "succession_coverage": {
            "n_veterans_matched": len(succession),
            "avg_candidates_per_veteran": round(
                np.mean([len(v) for v in succession.values()]), 2
            )
            if succession
            else 0.0,
        },
        "succession_matrix": succession,
        "method_notes": {
            "retire_risk": "sigmoid(career_years - 25) × (1 - norm_slope_last3y)",
            "candidate_score": "cosine_sim × 0.5 + same_studio × 0.2 + co_credit × 0.3",
        },
    }
