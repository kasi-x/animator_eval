"""ジャンル空白地分析 — Whitespace スコア / 遷移行列.

Input (from JSON):
    genre_ecosystem: {genre: {cagr_5y, penetration, specialist_count, ...}}
    genre_affinity: [{person_id, genre, affinity_score, ...}]
    scores: [{person_id, person_fe, ...}]
"""

from __future__ import annotations

from collections import defaultdict
from typing import Any

import numpy as np
import structlog

logger = structlog.get_logger()


def compute_whitespace_score(
    genre_ecosystem: dict[str, Any],
    scores: list[dict],
    genre_affinity: list[dict],
) -> dict[str, Any]:
    """W_g = CAGR_{g,5y} × penetration_{g,current} / log(specialist_g + 1).

    specialist_g = persons with genre share > 0.5 AND person_fe percentile > 75.

    Returns {genre: {whitespace_score, cagr, specialist_count, rank}}
    """
    # person_fe percentile rank
    fe_values = np.array([s.get("person_fe", 0.0) or 0.0 for s in scores])
    fe_sorted = np.sort(fe_values)

    def _fe_pct(fe: float) -> float:
        idx = np.searchsorted(fe_sorted, fe)
        return float(idx / max(len(fe_sorted) - 1, 1) * 100)

    fe_pct_map = {
        s["person_id"]: _fe_pct(s.get("person_fe", 0.0) or 0.0)
        for s in scores
        if "person_id" in s
    }

    # Genre share per person from genre_affinity
    person_genre_scores: dict[str, dict[str, float]] = defaultdict(dict)
    for row in genre_affinity:
        pid = row.get("person_id") or row.get("id")
        genre = row.get("genre")
        score = row.get("affinity_score") or row.get("score") or 0.0
        if pid and genre:
            person_genre_scores[pid][genre] = float(score)

    # Specialist count per genre
    specialist_count: dict[str, int] = defaultdict(int)
    for pid, genre_map in person_genre_scores.items():
        total = sum(genre_map.values())
        if total <= 0:
            continue
        fe_p = fe_pct_map.get(pid, 50.0)
        for genre, score in genre_map.items():
            share = score / total
            if share > 0.5 and fe_p > 75:
                specialist_count[genre] += 1

    results: dict = {}
    for genre, eco in genre_ecosystem.items():
        if not isinstance(eco, dict):
            continue

        cagr = float(eco.get("cagr_5y") or eco.get("cagr") or 0.0)
        penetration = float(
            eco.get("penetration") or eco.get("active_persons_pct") or 0.0
        )
        n_specialists = specialist_count.get(genre, 0)

        whitespace = (
            cagr
            * penetration
            / float(np.log(n_specialists + 1) if n_specialists > 0 else 1.0)
        )
        results[genre] = {
            "whitespace_score": round(whitespace, 4),
            "cagr": round(cagr, 4),
            "penetration": round(penetration, 4),
            "specialist_count": n_specialists,
        }

    # Add rank
    sorted_genres = sorted(
        results.items(), key=lambda x: x[1]["whitespace_score"], reverse=True
    )
    for rank, (genre, _) in enumerate(sorted_genres, 1):
        results[genre]["rank"] = rank

    return results


def compute_genre_transition_matrix(
    genre_affinity: list[dict],
    scores: list[dict],
) -> dict[str, Any]:
    """ジャンル移行確率行列: specialist が career 中にどのジャンルに移行したか.

    Uses genre_affinity timeline data if available, else derives from
    top genre per person cross-tab.

    Returns {g1: {g2: probability, ...}, ...}
    """
    # Group genre affinity by person — use top 2 genres per person as migration proxy
    person_top_genres: dict[str, list[str]] = {}
    person_genre_scores: dict[str, dict[str, float]] = defaultdict(dict)

    for row in genre_affinity:
        pid = row.get("person_id") or row.get("id")
        genre = row.get("genre")
        score = float(row.get("affinity_score") or row.get("score") or 0.0)
        if pid and genre:
            person_genre_scores[pid][genre] = score

    for pid, genre_map in person_genre_scores.items():
        sorted_genres = sorted(genre_map.items(), key=lambda x: x[1], reverse=True)
        person_top_genres[pid] = [g for g, _ in sorted_genres[:2]]

    # Transition counts g1 → g2
    transition_counts: dict[str, dict[str, int]] = defaultdict(lambda: defaultdict(int))
    for pid, top_genres in person_top_genres.items():
        if len(top_genres) >= 2:
            g1, g2 = top_genres[0], top_genres[1]
            if g1 != g2:
                transition_counts[g1][g2] += 1

    # Normalize to probabilities
    matrix: dict[str, dict[str, float]] = {}
    for g1, targets in transition_counts.items():
        total = sum(targets.values())
        if total > 0:
            matrix[g1] = {g2: round(n / total, 4) for g2, n in targets.items()}

    return matrix


def run_genre_whitespace(
    genre_ecosystem: dict[str, Any],
    genre_affinity: list[dict],
    scores: list[dict],
) -> dict[str, Any]:
    """ジャンル空白地分析 — メインエントリポイント."""
    if not genre_ecosystem:
        return {"error": "no_genre_ecosystem_data"}

    whitespace = compute_whitespace_score(genre_ecosystem, scores, genre_affinity)
    transition_matrix = compute_genre_transition_matrix(genre_affinity, scores)

    top_10 = dict(
        sorted(
            whitespace.items(), key=lambda x: x[1]["whitespace_score"], reverse=True
        )[:10]
    )

    return {
        "whitespace_scores": whitespace,
        "top_10_whitespace": top_10,
        "transition_matrix": transition_matrix,
        "n_genres": len(whitespace),
        "method_notes": {
            "whitespace": "W_g = CAGR × penetration / log(specialist_g + 1)",
            "specialist_def": "genre_share > 0.5 AND person_fe_percentile > 75",
            "transition": "Top-2 genre per person, migration direction",
        },
    }
