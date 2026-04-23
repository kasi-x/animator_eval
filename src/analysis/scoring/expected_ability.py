"""Expected vs Actual Ability — comparison of expected and realized contribution.

Estimates expected performance from environment (collaborators, studio, works)
and compares with actual contribution (person_fe).

Expected ability formula (from CALCULATION_COMPENDIUM §5):
    E[score_i] = α·avg_collaborator_iv + β·avg_anime_quality + γ·studio_fe_exposure + δ·avg_director_birank

Coefficients estimated via OLS. gap = actual - expected (positive = exceeded expectations).
"""

from collections import defaultdict
from dataclasses import dataclass

import numpy as np
import structlog

from src.models import AnimeAnalysis as Anime, Credit

logger = structlog.get_logger()


@dataclass
class ExpectedActualResult:
    """Result of expected vs actual ability computation."""

    expected: dict[str, float]  # person_id → expected score
    actual: dict[str, float]  # person_id → actual contribution (person_fe)
    gap: dict[str, float]  # person_id → actual - expected
    model_r_squared: float
    total_persons: int


def compute_expected_ability(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_fe: dict[str, float],
    birank: dict[str, float],
    studio_fe: dict[str, float],
    studio_assignments: dict[str, dict[int, str]],
    iv_scores: dict[str, float],
) -> ExpectedActualResult:
    """Compute expected vs actual ability scores.

    Expected ability uses environmental factors:
    - avg collaborator IV (quality of peers)
    - avg anime score of participated works
    - studio FE exposure (institutional baseline)
    - avg director birank of works (leadership quality)

    OLS estimates coefficients, then:
    expected[i] = X_i @ β
    actual[i] = person_fe[i]
    gap = actual - expected

    Args:
        credits: all credits
        anime_map: anime_id → Anime
        person_fe: person_id → person fixed effect
        birank: person_id → BiRank score
        studio_fe: studio_name → studio fixed effect
        studio_assignments: person_id → {year → studio}
        iv_scores: person_id → IV score

    Returns:
        ExpectedActualResult
    """
    from src.utils.role_groups import DIRECTOR_ROLES

    # Build per-anime staff sets and director sets
    anime_staff: dict[str, set[str]] = defaultdict(set)
    anime_directors: dict[str, set[str]] = defaultdict(set)
    for c in credits:
        anime_staff[c.anime_id].add(c.person_id)
        if c.role in DIRECTOR_ROLES:
            anime_directors[c.anime_id].add(c.person_id)

    # Build person → anime set
    person_anime: dict[str, set[str]] = defaultdict(set)
    for c in credits:
        person_anime[c.person_id].add(c.anime_id)

    # D16: Simultaneity note — collaborator IV and a person's own IV are computed
    # from the same pipeline run. This means "expected ability" uses information
    # that is contemporaneous, not strictly prior. However, the collaborator
    # average (excluding self) is a leave-one-out estimator, which mitigates
    # direct self-influence. For strict causal claims, a two-stage approach
    # (prior-year collaborator IV) would be needed.
    # Compute features for each person
    target_pids = sorted(set(person_fe.keys()) & set(iv_scores.keys()))
    if len(target_pids) < 10:
        return ExpectedActualResult(
            expected={}, actual={}, gap={}, model_r_squared=0.0, total_persons=0
        )

    features = np.zeros((len(target_pids), 4), dtype=np.float64)
    y = np.zeros(len(target_pids), dtype=np.float64)

    for i, pid in enumerate(target_pids):
        # 1. Avg collaborator IV (from CALCULATION_COMPENDIUM §5.1-5.2)
        collab_ivs = []
        anime_score_weights = []
        director_biranks = []

        for aid in person_anime.get(pid, set()):
            anime = anime_map.get(aid)
            if not anime:
                continue

            # Collaborator quality
            staff_ivs = [
                iv_scores.get(other, 0.0)
                for other in anime_staff.get(aid, set())
                if other != pid and other in iv_scores
            ]
            if staff_ivs:
                avg_collab = sum(staff_ivs) / len(staff_ivs)
                collab_ivs.append(avg_collab)
                anime_score_weights.append(1.0)

            # Director quality
            for dir_id in anime_directors.get(aid, set()):
                dir_br = birank.get(dir_id, 0.0)
                if dir_br > 0:
                    director_biranks.append(dir_br)

        # Feature 0: avg collaborator IV (weighted by anime score)
        total_w = sum(anime_score_weights)
        features[i, 0] = sum(collab_ivs) / total_w if total_w > 0 else 0.0

        # Feature 1: avg production scale (staff count of participated anime)
        anime_staff_counts = [
            len(anime_staff.get(aid, set()))
            for aid in person_anime.get(pid, set())
            if aid in anime_staff
        ]
        features[i, 1] = (
            sum(anime_staff_counts) / len(anime_staff_counts)
            if anime_staff_counts
            else 0.0
        )

        # Feature 2: studio FE exposure
        year_studio = studio_assignments.get(pid, {})
        if year_studio:
            studio_fes = [studio_fe.get(s, 0.0) for s in set(year_studio.values())]
            features[i, 2] = sum(studio_fes) / len(studio_fes) if studio_fes else 0.0

        # Feature 3: avg director birank
        features[i, 3] = (
            sum(director_biranks) / len(director_biranks) if director_biranks else 0.0
        )

        # Target: person_fe (actual contribution)
        y[i] = person_fe.get(pid, 0.0)

    # OLS: y = X @ β + ε
    X = np.column_stack([np.ones(len(target_pids)), features])
    try:
        XtX = X.T @ X + np.eye(X.shape[1]) * 1e-8
        beta = np.linalg.solve(XtX, X.T @ y)
        y_hat = X @ beta

        ss_res = np.sum((y - y_hat) ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        r_squared = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
    except np.linalg.LinAlgError:
        y_hat = np.full(len(target_pids), np.mean(y))
        r_squared = 0.0

    # Build result
    expected = {}
    actual = {}
    gap = {}
    for i, pid in enumerate(target_pids):
        expected[pid] = round(float(y_hat[i]), 4)
        actual[pid] = round(float(y[i]), 4)
        gap[pid] = round(float(y[i] - y_hat[i]), 4)

    logger.info(
        "expected_ability_computed",
        persons=len(target_pids),
        r_squared=round(r_squared, 4),
    )

    return ExpectedActualResult(
        expected=expected,
        actual=actual,
        gap=gap,
        model_r_squared=round(r_squared, 4),
        total_persons=len(target_pids),
    )
