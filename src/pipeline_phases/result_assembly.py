"""Phase 7: Result Assembly — build comprehensive result dictionaries."""

import datetime
import math
from collections import defaultdict

import numpy as np
import structlog

from src.analysis.explain import explain_authority, explain_skill, explain_trust
from src.analysis.io.mart_writer import GoldWriter
from src.analysis.scoring.integrated_value import compute_studio_exposure
from src.runtime.models import ScoreResult
from src.pipeline_phases.pipeline_types import (
    CoreScoresResult,
    EntityResolutionResult,
    GraphsResult,
    SupplementaryMetricsResult,
)
from src.utils.role_groups import DIRECTOR_ROLES

logger = structlog.get_logger()


def assemble_result_entries(
    resolved: EntityResolutionResult,
    graphs: GraphsResult,
    core: CoreScoresResult,
    supp: SupplementaryMetricsResult,
) -> list[dict]:
    """Build comprehensive result dictionaries for each person.

    This phase:
    1. Creates ScoreResult objects and writes to gold.duckdb
    2. Builds rich result dictionaries with all computed metrics
    3. Adds score breakdowns (top contributing factors)
    4. Sorts results by iv_score descending

    Returns list[dict] with all person results.
    """
    logger.info("step_start", step="result_assembly")

    credits = resolved.resolved_credits
    anime_map = resolved.anime_map

    # Use career-aware iv/dormancy from supp (overrides Phase 5 values)
    iv_scores = supp.iv_scores
    dormancy_scores = supp.dormancy_scores

    all_person_ids = set(core.person_fe) | set(core.birank_person_scores) | set(iv_scores)

    results: list[dict] = []

    credits_by_person: dict[str, list] = defaultdict(list)
    for credit in credits:
        credits_by_person[credit.person_id].append(credit)

    anime_directors: dict[str, set[str]] = defaultdict(set)
    for credit in credits:
        if credit.role in DIRECTOR_ROLES:
            anime_directors[credit.anime_id].add(credit.person_id)

    studio_exposure = compute_studio_exposure(
        core.person_fe,
        core.studio_fe,
        studio_assignments=core.studio_assignments,
    )

    person_fe_se: dict[str, float] = {}
    person_n_obs: dict[str, int] = {}
    if core.akm_result and core.akm_result.residuals:
        person_resids: dict[str, list[float]] = defaultdict(list)
        for (pid, _aid), resid in core.akm_result.residuals.items():
            person_resids[pid].append(resid)
        for pid, resids in person_resids.items():
            n = len(resids)
            person_n_obs[pid] = n
            if n >= 2:
                sigma_i = float(np.std(resids, ddof=1))
                person_fe_se[pid] = sigma_i / math.sqrt(n)

    scores_by_pid: dict[str, ScoreResult] = {}
    score_rows = []
    for pid in all_person_ids:
        ks = core.knowledge_spanner_scores.get(pid)

        score = ScoreResult(
            person_id=pid,
            person_fe=core.person_fe.get(pid, 0.0),
            studio_fe_exposure=studio_exposure.get(pid, 0.0),
            birank=core.birank_person_scores.get(pid, 0.0),
            patronage=core.patronage_scores.get(pid, 0.0),
            dormancy=dormancy_scores.get(pid, 1.0),
            awcc=ks.awcc if ks else 0.0,
            ndi=ks.ndi if ks else 0.0,
            iv_score=iv_scores.get(pid, 0.0),
            iv_score_historical=core.iv_scores_historical.get(pid, 0.0),
        )
        scores_by_pid[pid] = score
        score_rows.append(
            (
                pid,
                score.person_fe,
                score.studio_fe_exposure,
                score.birank,
                score.patronage,
                score.dormancy,
                score.awcc,
                score.iv_score,
            )
        )

    now = datetime.datetime.now()
    run_year = now.year
    run_quarter = (now.month - 1) // 3 + 1
    history_rows = [(*row, run_year, run_quarter) for row in score_rows]
    with GoldWriter() as gw:
        gw.write_person_scores(score_rows)
        gw.write_score_history(history_rows)
    logger.info(
        "scores_batch_saved",
        count=len(score_rows),
        year=run_year,
        quarter=run_quarter,
    )

    if supp.career_tracks:
        logger.info("career_tracks_computed", count=len(supp.career_tracks))

    for pid in all_person_ids:
        score = scores_by_pid[pid]

        node_data = (
            graphs.person_anime_graph.nodes.get(pid, {}) if graphs.person_anime_graph else {}
        )

        peer_boost = 0.0
        if supp.peer_effect_result and supp.peer_effect_result.person_peer_boost:
            peer_boost = supp.peer_effect_result.person_peer_boost.get(pid, 0.0)

        n_obs = person_n_obs.get(pid, 0)
        se = person_fe_se.get(pid)

        result_entry = {
            "person_id": pid,
            "name": node_data.get("name", ""),
            "name_ja": node_data.get("name_ja", ""),
            "name_en": node_data.get("name_en", ""),
            "career_track": supp.career_tracks.get(pid, "multi_track"),
            "iv_score": round(score.iv_score, 4),
            "person_fe": round(score.person_fe, 4),
            "studio_fe_exposure": round(score.studio_fe_exposure, 4),
            "birank": round(score.birank, 4),
            "patronage": round(score.patronage, 4),
            "dormancy": round(score.dormancy, 4),
            "awcc": round(score.awcc, 4),
            "ndi": round(score.ndi, 4),
            "career_friction": round(supp.career_friction.get(pid, 0), 4),
            "peer_boost": round(peer_boost, 4),
            "iv_score_historical": round(score.iv_score_historical, 4),
            "person_fe_se": round(se, 4) if se is not None else None,
            "person_fe_n_obs": n_obs,
        }

        result_entry["score_layers"] = {
            "causal": {
                "person_fe": round(score.person_fe, 4),
                "person_fe_se": round(se, 4) if se is not None else None,
                "person_fe_ci_95": [
                    round(score.person_fe - 1.96 * se, 4),
                    round(score.person_fe + 1.96 * se, 4),
                ]
                if se is not None
                else None,
                "n_obs": n_obs,
                "interpretation": "AKM固定効果: 因果推論に基づく個人の生産貢献",
            },
            "structural": {
                "birank": round(score.birank, 4),
                "awcc": round(score.awcc, 4),
                "ndi": round(score.ndi, 4),
                "interpretation": "ネットワーク構造指標: 記述統計（因果推論なし）",
            },
            "collaboration": {
                "patronage": round(score.patronage, 4),
                "studio_fe_exposure": round(score.studio_fe_exposure, 4),
                "interpretation": "協業環境指標: 協力関係・スタジオ環境の質",
            },
            "combined": {
                "iv_score": round(score.iv_score, 4),
                "method": "PCA_PC1",
                "variance_explained": core.pca_variance_explained,
                "interpretation": "総合便利指標（OPS的）: PCA第1主成分による重み付き合算",
            },
        }

        if pid in supp.centrality:
            result_entry["centrality"] = {
                k: round(v, 4) for k, v in supp.centrality[pid].items()
            }

        if pid in supp.decay_results:
            result_entry["engagement_decay"] = supp.decay_results[pid]

        if pid in supp.role_profiles:
            result_entry["primary_role"] = supp.role_profiles[pid]["primary_category"]
            result_entry["total_credits"] = supp.role_profiles[pid]["total_credits"]

        if pid in supp.career_data:
            career_snapshot = supp.career_data[pid]
            if career_snapshot.total_credits > 0:
                result_entry["career"] = {
                    "first_year": career_snapshot.first_year,
                    "latest_year": career_snapshot.latest_year,
                    "active_years": career_snapshot.active_years,
                    "highest_stage": career_snapshot.highest_stage,
                    "highest_roles": career_snapshot.highest_roles,
                    "peak_year": career_snapshot.peak_year,
                    "peak_credits": career_snapshot.peak_credits,
                }

        if pid in supp.network_density:
            nd = supp.network_density[pid]
            result_entry["network"] = {
                "collaborators": nd.collaborator_count,
                "unique_anime": nd.unique_anime,
                "hub_score": nd.hub_score,
            }

        if supp.growth_data and pid in supp.growth_data:
            gd = supp.growth_data[pid]
            result_entry["growth"] = {
                "trend": gd.trend,
                "activity_ratio": gd.activity_ratio,
                "recent_credits": gd.recent_credits,
            }

        if pid in supp.versatility:
            v = supp.versatility[pid]
            result_entry["versatility"] = {
                "score": v.versatility_score,
                "categories": v.category_count,
                "roles": v.role_count,
            }

        person_credits = credits_by_person.get(pid, [])
        birank_factors = explain_authority(pid, credits, anime_map, _person_credits=person_credits)
        patronage_factors = explain_trust(
            pid,
            credits,
            anime_map,
            _person_credits=person_credits,
            _anime_directors=anime_directors,
        )
        person_fe_factors = explain_skill(
            pid, credits, anime_map, _person_credits=person_credits
        )
        if birank_factors or patronage_factors or person_fe_factors:
            result_entry["breakdown"] = {}
            if birank_factors:
                result_entry["breakdown"]["birank"] = birank_factors[:5]
            if patronage_factors:
                result_entry["breakdown"]["patronage"] = patronage_factors[:5]
            if person_fe_factors:
                result_entry["breakdown"]["person_fe"] = person_fe_factors[:5]

        results.append(result_entry)

    results.sort(key=lambda x: x["iv_score"], reverse=True)
    return results
