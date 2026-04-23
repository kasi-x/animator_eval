"""Phase 7: Result Assembly — build comprehensive result dictionaries."""

import datetime
import math
from collections import defaultdict

import numpy as np
import structlog

from src.analysis.explain import explain_authority, explain_skill, explain_trust
from src.analysis.gold_writer import GoldWriter
from src.analysis.scoring.integrated_value import compute_studio_exposure
from src.models import ScoreResult
from src.pipeline_phases.context import PipelineContext
from src.utils.role_groups import DIRECTOR_ROLES

logger = structlog.get_logger()


def assemble_result_entries(context: PipelineContext) -> None:
    """Build comprehensive result dictionaries for each person.

    This phase:
    1. Creates ScoreResult objects and writes to gold.duckdb
    2. Builds rich result dictionaries with all computed metrics
    3. Adds score breakdowns (top contributing factors)
    4. Sorts results by iv_score descending

    Updates context fields:
        - results: List[dict] with all person results
    """
    logger.info("step_start", step="result_assembly")

    # Collect all person IDs from any scoring component
    all_person_ids = (
        set(context.person_fe)
        | set(context.birank_person_scores)
        | set(context.iv_scores)
    )

    context.results = []

    # Pre-group credits by person_id: O(m) instead of O(n*m) per explain call
    credits_by_person: dict[str, list] = defaultdict(list)
    for credit in context.credits:
        credits_by_person[credit.person_id].append(credit)

    # Pre-build anime_directors index for explain_trust
    anime_directors: dict[str, set[str]] = defaultdict(set)
    for credit in context.credits:
        if credit.role in DIRECTOR_ROLES:
            anime_directors[credit.anime_id].add(credit.person_id)

    # Pre-compute studio exposure once (time-weighted average, consistent with IV)
    studio_exposure = compute_studio_exposure(
        context.person_fe,
        context.studio_fe,
        studio_assignments=context.studio_assignments,
    )

    # Pre-compute person_fe SE from AKM residuals (per-person clustered SE)
    # Uses per-person residual std instead of global sigma to handle
    # heteroscedasticity (episode-level vs through-role credits have
    # different residual variance).
    person_fe_se: dict[str, float] = {}
    person_n_obs: dict[str, int] = {}
    if context.akm_result and context.akm_result.residuals:
        person_resids: dict[str, list[float]] = defaultdict(list)
        for (pid, _aid), resid in context.akm_result.residuals.items():
            person_resids[pid].append(resid)
        for pid, resids in person_resids.items():
            n = len(resids)
            person_n_obs[pid] = n
            if n >= 2:
                sigma_i = float(np.std(resids, ddof=1))
                person_fe_se[pid] = sigma_i / math.sqrt(n)
            # n=1: no SE estimable

    # Pre-compute all scores and batch DB writes
    scores_by_pid: dict[str, ScoreResult] = {}
    score_rows = []
    for pid in all_person_ids:
        # Knowledge spanner metrics
        ks = context.knowledge_spanner_scores.get(pid)

        score = ScoreResult(
            person_id=pid,
            person_fe=context.person_fe.get(pid, 0.0),
            studio_fe_exposure=studio_exposure.get(pid, 0.0),
            birank=context.birank_person_scores.get(pid, 0.0),
            patronage=context.patronage_scores.get(pid, 0.0),
            dormancy=context.dormancy_scores.get(pid, 1.0),
            awcc=ks.awcc if ks else 0.0,
            ndi=ks.ndi if ks else 0.0,
            iv_score=context.iv_scores.get(pid, 0.0),
            iv_score_historical=context.iv_scores_historical.get(pid, 0.0),
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

    # Write scores to gold.duckdb (atomic swap — DuckDB is now canonical GOLD store)
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

    if context.career_tracks:
        logger.info("career_tracks_computed", count=len(context.career_tracks))

    for pid in all_person_ids:
        score = scores_by_pid[pid]

        # Build result entry
        node_data = (
            context.person_anime_graph.nodes.get(pid, {})
            if context.person_anime_graph
            else {}
        )

        # Peer boost
        peer_boost = 0.0
        if context.peer_effect_result and context.peer_effect_result.person_peer_boost:
            peer_boost = context.peer_effect_result.person_peer_boost.get(pid, 0.0)

        # person_fe standard error (analytical CI for compensation basis)
        n_obs = person_n_obs.get(pid, 0)
        se = person_fe_se.get(pid)

        result_entry = {
            "person_id": pid,
            "name": node_data.get("name", ""),
            "name_ja": node_data.get("name_ja", ""),
            "name_en": node_data.get("name_en", ""),
            "career_track": context.career_tracks.get(pid, "multi_track"),
            # 8-component structural scores
            "iv_score": round(score.iv_score, 4),
            "person_fe": round(score.person_fe, 4),
            "studio_fe_exposure": round(score.studio_fe_exposure, 4),
            "birank": round(score.birank, 4),
            "patronage": round(score.patronage, 4),
            "dormancy": round(score.dormancy, 4),
            "awcc": round(score.awcc, 4),
            "ndi": round(score.ndi, 4),
            "career_friction": round(context.career_friction.get(pid, 0), 4),
            "peer_boost": round(peer_boost, 4),
            "iv_score_historical": round(score.iv_score_historical, 4),
            "person_fe_se": round(se, 4) if se is not None else None,
            "person_fe_n_obs": n_obs,
        }

        # 3-layer score structure + combined IV
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
                "variance_explained": context.pca_variance_explained,
                "interpretation": "総合便利指標（OPS的）: PCA第1主成分による重み付き合算",
            },
        }

        # Add centrality metrics (if available)
        if pid in context.centrality:
            result_entry["centrality"] = {
                k: round(v, 4) for k, v in context.centrality[pid].items()
            }

        # Add engagement decay (if detected)
        if pid in context.decay_results:
            result_entry["engagement_decay"] = context.decay_results[pid]

        # Add role profile
        if pid in context.role_profiles:
            result_entry["primary_role"] = context.role_profiles[pid][
                "primary_category"
            ]
            result_entry["total_credits"] = context.role_profiles[pid]["total_credits"]

        # Add career data
        if pid in context.career_data:
            career_snapshot = context.career_data[pid]
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

        # Add network density (dataclass instance -> dict fields)
        if pid in context.network_density:
            nd = context.network_density[pid]
            result_entry["network"] = {
                "collaborators": nd.collaborator_count,
                "unique_anime": nd.unique_anime,
                "hub_score": nd.hub_score,
            }

        # Add growth trend (dataclass instance -> dict fields)
        if context.growth_data and pid in context.growth_data:
            gd = context.growth_data[pid]
            result_entry["growth"] = {
                "trend": gd.trend,
                "activity_ratio": gd.activity_ratio,
                "recent_credits": gd.recent_credits,
            }

        # Add versatility (dataclass instance -> dict fields)
        if pid in context.versatility:
            v = context.versatility[pid]
            result_entry["versatility"] = {
                "score": v.versatility_score,
                "categories": v.category_count,
                "roles": v.role_count,
            }

        # Add score breakdown (top contributing factors)
        person_credits = credits_by_person.get(pid, [])
        # birank explanation: top works contributing to network centrality
        birank_factors = explain_authority(
            pid, context.credits, context.anime_map, _person_credits=person_credits
        )
        # patronage explanation: director backing relationships
        patronage_factors = explain_trust(
            pid,
            context.credits,
            context.anime_map,
            _person_credits=person_credits,
            _anime_directors=anime_directors,
        )
        # person_fe explanation: recent high-quality works
        person_fe_factors = explain_skill(
            pid, context.credits, context.anime_map, _person_credits=person_credits
        )
        if birank_factors or patronage_factors or person_fe_factors:
            result_entry["breakdown"] = {}
            if birank_factors:
                result_entry["breakdown"]["birank"] = birank_factors[:5]
            if patronage_factors:
                result_entry["breakdown"]["patronage"] = patronage_factors[:5]
            if person_fe_factors:
                result_entry["breakdown"]["person_fe"] = person_fe_factors[:5]

        context.results.append(result_entry)

    # Sort by iv_score descending
    context.results.sort(key=lambda x: x["iv_score"], reverse=True)
