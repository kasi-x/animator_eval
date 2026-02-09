"""Phase 7: Result Assembly — build comprehensive result dictionaries."""
import sqlite3

import structlog

from src.analysis.explain import explain_authority, explain_skill, explain_trust
from src.database import save_score_history, upsert_score
from src.models import ScoreResult
from src.pipeline_phases.context import PipelineContext

logger = structlog.get_logger()


def assemble_result_entries(context: PipelineContext, conn: sqlite3.Connection) -> None:
    """Build comprehensive result dictionaries for each person.

    This phase:
    1. Creates ScoreResult objects and saves to database
    2. Builds rich result dictionaries with all computed metrics
    3. Adds score breakdowns (top contributing factors)
    4. Sorts results by composite score descending

    Args:
        context: Pipeline context
        conn: Database connection

    Updates context fields:
        - results: List[dict] with all person results
        - composite_scores: Dict[person_id, composite_score]
    """
    logger.info("step_start", step="composite_scores")

    all_person_ids = (
        set(context.authority_scores) | set(context.trust_scores) | set(context.skill_scores)
    )

    context.results = []
    context.composite_scores = {}

    for pid in all_person_ids:
        # Create score object
        score = ScoreResult(
            person_id=pid,
            authority=context.authority_scores.get(pid, 0.0),
            trust=context.trust_scores.get(pid, 0.0),
            skill=context.skill_scores.get(pid, 0.0),
        )

        # Save to database
        upsert_score(conn, score)
        save_score_history(conn, score)

        # Track composite score
        context.composite_scores[pid] = score.composite

        # Build result entry
        node_data = context.person_anime_graph.nodes.get(pid, {})
        result_entry = {
            "person_id": pid,
            "name": node_data.get("name", ""),
            "name_ja": node_data.get("name_ja", ""),
            "name_en": node_data.get("name_en", ""),
            "authority": round(score.authority, 2),
            "trust": round(score.trust, 2),
            "skill": round(score.skill, 2),
            "composite": round(score.composite, 2),
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
            result_entry["primary_role"] = context.role_profiles[pid]["primary_category"]
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
        auth_factors = explain_authority(pid, context.credits, context.anime_map)
        trust_factors = explain_trust(pid, context.credits, context.anime_map)
        skill_factors = explain_skill(pid, context.credits, context.anime_map)
        if auth_factors or trust_factors or skill_factors:
            result_entry["breakdown"] = {}
            if auth_factors:
                result_entry["breakdown"]["authority"] = auth_factors[:5]
            if trust_factors:
                result_entry["breakdown"]["trust"] = trust_factors[:5]
            if skill_factors:
                result_entry["breakdown"]["skill"] = skill_factors[:5]

        context.results.append(result_entry)

    # Sort by composite score descending
    context.results.sort(key=lambda x: x["composite"], reverse=True)
