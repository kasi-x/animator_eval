"""Phase 6: Supplementary Metrics — additional scoring dimensions."""
import structlog

from src.analysis.career import batch_career_analysis
from src.analysis.circles import find_director_circles
from src.analysis.graph import (
    calculate_network_centrality_scores,
    create_person_collaboration_network,
    determine_primary_role_for_each_person,
)
from src.analysis.growth import compute_growth_trends
from src.analysis.network_density import compute_network_density
from src.analysis.trust import detect_engagement_decay
from src.analysis.versatility import compute_versatility
from src.pipeline_phases.context import PipelineContext
from src.utils.role_groups import DIRECTOR_ROLES

logger = structlog.get_logger()


def compute_supplementary_metrics_phase(context: PipelineContext) -> None:
    """Compute supplementary metrics beyond the three core scores.

    Metrics computed:
    - Engagement decay (directors whose engagement with an animator has declined)
    - Role classification (primary role category per person)
    - Career analysis (career stage, trajectory, peaks)
    - Director circles (frequent collaborator groups)
    - Versatility (breadth of role categories and specific roles)
    - Centrality (betweenness, closeness, degree in collaboration network)
    - Network density (collaborator count, hub score)
    - Growth trends (career trajectory: rising, stable, declining)

    Args:
        context: Pipeline context

    Updates context fields:
        - decay_results: Dict[person_id, List[decay_event]]
        - role_profiles: Dict[person_id, role_info]
        - career_data: Dict[person_id, CareerSnapshot]
        - circles: Dict[director_id, DirectorCircle]
        - versatility: Dict[person_id, versatility_metrics]
        - centrality: Dict[person_id, centrality_metrics]
        - network_density: Dict[person_id, density_metrics]
        - growth_data: Dict[person_id, growth_metrics]
        - collaboration_graph: NetworkX graph (side effect)
    """
    # Engagement Decay Detection
    logger.info("step_start", step="engagement_decay")
    with context.monitor.measure("engagement_decay"):
        director_ids = {c.person_id for c in context.credits if c.role in DIRECTOR_ROLES}
        context.decay_results = {}
        for pid in set(context.trust_scores) - director_ids:
            person_decays = []
            for dir_id in director_ids:
                decay = detect_engagement_decay(pid, dir_id, context.credits, context.anime_map)
                if decay.get("status") == "decayed":
                    person_decays.append({"director_id": dir_id, **decay})
            if person_decays:
                context.decay_results[pid] = person_decays
        logger.info("engagement_decay_detected", persons_with_decay=len(context.decay_results))

    # Role Classification
    logger.info("step_start", step="role_classification")
    with context.monitor.measure("role_classification"):
        context.role_profiles = determine_primary_role_for_each_person(context.credits)

    # Career Analysis
    logger.info("step_start", step="career_analysis")
    with context.monitor.measure("career_analysis"):
        context.career_data = batch_career_analysis(context.credits, context.anime_map)

    # Director Circles
    logger.info("step_start", step="director_circles")
    with context.monitor.measure("director_circles"):
        context.circles = find_director_circles(context.credits, context.anime_map)
    context.monitor.increment_counter("circles_found", len(context.circles))

    # Versatility
    logger.info("step_start", step="versatility")
    with context.monitor.measure("versatility"):
        context.versatility = compute_versatility(context.credits)

    # Centrality Metrics (requires collaboration graph)
    logger.info("step_start", step="centrality_metrics")
    with context.monitor.measure("centrality_metrics"):
        context.collaboration_graph = create_person_collaboration_network(
            context.persons, context.credits
        )
        person_ids = {p.id for p in context.persons}
        context.centrality = calculate_network_centrality_scores(
            context.collaboration_graph, person_ids
        )
    context.monitor.record_memory("after_centrality")

    # Network Density
    logger.info("step_start", step="network_density")
    with context.monitor.measure("network_density"):
        context.network_density = compute_network_density(context.credits)

    # Growth Trends (pre-compute for result entries)
    logger.info("step_start", step="growth_trends_precompute")
    with context.monitor.measure("growth_trends"):
        context.growth_data = compute_growth_trends(context.credits, context.anime_map)
