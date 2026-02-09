"""統合パイプライン — データ収集→名寄せ→グラフ構築→スコアリング→出力.

全フェーズを順次実行するオーケストレーター。
"""

import json
import time
from datetime import datetime

import structlog

from src.analysis.anime_stats import compute_anime_stats
from src.analysis.bridges import detect_bridges
from src.analysis.collaboration_strength import compute_collaboration_strength
from src.analysis.genre_affinity import compute_genre_affinity
from src.analysis.mentorship import infer_mentorships, build_mentorship_tree
from src.analysis.milestones import compute_milestones
from src.analysis.network_evolution import compute_network_evolution
from src.analysis.productivity import compute_productivity
from src.analysis.confidence import batch_compute_confidence
from src.analysis.crossval import cross_validate_scores
from src.analysis.graphml_export import export_graphml
from src.analysis.growth import compute_growth_trends
from src.analysis.network_density import compute_network_density
from src.analysis.outliers import detect_outliers
from src.analysis.decade_analysis import compute_decade_analysis
from src.analysis.person_tags import compute_person_tags
from src.analysis.role_flow import compute_role_flow
from src.analysis.time_series import compute_time_series
from src.analysis.seasonal import compute_seasonal_trends
from src.analysis.studio import compute_studio_analysis
from src.analysis.team_composition import analyze_team_patterns
from src.analysis.versatility import compute_versatility
from src.analysis.influence import compute_influence_tree
from src.analysis.transitions import compute_role_transitions
from src.analysis.career import batch_career_analysis
from src.analysis.explain import explain_authority, explain_trust, explain_skill
from src.analysis.normalize import normalize_all_axes
from src.analysis.entity_resolution import resolve_all
from src.analysis.stability import compare_scores
from src.validation import validate_all
from src.analysis.circles import find_director_circles
from src.analysis.graph import (
    create_person_anime_network,
    create_person_collaboration_network,
    determine_primary_role_for_each_person,
    calculate_network_centrality_scores,
    compute_graph_summary,
)
from src.analysis.pagerank import compute_authority_scores
from src.analysis.skill import compute_skill_scores
from src.analysis.trust import compute_trust_scores, detect_engagement_decay, DIRECTOR_ROLES
from src.database import (
    get_connection,
    init_db,
    load_all_anime,
    load_all_credits,
    load_all_persons,
    record_pipeline_run,
    save_score_history,
    upsert_score,
)
from src.models import Anime, Credit, ScoreResult
from src.utils.config import JSON_DIR
from src.utils.performance import get_monitor, reset_monitor

logger = structlog.get_logger()


def run_scoring_pipeline(visualize: bool = False, dry_run: bool = False) -> list[dict]:
    """スコアリングパイプラインを実行する.

    前提: DBにクレジットデータが既に存在すること。

    Args:
        visualize: 可視化を生成するか
        dry_run: True の場合、データ検証のみ行いスコア計算は行わない
    """
    # Initialize performance monitoring
    reset_monitor()
    monitor = get_monitor()
    monitor.record_memory("pipeline_start")

    t_start = time.monotonic()
    conn = get_connection()
    init_db(conn)

    with monitor.measure("data_loading"):
        persons = load_all_persons(conn)
        anime_list = load_all_anime(conn)
        credits = load_all_credits(conn)

    monitor.increment_counter("persons_loaded", len(persons))
    monitor.increment_counter("anime_loaded", len(anime_list))
    monitor.increment_counter("credits_loaded", len(credits))
    monitor.record_memory("after_data_load")

    if not credits:
        logger.warning("No credits in DB. Run scrapers first.")
        conn.close()
        return []

    logger.info(
        "data_loaded",
        persons=len(persons),
        anime=len(anime_list),
        credits=len(credits),
    )

    # Step 0: データバリデーション
    logger.info("step_start", step="validation")
    with monitor.measure("validation"):
        validation = validate_all(conn)
    if not validation.passed:
        for err in validation.errors:
            logger.error("validation_error", message=err)
    for warn in validation.warnings:
        logger.warning("validation_warning", message=warn)

    if dry_run:
        conn.close()
        elapsed = time.monotonic() - t_start
        logger.info(
            "dry_run_complete",
            elapsed=round(elapsed, 2),
            persons=len(persons),
            anime=len(anime_list),
            credits=len(credits),
            validation_passed=validation.passed,
            errors=len(validation.errors),
            warnings=len(validation.warnings),
        )
        return []

    # Step 1: 名寄せ
    logger.info("step_start", step="entity_resolution")
    with monitor.measure("entity_resolution"):
        canonical_map = resolve_all(persons)

        # クレジットの person_id を正規IDに置換
        if canonical_map:
            resolved_credits = []
            for c in credits:
                new_pid = canonical_map.get(c.person_id, c.person_id)
                resolved_credits.append(
                    Credit(
                        person_id=new_pid,
                        anime_id=c.anime_id,
                        role=c.role,
                        episode=c.episode,
                        source=c.source,
                    )
                )
            credits = resolved_credits
            logger.info("person_ids_resolved", count=len(canonical_map))
            monitor.increment_counter("persons_resolved", len(canonical_map))

    # Step 2: グラフ構築
    logger.info("step_start", step="graph_construction")
    with monitor.measure("graph_construction"):
        graph = create_person_anime_network(persons, anime_list, credits)
    monitor.record_memory("after_graph_build")

    # Step 3: Authority (PageRank)
    logger.info("step_start", step="authority_pagerank")
    with monitor.measure("authority_pagerank"):
        authority_scores = compute_authority_scores(graph)
    monitor.increment_counter("persons_with_authority", len(authority_scores))

    # Step 4: Trust (継続起用)
    logger.info("step_start", step="trust_repeat_engagement")
    anime_map: dict[str, Anime] = {a.id: a for a in anime_list}
    with monitor.measure("trust_scores"):
        trust_scores = compute_trust_scores(credits, anime_map)
    monitor.increment_counter("persons_with_trust", len(trust_scores))

    # Step 5: Skill (OpenSkill)
    logger.info("step_start", step="skill_openskill")
    with monitor.measure("skill_scores"):
        skill_scores = compute_skill_scores(credits, anime_map)
    monitor.increment_counter("persons_with_skill", len(skill_scores))

    # Step 5.1: Score Normalization (0-100)
    logger.info("step_start", step="score_normalization")
    with monitor.measure("score_normalization"):
        authority_scores, trust_scores, skill_scores = normalize_all_axes(
            authority_scores, trust_scores, skill_scores,
        )
    monitor.record_memory("after_scoring")

    # Step 5.5: Engagement Decay Detection
    logger.info("step_start", step="engagement_decay")
    with monitor.measure("engagement_decay"):
        director_ids = {
            c.person_id for c in credits if c.role in DIRECTOR_ROLES
        }
        decay_results: dict[str, list[dict]] = {}
        for pid in set(trust_scores) - director_ids:
            person_decays = []
            for dir_id in director_ids:
                decay = detect_engagement_decay(pid, dir_id, credits, anime_map)
                if decay.get("status") == "decayed":
                    person_decays.append({"director_id": dir_id, **decay})
            if person_decays:
                decay_results[pid] = person_decays
        logger.info("engagement_decay_detected", persons_with_decay=len(decay_results))

    # Step 5.6: Role Classification
    logger.info("step_start", step="role_classification")
    with monitor.measure("role_classification"):
        role_profiles = determine_primary_role_for_each_person(credits)

    # Step 5.7: Career Analysis
    logger.info("step_start", step="career_analysis")
    with monitor.measure("career_analysis"):
        career_data = batch_career_analysis(credits, anime_map)

    # Step 5.8: Director Circles
    logger.info("step_start", step="director_circles")
    with monitor.measure("director_circles"):
        circles = find_director_circles(credits, anime_map)
    monitor.increment_counter("circles_found", len(circles))

    # Step 5.95: Versatility
    logger.info("step_start", step="versatility")
    with monitor.measure("versatility"):
        versatility = compute_versatility(credits)

    # Step 5.9: Centrality Metrics (supplementary)
    logger.info("step_start", step="centrality_metrics")
    with monitor.measure("centrality_metrics"):
        collab_graph = create_person_collaboration_network(persons, credits)
        person_ids = {p.id for p in persons}
        centrality = calculate_network_centrality_scores(collab_graph, person_ids)
    monitor.record_memory("after_centrality")

    # Step 5.95: Network density
    logger.info("step_start", step="network_density")
    with monitor.measure("network_density"):
        network_density = compute_network_density(credits)

    # Step 5.96: Growth trends (pre-compute for result entries)
    logger.info("step_start", step="growth_trends_precompute")
    with monitor.measure("growth_trends"):
        growth_data = compute_growth_trends(credits, anime_map)

    # Step 6: 統合スコアの算出と保存
    logger.info("step_start", step="composite_scores")
    all_person_ids = set(authority_scores) | set(trust_scores) | set(skill_scores)

    results = []
    for pid in all_person_ids:
        score = ScoreResult(
            person_id=pid,
            authority=authority_scores.get(pid, 0.0),
            trust=trust_scores.get(pid, 0.0),
            skill=skill_scores.get(pid, 0.0),
        )
        upsert_score(conn, score)
        save_score_history(conn, score)

        node_data = graph.nodes.get(pid, {})
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
        # Centrality metrics (if available)
        if pid in centrality:
            result_entry["centrality"] = {
                k: round(v, 4) for k, v in centrality[pid].items()
            }
        # Engagement decay (if detected)
        if pid in decay_results:
            result_entry["engagement_decay"] = decay_results[pid]
        # Role profile
        if pid in role_profiles:
            result_entry["primary_role"] = role_profiles[pid]["primary_category"]
            result_entry["total_credits"] = role_profiles[pid]["total_credits"]
        # Career data
        if pid in career_data:
            career_snapshot = career_data[pid]
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
        # Network density
        if pid in network_density:
            nd = network_density[pid]
            result_entry["network"] = {
                "collaborators": nd["collaborator_count"],
                "unique_anime": nd["unique_anime"],
                "hub_score": nd["hub_score"],
            }
        # Growth trend
        if growth_data and pid in growth_data:
            gd = growth_data[pid]
            result_entry["growth"] = {
                "trend": gd["trend"],
                "activity_ratio": gd["activity_ratio"],
                "recent_credits": gd["recent_credits"],
            }
        # Versatility
        if pid in versatility:
            v = versatility[pid]
            result_entry["versatility"] = {
                "score": v["versatility_score"],
                "categories": v["category_count"],
                "roles": v["role_count"],
            }
        # Score breakdown (top contributing factors)
        auth_factors = explain_authority(pid, credits, anime_map)
        trust_factors = explain_trust(pid, credits, anime_map)
        skill_factors = explain_skill(pid, credits, anime_map)
        if auth_factors or trust_factors or skill_factors:
            result_entry["breakdown"] = {}
            if auth_factors:
                result_entry["breakdown"]["authority"] = auth_factors[:5]
            if trust_factors:
                result_entry["breakdown"]["trust"] = trust_factors[:5]
            if skill_factors:
                result_entry["breakdown"]["skill"] = skill_factors[:5]
        results.append(result_entry)

    conn.commit()
    conn.close()

    # ソート（composite降順）
    results.sort(key=lambda x: x["composite"], reverse=True)

    # パーセンタイルランク計算
    n = len(results)
    if n > 1:
        for axis in ("authority", "trust", "skill", "composite"):
            sorted_vals = sorted(r[axis] for r in results)
            for r in results:
                val = r[axis]
                rank = sum(1 for v in sorted_vals if v <= val)
                r[f"{axis}_pct"] = round(rank / n * 100, 1)
    elif n == 1:
        for r in results:
            for axis in ("authority", "trust", "skill", "composite"):
                r[f"{axis}_pct"] = 100.0

    # Score confidence intervals
    logger.info("step_start", step="confidence")
    with monitor.measure("confidence_intervals"):
        _sources_per_person: dict[str, set] = {}
        for c in credits:
            if c.person_id not in _sources_per_person:
                _sources_per_person[c.person_id] = set()
            _sources_per_person[c.person_id].add(c.source)
        source_counts = {pid: len(srcs) for pid, srcs in _sources_per_person.items()}
        batch_compute_confidence(results, sources_per_person=source_counts)

    # Score stability check (compare with previous run)
    JSON_DIR.mkdir(parents=True, exist_ok=True)
    output_path = JSON_DIR / "scores.json"
    with monitor.measure("stability_check"):
        stability = compare_scores(results, output_path)
    if stability["significant_changes"]:
        for sc in stability["significant_changes"][:5]:
            logger.warning(
                "score_shift",
                person=sc["name"],
                old=sc["old_composite"],
                new=sc["new_composite"],
                delta=sc["delta"],
            )

    # JSON 出力
    logger.info("step_start", step="json_export")
    monitor.record_memory("before_export")
    with monitor.measure("json_export"):
        with open(output_path, "w") as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
    logger.info("scores_saved", path=str(output_path), persons=len(results))

    # Director circles 出力
    if circles:
        from dataclasses import asdict

        circles_path = JSON_DIR / "circles.json"
        # person_id → name のルックアップを構築
        pid_to_name = {r["person_id"]: r["name"] or r["person_id"] for r in results}
        circles_output = {}
        for dir_id, circle in circles.items():
            circle_dict = asdict(circle)
            circles_output[dir_id] = {
                "director_name": pid_to_name.get(dir_id, dir_id),
                **circle_dict,
                "members": [
                    {**member, "name": pid_to_name.get(member["person_id"], member["person_id"])}
                    for member in circle_dict["members"]
                ],
            }
        with open(circles_path, "w") as f:
            json.dump(circles_output, f, indent=2, ensure_ascii=False)
        logger.info("circles_saved", path=str(circles_path), directors=len(circles_output))

    # Anime statistics 出力
    composite_scores = {r["person_id"]: r["composite"] for r in results}
    anime_quality_statistics = compute_anime_stats(credits, anime_map, composite_scores)
    if anime_quality_statistics:
        anime_stats_output_path = JSON_DIR / "anime_stats.json"
        with open(anime_stats_output_path, "w") as f:
            json.dump(anime_quality_statistics, f, indent=2, ensure_ascii=False)
        logger.info("anime_stats_saved", path=str(anime_stats_output_path), anime=len(anime_quality_statistics))

    # Studio analysis 出力
    studio_performance_analysis = compute_studio_analysis(credits, anime_map, composite_scores)
    if studio_performance_analysis:
        studio_output_path = JSON_DIR / "studios.json"
        with open(studio_output_path, "w") as f:
            json.dump(studio_performance_analysis, f, indent=2, ensure_ascii=False)
        logger.info("studios_saved", path=str(studio_output_path), studios=len(studio_performance_analysis))

    # Seasonal trends 出力
    seasonal_activity_patterns = compute_seasonal_trends(credits, anime_map, composite_scores)
    if seasonal_activity_patterns.get("by_season"):
        seasonal_output_path = JSON_DIR / "seasonal.json"
        with open(seasonal_output_path, "w") as f:
            json.dump(seasonal_activity_patterns, f, indent=2, ensure_ascii=False)
        logger.info("seasonal_saved", path=str(seasonal_output_path))

    # Collaboration strength 出力
    logger.info("step_start", step="collaboration_strength")
    with monitor.measure("collaboration_strength"):
        strongest_collaboration_pairs = compute_collaboration_strength(
            credits, anime_map, min_shared=2, person_scores=composite_scores,
        )
    if strongest_collaboration_pairs:
        collaborations_output_path = JSON_DIR / "collaborations.json"
        with open(collaborations_output_path, "w") as f:
            json.dump(strongest_collaboration_pairs[:500], f, indent=2, ensure_ascii=False)
        logger.info("collaborations_saved", path=str(collaborations_output_path), pairs=len(strongest_collaboration_pairs))

    # Outlier detection
    logger.info("step_start", step="outlier_detection")
    outlier_data = detect_outliers(results)
    if outlier_data["total_outliers"] > 0:
        outlier_path = JSON_DIR / "outliers.json"
        with open(outlier_path, "w") as f:
            json.dump(outlier_data, f, indent=2, ensure_ascii=False)
        logger.info("outliers_saved", path=str(outlier_path), total=outlier_data["total_outliers"])

    # Team composition analysis
    logger.info("step_start", step="team_composition")
    team_data = analyze_team_patterns(credits, anime_map, person_scores=composite_scores)
    if team_data["total_high_score"] > 0:
        team_path = JSON_DIR / "teams.json"
        with open(team_path, "w") as f:
            json.dump(team_data, f, indent=2, ensure_ascii=False)
        logger.info("teams_saved", path=str(team_path), high_score=team_data["total_high_score"])

    # Growth trends (output to JSON — data already computed above)
    if growth_data:
        growth_path = JSON_DIR / "growth.json"
        # Summarize trend counts for the JSON
        trend_counts: dict[str, int] = {}
        for gd in growth_data.values():
            trend_counts[gd["trend"]] = trend_counts.get(gd["trend"], 0) + 1
        growth_output = {
            "trend_summary": trend_counts,
            "total_persons": len(growth_data),
            "persons": {
                pid: data for pid, data in sorted(
                    growth_data.items(),
                    key=lambda x: x[1].get("activity_ratio", 0),
                    reverse=True,
                )[:200]
            },
        }
        with open(growth_path, "w") as f:
            json.dump(growth_output, f, indent=2, ensure_ascii=False)
        logger.info("growth_saved", path=str(growth_path), persons=len(growth_data))

    # GraphML export
    logger.info("step_start", step="graphml_export")
    scores_for_graphml = {
        r["person_id"]: {
            "authority": r["authority"],
            "trust": r["trust"],
            "skill": r["skill"],
            "composite": r["composite"],
            "primary_role": r.get("primary_role", ""),
        }
        for r in results
    }
    export_graphml(persons, credits, person_scores=scores_for_graphml)

    # Time series
    logger.info("step_start", step="time_series")
    credit_timeline_by_year = compute_time_series(credits, anime_map)
    if credit_timeline_by_year["years"]:
        time_series_output_path = JSON_DIR / "time_series.json"
        with open(time_series_output_path, "w") as f:
            json.dump(credit_timeline_by_year, f, indent=2, ensure_ascii=False)
        logger.info("time_series_saved", path=str(time_series_output_path), years=len(credit_timeline_by_year["years"]))

    # Decade analysis
    logger.info("step_start", step="decade_analysis")
    decade_data = compute_decade_analysis(credits, anime_map, person_scores=composite_scores)
    if decade_data["decades"]:
        decade_path = JSON_DIR / "decades.json"
        with open(decade_path, "w") as f:
            json.dump(decade_data, f, indent=2, ensure_ascii=False)
        logger.info("decades_saved", path=str(decade_path), decades=len(decade_data["decades"]))

    # Person tags (auto-labeling)
    logger.info("step_start", step="person_tags")
    person_tags = compute_person_tags(results)
    if person_tags:
        # Add tags to result entries
        for r in results:
            pid = r["person_id"]
            if pid in person_tags:
                r["tags"] = person_tags[pid]

        # Also save standalone tags file
        tags_path = JSON_DIR / "tags.json"
        with open(tags_path, "w") as f:
            # Summary: count per tag
            tag_summary: dict[str, int] = {}
            for t_list in person_tags.values():
                for t in t_list:
                    tag_summary[t] = tag_summary.get(t, 0) + 1
            json.dump(
                {"tag_summary": dict(sorted(tag_summary.items(), key=lambda x: -x[1])), "person_tags": person_tags},
                f, indent=2, ensure_ascii=False,
            )
        logger.info("tags_saved", path=str(tags_path), unique_tags=len(tag_summary))

    # Re-save scores.json with tags added
    with open(output_path, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    # Role flow (Sankey data)
    logger.info("step_start", step="role_flow")
    role_flow = compute_role_flow(credits, anime_map)
    if role_flow["total_transitions"] > 0:
        flow_path = JSON_DIR / "role_flow.json"
        with open(flow_path, "w") as f:
            json.dump(role_flow, f, indent=2, ensure_ascii=False)
        logger.info("role_flow_saved", path=str(flow_path), transitions=role_flow["total_transitions"])

    # Bridge detection
    logger.info("step_start", step="bridge_detection")
    # Use clusters if available, otherwise let detect_bridges compute its own
    bridge_data = detect_bridges(credits)
    if bridge_data["bridge_persons"]:
        bridge_path = JSON_DIR / "bridges.json"
        with open(bridge_path, "w") as f:
            json.dump(bridge_data, f, indent=2, ensure_ascii=False)
        logger.info("bridges_saved", path=str(bridge_path), bridges=len(bridge_data["bridge_persons"]))

    # Mentorship inference
    logger.info("step_start", step="mentorship_inference")
    mentorship_data = infer_mentorships(credits, anime_map, min_shared_works=3)
    if mentorship_data:
        mentorship_tree = build_mentorship_tree(mentorship_data)
        mentorship_output = {
            "mentorships": mentorship_data[:200],
            "tree": mentorship_tree,
            "total": len(mentorship_data),
        }
        mentorship_path = JSON_DIR / "mentorships.json"
        with open(mentorship_path, "w") as f:
            json.dump(mentorship_output, f, indent=2, ensure_ascii=False)
        logger.info("mentorships_saved", path=str(mentorship_path), total=len(mentorship_data))

    # Career milestones
    logger.info("step_start", step="milestones")
    milestones_data = compute_milestones(credits, anime_map)
    if milestones_data:
        milestones_path = JSON_DIR / "milestones.json"
        with open(milestones_path, "w") as f:
            json.dump(milestones_data, f, indent=2, ensure_ascii=False)
        logger.info("milestones_saved", path=str(milestones_path), persons=len(milestones_data))

    # Network evolution
    logger.info("step_start", step="network_evolution")
    network_growth_over_decades = compute_network_evolution(credits, anime_map)
    if network_growth_over_decades["years"]:
        network_evolution_output_path = JSON_DIR / "network_evolution.json"
        with open(network_evolution_output_path, "w") as f:
            json.dump(network_growth_over_decades, f, indent=2, ensure_ascii=False)
        logger.info("network_evolution_saved", path=str(network_evolution_output_path), years=len(network_growth_over_decades["years"]))

    # Genre affinity
    logger.info("step_start", step="genre_affinity")
    person_genre_specialization = compute_genre_affinity(credits, anime_map)
    if person_genre_specialization:
        genre_affinity_output_path = JSON_DIR / "genre_affinity.json"
        # Save top 200 by total_credits
        top_genre_specialists = dict(
            sorted(person_genre_specialization.items(), key=lambda x: x[1]["total_credits"], reverse=True)[:200]
        )
        with open(genre_affinity_output_path, "w") as f:
            json.dump(top_genre_specialists, f, indent=2, ensure_ascii=False)
        logger.info("genre_affinity_saved", path=str(genre_affinity_output_path), persons=len(person_genre_specialization))

    # Productivity
    logger.info("step_start", step="productivity")
    person_productivity_metrics = compute_productivity(credits, anime_map)
    if person_productivity_metrics:
        productivity_output_path = JSON_DIR / "productivity.json"
        most_productive_persons = dict(
            sorted(person_productivity_metrics.items(), key=lambda x: x[1]["credits_per_year"], reverse=True)[:200]
        )
        with open(productivity_output_path, "w") as f:
            json.dump(most_productive_persons, f, indent=2, ensure_ascii=False)
        logger.info("productivity_saved", path=str(productivity_output_path), persons=len(person_productivity_metrics))

    # Role transitions 出力
    from dataclasses import asdict

    transitions = compute_role_transitions(credits, anime_map)
    # Convert dataclass objects to dicts for JSON serialization
    transitions_serializable = {
        "transitions": [asdict(t) for t in transitions["transitions"]],
        "career_paths": [asdict(p) for p in transitions["career_paths"]],
        "avg_time_to_stage": {
            stage: asdict(stats) for stage, stats in transitions["avg_time_to_stage"].items()
        },
        "total_persons_analyzed": transitions["total_persons_analyzed"],
    }

    if transitions["total_persons_analyzed"] > 0:
        trans_path = JSON_DIR / "transitions.json"
        with open(trans_path, "w") as f:
            json.dump(transitions_serializable, f, indent=2, ensure_ascii=False)
        logger.info("transitions_saved", path=str(trans_path), persons=transitions["total_persons_analyzed"])

    # Influence tree (mentor-mentee relationships)
    logger.info("step_start", step="influence_tree")
    influence = compute_influence_tree(credits, anime_map, person_scores=composite_scores)
    if influence["total_mentors"] > 0:
        influence_path = JSON_DIR / "influence.json"
        with open(influence_path, "w") as f:
            json.dump(influence, f, indent=2, ensure_ascii=False)
        logger.info(
            "influence_saved",
            path=str(influence_path),
            mentors=influence["total_mentors"],
            mentees=influence["total_mentees"],
        )

    # Cross-validation (score stability measurement) — skip for very small datasets
    if len(credits) >= 20:
        logger.info("step_start", step="cross_validation")
        with monitor.measure("cross_validation"):
            cv_folds = 3 if len(credits) < 100 else 5
            crossval_result = cross_validate_scores(
            persons, anime_list, credits, n_folds=cv_folds, holdout_ratio=0.2,
        )
        crossval_path = JSON_DIR / "crossval.json"
        with open(crossval_path, "w") as f:
            json.dump(crossval_result, f, indent=2, ensure_ascii=False)
        logger.info(
            "crossval_saved",
            path=str(crossval_path),
            avg_correlation=crossval_result["avg_rank_correlation"],
            avg_top10=crossval_result["avg_top10_overlap"],
        )
    else:
        crossval_result = {"avg_rank_correlation": 0, "avg_top10_overlap": 0}

    # Top 20 表示
    logger.info("step_start", step="top_20_composite_scores")
    for i, r in enumerate(results[:20], 1):
        logger.info(
            "top_score",
            rank=i,
            name=r["name"] or r["person_id"],
            authority=r["authority"],
            trust=r["trust"],
            skill=r["skill"],
            composite=r["composite"],
        )

    # Step 7: 可視化（オプション）
    if visualize and results:
        logger.info("step_start", step="visualization")
        with monitor.measure("visualization"):
            try:
                from src.analysis.visualize import (
                    plot_anime_stats,
                    plot_bridge_analysis,
                    plot_collaboration_network,
                    plot_collaboration_strength,
                    plot_crossval_stability,
                    plot_decade_comparison,
                    plot_genre_affinity,
                    plot_growth_trends,
                    plot_influence_tree,
                    plot_milestone_summary,
                    plot_network_evolution,
                    plot_outlier_summary,
                    plot_performance_metrics,
                    plot_productivity_distribution,
                    plot_role_flow_sankey,
                    plot_score_distribution,
                    plot_seasonal_trends,
                    plot_studio_comparison,
                    plot_tag_summary,
                    plot_time_series,
                    plot_top_persons_radar,
                    plot_transition_heatmap,
                )

                scores_dict = {r["person_id"]: r for r in results}
                plot_score_distribution(scores_dict)
                plot_top_persons_radar(results, top_n=min(10, len(results)))

                composite_scores = {r["person_id"]: r["composite"] for r in results}
                plot_collaboration_network(
                    collab_graph, composite_scores, top_n=min(50, len(results))
                )

                # Growth trend chart
                if growth_data:
                    trend_counts: dict[str, int] = {}
                    for gd in growth_data.values():
                        trend_counts[gd["trend"]] = trend_counts.get(gd["trend"], 0) + 1
                    plot_growth_trends({"trend_summary": trend_counts})

                # Network evolution chart
                if network_growth_over_decades.get("years"):
                    plot_network_evolution(network_growth_over_decades)

                # Decade comparison chart
                if decade_data.get("decades"):
                    plot_decade_comparison(decade_data)

                # Role flow chart
                if role_flow.get("links"):
                    plot_role_flow_sankey(role_flow)

                # Time series chart
                if credit_timeline_by_year.get("years"):
                    plot_time_series(credit_timeline_by_year)

                # Productivity chart
                if person_productivity_metrics:
                    plot_productivity_distribution(person_productivity_metrics)

                # Influence tree chart
                if influence.get("total_mentors", 0) > 0:
                    plot_influence_tree(influence)

                # Milestone summary chart
                if milestones_data:
                    plot_milestone_summary(milestones_data)

                # Seasonal trends chart
                if seasonal_activity_patterns.get("by_season"):
                    plot_seasonal_trends(seasonal_activity_patterns)

                # Bridge analysis chart
                if bridge_data.get("bridge_persons"):
                    plot_bridge_analysis(bridge_data)

                # Collaboration strength chart
                if strongest_collaboration_pairs:
                    plot_collaboration_strength(strongest_collaboration_pairs)

                # Tag summary chart
                if person_tags:
                    tag_summary_data: dict[str, int] = {}
                    for t_list in person_tags.values():
                        for t in t_list:
                            tag_summary_data[t] = tag_summary_data.get(t, 0) + 1
                    plot_tag_summary({"tag_summary": tag_summary_data})

                # Studio comparison chart
                if studio_performance_analysis:
                    plot_studio_comparison(studio_performance_analysis)

                # Outlier chart
                if outlier_data.get("total_outliers", 0) > 0:
                    plot_outlier_summary(outlier_data)

                # Transition heatmap
                if transitions_serializable.get("transitions"):
                    plot_transition_heatmap(transitions_serializable)

                # Anime stats chart
                if anime_quality_statistics:
                    plot_anime_stats(anime_quality_statistics)

                # Genre affinity chart
                if person_genre_specialization:
                    plot_genre_affinity(person_genre_specialization)

                # Cross-validation stability chart
                if crossval_result.get("fold_results"):
                    plot_crossval_stability(crossval_result)

                # Performance metrics chart (generated last so it captures all timing data)
                perf_summary = monitor.get_summary()
                plot_performance_metrics(perf_summary)

                # Generate visual dashboard (HTML with embedded charts)
                from src.report import generate_visual_dashboard

                generate_visual_dashboard(
                    results,
                    png_dir=JSON_DIR.parent,
                    output_path=JSON_DIR.parent / "dashboard.html",
                )

            except Exception:
                logger.exception("Visualization failed (non-critical)")

    # Pipeline summary
    elapsed = time.monotonic() - t_start
    graph_summary = compute_graph_summary(collab_graph)

    # Record pipeline run
    run_conn = get_connection()
    init_db(run_conn)
    record_pipeline_run(run_conn, len(credits), len(results), elapsed, mode="full")
    run_conn.commit()
    run_conn.close()

    summary = {
        "generated_at": datetime.now().isoformat(),
        "elapsed_seconds": round(elapsed, 2),
        "mode": "full",
        "data": {
            "persons": len(persons),
            "anime": len(anime_list),
            "credits": len(credits),
            "scored_persons": len(results),
        },
        "scores": {
            "top_composite": results[0]["composite"] if results else 0,
            "median_composite": (
                results[len(results) // 2]["composite"] if results else 0
            ),
        },
        "graph": graph_summary,
        "crossval": {
            "avg_rank_correlation": crossval_result["avg_rank_correlation"],
            "avg_top10_overlap": crossval_result["avg_top10_overlap"],
        },
    }
    summary_path = JSON_DIR / "summary.json"
    with open(summary_path, "w") as f:
        json.dump(summary, f, indent=2, ensure_ascii=False)

    # Performance metrics summary
    monitor.record_memory("pipeline_end")
    perf_summary = monitor.get_summary()
    monitor.log_summary()

    # Save performance metrics to JSON
    perf_path = JSON_DIR / "performance.json"
    with open(perf_path, "w") as f:
        json.dump(perf_summary, f, indent=2, ensure_ascii=False)
    logger.info("performance_saved", path=str(perf_path))

    logger.info("pipeline_complete", elapsed=round(elapsed, 2), persons=len(results))

    return results


def main() -> None:
    """エントリーポイント."""
    import argparse

    from src.log import setup_logging

    setup_logging()

    parser = argparse.ArgumentParser(description="Animetor Eval パイプライン")
    parser.add_argument(
        "--visualize", action="store_true", help="可視化を生成"
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="データ検証のみ（スコア計算なし）"
    )
    args = parser.parse_args()

    run_scoring_pipeline(visualize=args.visualize, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
