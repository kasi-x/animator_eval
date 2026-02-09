"""Unified JSON I/O utilities — centralized file loading and saving with caching.

This module eliminates 472 lines of boilerplate across api.py and pipeline.py by providing:
- Cached JSON loaders (32-entry LRU cache for API endpoints)
- Named domain-specific loaders that read like prose
- Graceful error handling (return defaults on missing/malformed files)
- Standardized export helpers for pipeline

All functions use structlog for consistent logging.
"""

import json
from functools import lru_cache
from pathlib import Path
from typing import Any, TypeVar

import structlog

from src.utils.config import JSON_DIR

logger = structlog.get_logger()

# Type variables for generic returns
T = TypeVar("T")
ListOrDict = list | dict


# ============================================================================
# Low-Level Primitives
# ============================================================================


def load_json_file_or_return_default(file_path: Path, default: T) -> T:
    """Load JSON from a file path, returning default if file is missing or invalid.

    Args:
        file_path: Absolute path to the JSON file
        default: Default value to return on error (list, dict, etc.)

    Returns:
        Parsed JSON data or the default value

    Example:
        >>> data = load_json_file_or_return_default(Path("result/json/scores.json"), [])
    """
    if not file_path.exists():
        return default

    try:
        with open(file_path) as f:
            return json.load(f)
    except (json.JSONDecodeError, OSError) as e:
        logger.warning(
            "json_load_failed",
            path=str(file_path),
            error=str(e),
            returning="default",
        )
        return default


def load_json_file_by_name_or_return_default(
    filename: str,
    default: T,
    json_dir: Path | None = None,
) -> T:
    """Load JSON by filename from the JSON directory, returning default on error.

    Args:
        filename: Name of the JSON file (e.g., "scores.json")
        default: Default value to return on error
        json_dir: Directory containing JSON files (defaults to JSON_DIR from config)

    Returns:
        Parsed JSON data or the default value

    Example:
        >>> scores = load_json_file_by_name_or_return_default("scores.json", [])
    """
    if json_dir is None:
        json_dir = JSON_DIR

    file_path = json_dir / filename
    return load_json_file_or_return_default(file_path, default)


# ============================================================================
# Cached Loaders for API (LRU cache with 32 entries)
# ============================================================================


@lru_cache(maxsize=32)
def load_json_file_with_caching(file_path_str: str, default_type: str) -> ListOrDict:
    """Load JSON with caching for API endpoints (LRU cache, thread-safe).

    Args:
        file_path_str: String path to JSON file (must be str for LRU cache hashing)
        default_type: Type of default to return ("list" or "dict")

    Returns:
        Parsed JSON data (list or dict) or default

    Note:
        This function is cached with maxsize=32. Call clear_json_cache() to invalidate.

    Example:
        >>> data = load_json_file_with_caching("/path/to/scores.json", "list")
    """
    default: ListOrDict = [] if default_type == "list" else {}
    file_path = Path(file_path_str)
    return load_json_file_or_return_default(file_path, default)


def load_pipeline_json_with_caching(filename: str, default: ListOrDict) -> ListOrDict:
    """Load pipeline JSON by filename with caching for API endpoints.

    Args:
        filename: Name of the JSON file (e.g., "scores.json")
        default: Default value (list or dict)

    Returns:
        Parsed JSON data or default

    Example:
        >>> scores = load_pipeline_json_with_caching("scores.json", [])
    """
    file_path = JSON_DIR / filename
    default_type = "list" if isinstance(default, list) else "dict"
    return load_json_file_with_caching(str(file_path), default_type)


def clear_json_cache() -> None:
    """Clear the LRU cache for JSON file loading.

    Call this after pipeline runs to force API endpoints to reload fresh data.

    Example:
        >>> clear_json_cache()  # Force API to reload after pipeline
    """
    load_json_file_with_caching.cache_clear()
    logger.debug("json_cache_cleared")


# ============================================================================
# Named Domain-Specific Loaders (Read Like Prose)
# ============================================================================


def load_person_scores_from_json() -> list[dict]:
    """Load person scores from scores.json.

    Returns:
        List of person score dictionaries with authority/trust/skill/composite

    Example:
        >>> scores = load_person_scores_from_json()
        >>> top_person = scores[0]
    """
    return load_pipeline_json_with_caching("scores.json", [])


def load_anime_statistics_from_json() -> dict[str, dict]:
    """Load anime statistics from anime_stats.json.

    Returns:
        Dict mapping anime_id to stats (credit_count, avg_person_score, etc.)

    Example:
        >>> stats = load_anime_statistics_from_json()
        >>> anime_data = stats.get("anime_123")
    """
    return load_pipeline_json_with_caching("anime_stats.json", {})


def load_pipeline_summary_from_json() -> dict:
    """Load pipeline execution summary from summary.json.

    Returns:
        Summary dict with timing, counts, and metadata

    Example:
        >>> summary = load_pipeline_summary_from_json()
        >>> total_persons = summary.get("total_persons", 0)
    """
    return load_pipeline_json_with_caching("summary.json", {})


def load_role_transitions_from_json() -> dict:
    """Load role transition analysis from transitions.json.

    Returns:
        Dict with transition matrices and career path statistics

    Example:
        >>> transitions = load_role_transitions_from_json()
    """
    return load_pipeline_json_with_caching("transitions.json", {})


def load_cross_validation_results_from_json() -> dict:
    """Load score cross-validation results from crossval.json.

    Returns:
        Dict with MAE, RMSE, correlation metrics

    Example:
        >>> crossval = load_cross_validation_results_from_json()
        >>> mae = crossval.get("mae", 0)
    """
    return load_pipeline_json_with_caching("crossval.json", {})


def load_influence_tree_from_json() -> dict:
    """Load mentor-mentee influence tree from influence.json.

    Returns:
        Dict with influence chains and mentorship relationships

    Example:
        >>> influence = load_influence_tree_from_json()
    """
    return load_pipeline_json_with_caching("influence.json", {})


def load_studio_analysis_from_json() -> dict:
    """Load studio performance analysis from studios.json.

    Returns:
        Dict mapping studio names to performance metrics

    Example:
        >>> studios = load_studio_analysis_from_json()
    """
    return load_pipeline_json_with_caching("studios.json", {})


def load_seasonal_trends_from_json() -> dict:
    """Load seasonal activity patterns from seasonal.json.

    Returns:
        Dict with season-based trends and activity metrics

    Example:
        >>> seasonal = load_seasonal_trends_from_json()
    """
    return load_pipeline_json_with_caching("seasonal.json", {})


def load_collaboration_pairs_from_json() -> list[dict]:
    """Load collaboration strength pairs from collaborations.json.

    Returns:
        List of collaboration pairs with strength scores

    Example:
        >>> collabs = load_collaboration_pairs_from_json()
    """
    return load_pipeline_json_with_caching("collaborations.json", [])


def load_outlier_analysis_from_json() -> dict:
    """Load score outlier detection results from outliers.json.

    Returns:
        Dict with outlier persons and anomaly scores

    Example:
        >>> outliers = load_outlier_analysis_from_json()
    """
    return load_pipeline_json_with_caching("outliers.json", {})


def load_team_patterns_from_json() -> dict:
    """Load team composition analysis from teams.json.

    Returns:
        Dict with team patterns and high-scoring team structures

    Example:
        >>> teams = load_team_patterns_from_json()
    """
    return load_pipeline_json_with_caching("teams.json", {})


def load_growth_trends_from_json() -> dict:
    """Load person growth trends from growth.json.

    Returns:
        Dict mapping person_id to growth metrics (rising/stable/declining)

    Example:
        >>> growth = load_growth_trends_from_json()
    """
    return load_pipeline_json_with_caching("growth.json", {})


def load_time_series_from_json() -> dict:
    """Load annual time series data from time_series.json.

    Returns:
        Dict with year-by-year metrics

    Example:
        >>> time_series = load_time_series_from_json()
    """
    return load_pipeline_json_with_caching("time_series.json", {})


def load_decade_analysis_from_json() -> dict:
    """Load decade-based analysis from decades.json.

    Returns:
        Dict with per-decade statistics

    Example:
        >>> decades = load_decade_analysis_from_json()
    """
    return load_pipeline_json_with_caching("decades.json", {})


def load_person_tags_from_json() -> dict:
    """Load person tag assignments from tags.json.

    Returns:
        Dict mapping person_id to tag lists

    Example:
        >>> tags = load_person_tags_from_json()
    """
    return load_pipeline_json_with_caching("tags.json", {})


def load_role_flow_from_json() -> dict:
    """Load role transition flow (Sankey diagram data) from role_flow.json.

    Returns:
        Dict with source/target/value for Sankey diagrams

    Example:
        >>> role_flow = load_role_flow_from_json()
    """
    return load_pipeline_json_with_caching("role_flow.json", {})


def load_bridge_analysis_from_json() -> dict:
    """Load community bridge persons from bridges.json.

    Returns:
        Dict with bridge scores and community connections

    Example:
        >>> bridges = load_bridge_analysis_from_json()
    """
    return load_pipeline_json_with_caching("bridges.json", {})


def load_mentorship_relationships_from_json() -> dict:
    """Load inferred mentorship relationships from mentorships.json.

    Returns:
        Dict with mentor-mentee pairs and confidence scores

    Example:
        >>> mentorships = load_mentorship_relationships_from_json()
    """
    return load_pipeline_json_with_caching("mentorships.json", {})


def load_career_milestones_from_json() -> dict:
    """Load career milestone data from milestones.json.

    Returns:
        Dict mapping person_id to milestone events

    Example:
        >>> milestones = load_career_milestones_from_json()
    """
    return load_pipeline_json_with_caching("milestones.json", {})


def load_network_evolution_from_json() -> dict:
    """Load network evolution time series from network_evolution.json.

    Returns:
        Dict with temporal network metrics

    Example:
        >>> net_evolution = load_network_evolution_from_json()
    """
    return load_pipeline_json_with_caching("network_evolution.json", {})


def load_genre_affinity_from_json() -> dict:
    """Load genre affinity data from genre_affinity.json.

    Returns:
        Dict mapping person_id to genre preferences and scores

    Example:
        >>> genre_affinity = load_genre_affinity_from_json()
    """
    return load_pipeline_json_with_caching("genre_affinity.json", {})


def load_productivity_metrics_from_json() -> dict:
    """Load productivity metrics from productivity.json.

    Returns:
        Dict mapping person_id to productivity indicators

    Example:
        >>> productivity = load_productivity_metrics_from_json()
    """
    return load_pipeline_json_with_caching("productivity.json", {})


# ============================================================================
# Export Helpers
# ============================================================================


def save_json_to_file(
    data: Any,
    file_path: Path,
    ensure_parent_dir: bool = True,
) -> None:
    """Save data to a JSON file with pretty formatting.

    Args:
        data: Data to serialize (must be JSON-serializable)
        file_path: Absolute path to output file
        ensure_parent_dir: Create parent directory if missing (default: True)

    Example:
        >>> save_json_to_file({"key": "value"}, Path("result/output.json"))
    """
    if ensure_parent_dir:
        file_path.parent.mkdir(parents=True, exist_ok=True)

    with open(file_path, "w") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)


def save_pipeline_json_if_data_present(
    filename: str,
    data: Any,
    condition: bool | None = None,
    log_message: str = "",
    **log_context: Any,
) -> bool:
    """Save pipeline JSON if data is present and condition is met.

    Args:
        filename: Output filename (e.g., "scores.json")
        data: Data to save (list, dict, etc.)
        condition: Optional boolean condition (if False, skip save)
        log_message: structlog message key for successful save
        **log_context: Additional structlog context fields

    Returns:
        True if file was saved, False if skipped

    Note:
        This function will save even if data is an empty dict/list, as long as
        data is not None and condition is not False. Use condition parameter
        to control whether empty containers should be saved.

    Example:
        >>> saved = save_pipeline_json_if_data_present(
        ...     "anime_stats.json",
        ...     anime_stats,
        ...     log_message="anime_stats_saved",
        ...     anime=len(anime_stats),
        ... )
    """
    # Check condition first
    if condition is not None and not condition:
        return False

    # Check if data is None (but allow empty containers like {} or [])
    if data is None:
        return False

    # Save to file
    file_path = JSON_DIR / filename
    save_json_to_file(data, file_path)

    # Log success
    if log_message:
        context = {"path": str(file_path), **log_context}
        logger.info(log_message, **context)

    return True
