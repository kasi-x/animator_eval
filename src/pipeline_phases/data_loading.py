"""Phase 1: Data Loading — load persons, anime, and credits from database."""

import sqlite3

import structlog

from src.database import load_all_anime, load_all_credits, load_all_persons
from src.pipeline_phases.context import PipelineContext

logger = structlog.get_logger()


def load_pipeline_data(context: PipelineContext, conn: sqlite3.Connection) -> None:
    """Load all data from database into context.

    Args:
        context: Pipeline context to populate
        conn: Database connection

    Updates context fields:
        - persons: List of all Person objects
        - anime_list: List of all Anime objects
        - credits: List of all Credit objects
        - anime_map: Dict mapping anime_id to Anime object
    """
    with context.monitor.measure("data_loading"):
        context.persons = load_all_persons(conn)
        context.anime_list = load_all_anime(conn)
        all_credits = load_all_credits(conn)

    # Filter out placeholder N/A credits (no real person data)
    person_ids = {p.id for p in context.persons}
    context.credits = [c for c in all_credits if c.person_id in person_ids]
    na_count = len(all_credits) - len(context.credits)
    if na_count > 0:
        logger.info("filtered_orphan_credits", count=na_count)

    # Build anime_map for quick lookups
    context.anime_map = {a.id: a for a in context.anime_list}

    # Update monitoring counters
    context.monitor.increment_counter("persons_loaded", len(context.persons))
    context.monitor.increment_counter("anime_loaded", len(context.anime_list))
    context.monitor.increment_counter("credits_loaded", len(context.credits))
    context.monitor.record_memory("after_data_load")

    logger.info(
        "data_loaded",
        persons=len(context.persons),
        anime=len(context.anime_list),
        credits=len(context.credits),
    )
