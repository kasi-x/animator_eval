"""Phase 1: Data Loading — load persons, anime, and credits from database."""

import sqlite3
from collections import defaultdict

import structlog

from src.database import load_all_anime, load_all_credits, load_all_persons
from src.models import Credit, Person
from src.pipeline_phases.context import PipelineContext
from src.utils.role_groups import NON_PRODUCTION_ROLES

logger = structlog.get_logger()

# プレースホルダー・ゴミデータとして除外する人物名パターン
# これらは実在の個人ではなく、クレジットデータの集合名やメタデータ
GARBAGE_PERSON_NAMES: frozenset[str] = frozenset(
    {
        "アニメ",
        "ほか",
        "他",
        "その他",
        "スタッフ",
        "制作スタッフ",
    }
)


def _is_garbage_person(person: Person) -> bool:
    """Check if a person entry is garbage/placeholder data.

    Detects:
    - Known placeholder names (e.g. "ほか", "アニメ")
    - Persons with no name at all
    """
    name = person.name_ja or person.name_en
    if not name:
        return True
    return name.strip() in GARBAGE_PERSON_NAMES


def _filter_non_production_persons(
    persons: list[Person],
    credits: list[Credit],
) -> tuple[list[Person], set[str]]:
    """Remove persons who have ONLY non-production credits (voice actor, theme song, etc.).

    Persons with at least one production credit (e.g. voice actor who also did key animation)
    are preserved.

    Args:
        persons: All person objects
        credits: All credit objects

    Returns:
        Tuple of (filtered persons list, set of removed person IDs)
    """
    credits_by_person: dict[str, list[Credit]] = defaultdict(list)
    for c in credits:
        credits_by_person[c.person_id].append(c)

    non_production_ids: set[str] = set()
    for pid, person_credits in credits_by_person.items():
        if all(c.role in NON_PRODUCTION_ROLES for c in person_credits):
            non_production_ids.add(pid)

    filtered = [p for p in persons if p.id not in non_production_ids]
    return filtered, non_production_ids


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
        all_persons = load_all_persons(conn)
        context.anime_list = load_all_anime(conn)
        all_credits = load_all_credits(conn)

    # Filter out garbage/placeholder person entries
    garbage_ids: set[str] = set()
    valid_persons: list[Person] = []
    for p in all_persons:
        if _is_garbage_person(p):
            garbage_ids.add(p.id)
        else:
            valid_persons.append(p)
    if garbage_ids:
        logger.info("filtered_garbage_persons", count=len(garbage_ids))

    # Filter out persons with ONLY non-production credits (voice actors, singers, etc.)
    # Persons with at least one production credit (兼任者) are preserved.
    filtered_persons, non_production_ids = _filter_non_production_persons(
        valid_persons, all_credits
    )
    context.persons = filtered_persons
    if non_production_ids:
        logger.info(
            "filtered_non_production_persons", count=len(non_production_ids)
        )

    # Filter out orphan credits and credits for garbage/non-production persons
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
