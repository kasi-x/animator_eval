"""Public database API."""

from src.db.init import (
    get_connection,
    db_connection,
    init_db,
    get_schema_version,
    DEFAULT_DB_PATH,
    SCHEMA_VERSION,
    _FUZZY_MATCH_RULES,
)
from src.db.etl import (
    upsert_person,
    normalize_primary_names_by_credits,
    upsert_anime,
    upsert_anime_analysis,
    ensure_meta_quality_snapshot,
    ensure_calc_execution_records,
    get_calc_execution_hashes,
    record_calc_execution,
    register_meta_lineage,
    upsert_meta_entity_resolution_audit,
    insert_credit,
)
from src.db.scraper import (
    upsert_character,
    insert_character_voice_actor,
    upsert_studio,
    insert_anime_studio,
    insert_anime_relation,
    get_llm_decision,
    upsert_llm_decision,
    upsert_src_anilist_anime,
)

__all__ = [
    # init
    "get_connection",
    "db_connection",
    "init_db",
    "get_schema_version",
    "DEFAULT_DB_PATH",
    "SCHEMA_VERSION",
    "_FUZZY_MATCH_RULES",
    # etl
    "upsert_person",
    "normalize_primary_names_by_credits",
    "upsert_anime",
    "upsert_anime_analysis",
    "ensure_meta_quality_snapshot",
    "ensure_calc_execution_records",
    "get_calc_execution_hashes",
    "record_calc_execution",
    "register_meta_lineage",
    "upsert_meta_entity_resolution_audit",
    "insert_credit",
    # scraper
    "upsert_character",
    "insert_character_voice_actor",
    "upsert_studio",
    "insert_anime_studio",
    "insert_anime_relation",
    "get_llm_decision",
    "upsert_llm_decision",
    "upsert_src_anilist_anime",
]
