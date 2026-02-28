"""Phase 1: Data Loading — load persons, anime, and credits from database."""

import re
import sqlite3
from collections import defaultdict

import structlog

from src.database import load_all_anime, load_all_credits, load_all_persons
from src.models import Credit, Person
from src.pipeline_phases.context import PipelineContext
from src.models import Role
from src.utils.role_groups import NON_PRODUCTION_ROLES

# Roles that definitively identify animation production staff.
# Manga artists occasionally receive courtesy credits (e.g. character_designer for
# original designs), but only genuine direction/animation credits confirm that a person
# actually worked on anime production — not just provided the source material.
_ANCHOR_PRODUCTION_ROLES: frozenset[Role] = frozenset(
    {
        Role.DIRECTOR,
        Role.EPISODE_DIRECTOR,
        Role.STORYBOARD,
        Role.ANIMATION_DIRECTOR,
        Role.CHIEF_ANIMATION_DIRECTOR,
    }
)

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

# 組織名として認識するためのパターン
# 実在の個人ではなく、放送局・スタジオ・制作会社がクレジット行に記載されたケース
_ORG_SUFFIX_RE = re.compile(
    r"(?:"
    r"テレビ$|テレビジョン$|テレビ動画$|テレビ東京$|テレビ朝日$|テレビ大阪$|"
    r"放送$|放送局$|フジテレビ|NHK$|TBS$|日本テレビ$|"
    r"アニメーション$|スタジオ$|プロダクション$|エンタプライズ$|"
    r"エンタープライズ$|エンタテインメント$|エンターテインメント$|"
    r"ホールディングス$|コミュニケーションズ$|エージェンシー$|"
    r"動画$"  # 東映動画, テレビ動画 etc.
    r")",
    re.IGNORECASE,
)

# 先頭が記号類 → ゴミデータ (例: "○フジテレビ[ロゴ]", "※以下…", "★")
_ORG_SYMBOL_RE = re.compile(r"^[○◎★☆●■□\[【※〔◆▼▶]")


def _is_organization_name(person: Person) -> bool:
    """Check if a person entry is an organization (broadcaster, studio, etc.) not an individual.

    Strategy:
    1. Names starting with special symbols (○, ※, ★ etc.) → exclude.
    2. Extract the "base name" — the part before any parenthetical note.
       - "前田和也(フジテレビ)" → base = "前田和也"  (human name) → keep
       - "テレビ動画"          → base = "テレビ動画" (org name)   → exclude
       - "(フジテレビ)"        → base = ""           (no human)   → check full
    3. If base has hiragana → it's a person name → keep.
    4. If base matches known org suffixes → it's a company → exclude.

    Examples excluded:  テレビ動画, フジテレビ, (フジテレビ), ○フジテレビ[ロゴ]
    Examples kept:      前田和也(フジテレビ), 釘宮洋(テレビ版監督), 花澤香菜
    """
    name = (person.name_ja or person.name_en or "").strip()
    if not name:
        return False

    # Rule 1: starts with special symbol → garbage/org
    if _ORG_SYMBOL_RE.match(name):
        return True

    # Rule 2-4: use the base name (before parentheses) for org-suffix matching
    paren_pos = min(
        (name.find(c) for c in ("(", "（") if c in name),
        default=len(name),
    )
    base = name[:paren_pos].strip() or name  # fall back to full name if no base

    # If base contains hiragana it's almost certainly a person name → keep
    if re.search(r"[\u3041-\u3096]", base):
        return False

    # If base matches an org-name suffix pattern → exclude
    return bool(_ORG_SUFFIX_RE.search(base))


def _is_garbage_person(person: Person) -> bool:
    """Check if a person entry is garbage/placeholder data.

    Detects:
    - Known placeholder names (e.g. "ほか", "アニメ")
    - Persons with no name at all
    - Organization/company names mistakenly entered as persons
    """
    name = person.name_ja or person.name_en
    if not name:
        return True
    if name.strip() in GARBAGE_PERSON_NAMES:
        return True
    return _is_organization_name(person)


def _filter_non_production_persons(
    persons: list[Person],
    credits: list[Credit],
) -> tuple[list[Person], set[str]]:
    """Remove persons who are not anime production staff.

    Two exclusion rules:
    1. All credits are non-production (voice actors, theme song artists, etc.).
    2. Primarily original creators (manga/novel authors) without any genuine
       animation production credit. Manga artists often receive courtesy credits
       (e.g. character_designer for their original designs) that do not mean they
       actually worked on anime production. We require at least one anchor role
       (director, episode_director, storyboard, animation_director,
       chief_animation_director) to override this exclusion.

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
        # Rule 1: all credits are non-production
        if all(c.role in NON_PRODUCTION_ROLES for c in person_credits):
            non_production_ids.add(pid)
            continue

        # Rule 2: original creator credits outnumber actual production credits.
        # Manga artists sometimes receive courtesy credits (e.g. character_designer for
        # their original designs) that are not genuine anime production work.
        # If OC credits > production credits and the person has no anchor role
        # (director/storyboard/etc.), they are treated as a source-material author,
        # not as animation production staff.
        role_set = {c.role for c in person_credits}
        if Role.ORIGINAL_CREATOR in role_set and not (role_set & _ANCHOR_PRODUCTION_ROLES):
            oc_count = sum(1 for c in person_credits if c.role == Role.ORIGINAL_CREATOR)
            prod_count = sum(
                1 for c in person_credits
                if c.role not in NON_PRODUCTION_ROLES and c.role != Role.ORIGINAL_CREATOR
            )
            if oc_count > prod_count:
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
