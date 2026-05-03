"""Phase 1: Data Loading — load persons, anime, and credits from silver.duckdb."""

import re
from collections import defaultdict

import structlog

from src.analysis.io.conformed_reader import load_anime_silver, load_credits_silver, load_persons_silver
from src.pipeline_phases.pipeline_types import LoadedData
from src.runtime.models import Credit, Person, Role
from src.utils.role_groups import NON_PRODUCTION_ROLES

# Roles that definitively identify animation production staff.
# Manga artists occasionally receive courtesy credits (e.g. character_designer for
# original designs), but only genuine direction/animation credits confirm that a person
# actually worked on anime production — not just provided the source material.
_ANCHOR_PRODUCTION_ROLES: frozenset[Role] = frozenset(
    {
        Role.DIRECTOR,
        Role.EPISODE_DIRECTOR,
        Role.ANIMATION_DIRECTOR,
    }
)

logger = structlog.get_logger()

# Person name patterns to exclude as placeholder / garbage data.
# These are collective names or metadata in credit records, not real individuals.
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

# Patterns to identify organization names.
# Broadcasters, studios, and production companies sometimes appear as credit entries.
_ORG_SUFFIX_RE = re.compile(
    r"(?:"
    r"テレビ$|テレビジョン$|テレビ動画$|テレビ東京$|テレビ朝日$|テレビ大阪$|"
    r"放送$|放送局$|フジテレビ|NHK$|TBS$|日本テレビ$|"
    r"アニメーション$|スタジオ$|プロダクション$|プロダクツ$|エンタプライズ$|"
    r"エンタープライズ$|エンタテインメント$|エンターテインメント$|"
    r"エンタテイメント$|"  # contracted spelling variant (e.g. ベガエンタテイメント)
    r"ホールディングス$|コミュニケーションズ$|エージェンシー$|"
    r"製作委員会$|制作委員会$|実行委員会$|"  # production committees
    r"現像所$|撮影所$|"  # film labs and studios (e.g. 東京現像所)
    r"動画$|"  # animation studios (e.g. 東映動画, テレビ動画)
    # English company suffixes (case-insensitive)
    r"Pictures$|Studio$|Studios$|Animation$|Entertainment$|Productions$|"
    r"Filmworks$|Arts$"
    r")",
    re.IGNORECASE,
)

# Leading special symbol → garbage data (e.g. "○broadcaster[logo]", "※footnote…", "★")
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
        if Role.ORIGINAL_CREATOR in role_set and not (
            role_set & _ANCHOR_PRODUCTION_ROLES
        ):
            oc_count = sum(1 for c in person_credits if c.role == Role.ORIGINAL_CREATOR)
            prod_count = sum(
                1
                for c in person_credits
                if c.role not in NON_PRODUCTION_ROLES
                and c.role != Role.ORIGINAL_CREATOR
            )
            if oc_count > prod_count:
                non_production_ids.add(pid)

    filtered = [p for p in persons if p.id not in non_production_ids]
    return filtered, non_production_ids


def _llm_filter_organizations(persons: list[Person]) -> set[str]:
    """Use LLM + studio DB to detect organizations masquerading as persons.

    Returns set of person_ids identified as organizations.
    """
    try:
        from src.analysis.llm_pipeline import classify_person_or_org
    except ImportError:
        return set()

    result = classify_person_or_org(persons, studio_names=set(), conn=None)
    return result.org_ids


def _llm_normalize_names(
    persons: list[Person],
    credits: list[Credit],
) -> tuple[list[Person], list[Credit]]:
    """Use LLM to normalize names with parenthetical annotations.

    When a name like "高畑勲、宮崎駿(7~最終話)" is split into individual names,
    new Person entries are created and credits are reassigned.

    Returns (updated_persons, extra_credits_for_new_persons).
    """
    try:
        from src.analysis.llm_pipeline import normalize_names
    except ImportError:
        return persons, []

    norm_results = normalize_names(persons, conn=None)
    if not norm_results:
        return persons, []

    # Build lookup: original name → normalization result
    name_to_norm: dict[str, list] = {}
    org_names: set[str] = set()
    for nr in norm_results:
        name_to_norm[nr.original] = nr.names
        if nr.is_org:
            org_names.add(nr.original)

    # Process persons
    updated: list[Person] = []
    extra_credits: list[Credit] = []
    credits_by_person: dict[str, list[Credit]] = defaultdict(list)
    for c in credits:
        credits_by_person[c.person_id].append(c)

    for p in persons:
        name = p.name_ja or ""
        if name in org_names:
            # LLM identified as organization — skip
            continue
        if name not in name_to_norm:
            updated.append(p)
            continue

        normalized_names = name_to_norm[name]
        if len(normalized_names) == 1 and normalized_names[0] == name:
            # No change needed
            updated.append(p)
            continue

        if len(normalized_names) == 1:
            # Simple rename — update the person's name
            p.name_ja = normalized_names[0]
            updated.append(p)
            logger.debug("name_normalized", old=name, new=normalized_names[0], pid=p.id)
        else:
            # Multi-person split — keep first as canonical, create new for rest
            p.name_ja = normalized_names[0]
            updated.append(p)

            person_credits = credits_by_person.get(p.id, [])
            for extra_name in normalized_names[1:]:
                new_id = f"{p.id}:split:{extra_name}"
                new_person = Person(
                    id=new_id,
                    name_ja=extra_name,
                    name_en=None,
                    source=p.source,
                )
                updated.append(new_person)
                # Copy credits to the split person
                for c in person_credits:
                    extra_credits.append(
                        Credit(
                            person_id=new_id,
                            anime_id=c.anime_id,
                            role=c.role,
                            episode=c.episode,
                            source=c.source,
                        )
                    )
            logger.debug(
                "name_split",
                original=name,
                names=normalized_names,
                pid=p.id,
            )

    if len(persons) != len(updated):
        logger.info(
            "llm_name_normalization_applied",
            persons_before=len(persons),
            persons_after=len(updated),
            extra_credits=len(extra_credits),
        )

    return updated, extra_credits


def load_pipeline_data(visualize: bool = False, dry_run: bool = False) -> LoadedData:
    """Load all data from silver.duckdb.

    Returns:
        LoadedData with persons, anime_list, credits, anime_map populated.
    """
    all_persons = load_persons_silver()
    anime_list = load_anime_silver()
    all_credits = load_credits_silver()

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
    filtered_persons, non_production_ids = _filter_non_production_persons(
        valid_persons, all_credits
    )
    if non_production_ids:
        logger.info("filtered_non_production_persons", count=len(non_production_ids))

    # LLM-assisted organization detection
    llm_org_ids = _llm_filter_organizations(filtered_persons)
    if llm_org_ids:
        filtered_persons = [p for p in filtered_persons if p.id not in llm_org_ids]
        logger.info("filtered_llm_orgs", count=len(llm_org_ids))

    # LLM-assisted name normalization
    filtered_persons, extra_credits = _llm_normalize_names(filtered_persons, all_credits)

    # Filter out orphan credits
    person_ids = {p.id for p in filtered_persons}
    credits = [c for c in all_credits if c.person_id in person_ids]
    if extra_credits:
        credits.extend(extra_credits)
    na_count = len(all_credits) - len(credits) + len(extra_credits)
    if na_count > 0:
        logger.info("filtered_orphan_credits", count=na_count)

    anime_map = {a.id: a for a in anime_list}

    logger.info(
        "data_loaded",
        persons=len(filtered_persons),
        anime=len(anime_list),
        credits=len(credits),
    )

    return LoadedData(
        persons=filtered_persons,
        anime_list=anime_list,
        credits=credits,
        anime_map=anime_map,
        visualize=visualize,
        dry_run=dry_run,
    )
