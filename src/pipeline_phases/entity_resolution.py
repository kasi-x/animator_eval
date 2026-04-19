"""Phase 3: Entity Resolution — deduplicate person identities and merge credits.

Also resolves anime entities across sources (seesaa/keyframe/anilist/madb) by
title_ja matching, preferring wiki sources as canonical IDs.
"""

import json
from collections import defaultdict

import structlog

from src.analysis.entity_resolution import resolve_all
from src.models import Anime, Credit
from src.pipeline_phases.context import PipelineContext

logger = structlog.get_logger()


def _merge_duplicate_credits(credits: list[Credit]) -> list[Credit]:
    """Merge duplicate credits after entity resolution.

    After person ID and anime ID resolution, the same (person, anime, role)
    may appear multiple times from different sources (e.g. AniList + MADB).
    This function merges them while preserving episode info and source provenance.

    Rules:
    - Key: (person_id, anime_id, role)
    - episode=-1 (unknown) is replaced by specific episode numbers if available
    - Multiple specific episodes → kept as separate credits
    - source: concatenated from all sources (provenance tracking)
    - raw_role: prefer AniList (English) over MADB (Japanese)

    Args:
        credits: List of credits (already resolved person_ids)

    Returns:
        Merged list of credits
    """
    # Group by (person_id, anime_id, role)
    groups: dict[tuple[str, str, str], list[Credit]] = defaultdict(list)
    for c in credits:
        key = (c.person_id, c.anime_id, c.role.value)
        groups[key].append(c)

    merged: list[Credit] = []
    total_merged = 0
    episodes_preserved = 0

    for (person_id, anime_id, _role_val), group in groups.items():
        if len(group) == 1:
            merged.append(group[0])
            continue

        # Multiple credits for same (person, anime, role)
        total_merged += len(group) - 1

        # Collect all sources
        sources = sorted({c.source for c in group if c.source})
        merged_source = ",".join(sources) if sources else ""

        # Prefer non-MADB raw_role (English from AniList)
        raw_role = None
        for c in group:
            if c.raw_role and not c.source.startswith("madb"):
                raw_role = c.raw_role
                break
        if raw_role is None:
            # Fallback to any available raw_role
            for c in group:
                if c.raw_role:
                    raw_role = c.raw_role
                    break

        # Collect episode info
        specific_episodes: set[int] = set()
        for c in group:
            if c.episode is not None and c.episode >= 0:
                specific_episodes.add(c.episode)

        if specific_episodes:
            # Emit one credit per specific episode
            episodes_preserved += len(specific_episodes)
            for ep in sorted(specific_episodes):
                merged.append(
                    Credit(
                        person_id=person_id,
                        anime_id=anime_id,
                        role=group[0].role,
                        raw_role=raw_role,
                        episode=ep,
                        source=merged_source,
                    )
                )
        else:
            # No specific episodes → keep one credit with episode as-is
            merged.append(
                Credit(
                    person_id=person_id,
                    anime_id=anime_id,
                    role=group[0].role,
                    raw_role=raw_role,
                    episode=group[0].episode,
                    source=merged_source,
                )
            )

    if total_merged > 0:
        logger.info(
            "credits_merged",
            duplicates_removed=total_merged,
            episodes_preserved=episodes_preserved,
            credits_before=len(credits),
            credits_after=len(merged),
        )

    return merged


# Source priority for anime canonical ID selection.
# Wiki sources preferred because they have episode-level credit data.
_SOURCE_PRIORITY = {"seesaa": 0, "keyframe": 1, "anilist": 2, "ann": 3, "madb": 4, "allcinema": 5}


def _anime_source(anime_id: str) -> str:
    """Extract source prefix from anime_id.

    Handles both colon-separated ('seesaa:a_xxx' → 'seesaa') and
    hyphen-separated ('ann-123' → 'ann') formats.
    """
    if ":" in anime_id:
        return anime_id.split(":")[0]
    if anime_id.startswith("ann-"):
        return "ann"
    return "unknown"


def _resolve_anime_entities(
    anime_list: list[Anime],
    credits: list[Credit],
) -> tuple[dict[str, str], list[dict]]:
    """Resolve anime entities across sources by title_ja matching.

    Prefers wiki sources (seesaa > keyframe) as canonical IDs.
    Returns (canonical_map, anomalies) where anomalies lists cases where the
    wiki version has fewer credits than a non-wiki source.

    Args:
        anime_list: All loaded anime
        credits: All credits (for counting per anime)

    Returns:
        canonical_map: {non_canonical_anime_id: canonical_anime_id}
        anomalies: List of dicts describing wiki-has-fewer-credits cases
    """
    # Group anime by title_ja
    by_title: dict[str, list[Anime]] = defaultdict(list)
    for a in anime_list:
        title = (a.title_ja or "").strip()
        if not title:
            continue
        by_title[title].append(a)

    # Count credits per anime_id
    credit_counts: dict[str, int] = defaultdict(int)
    for c in credits:
        credit_counts[c.anime_id] += 1

    canonical_map: dict[str, str] = {}
    anomalies: list[dict] = []

    for title, anime_group in by_title.items():
        if len(anime_group) < 2:
            continue

        # Filter to groups with >1 distinct source
        sources = {_anime_source(a.id) for a in anime_group}
        if len(sources) < 2:
            continue

        # Pick canonical: best source priority, then most credits as tiebreak
        anime_group.sort(
            key=lambda a: (
                _SOURCE_PRIORITY.get(_anime_source(a.id), 99),
                -credit_counts.get(a.id, 0),
            )
        )
        canonical = anime_group[0]
        canonical_src = _anime_source(canonical.id)
        canonical_credits = credit_counts.get(canonical.id, 0)

        for other in anime_group[1:]:
            if other.id == canonical.id:
                continue
            canonical_map[other.id] = canonical.id

            other_src = _anime_source(other.id)
            other_credits = credit_counts.get(other.id, 0)

            # Log anomaly: wiki canonical has fewer credits than non-wiki source
            if (
                canonical_src in ("seesaa", "keyframe")
                and other_credits > canonical_credits
                and other_credits > 0
            ):
                anomalies.append(
                    {
                        "title_ja": title,
                        "canonical_id": canonical.id,
                        "canonical_source": canonical_src,
                        "canonical_credits": canonical_credits,
                        "other_id": other.id,
                        "other_source": other_src,
                        "other_credits": other_credits,
                        "deficit": other_credits - canonical_credits,
                    }
                )

    return canonical_map, anomalies


def run_entity_resolution(context: PipelineContext) -> None:
    """Perform entity resolution for both persons and anime.

    Person resolution: Deduplicate person identities across sources.
    Anime resolution: Merge anime across sources by title_ja, preferring wiki.

    Args:
        context: Pipeline context

    Updates context fields:
        - canonical_map: Dict mapping duplicate person_id to canonical person_id
        - credits: List of credits with resolved person_ids and anime_ids
        - anime_list / anime_map: Deduplicated anime (non-canonical removed)
    """
    with context.monitor.measure("entity_resolution"):
        # === Person entity resolution ===
        # クレジット・アニメメタを渡して ML ホモニム分割を有効化
        credits_by_person: dict[str, list] = defaultdict(list)
        for c in context.credits:
            credits_by_person[c.person_id].append(c)

        from src.analysis.ml_homonym_split import build_anime_meta
        anime_meta = build_anime_meta(context.anime_list)

        context.canonical_map = resolve_all(
            context.persons,
            credits_by_person=credits_by_person,
            anime_meta=anime_meta,
        )

        # Replace person_id in credits with canonical IDs
        if context.canonical_map:
            resolved_credits = []
            for c in context.credits:
                new_pid = context.canonical_map.get(c.person_id, c.person_id)
                resolved_credits.append(
                    Credit(
                        person_id=new_pid,
                        anime_id=c.anime_id,
                        role=c.role,
                        raw_role=c.raw_role,
                        episode=c.episode,
                        source=c.source,
                    )
                )
            context.credits = resolved_credits
            logger.info("person_ids_resolved", count=len(context.canonical_map))
            context.monitor.increment_counter(
                "persons_resolved", len(context.canonical_map)
            )

            # 数値IDを canonical person に伝播する
            # 例: ANN person が AniList person にマージされたとき、
            # AniList person の ann_id に ANN person の ann_id をセットする
            persons_by_id = {p.id: p for p in context.persons}
            for dup_id, canonical_id in context.canonical_map.items():
                dup = persons_by_id.get(dup_id)
                canon = persons_by_id.get(canonical_id)
                if not dup or not canon:
                    continue
                if dup.ann_id and not canon.ann_id:
                    canon.ann_id = dup.ann_id
                if dup.anilist_id and not canon.anilist_id:
                    canon.anilist_id = dup.anilist_id
                if dup.mal_id and not canon.mal_id:
                    canon.mal_id = dup.mal_id
                if dup.madb_id and not canon.madb_id:
                    canon.madb_id = dup.madb_id
                if dup.name_ja and not canon.name_ja:
                    canon.name_ja = dup.name_ja

        # === Anime entity resolution (cross-source, wiki-preferred) ===
        anime_canonical_map, anomalies = _resolve_anime_entities(
            context.anime_list,
            context.credits,
        )

        if anime_canonical_map:
            # Remap anime_id in credits
            remapped = []
            for c in context.credits:
                new_aid = anime_canonical_map.get(c.anime_id, c.anime_id)
                remapped.append(
                    Credit(
                        person_id=c.person_id,
                        anime_id=new_aid,
                        role=c.role,
                        raw_role=c.raw_role,
                        episode=c.episode,
                        source=c.source,
                    )
                )
            context.credits = remapped

            # Merge anime metadata: keep canonical, absorb useful fields from others
            canonical_anime: dict[str, Anime] = {}
            non_canonical: dict[str, list[Anime]] = defaultdict(list)
            for a in context.anime_list:
                cid = anime_canonical_map.get(a.id)
                if cid is None:
                    canonical_anime[a.id] = a
                else:
                    non_canonical[cid].append(a)

            # Enrich canonical anime with metadata from non-canonical sources
            for cid, others in non_canonical.items():
                canon = canonical_anime.get(cid)
                if not canon:
                    continue
                for other in others:
                    # Fill in missing metadata from richer sources
                    if not canon.year and other.year:
                        canon.year = other.year
                    if not canon.episodes and other.episodes:
                        canon.episodes = other.episodes
                    if not canon.format and other.format:
                        canon.format = other.format
                    if not canon.duration and other.duration:
                        canon.duration = other.duration
                    if not canon.score and other.score:
                        canon.score = other.score
                    if not canon.genres and other.genres:
                        canon.genres = other.genres
                    if not canon.studios and other.studios:
                        canon.studios = other.studios
                    if not canon.description and other.description:
                        canon.description = other.description
                    if not canon.cover_large and other.cover_large:
                        canon.cover_large = other.cover_large
                    if not canon.anilist_id and other.anilist_id:
                        canon.anilist_id = other.anilist_id
                    if not canon.mal_id and other.mal_id:
                        canon.mal_id = other.mal_id
                    if not canon.madb_id and other.madb_id:
                        canon.madb_id = other.madb_id
                    if not canon.ann_id and other.ann_id:
                        canon.ann_id = other.ann_id

            context.anime_list = list(canonical_anime.values())
            context.anime_map = {a.id: a for a in context.anime_list}

            logger.info(
                "anime_ids_resolved",
                merged=len(anime_canonical_map),
                anime_after=len(context.anime_list),
            )
            context.monitor.increment_counter(
                "anime_resolved", len(anime_canonical_map)
            )

        # Log anomalies (wiki canonical has fewer credits) as JSON
        if anomalies:
            anomalies.sort(key=lambda x: -x["deficit"])
            from src.utils.config import JSON_DIR

            anomaly_path = JSON_DIR / "anime_merge_anomalies.json"
            anomaly_path.parent.mkdir(parents=True, exist_ok=True)
            anomaly_path.write_text(
                json.dumps(anomalies, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            logger.warning(
                "anime_merge_anomalies",
                count=len(anomalies),
                top_deficit=anomalies[0]["title_ja"] if anomalies else None,
                output=str(anomaly_path),
            )

        # Merge duplicate credits (same person+anime+role from different sources)
        context.credits = _merge_duplicate_credits(context.credits)

        # Remove non-canonical person entries from context.persons.
        # After resolution, their credits have been remapped to canonical IDs,
        # so these entries would appear as 0-credit ghosts in the graph and scores.
        all_non_canonical = set(context.canonical_map.keys())
        if anime_canonical_map:
            # anime canonical_map keys are non-canonical anime_ids, not person_ids
            pass
        persons_before = len(context.persons)
        context.persons = [p for p in context.persons if p.id not in all_non_canonical]
        removed = persons_before - len(context.persons)
        if removed > 0:
            logger.info(
                "non_canonical_persons_removed",
                removed=removed,
                persons_after=len(context.persons),
            )
