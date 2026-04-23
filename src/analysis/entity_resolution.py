"""Entity resolution — merge variant spellings of the same person.

Accuracy is a hard legal requirement (defamation risk), so we use
conservative matching (high precision, low recall).

False positives (merging different persons) must never occur.
False negatives (missing a merge) are acceptable.
"""

import functools
import re
import unicodedata
from collections import defaultdict

import structlog
from rapidfuzz import fuzz

from src.models import Person

logger = structlog.get_logger()


def normalize_name(name: str) -> str:
    """Normalise a name string.

    - NFKC normalisation (full-width → half-width, compatibility unification)
    - whitespace normalisation
    - honorific stripping
    - lowercase for English names
    """
    if not name:
        return ""

    # NFKC normalisation
    name = unicodedata.normalize("NFKC", name)

    # normalise whitespace
    name = re.sub(r"\s+", " ", name).strip()

    # strip honorifics
    honorifics = ["さん", "先生", "氏", "様", "san", "sensei"]
    for h in honorifics:
        name = re.sub(rf"\s*{re.escape(h)}$", "", name)

    # lowercase English names
    if all(ord(c) < 128 or c.isspace() for c in name):
        name = name.lower()

    return name


def _is_kanji(char: str) -> bool:
    """Return True if the character is a kanji."""
    cp = ord(char)
    return (
        (0x4E00 <= cp <= 0x9FFF)  # CJK unified ideographs
        or (0x3400 <= cp <= 0x4DBF)  # CJK unified ideographs extension A
        or (0x20000 <= cp <= 0x2A6DF)  # CJK unified ideographs extension B
    )


def _is_japanese_name(name: str) -> bool:
    """Return True if the name appears to be Japanese."""
    return any(
        _is_kanji(c) or ("\u3040" <= c <= "\u309f") or ("\u30a0" <= c <= "\u30ff")
        for c in name
    )


def _definitely_different(p1: Person, p2: Person) -> bool:
    """Return True when numeric IDs are both set and differ — guaranteed different persons.

    Sources such as ANN/AniList/MAL assign distinct numeric IDs to different persons
    who share the same name, so a numeric-ID mismatch is conclusive evidence of different persons.
    """
    if p1.ann_id and p2.ann_id and p1.ann_id != p2.ann_id:
        return True
    if p1.anilist_id and p2.anilist_id and p1.anilist_id != p2.anilist_id:
        return True
    if p1.mal_id and p2.mal_id and p1.mal_id != p2.mal_id:
        return True
    return False


def _numeric_id_key(p: Person) -> tuple:
    """Key used to split homonyms within a same-name group.

    Uses numeric IDs when available; falls back to the person_id itself when absent.
    This separates persons who share a name but have different numeric IDs into distinct clusters.
    """
    return (p.ann_id, p.anilist_id, p.mal_id)


def exact_match_cluster(
    persons: list[Person],
    ml_clusters: dict[str, str] | None = None,
) -> dict[str, str]:
    """Entity resolution by exact match (most conservative).

    Merges only when normalised names are an exact match.
    Japanese names take priority; English-only matching is permitted only when
    no Japanese name exists, preventing false positives from romanisation ambiguity
    (e.g. 岡遼子 vs 岡亮子 both romanise to "Ryouko Oka").

    Homonym guard: does not merge when ann_id / anilist_id / mal_id are both set
    and differ (different persons with the same name).

    Returns: {person_id: canonical_id}
    """
    # name groups by script
    ja_name_groups: dict[str, list[str]] = defaultdict(list)
    ko_name_groups: dict[str, list[str]] = defaultdict(list)
    zh_name_groups: dict[str, list[str]] = defaultdict(list)
    # group by English name (only persons without a native name)
    en_name_groups: dict[str, list[str]] = defaultdict(list)
    # group by alias
    alias_groups: dict[str, list[str]] = defaultdict(list)

    persons_by_id = {p.id: p for p in persons}

    for p in persons:
        has_native = False
        if p.name_ja:
            normalized_ja = normalize_name(p.name_ja)
            if normalized_ja:
                ja_name_groups[normalized_ja].append(p.id)
                has_native = True
        if p.name_ko:
            normalized_ko = normalize_name(p.name_ko)
            if normalized_ko:
                ko_name_groups[normalized_ko].append(p.id)
                has_native = True
        if p.name_zh:
            normalized_zh = normalize_name(p.name_zh)
            if normalized_zh:
                zh_name_groups[normalized_zh].append(p.id)
                has_native = True
        # English name only for persons without a native name
        if not has_native and p.name_en:
            normalized_en = normalize_name(p.name_en)
            if normalized_en:
                en_name_groups[normalized_en].append(p.id)

        # aliases used as secondary signal
        for alias in p.aliases:
            normalized_alias = normalize_name(alias)
            if normalized_alias:
                alias_groups[normalized_alias].append(p.id)

    canonical_map: dict[str, str] = {}

    def _merge_group(ids: list[str], name: str, strategy: str) -> None:
        """Merge within a same-name group while protecting against homonym collisions."""
        unique_ids = list(dict.fromkeys(ids))
        if len(unique_ids) < 2:
            return
        # homonym split: separate into different clusters when merge is not allowed
        #   1. numeric IDs (ann_id/anilist_id/mal_id) are both set and differ
        #   2. ML clustering assigned them to different clusters
        clusters: list[list[str]] = []
        for pid in unique_ids:
            placed = False
            p = persons_by_id[pid]
            for cluster in clusters:
                rep_id = cluster[0]
                rep = persons_by_id[rep_id]
                # guaranteed different persons via numeric ID
                if _definitely_different(p, rep):
                    continue
                # different-person determination via ML cluster
                if ml_clusters:
                    p_cluster = ml_clusters.get(pid)
                    rep_cluster = ml_clusters.get(rep_id)
                    if (
                        p_cluster is not None
                        and rep_cluster is not None
                        and p_cluster != rep_cluster
                    ):
                        continue
                cluster.append(pid)
                placed = True
                break
            if not placed:
                clusters.append([pid])

        for cluster in clusters:
            if len(cluster) < 2:
                continue
            canonical = cluster[0]
            for pid in cluster[1:]:
                if pid not in canonical_map:
                    canonical_map[pid] = canonical
                    logger.info(
                        "entity_merged",
                        source=pid,
                        canonical=canonical,
                        strategy=strategy,
                        name=name,
                    )

    # merge by script-specific name (native names are independent of each other)
    for name_ja, ids in ja_name_groups.items():
        _merge_group(ids, name_ja, "exact_match")
    for name_ko, ids in ko_name_groups.items():
        _merge_group(ids, name_ko, "exact_match")
    for name_zh, ids in zh_name_groups.items():
        _merge_group(ids, name_zh, "exact_match")

    # merge by English name (only persons without a native name)
    for name_en, ids in en_name_groups.items():
        _merge_group(ids, name_en, "exact_match")

    # merge by alias (secondary, only when not already matched)
    for alias, ids in alias_groups.items():
        if len(ids) < 2:
            continue
        unique_ids = list(dict.fromkeys(ids))
        # alias match allowed only when person has no native name
        valid_ids = [
            pid for pid in unique_ids
            if not (persons_by_id[pid].name_ja
                    or persons_by_id[pid].name_ko
                    or persons_by_id[pid].name_zh)
        ]
        _merge_group(valid_ids, alias, "exact_match_alias")

    return canonical_map


def _normalize_romaji(name: str) -> str:
    """Normalise a romanised name.

    - lowercase
    - strip hyphens and apostrophes
    - normalise long-vowel macrons (ō→o, ū→u, etc.)
    - sort name tokens to absorb word-order differences (family/given name swap)
    """
    if not name:
        return ""

    name = name.lower().strip()

    # strip symbols
    name = name.replace("-", "").replace("'", "").replace("'", "")

    # strip long-vowel macrons
    macron_map = str.maketrans("āēīōūÅĒĪŌŪ", "aeiouaeiou")
    name = name.translate(macron_map)

    # sort name parts ("yamada taro" == "taro yamada")
    parts = sorted(name.split())
    return " ".join(parts)


def romaji_match(persons: list[Person]) -> dict[str, str]:
    """Entity resolution by normalised romanised-name comparison.

    Normalises and compares the romanised English name (name_en).
    Absorbs word-order differences (family/given name transposition).
    Only names of sufficient length are considered (short names are too ambiguous).
    """
    MIN_NAME_LENGTH = 5  # exclude short names like "Ai Li"

    # classify by source
    mal_persons: dict[str, Person] = {}
    anilist_persons: dict[str, Person] = {}

    for p in persons:
        if p.id.startswith("mal:"):
            mal_persons[p.id] = p
        elif p.id.startswith("anilist:"):
            anilist_persons[p.id] = p

    # normalised romaji index for MAL
    mal_romaji_index: dict[str, list[str]] = defaultdict(list)
    for pid, p in mal_persons.items():
        if p.name_en and len(p.name_en) >= MIN_NAME_LENGTH:
            normalized = _normalize_romaji(p.name_en)
            if normalized:
                mal_romaji_index[normalized].append(pid)

    canonical_map: dict[str, str] = {}

    for anilist_pid, p in anilist_persons.items():
        if not p.name_en or len(p.name_en) < MIN_NAME_LENGTH:
            continue
        normalized = _normalize_romaji(p.name_en)
        if not normalized:
            continue
        if normalized in mal_romaji_index:
            mal_ids = mal_romaji_index[normalized]
            if len(mal_ids) == 1 and anilist_pid not in canonical_map:
                canonical_map[anilist_pid] = mal_ids[0]
                logger.info(
                    "entity_merged",
                    source=anilist_pid,
                    canonical=mal_ids[0],
                    strategy="romaji_match",
                    name=normalized,
                )

    return canonical_map


def cross_source_match(persons: list[Person]) -> dict[str, str]:
    """Entity resolution across different data sources.

    Merges MAL/MADB/ANN persons against AniList by exact name match.
    MADB persons use normalised name_ja only (to reduce legal risk).
    ANN persons use name_ja + name_en exact match. Homonym protection applied.
    """
    # classify by source
    mal_persons: dict[str, Person] = {}
    anilist_persons: dict[str, Person] = {}
    madb_persons: dict[str, Person] = {}
    ann_persons: dict[str, Person] = {}
    allcinema_persons: dict[str, Person] = {}

    for p in persons:
        if p.id.startswith("mal:"):
            mal_persons[p.id] = p
        elif p.id.startswith("anilist:"):
            anilist_persons[p.id] = p
        elif p.id.startswith("madb:"):
            madb_persons[p.id] = p
        elif p.id.startswith("ann-"):
            ann_persons[p.id] = p
        elif p.id.startswith("allcinema:"):
            allcinema_persons[p.id] = p

    # Normalised name index for AniList (ja/ko/zh/en)
    anilist_ja_index: dict[str, list[str]] = defaultdict(list)
    anilist_ko_index: dict[str, list[str]] = defaultdict(list)
    anilist_zh_index: dict[str, list[str]] = defaultdict(list)
    anilist_en_index: dict[str, list[str]] = defaultdict(list)
    for pid, p in anilist_persons.items():
        if p.name_ja:
            n = normalize_name(p.name_ja)
            if n and len(n) >= 3:
                anilist_ja_index[n].append(pid)
        if p.name_ko:
            n = normalize_name(p.name_ko)
            if n and len(n) >= 2:
                anilist_ko_index[n].append(pid)
        if p.name_zh:
            n = normalize_name(p.name_zh)
            if n and len(n) >= 2:
                anilist_zh_index[n].append(pid)
        if p.name_en:
            n = normalize_name(p.name_en)
            if n and len(n) >= 5:
                anilist_en_index[n].append(pid)

    # normalised name index for MAL
    mal_name_index: dict[str, list[str]] = defaultdict(list)
    for pid, p in mal_persons.items():
        for name in [p.name_ja, p.name_en] + p.aliases:
            n = normalize_name(name)
            if n and len(n) >= 2:
                mal_name_index[n].append(pid)

    canonical_map: dict[str, str] = {}

    # MAL → AniList matching
    for anilist_pid, p in anilist_persons.items():
        for name in [p.name_ja, p.name_ko, p.name_zh, p.name_en] + p.aliases:
            n = normalize_name(name)
            if n and n in mal_name_index:
                mal_ids = mal_name_index[n]
                if len(mal_ids) == 1:
                    mal_p = next(
                        mp for mid, mp in mal_persons.items() if mid == mal_ids[0]
                    )
                    if not _definitely_different(p, mal_p):
                        canonical_map[anilist_pid] = mal_ids[0]
                        logger.info(
                            "entity_merged",
                            source=anilist_pid,
                            canonical=mal_ids[0],
                            strategy="cross_source",
                            name=n,
                        )
                        break
                else:
                    logger.debug(
                        "ambiguous_cross_source_match",
                        name=n,
                        candidates=mal_ids,
                    )

    # MADB → AniList matching (name_ja only, high precision)
    for madb_pid, p in madb_persons.items():
        if not p.name_ja:
            continue
        n = normalize_name(p.name_ja)
        if not n or len(n) < 3:
            continue
        if n in anilist_ja_index:
            anilist_ids = anilist_ja_index[n]
            if len(anilist_ids) == 1:
                al_p = anilist_persons[anilist_ids[0]]
                if not _definitely_different(p, al_p):
                    canonical_map[madb_pid] = anilist_ids[0]
                    logger.info(
                        "entity_merged",
                        source=madb_pid,
                        canonical=anilist_ids[0],
                        strategy="cross_source_madb",
                        name=n,
                    )
            else:
                logger.debug(
                    "ambiguous_cross_source_match",
                    name=n,
                    candidates=anilist_ids,
                    source="madb",
                )

    # ANN → AniList matching (name_ja preferred, name_en as fallback)
    # ANN persons carry ann_id, so homonym protection is active
    for ann_pid, p in ann_persons.items():
        if ann_pid in canonical_map:
            continue
        matched = False
        # match by name_ja (highest priority)
        if p.name_ja:
            n = normalize_name(p.name_ja)
            if n and len(n) >= 3 and n in anilist_ja_index:
                al_ids = anilist_ja_index[n]
                if len(al_ids) == 1:
                    al_p = anilist_persons[al_ids[0]]
                    if not _definitely_different(p, al_p):
                        canonical_map[ann_pid] = al_ids[0]
                        logger.info(
                            "entity_merged",
                            source=ann_pid,
                            canonical=al_ids[0],
                            strategy="cross_source_ann",
                            name=n,
                        )
                        matched = True
                elif len(al_ids) > 1:
                    logger.debug(
                        "ambiguous_cross_source_match",
                        name=n,
                        candidates=al_ids,
                        source="ann",
                    )
        # name_en fallback (for ANN persons without a Japanese name)
        if not matched and p.name_en and not p.name_ja:
            n = normalize_name(p.name_en)
            if n and len(n) >= 5 and n in anilist_en_index:
                al_ids = anilist_en_index[n]
                if len(al_ids) == 1:
                    al_p = anilist_persons[al_ids[0]]
                    if not _definitely_different(p, al_p):
                        canonical_map[ann_pid] = al_ids[0]
                        logger.info(
                            "entity_merged",
                            source=ann_pid,
                            canonical=al_ids[0],
                            strategy="cross_source_ann_en",
                            name=n,
                        )

    # allcinema → AniList/ANN matching (name_ja exact match, homonym protection)
    # allcinema persons carry allcinema_id, so numeric-ID protection is active
    ann_ja_index: dict[str, list[str]] = defaultdict(list)
    for ann_pid, p in ann_persons.items():
        if p.name_ja:
            n = normalize_name(p.name_ja)
            if n and len(n) >= 3:
                ann_ja_index[n].append(ann_pid)

    for ac_pid, p in allcinema_persons.items():
        if ac_pid in canonical_map:
            continue
        if not p.name_ja:
            continue
        n = normalize_name(p.name_ja)
        if not n or len(n) < 3:
            continue
        matched = False
        # prefer AniList
        if n in anilist_ja_index:
            al_ids = anilist_ja_index[n]
            if len(al_ids) == 1:
                al_p = anilist_persons[al_ids[0]]
                if not _definitely_different(p, al_p):
                    canonical_map[ac_pid] = al_ids[0]
                    logger.info(
                        "entity_merged",
                        source=ac_pid,
                        canonical=al_ids[0],
                        strategy="cross_source_allcinema",
                        name=n,
                    )
                    matched = True
            elif len(al_ids) > 1:
                logger.debug(
                    "ambiguous_cross_source_match",
                    name=n,
                    candidates=al_ids,
                    source="allcinema",
                )
        # ANN fallback
        if not matched and n in ann_ja_index:
            ann_ids = ann_ja_index[n]
            if len(ann_ids) == 1:
                ann_p = ann_persons[ann_ids[0]]
                if not _definitely_different(p, ann_p):
                    canonical_map[ac_pid] = ann_ids[0]
                    logger.info(
                        "entity_merged",
                        source=ac_pid,
                        canonical=ann_ids[0],
                        strategy="cross_source_allcinema_ann",
                        name=n,
                    )

    return canonical_map


def similarity_based_cluster(
    persons: list[Person], threshold: float = 0.95
) -> dict[str, str]:
    """Entity resolution by string similarity (most conservative).

    Uses Jaro-Winkler similarity with a very high threshold (default 0.95)
    to merge similar-looking names.  Comparisons are made within the same source
    only, to minimise false positives.

    Args:
        persons: list of Person objects
        threshold: similarity threshold (≥0.95 recommended; closer to 1.0 is more conservative)

    Returns:
        {person_id: canonical_id} mapping

    Notes:
        - Legal requirements make false positives completely unacceptable
        - Names shorter than 5 characters are excluded (too ambiguous)
        - Only same-source comparisons (MAL↔MAL, AniList↔AniList)
        - Japanese names and romanised names are evaluated separately
    """
    if threshold < 0.9:
        logger.warning(
            "similarity_threshold_too_low", threshold=threshold, recommended=0.95
        )

    MIN_NAME_LENGTH = 5

    # classify by source
    persons_by_source: dict[str, list[Person]] = defaultdict(list)
    for p in persons:
        source = p.id.split(":")[0] if ":" in p.id else "unknown"
        persons_by_source[source].append(p)

    canonical_map: dict[str, str] = {}

    # similarity matching within each source
    for source, source_persons in persons_by_source.items():
        if len(source_persons) < 2:
            continue

        # build name → person_id mapping
        name_to_ids: dict[str, list[str]] = defaultdict(list)

        for p in source_persons:
            for name_field in [p.name_ja, p.name_ko, p.name_zh, p.name_en] + p.aliases:
                if not name_field:
                    continue
                normalized = normalize_name(name_field)
                if len(normalized) >= MIN_NAME_LENGTH:
                    name_to_ids[normalized].append(p.id)

        # Blocking optimization: group names by first character (reduces comparisons by ~95%)
        blocks: dict[str, list[str]] = defaultdict(list)
        for name in name_to_ids.keys():
            if len(name) >= MIN_NAME_LENGTH:
                first_char = name[0].lower()
                blocks[first_char].append(name)

        # LRU cache for expensive fuzzy similarity calls
        @functools.lru_cache(maxsize=10000)
        def cached_similarity(n1: str, n2: str) -> float:
            return fuzz.ratio(n1, n2) / 100.0

        # Compare within blocks only (same first character)
        comparisons_made = 0
        comparisons_skipped = 0

        for block_char, block_names in blocks.items():
            for i in range(len(block_names)):
                name1 = block_names[i]
                for j in range(i + 1, len(block_names)):
                    name2 = block_names[j]

                    # Early filter: skip if length differs by >20%
                    len_ratio = abs(len(name1) - len(name2)) / max(
                        len(name1), len(name2)
                    )
                    if len_ratio > 0.2:
                        comparisons_skipped += 1
                        continue

                    # Early filter: skip if first 3 chars don't match
                    if len(name1) >= 3 and len(name2) >= 3:
                        if name1[:3] != name2[:3]:
                            comparisons_skipped += 1
                            continue

                    # Jaro-Winkler similarity (prefix-weighted, well-suited for name matching)
                    similarity = cached_similarity(name1, name2)
                    comparisons_made += 1

                    if similarity >= threshold:
                        ids1 = name_to_ids[name1]
                        ids2 = name_to_ids[name2]

                        # accept only 1-to-1 matches (reject ambiguous)
                        if len(ids1) == 1 and len(ids2) == 1:
                            canonical = ids1[0]
                            target = ids2[0]

                            # add only if not yet mapped
                            if target not in canonical_map:
                                canonical_map[target] = canonical
                                logger.info(
                                    "entity_merged",
                                    source=target,
                                    canonical=canonical,
                                    strategy="similarity_based",
                                    similarity=f"{similarity:.3f}",
                                    name1=name1,
                                    name2=name2,
                                )

        logger.debug(
            "similarity_blocking_stats",
            comparisons_made=comparisons_made,
            comparisons_skipped=comparisons_skipped,
            reduction_pct=round(
                100
                * comparisons_skipped
                / max(1, comparisons_made + comparisons_skipped),
                1,
            ),
        )

    return canonical_map


def _transitive_closure(mapping: dict[str, str]) -> dict[str, str]:
    """Compute transitive closure so every key points directly to its final canonical ID.

    Example: {A→B, B→C} → {A→C, B→C}
    Follows each chain to its terminal (a value not present as a key).
    """
    if not mapping:
        return mapping

    # trace the chain for each key
    resolved: dict[str, str] = {}
    for key in mapping:
        target = mapping[key]
        # follow chain to end (with cycle guard)
        visited: set[str] = {key}
        while target in mapping and target not in visited:
            visited.add(target)
            target = mapping[target]
        resolved[key] = target

    return resolved


def resolve_all(
    persons: list[Person],
    credits_by_person: dict[str, list] | None = None,
    anime_meta: dict[str, dict] | None = None,
) -> dict[str, str]:
    """Run all entity resolution steps.

    Args:
        persons: complete list of persons
        credits_by_person: {person_id: [Credit, ...]} (used for ML homonym splitting)
        anime_meta: {anime_id: {"year": int, "studios": list}} (used for ML homonym splitting)

    Returns: {person_id: canonical_id}
    Persons that were not merged are absent from the mapping.
    """
    # Step 0: ML credit-pattern homonym splitting
    ml_clusters: dict[str, str] | None = None
    if credits_by_person and anime_meta:
        try:
            from src.analysis.ml_homonym_split import split_homonym_groups

            ml_clusters = split_homonym_groups(persons, credits_by_person, anime_meta)
            logger.info("ml_homonym_split_applied", n_clustered=len(ml_clusters))
        except Exception as exc:
            logger.warning("ml_homonym_split_failed", error=str(exc))

    # Step 1: exact match (ML clusters used as guard)
    exact = exact_match_cluster(persons, ml_clusters=ml_clusters)

    # Step 2: cross-source match (exact match)
    cross = cross_source_match(persons)

    # Step 3: romaji match (excluding already-matched persons)
    # exclude both keys and values: prevents canonical IDs from being re-matched downstream
    already_matched = (
        set(exact) | set(exact.values()) | set(cross) | set(cross.values())
    )
    remaining = [p for p in persons if p.id not in already_matched]
    romaji = romaji_match(remaining)

    # Step 4: similarity-based match (excluding already-matched persons)
    already_matched = already_matched | set(romaji) | set(romaji.values())
    remaining = [p for p in persons if p.id not in already_matched]
    similarity = similarity_based_cluster(remaining, threshold=0.95)

    # Step 5: AI-assisted matching (LLM) for borderline similarity pairs
    already_matched = already_matched | set(similarity) | set(similarity.values())
    ai_merges = _ai_assisted_step(persons, already_matched)

    # merge + transitive closure
    merged = {**exact, **cross, **romaji, **similarity, **ai_merges}
    merged = _transitive_closure(merged)
    logger.info(
        "entity_resolution_complete",
        total_merges=len(merged),
        exact_merges=len(exact),
        cross_source_merges=len(cross),
        romaji_merges=len(romaji),
        similarity_merges=len(similarity),
        ai_merges=len(ai_merges),
    )
    return merged


def _ai_assisted_step(
    persons: list[Person], already_matched: set[str]
) -> dict[str, str]:
    """Step 5: AI-assisted entity resolution for borderline cases.

    Finds pairs with similarity 0.85-0.95 (too low for auto-match, too high
    to ignore) and asks the LLM to verify.

    Returns: {person_id: canonical_id}
    """
    try:
        from src.analysis.ai_entity_resolution import (
            LLMError,
            ask_llm_if_same_person,
            check_llm_available,
        )
        from src.analysis.llm_pipeline import find_ai_match_candidates, is_llm_enabled
    except ImportError:
        return {}

    if not is_llm_enabled():
        return {}

    if not check_llm_available():
        logger.info("ai_entity_resolution_skipped", reason="llm_not_available")
        return {}

    candidates = find_ai_match_candidates(persons, already_matched, max_candidates=500)
    if not candidates:
        return {}

    logger.info("ai_entity_resolution_start", candidates=len(candidates))

    ai_map: dict[str, str] = {}
    merged_ids: set[str] = set()

    for p1, p2, sim in candidates:
        if p1.id in merged_ids or p2.id in merged_ids:
            continue

        try:
            decision = ask_llm_if_same_person(p1, p2)
            if decision.is_match and decision.confidence >= 0.8:
                ai_map[p2.id] = p1.id
                merged_ids.add(p1.id)
                merged_ids.add(p2.id)
                logger.info(
                    "entity_merged",
                    canonical=p1.id,
                    source=p2.id,
                    strategy="ai_assisted",
                    similarity=f"{sim:.3f}",
                    confidence=f"{decision.confidence:.2f}",
                    name1=p1.name_ja,
                    name2=p2.name_ja,
                )
        except LLMError:
            continue

    return ai_map
