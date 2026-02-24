"""アニメタイトルマッチング — MADB ↔ AniList のタイトル照合.

MADB (Media Arts Database) のアニメと AniList のアニメを
タイトルの正規化一致・fuzzy マッチング・年検証で紐づける。

entity_resolution.py は person matching 専用。
anime matching は別の関心事（タイトル fuzzy matching + 年検証）なのでここに分離。

設計方針:
- false positive（別作品を同一と判定）を避ける保守的マッチング
- 1:1 マッピング強制（多対一を防止）
- 曖昧なケース（同一年で複数候補）は安全側にスキップ
"""

from __future__ import annotations

import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass

import structlog
from rapidfuzz import fuzz

log = structlog.get_logger()


@dataclass(frozen=True, slots=True)
class AnimeMatch:
    """マッチング結果."""

    madb_anime_id: str
    anilist_anime_id: str
    madb_title: str
    anilist_title: str
    score: float
    strategy: str  # "exact" or "fuzzy"


# Patterns to strip from titles before comparison
_STRIP_PATTERNS = re.compile(
    r"\s*[\(\[（【]"
    r"(?:TV|OVA|OAD|ONA|劇場版|映画|第?\d+期|第?\d+シーズン|Season\s*\d+|\d+st|\d+nd|\d+rd|\d+th)"
    r"[\)\]）】]\s*",
    re.IGNORECASE,
)

# Whitespace / punctuation normalization
_WHITESPACE = re.compile(r"\s+")
_PUNCTUATION = re.compile(r"[～〜~・:：\-−–—]+")


def normalize_anime_title(title: str) -> str:
    """タイトルを正規化する.

    - NFKC正規化（全角→半角、互換文字統一）
    - (TV)/(OVA) 等のサフィックス除去
    - 句読点・記号の統一
    - 空白正規化
    """
    if not title:
        return ""

    title = unicodedata.normalize("NFKC", title)
    title = _STRIP_PATTERNS.sub("", title)
    title = _PUNCTUATION.sub(" ", title)
    title = _WHITESPACE.sub(" ", title).strip()

    return title


def _build_anilist_index(
    anilist_anime: list[dict],
) -> tuple[dict[str, list[str]], dict[str, dict]]:
    """AniList anime のタイトルインデックスを構築する.

    Returns:
        (normalized_title → [anime_id, ...], anime_id → anime_dict)
    """
    title_index: dict[str, list[str]] = defaultdict(list)
    anime_by_id: dict[str, dict] = {}

    for anime in anilist_anime:
        aid = anime["id"]
        anime_by_id[aid] = anime

        # Index all title variants
        titles: list[str] = []
        if anime.get("title_ja"):
            titles.append(anime["title_ja"])
        if anime.get("title_en"):
            titles.append(anime["title_en"])
        for syn in anime.get("synonyms", []) or []:
            if syn:
                titles.append(syn)

        for t in titles:
            normalized = normalize_anime_title(t)
            if normalized:
                title_index[normalized].append(aid)

    return title_index, anime_by_id


def _year_compatible(
    madb_year: int | None,
    anilist_year: int | None,
    tolerance: int,
) -> bool:
    """年が互換性があるか確認する.

    両方に year がある場合のみ検証。片方が欠損なら通す。
    """
    if madb_year is None or anilist_year is None:
        return True
    return abs(madb_year - anilist_year) <= tolerance


def match_anime_titles(
    madb_anime: list[dict],
    anilist_anime: list[dict],
    threshold: int = 90,
    year_tolerance: int = 1,
) -> list[AnimeMatch]:
    """MADB anime と AniList anime をタイトルマッチングする.

    Args:
        madb_anime: MADB anime dicts (id, title, year, ...)
        anilist_anime: AniList anime dicts (id, title_ja, title_en, year, synonyms, ...)
        threshold: fuzzy match の最低スコア (0-100)
        year_tolerance: 年の許容誤差

    Returns:
        マッチ結果のリスト

    マッチング戦略:
        1. 正規化タイトルの完全一致（高速パス）
        2. rapidfuzz.fuzz.token_sort_ratio で fuzzy match
        3. 年の検証: 両方に year がある場合 ±tolerance 年以内
        4. 1:1 マッピング強制（多対一を防止）
    """
    title_index, anime_by_id = _build_anilist_index(anilist_anime)

    matches: list[AnimeMatch] = []
    used_anilist_ids: set[str] = set()
    used_madb_ids: set[str] = set()

    # --- Pass 1: Exact match (fast path) ---
    for madb in madb_anime:
        madb_id = madb["id"]
        if madb_id in used_madb_ids:
            continue

        madb_title = madb.get("title", "")
        normalized = normalize_anime_title(madb_title)
        if not normalized:
            continue

        candidates = title_index.get(normalized, [])
        # Filter by year and already-used
        valid = [
            aid
            for aid in candidates
            if aid not in used_anilist_ids
            and _year_compatible(
                madb.get("year"), anime_by_id[aid].get("year"), year_tolerance
            )
        ]

        # Deduplicate (same anime_id can appear multiple times via different titles)
        valid = list(dict.fromkeys(valid))

        if len(valid) == 1:
            aid = valid[0]
            anilist_title = (
                anime_by_id[aid].get("title_ja")
                or anime_by_id[aid].get("title_en")
                or ""
            )
            matches.append(
                AnimeMatch(
                    madb_anime_id=madb_id,
                    anilist_anime_id=aid,
                    madb_title=madb_title,
                    anilist_title=anilist_title,
                    score=100.0,
                    strategy="exact",
                )
            )
            used_anilist_ids.add(aid)
            used_madb_ids.add(madb_id)
        elif len(valid) > 1:
            log.debug(
                "anime_match_ambiguous_exact",
                madb_id=madb_id,
                title=madb_title,
                candidates=len(valid),
            )

    # --- Pass 2: Fuzzy match (remaining) ---
    # Build first-character blocking index for performance
    char_blocks: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for norm_title, aids in title_index.items():
        if norm_title:
            first = norm_title[0].lower()
            for aid in aids:
                if aid not in used_anilist_ids:
                    char_blocks[first].append((norm_title, aid))

    for madb in madb_anime:
        madb_id = madb["id"]
        if madb_id in used_madb_ids:
            continue

        madb_title = madb.get("title", "")
        normalized = normalize_anime_title(madb_title)
        if not normalized or len(normalized) < 3:
            continue

        first_char = normalized[0].lower()
        block = char_blocks.get(first_char, [])

        best_score = 0.0
        best_aid: str | None = None
        best_anilist_title = ""
        ambiguous = False

        for anilist_norm, aid in block:
            if aid in used_anilist_ids:
                continue

            # Year filter (cheap, applied before expensive fuzzy)
            if not _year_compatible(
                madb.get("year"), anime_by_id[aid].get("year"), year_tolerance
            ):
                continue

            # Length filter: skip if lengths differ too much (>50%)
            len_ratio = abs(len(normalized) - len(anilist_norm)) / max(
                len(normalized), len(anilist_norm)
            )
            if len_ratio > 0.5:
                continue

            score = fuzz.ratio(normalized, anilist_norm)
            if score >= threshold:
                if score > best_score:
                    best_score = score
                    best_aid = aid
                    best_anilist_title = (
                        anime_by_id[aid].get("title_ja")
                        or anime_by_id[aid].get("title_en")
                        or ""
                    )
                    ambiguous = False
                elif score == best_score and aid != best_aid:
                    ambiguous = True

        if best_aid and not ambiguous:
            matches.append(
                AnimeMatch(
                    madb_anime_id=madb_id,
                    anilist_anime_id=best_aid,
                    madb_title=madb_title,
                    anilist_title=best_anilist_title,
                    score=best_score,
                    strategy="fuzzy",
                )
            )
            used_anilist_ids.add(best_aid)
            used_madb_ids.add(madb_id)
        elif ambiguous:
            log.debug(
                "anime_match_ambiguous_fuzzy",
                madb_id=madb_id,
                title=madb_title,
                score=best_score,
            )

    exact_count = sum(1 for m in matches if m.strategy == "exact")
    fuzzy_count = sum(1 for m in matches if m.strategy == "fuzzy")
    log.info(
        "anime_title_matching_complete",
        total_matches=len(matches),
        exact=exact_count,
        fuzzy=fuzzy_count,
        madb_total=len(madb_anime),
        anilist_total=len(anilist_anime),
        match_rate=round(100 * len(matches) / max(1, len(madb_anime)), 1),
    )

    return matches
