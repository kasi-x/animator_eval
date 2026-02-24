"""アニメタイトルマッチング — MADB ↔ AniList のタイトル照合.

MADB (Media Arts Database) のアニメと AniList のアニメを
タイトルの正規化一致・fuzzy マッチング・年検証で紐づける。

entity_resolution.py は person matching 専用。
anime matching は別の関心事（タイトル fuzzy matching + 年検証）なのでここに分離。

設計方針:
- false positive（別作品を同一と判定）を避ける保守的マッチング
- 1:1 マッピング強制（多対一を防止）
- 曖昧なケース（同一年で複数候補）は format 優先度で解決
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
    strategy: str  # "exact", "fuzzy", or "contains"


# Format priority: prefer main content over specials/shorts
# Lower number = higher priority
_FORMAT_PRIORITY: dict[str | None, int] = {
    "TV": 0,
    "MOVIE": 1,
    "OVA": 2,
    "ONA": 3,
    "TV_SHORT": 4,
    "SPECIAL": 5,
    "MUSIC": 6,
}
_DEFAULT_FORMAT_PRIORITY = 99

# Patterns to strip from titles before comparison
_STRIP_PATTERNS = re.compile(
    r"\s*[\(\[（【［]"
    r"(?:"
    r"TV|OVA|OAD|ONA|劇場版|映画"
    r"|第?\d+期|第?\d+シーズン|第?\d+シリーズ"
    r"|[一二三四五六七八九十]+期|[一二三四五六七八九十]+シーズン"
    r"|第?\d+作"  # (第2作), (第3作)
    r"|新|シリーズ|スペシャル|特別編|総集編|続章|前編|後編"
    r"|Season\s*\d+"
    r"|\d+st|\d+nd|\d+rd|\d+th"
    r"|\d{4}"  # bare year like [2017]
    r")"
    r"[\)\]）】］]\s*",
    re.IGNORECASE,
)

# Whitespace / punctuation normalization
_WHITESPACE = re.compile(r"\s+")
_PUNCTUATION = re.compile(r"[～〜~・:：\-−–—]+")
# Trailing punctuation (e.g. 銀魂.)
_TRAILING_PUNCT = re.compile(r"[.。!！?？]+$")


def normalize_anime_title(title: str) -> str:
    """タイトルを正規化する.

    - NFKC正規化（全角→半角、互換文字統一）
    - (TV)/(OVA) 等のサフィックス除去
    - 句読点・記号の統一
    - 空白正規化
    - 小文字化
    """
    if not title:
        return ""

    title = unicodedata.normalize("NFKC", title)
    title = _STRIP_PATTERNS.sub("", title)
    title = _PUNCTUATION.sub(" ", title)
    title = _TRAILING_PUNCT.sub("", title)
    title = _WHITESPACE.sub(" ", title).strip()
    title = title.lower()

    return title


def _format_priority(anime: dict) -> int:
    """Anime の format 優先度を返す (低い = 優先)."""
    return _FORMAT_PRIORITY.get(anime.get("format"), _DEFAULT_FORMAT_PRIORITY)


def _pick_best_by_format(candidates: list[str], anime_by_id: dict[str, dict]) -> str:
    """複数候補から format 優先度が最も高い1件を返す."""
    return min(candidates, key=lambda aid: _format_priority(anime_by_id[aid]))


def _disambiguate(
    valid: list[str],
    madb_year: int | None,
    anime_by_id: dict[str, dict],
) -> list[str]:
    """複数候補を year + format で絞り込む.

    1. 年の完全一致で絞る
    2. まだ複数なら format 優先度で1件に絞る
    """
    if len(valid) <= 1:
        return valid

    # Step 1: exact year
    if madb_year is not None:
        exact_year = [
            aid for aid in valid if anime_by_id[aid].get("year") == madb_year
        ]
        if len(exact_year) == 1:
            return exact_year
        if exact_year:
            valid = exact_year

    # Step 2: format priority
    if len(valid) > 1:
        best = _pick_best_by_format(valid, anime_by_id)
        best_prio = _format_priority(anime_by_id[best])
        # Only disambiguate if the best is strictly better than the rest
        same_prio = [
            aid
            for aid in valid
            if _format_priority(anime_by_id[aid]) == best_prio
        ]
        if len(same_prio) == 1:
            return [best]

    return valid


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
    """年が互換性があるか確認する."""
    if madb_year is None or anilist_year is None:
        return True
    return abs(madb_year - anilist_year) <= tolerance


def _get_display_title(anime: dict) -> str:
    return anime.get("title_ja") or anime.get("title_en") or ""


def match_anime_titles(
    madb_anime: list[dict],
    anilist_anime: list[dict],
    threshold: int = 90,
    year_tolerance: int = 1,
) -> list[AnimeMatch]:
    """MADB anime と AniList anime をタイトルマッチングする.

    マッチング戦略 (4 パス):
        1. 正規化タイトルの完全一致（高速パス）
        2. fuzzy match (fuzz.ratio >= threshold)
        3. 部分文字列マッチ（MADB タイトルが AniList に含まれる場合）
        - 各パスで年検証 + format 優先度による曖昧性解消
        - 1:1 マッピング強制
    """
    title_index, anime_by_id = _build_anilist_index(anilist_anime)

    matches: list[AnimeMatch] = []
    used_anilist_ids: set[str] = set()
    used_madb_ids: set[str] = set()

    def _try_match(
        madb_id: str,
        madb_title: str,
        madb_year: int | None,
        valid: list[str],
        score: float,
        strategy: str,
    ) -> bool:
        """候補リストからマッチを試みる. 成功したら True."""
        valid = [aid for aid in valid if aid not in used_anilist_ids]
        valid = list(dict.fromkeys(valid))  # dedup
        valid = _disambiguate(valid, madb_year, anime_by_id)

        if len(valid) == 1:
            aid = valid[0]
            matches.append(
                AnimeMatch(
                    madb_anime_id=madb_id,
                    anilist_anime_id=aid,
                    madb_title=madb_title,
                    anilist_title=_get_display_title(anime_by_id[aid]),
                    score=score,
                    strategy=strategy,
                )
            )
            used_anilist_ids.add(aid)
            used_madb_ids.add(madb_id)
            return True
        if len(valid) > 1:
            log.debug(
                f"anime_match_ambiguous_{strategy}",
                madb_id=madb_id,
                title=madb_title,
                candidates=len(valid),
            )
        return False

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
        valid = [
            aid
            for aid in candidates
            if _year_compatible(
                madb.get("year"), anime_by_id[aid].get("year"), year_tolerance
            )
        ]
        _try_match(madb_id, madb_title, madb.get("year"), valid, 100.0, "exact")

    # --- Pass 2: Fuzzy match (remaining) ---
    char_blocks: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for norm_title, aids in title_index.items():
        if norm_title:
            first = norm_title[0]
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

        first_char = normalized[0]
        block = char_blocks.get(first_char, [])

        # Collect all candidates above threshold
        scored: list[tuple[float, str]] = []
        for anilist_norm, aid in block:
            if aid in used_anilist_ids:
                continue
            if not _year_compatible(
                madb.get("year"), anime_by_id[aid].get("year"), year_tolerance
            ):
                continue
            # Length filter
            len_ratio = abs(len(normalized) - len(anilist_norm)) / max(
                len(normalized), len(anilist_norm)
            )
            if len_ratio > 0.5:
                continue

            score = fuzz.ratio(normalized, anilist_norm)
            if score >= threshold:
                scored.append((score, aid))

        if scored:
            best_score = max(s for s, _ in scored)
            best_aids = [aid for s, aid in scored if s == best_score]
            # Deduplicate
            best_aids = list(dict.fromkeys(best_aids))
            _try_match(
                madb_id, madb_title, madb.get("year"), best_aids, best_score, "fuzzy"
            )

    # --- Pass 3: Contains match (MADB title is substring of AniList title) ---
    # For cases like "ソード・オラトリア" → "ダンジョンに出会いを...ソード・オラトリア"
    # Build a flat list of (normalized_anilist_title, aid) for substring search
    anilist_flat: list[tuple[str, str]] = []
    for norm_title, aids in title_index.items():
        for aid in aids:
            if aid not in used_anilist_ids:
                anilist_flat.append((norm_title, aid))

    for madb in madb_anime:
        madb_id = madb["id"]
        if madb_id in used_madb_ids:
            continue

        madb_title = madb.get("title", "")
        normalized = normalize_anime_title(madb_title)
        # Require meaningful length to avoid false positives on short titles
        if not normalized or len(normalized) < 4:
            continue

        contain_hits: list[str] = []
        for anilist_norm, aid in anilist_flat:
            if aid in used_anilist_ids:
                continue
            if not _year_compatible(
                madb.get("year"), anime_by_id[aid].get("year"), year_tolerance
            ):
                continue
            # MADB title must be a substantial portion of AniList title
            if normalized in anilist_norm and len(normalized) >= len(anilist_norm) * 0.4:
                contain_hits.append(aid)

        if contain_hits:
            _try_match(
                madb_id,
                madb_title,
                madb.get("year"),
                contain_hits,
                80.0,
                "contains",
            )

    exact_count = sum(1 for m in matches if m.strategy == "exact")
    fuzzy_count = sum(1 for m in matches if m.strategy == "fuzzy")
    contains_count = sum(1 for m in matches if m.strategy == "contains")
    log.info(
        "anime_title_matching_complete",
        total_matches=len(matches),
        exact=exact_count,
        fuzzy=fuzzy_count,
        contains=contains_count,
        madb_total=len(madb_anime),
        anilist_total=len(anilist_anime),
        match_rate=round(100 * len(matches) / max(1, len(madb_anime)), 1),
    )

    return matches
