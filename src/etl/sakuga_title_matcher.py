"""sakuga@wiki work_title → silver anime_id matcher.

Conservative: only exact and normalized matches. Falls back to NULL for
ambiguous cases (multiple hits or no hit). Full fuzzy entity resolution is
left to a downstream ETL step (out of scope for Card 14).

Stages:
    1. exact_title  — literal string equality against title_ja / title_en
    2. normalized   — NFKC + whitespace/punctuation strip + lowercase
    3. unresolved   — NULL (returned when either stage yields 0 or ≥2 hits)

Year guard: if both work_year and anime year are non-NULL and differ by more
than 1, the candidate is skipped.  A tolerance of ±1 year accommodates
December/January boundary mismatches in credit records.
"""
from __future__ import annotations

import re
import unicodedata


def _normalize(s: str) -> str:
    """NFKC normalise, strip whitespace and common CJK/ASCII punctuation."""
    s = unicodedata.normalize("NFKC", s)
    s = re.sub(r"\s+", "", s)
    s = re.sub(r"[!！?？・･、，,.。〜~：:；;]", "", s)
    return s.lower()


def match_title(
    work_title: str | None,
    work_year: int | None,
    anime_rows: list[tuple[str, str | None, str | None, int | None]],
) -> tuple[str | None, str, float]:
    """Attempt conservative title match against SILVER anime rows.

    Args:
        work_title: Raw work title from sakuga@wiki BRONZE (may be None).
        work_year:  Year extracted from BRONZE (may be None).
        anime_rows: Iterable of (id, title_ja, title_en, year) from SILVER anime.

    Returns:
        Tuple of (anime_id, match_method, match_score) where:
        - match_method is one of 'exact_title', 'normalized', 'unresolved'
        - match_score is 1.0 / 0.95 / 0.0 respectively
        - anime_id is None when method is 'unresolved'
    """
    if not work_title:
        return None, "unresolved", 0.0

    norm_target = _normalize(work_title)
    exact: list[str] = []
    normalized: list[str] = []

    for aid, title_ja, title_en, year in anime_rows:
        # Year guard: skip if year mismatch is too large.
        if work_year is not None and year is not None and abs(year - work_year) > 1:
            continue
        for cand in (title_ja, title_en):
            if not cand:
                continue
            if cand == work_title:
                if aid not in exact:
                    exact.append(aid)
            elif _normalize(cand) == norm_target:
                if aid not in normalized:
                    normalized.append(aid)

    # Require exactly one hit to avoid false positives (H3 principle: conservative).
    if len(exact) == 1:
        return exact[0], "exact_title", 1.0
    if len(normalized) == 1:
        return normalized[0], "normalized", 0.95
    return None, "unresolved", 0.0
