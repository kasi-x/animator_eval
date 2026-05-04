"""Multi-format date parser for cross-source consensus normalization (Card 24/04).

Parses date strings from various sources into a canonical (year, month, day) tuple
and re-serializes to ISO 8601 with 'XX' placeholders for unknown components.

Supported input formats:
    ISO:        YYYY-MM-DD / YYYY-MM / YYYY
    Slash:      YYYY/MM/DD
    Dot:        YYYY.MM.DD
    English:    "April 15, 2020" / "Apr 15 2020" / "2020 Apr 15"
    JSON struct: {"year": Y, "month": M, "day": D}

Output: "YYYY-MM-DD" | "YYYY-MM-XX" | "YYYY-XX-XX" | None (unparseable)

Subset-compatibility rule:
    "2020" vs "2020-04-15" → subset_compatible (year matches, extra precision ok)
    "2020-04" vs "2020-04-15" → subset_compatible (year+month match)
    "2020-04-15" vs "2020-04-16" → NOT compatible (both have day, they differ)

Design:
    - Read-only, no DB access.
    - Never raises: unparseable input returns None.
    - Prefer the most detail-rich date when choosing among subset-compatible values.
"""

from __future__ import annotations

import json
import re
from typing import Optional

# ---------------------------------------------------------------------------
# Type alias for parsed date tuple: (year, month, day)
# Each component is an int or None (unknown).
# ---------------------------------------------------------------------------

DateTuple = tuple[Optional[int], Optional[int], Optional[int]]

# ---------------------------------------------------------------------------
# English month name → int map
# ---------------------------------------------------------------------------

_MONTH_NAMES: dict[str, int] = {
    "january": 1, "jan": 1,
    "february": 2, "feb": 2,
    "march": 3, "mar": 3,
    "april": 4, "apr": 4,
    "may": 5,
    "june": 6, "jun": 6,
    "july": 7, "jul": 7,
    "august": 8, "aug": 8,
    "september": 9, "sep": 9, "sept": 9,
    "october": 10, "oct": 10,
    "november": 11, "nov": 11,
    "december": 12, "dec": 12,
}

# ---------------------------------------------------------------------------
# Regex patterns for ISO / slash / dot / English formats
# ---------------------------------------------------------------------------

# ISO / slash / dot: YYYY[-/.]MM[-/.]DD  or  YYYY[-/.]MM  or  YYYY alone
_ISO_FULL_RE = re.compile(
    r"^(\d{4})[-/.](\d{1,2})[-/.](\d{1,2})$"
)
_ISO_YM_RE = re.compile(
    r"^(\d{4})[-/.](\d{1,2})$"
)
_ISO_Y_RE = re.compile(
    r"^(\d{4})$"
)

# ISO with XX placeholders: YYYY-MM-XX / YYYY-XX-XX (our own output format)
_ISO_XX_FULL_RE = re.compile(
    r"^(\d{4})-(XX|\d{2})-(XX|\d{2})$", re.IGNORECASE
)
_ISO_XX_YM_RE = re.compile(
    r"^(\d{4})-(XX|\d{2})-XX$", re.IGNORECASE
)
_ISO_XX_Y_RE = re.compile(
    r"^(\d{4})-XX-XX$", re.IGNORECASE
)

# English: "April 15, 2020" / "Apr 15 2020"
_EN_MONTH_DAY_YEAR_RE = re.compile(
    r"^([A-Za-z]+)\s+(\d{1,2})[,\s]+(\d{4})$"
)
# English: "2020 Apr 15"
_EN_YEAR_MONTH_DAY_RE = re.compile(
    r"^(\d{4})\s+([A-Za-z]+)\s+(\d{1,2})$"
)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _clamp_month(m: int) -> Optional[int]:
    """Return month if in 1-12 range, else None."""
    return m if 1 <= m <= 12 else None


def _clamp_day(d: int) -> Optional[int]:
    """Return day if in 1-31 range, else None."""
    return d if 1 <= d <= 31 else None


def _try_json_struct(value: str) -> Optional[DateTuple]:
    """Parse JSON struct: {"year": Y, "month": M, "day": D}."""
    try:
        obj = json.loads(value)
    except (json.JSONDecodeError, ValueError):
        return None
    if not isinstance(obj, dict):
        return None

    y = obj.get("year")
    m = obj.get("month")
    d = obj.get("day")

    year = int(y) if y is not None else None
    month = _clamp_month(int(m)) if m is not None else None
    day = _clamp_day(int(d)) if d is not None else None
    return (year, month, day)


def _try_iso_slash_dot(value: str) -> Optional[DateTuple]:
    """Parse ISO / slash / dot variants, including XX-placeholder forms."""
    s = value.strip()

    # Handle ISO with XX placeholders (our own output format: "YYYY-MM-XX" etc.)
    m = _ISO_XX_Y_RE.match(s)
    if m:
        return (int(m.group(1)), None, None)

    m = _ISO_XX_FULL_RE.match(s)
    if m:
        y = int(m.group(1))
        mo_str, d_str = m.group(2).upper(), m.group(3).upper()
        mo = _clamp_month(int(mo_str)) if mo_str != "XX" else None
        d = _clamp_day(int(d_str)) if d_str != "XX" else None
        return (y, mo, d)

    # Standard ISO / slash / dot
    m = _ISO_FULL_RE.match(s)
    if m:
        y, mo, d = int(m.group(1)), int(m.group(2)), int(m.group(3))
        return (y, _clamp_month(mo), _clamp_day(d))

    m = _ISO_YM_RE.match(s)
    if m:
        y, mo = int(m.group(1)), int(m.group(2))
        return (y, _clamp_month(mo), None)

    m = _ISO_Y_RE.match(s)
    if m:
        return (int(m.group(1)), None, None)

    return None


def _try_english(value: str) -> Optional[DateTuple]:
    """Parse English month-name formats."""
    s = value.strip()

    m = _EN_MONTH_DAY_YEAR_RE.match(s)
    if m:
        month_name = m.group(1).lower()
        month_int = _MONTH_NAMES.get(month_name)
        if month_int is None:
            return None
        day = _clamp_day(int(m.group(2)))
        year = int(m.group(3))
        return (year, month_int, day)

    m = _EN_YEAR_MONTH_DAY_RE.match(s)
    if m:
        year = int(m.group(1))
        month_name = m.group(2).lower()
        month_int = _MONTH_NAMES.get(month_name)
        if month_int is None:
            return None
        day = _clamp_day(int(m.group(3)))
        return (year, month_int, day)

    return None


# ---------------------------------------------------------------------------
# Public: parse_date
# ---------------------------------------------------------------------------

def parse_date(value: str | None) -> Optional[DateTuple]:
    """Parse a date string into a (year, month, day) tuple.

    Args:
        value: Raw date string from any supported source format.

    Returns:
        Tuple of (year, month, day) where unknown components are None.
        Returns None if the value is None, empty, or unparseable.
    """
    if not value or not value.strip():
        return None

    s = value.strip()

    # Try JSON struct first (most specific format indicator)
    if s.startswith("{"):
        result = _try_json_struct(s)
        if result is not None:
            return result

    # Try ISO / slash / dot
    result = _try_iso_slash_dot(s)
    if result is not None:
        return result

    # Try English month name
    result = _try_english(s)
    if result is not None:
        return result

    return None


# ---------------------------------------------------------------------------
# Public: to_iso8601
# ---------------------------------------------------------------------------

def to_iso8601(parsed: DateTuple) -> str:
    """Serialize a DateTuple to ISO 8601 with 'XX' for unknown components.

    Args:
        parsed: (year, month, day) tuple from parse_date.

    Returns:
        "YYYY-MM-DD" / "YYYY-MM-XX" / "YYYY-XX-XX" string.
        If year is None, returns "XXXX-XX-XX".
    """
    y, m, d = parsed
    y_str = f"{y:04d}" if y is not None else "XXXX"
    m_str = f"{m:02d}" if m is not None else "XX"
    d_str = f"{d:02d}" if d is not None else "XX"
    return f"{y_str}-{m_str}-{d_str}"


# ---------------------------------------------------------------------------
# Public: normalize_date
# ---------------------------------------------------------------------------

def normalize_date(value: str | None) -> Optional[str]:
    """Parse and re-serialize a date string to ISO 8601 with XX placeholders.

    Convenience wrapper combining parse_date + to_iso8601.

    Args:
        value: Raw date string.

    Returns:
        ISO 8601 string, or None if unparseable.
    """
    parsed = parse_date(value)
    if parsed is None:
        return None
    return to_iso8601(parsed)


# ---------------------------------------------------------------------------
# Public: precision_score
# ---------------------------------------------------------------------------

def precision_score(parsed: DateTuple) -> int:
    """Return numeric precision (higher = more detail).

    0 = no data, 1 = year only, 2 = year+month, 3 = year+month+day.
    """
    y, m, d = parsed
    if y is None:
        return 0
    if m is None:
        return 1
    if d is None:
        return 2
    return 3


# ---------------------------------------------------------------------------
# Public: is_date_subset_compatible
# ---------------------------------------------------------------------------

def is_date_subset_compatible(a: str | None, b: str | None) -> bool:
    """Return True if dates a and b are subset-compatible.

    Two dates are subset-compatible when all components that are non-None in
    both parsed tuples agree.  A year-only date ("2020") is compatible with a
    full date ("2020-04-15") because the year component matches and the extra
    month/day precision in the full date does not contradict the year-only date.

    Args:
        a: First date string.
        b: Second date string.

    Returns:
        True if compatible (including when either is None/unparseable — treat
        unparseable as not-compatible; if both unparseable, treat as compatible
        only if they are literally equal).
    """
    pa = parse_date(a)
    pb = parse_date(b)

    if pa is None and pb is None:
        # Both unparseable: fall back to string equality
        return (a or "").strip() == (b or "").strip()
    if pa is None or pb is None:
        return False

    # Check each component: if both sides have a value, they must match.
    for xa, xb in zip(pa, pb):
        if xa is not None and xb is not None and xa != xb:
            return False
    return True


# ---------------------------------------------------------------------------
# Public: pick_most_precise_date
# ---------------------------------------------------------------------------

def pick_most_precise_date(values: list[str | None]) -> Optional[str]:
    """From a list of date strings, return the ISO 8601 form of the most precise.

    "Most precise" = highest precision_score (year+month+day > year+month > year).
    Tie-breaking: first occurrence in source-priority order (caller should
    pre-order the list by source priority before passing).

    Args:
        values: List of raw date strings (may include None / empty).

    Returns:
        ISO 8601 string of the most precise date, or None if all unparseable.
    """
    best_parsed: Optional[DateTuple] = None
    best_score = -1

    for v in values:
        p = parse_date(v)
        if p is None:
            continue
        s = precision_score(p)
        if s > best_score:
            best_score = s
            best_parsed = p

    if best_parsed is None:
        return None
    return to_iso8601(best_parsed)
