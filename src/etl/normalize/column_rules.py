"""Column-specific normalization rules for cross-source consensus aggregation.

Each rule type defines how values for a specific attribute should be normalized
before consensus comparison. The goal is to collapse superficial differences
(case variants, punctuation styles, alias maps) so that semantically equivalent
values are recognized as identical.

Usage:
    from src.etl.normalize.column_rules import normalize_for_consensus

    normalized = normalize_for_consensus("Gender", "Male")
    # → "male"

    normalized = normalize_for_consensus("name_en", "STUDIO GHIBLI")
    # → "STUDIO GHIBLI" (already all-caps: check info_richest logic)

Design:
    - normalize_for_consensus(attribute, value) → str
    - Returns the original value (stripped) if no rule applies.
    - Never raises: malformed input falls back to stripped str.
    - Read-only: no DB access.
"""

from __future__ import annotations

import re
import unicodedata

from src.etl.normalize.canonical_name import KYU_SHIN_MAP
from src.etl.normalize.date_parser import normalize_date

# ---------------------------------------------------------------------------
# Type alias
# ---------------------------------------------------------------------------

_NormFn = "Callable[[str], str]"  # for annotation only — not imported at runtime

# ---------------------------------------------------------------------------
# Gender alias map
# ---------------------------------------------------------------------------

_GENDER_ALIAS_MAP: dict[str, str] = {
    # Male variants
    "m": "male",
    "M": "male",
    "Male": "male",
    "MALE": "male",
    "male": "male",
    "man": "male",
    "Man": "male",
    "MAN": "male",
    # Female variants
    "f": "female",
    "F": "female",
    "Female": "female",
    "FEMALE": "female",
    "female": "female",
    "woman": "female",
    "Woman": "female",
    "WOMAN": "female",
    # Non-binary
    "non-binary": "non_binary",
    "Non-binary": "non_binary",
    "Non-Binary": "non_binary",
    "NON-BINARY": "non_binary",
    "NB": "non_binary",
    "nb": "non_binary",
    "nonbinary": "non_binary",
    "Nonbinary": "non_binary",
}

# ---------------------------------------------------------------------------
# Punctuation strip regex (same as cross_source_diff.py)
# ---------------------------------------------------------------------------

_PUNCT_RE = re.compile(r"[・．。、，,.\-\s　]+")

# ---------------------------------------------------------------------------
# Individual normalizer functions
# ---------------------------------------------------------------------------


def _apply_alias_map(value: str, alias_map: dict[str, str]) -> str:
    """Apply a string-keyed alias map; fall back to lowercase if key not found."""
    stripped = value.strip()
    return alias_map.get(stripped, stripped.lower())


def _info_richest(value: str) -> str:
    """Return value unchanged — caller will pick richest from a set of values.

    The 'info_richest' rule means we *prefer* mixed-case over all-upper or
    all-lower. This function is a no-op for a single value; the selection
    logic lives in pick_info_richest_from_set().
    """
    return value.strip()


def _info_richest_punct_clean(value: str) -> str:
    """Strip punctuation/whitespace then apply NFKC; for 'richest' selection.

    Used for studio names where J.C.STAFF and JC STAFF should collapse.
    The 'richest' preference (keeping dots) is handled by pick_info_richest_from_set().
    """
    s = unicodedata.normalize("NFKC", value.strip())
    return _PUNCT_RE.sub("", s).upper()


def _kyu_to_shin(value: str) -> str:
    """Apply 旧字体→新字体 + NFKC normalization."""
    s = unicodedata.normalize("NFKC", value.strip())
    return "".join(KYU_SHIN_MAP.get(ch, ch) for ch in s)


def _iso_3166_upper(value: str) -> str:
    """Normalize country code to uppercase (e.g. 'jp' → 'JP')."""
    return value.strip().upper()


def _title_case_media(value: str) -> str:
    """Normalize media format strings (TV → TV, movie → Movie, OVA → OVA).

    Known all-caps formats stay all-caps; others are title-cased.
    """
    _KNOWN_UPPER = frozenset({"TV", "OVA", "ONA", "CM", "PV"})
    stripped = value.strip()
    if stripped.upper() in _KNOWN_UPPER:
        return stripped.upper()
    return stripped.title()


# ---------------------------------------------------------------------------
# Format 8-category taxonomy (fine_format → broad_format)
# ---------------------------------------------------------------------------

# Maps source-level format labels (case-insensitive key) → broad_format category.
# Lookup is performed after stripping + uppercasing the raw value.
# Unmapped values fall through to "other".
_FORMAT_8_CATEGORY_MAP: dict[str, str] = {
    # tv
    "TV": "tv",
    "TV_SHORT": "short",  # TV_SHORT goes to short (< 15 min label)
    "TV SPECIAL": "tv",   # TV special → tv (放送経路優先)
    "TV_SPECIAL": "tv",
    # movie
    "MOVIE": "movie",
    # ova_special
    "OVA": "ova_special",
    "OAV": "ova_special",
    "SPECIAL": "ova_special",
    # ona
    "ONA": "ona",
    # short
    "SHORT": "short",
    # music
    "MUSIC": "music",
    "MUSIC_VIDEO": "music",
    "PV": "music",
    "PV_CM": "music",
    # cm
    "CM": "cm",
    # other
    "OTHER": "other",
    "GAME": "other",
}


def _to_broad_format(value: str) -> str:
    """Map a source-level format label to one of the 8 broad_format categories.

    Mapping is case-insensitive. Unmapped values fall back to "other".

    Categories:
        tv          – TV / TV_SPECIAL / TV special
        movie       – Movie / MOVIE
        ova_special – OVA / OAV / Special / SPECIAL
        ona         – ONA
        short       – TV_SHORT / SHORT
        music       – Music / MUSIC / PV / PV_CM
        cm          – CM
        other       – OTHER / GAME / unmapped
    """
    key = value.strip().upper()
    return _FORMAT_8_CATEGORY_MAP.get(key, "other")


def _date_iso8601_with_subset(value: str) -> str:
    """Normalize a date string to ISO 8601 with 'XX' placeholders.

    Parses multi-format date strings (ISO, slash, dot, English, JSON struct)
    and returns the canonical "YYYY-MM-DD" / "YYYY-MM-XX" / "YYYY-XX-XX" form.

    Falls back to the stripped original value if the date is unparseable,
    so consensus comparison can still detect literal equality.

    This is the normalization function for the 'date_iso8601_with_subset' rule
    type used by date columns.  Subset-compatible detection
    (e.g. "2020" vs "2020-04-15" → same) is handled in classify_consensus_date.
    """
    normalized = normalize_date(value)
    if normalized is None:
        return value.strip()
    return normalized


# ---------------------------------------------------------------------------
# Column-rule dispatch table
# ---------------------------------------------------------------------------

# Maps attribute name → (normalization_fn, normalization_type_label)
# The label is surfaced for debugging/logging; the fn is what gets applied.
_COLUMN_RULES: dict[str, tuple[object, str]] = {
    "gender": (
        lambda v: _apply_alias_map(v, _GENDER_ALIAS_MAP),
        "alias_map",
    ),
    "country_of_origin": (
        _iso_3166_upper,
        "iso_3166",
    ),
    "format": (
        _title_case_media,
        "title_case",
    ),
    # broad_format: map fine-grained source label → 8-category taxonomy.
    # Used by cross_source_consensus to compute a separate broad-format consensus
    # that collapses OVA/OAV/Special and other superficial label differences.
    "format_8_category": (
        _to_broad_format,
        "format_8_category",
    ),
    "name_en": (
        _info_richest,
        "info_richest",
    ),
    "name_ja": (
        _kyu_to_shin,
        "kyu_to_shin",
    ),
    "title_ja": (
        _kyu_to_shin,
        "kyu_to_shin",
    ),
    # Studio name: strip punct for comparison; richest selected by pick_info_richest_from_set
    "name": (
        _info_richest_punct_clean,
        "info_richest_punct_clean",
    ),
    # Date columns: multi-format → ISO 8601 with XX placeholders
    # Subset-compatible detection ("2020" ⊆ "2020-04-15") is handled downstream
    # in classify_consensus_date; this normalizer enables literal-equality matches.
    "start_date": (
        _date_iso8601_with_subset,
        "date_iso8601_with_subset",
    ),
    "end_date": (
        _date_iso8601_with_subset,
        "date_iso8601_with_subset",
    ),
    "aired_from": (
        _date_iso8601_with_subset,
        "date_iso8601_with_subset",
    ),
    "aired_to": (
        _date_iso8601_with_subset,
        "date_iso8601_with_subset",
    ),
    "release_date": (
        _date_iso8601_with_subset,
        "date_iso8601_with_subset",
    ),
    "first_air_date": (
        _date_iso8601_with_subset,
        "date_iso8601_with_subset",
    ),
    "last_air_date": (
        _date_iso8601_with_subset,
        "date_iso8601_with_subset",
    ),
    "birth_date": (
        _date_iso8601_with_subset,
        "date_iso8601_with_subset",
    ),
    "death_date": (
        _date_iso8601_with_subset,
        "date_iso8601_with_subset",
    ),
}

# ---------------------------------------------------------------------------
# Public: normalize_for_consensus
# ---------------------------------------------------------------------------


def to_broad_format(value: str | None) -> str | None:
    """Public wrapper: map a raw format label to one of the 8 broad_format categories.

    Returns None when value is None or empty; never raises.

    Args:
        value: Raw source format string (e.g. "OVA", "TV", "Special").

    Returns:
        One of: "tv", "movie", "ova_special", "ona", "short", "music", "cm", "other".
        None when input is None or empty.
    """
    if value is None or value == "":
        return value
    try:
        return _to_broad_format(value)
    except Exception:
        return "other"


def normalize_for_consensus(attribute: str, value: str | None) -> str | None:
    """Return the column-normalized form of value for the given attribute.

    Args:
        attribute: Column name (e.g. "gender", "name_en", "year").
        value: Raw string value (may be None or empty).

    Returns:
        Normalized string, or None/empty passthrough if input is None/empty.
    """
    if value is None or value == "":
        return value

    rule = _COLUMN_RULES.get(attribute)
    if rule is None:
        # No specific rule → strip only.
        return value.strip()

    fn, _label = rule
    try:
        return fn(value)  # type: ignore[operator]
    except Exception:
        return value.strip()


# ---------------------------------------------------------------------------
# Public: pick_info_richest_from_set
# ---------------------------------------------------------------------------


def _case_richness_score(s: str) -> int:
    """Return a numeric score reflecting 'information richness' of case pattern.

    Higher score = more informative:
        mixed-case = 2  (both upper and lower present)
        all-lower  = 1
        all-upper  = 0
    """
    has_upper = any(ch.isupper() for ch in s)
    has_lower = any(ch.islower() for ch in s)
    if has_upper and has_lower:
        return 2
    if has_lower:
        return 1
    return 0


def pick_info_richest_from_set(values: list[str]) -> str:
    """From a list of string values, pick the one with the most case 'richness'.

    Used for the info_richest and info_richest_punct_clean rule types when
    multiple source values need to be compared for the preferred canonical form.

    Tie-breaking: lexicographic order on the original string (deterministic).

    Args:
        values: Non-empty list of candidate strings.

    Returns:
        The string with the highest case-richness score.
    """
    if not values:
        raise ValueError("pick_info_richest_from_set requires a non-empty list")
    if len(values) == 1:
        return values[0]

    scored = [(s, _case_richness_score(s)) for s in values]
    best_score = max(sc for _, sc in scored)
    candidates = [s for s, sc in scored if sc == best_score]
    # Deterministic tie-break: lexicographic descending (prefer "Studio Ghibli" over "studio ghibli")
    return sorted(candidates)[-1]
