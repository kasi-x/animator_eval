"""Episode number parser for raw_role strings.

Extracts episode numbers from parenthetical annotations in credit role strings.
Examples: "Key Animation (ep 10)" → {10}, "Episode Director (eps 2, 18, 22)" → {2, 18, 22}
"""

import re

# Match parenthetical episode annotations like (ep 10), (eps 1-12), (ep. 2, 5)
_EP_PAREN_RE = re.compile(r"\(([^)]*\beps?\.?\s[^)]*)\)", re.IGNORECASE)

# Match individual episode numbers or ranges within the parenthetical
_EP_RANGE_RE = re.compile(r"(\d+)\s*[-–]\s*(\d+)")
_EP_SINGLE_RE = re.compile(r"(?<!\w)(\d+)(?!\w*[A-Za-z])")

# OP/ED/Special prefixes to skip (e.g. "OP24", "ED3")
_SKIP_PREFIX_RE = re.compile(r"(?:OP|ED|SP|OVA)\s*\d+", re.IGNORECASE)


def _extract_paren_content(raw_role: str) -> str | None:
    m = _EP_PAREN_RE.search(raw_role)
    return _SKIP_PREFIX_RE.sub("", m.group(1)) if m else None


def _extract_range_episodes(content: str) -> tuple[set[int], str]:
    """Return episodes from ranges and content with ranges stripped."""
    episodes: set[int] = set()
    for m in _EP_RANGE_RE.finditer(content):
        start, end = int(m.group(1)), int(m.group(2))
        if end >= start and (end - start) <= 500:
            episodes.update(range(start, end + 1))
    return episodes, _EP_RANGE_RE.sub("", content)


def _extract_single_episodes(content_no_ranges: str) -> set[int]:
    return {int(m.group(1)) for m in _EP_SINGLE_RE.finditer(content_no_ranges)}


def parse_episodes(raw_role: str) -> set[int]:
    """Parse episode numbers from a raw_role string.

    Handles formats:
    - Single: "(ep 10)" → {10}
    - List: "(eps 2, 18, 22)" → {2, 18, 22}
    - Range: "(eps 1-12)" → {1, 2, ..., 12}
    - Mixed: "(OP24, eps 903, 1000)" → {903, 1000} (skip OP/ED)
    - Dot format: "(ep. 2)" → {2}
    - No episodes: "Director" → set()
    """
    if not raw_role:
        return set()
    content = _extract_paren_content(raw_role)
    if content is None:
        return set()
    range_eps, remainder = _extract_range_episodes(content)
    return range_eps | _extract_single_episodes(remainder)
