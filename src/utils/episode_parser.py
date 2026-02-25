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


def parse_episodes(raw_role: str) -> set[int]:
    """Parse episode numbers from a raw_role string.

    Handles formats:
    - Single: "(ep 10)" → {10}
    - List: "(eps 2, 18, 22)" → {2, 18, 22}
    - Range: "(eps 1-12)" → {1, 2, ..., 12}
    - Mixed: "(OP24, eps 903, 1000)" → {903, 1000} (skip OP/ED)
    - Dot format: "(ep. 2)" → {2}
    - No episodes: "Director" → set()

    Args:
        raw_role: Raw role string from credit data

    Returns:
        Set of episode numbers (empty if none found)
    """
    if not raw_role:
        return set()

    match = _EP_PAREN_RE.search(raw_role)
    if not match:
        return set()

    content = match.group(1)

    # Remove OP/ED/SP tokens before parsing numbers
    content = _SKIP_PREFIX_RE.sub("", content)

    episodes: set[int] = set()

    # Extract ranges first (e.g., "1-12")
    for range_match in _EP_RANGE_RE.finditer(content):
        start = int(range_match.group(1))
        end = int(range_match.group(2))
        if end >= start and (end - start) <= 500:  # sanity limit
            episodes.update(range(start, end + 1))

    # Remove ranges from content so we don't double-count
    content_no_ranges = _EP_RANGE_RE.sub("", content)

    # Extract individual numbers
    for num_match in _EP_SINGLE_RE.finditer(content_no_ranges):
        episodes.add(int(num_match.group(1)))

    return episodes
