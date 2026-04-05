"""Forbidden phrase dictionary and exemptions for the compliance lint.

The intent of the lint is to enforce the project's legal positioning:
scores reflect **network position and collaboration density**, never a
judgement of individual ability. Phrases framing scores as ability or
talent must not appear in published narratives.

Exemptions exist for explicitly *refusing* such a framing
("does not measure ability").
"""

from __future__ import annotations

#: Phrases that trigger a lint error (rule L-1) when they appear inside
#: narrative strings, claims, or explanations.
#:
#: Keys are the substring to match (case-sensitive for Japanese; matching
#: is done after Unicode normalization). Values are suggested replacements
#: shown in the error message — they are not applied automatically.
FORBIDDEN_PHRASES: dict[str, str] = {
    "能力が低い": "ネットワーク可視性が低い / スコアが低い",
    "能力が高い": "スコアが高い / ネットワーク中心性が高い",
    "才能がある": "スコアが高い",
    "才能が低い": "スコアが低い",
    "才能不足": "スコアが低い",
    "実力不足": "スコアが低い",
    "実力がある": "スコアが高い",
    "実力が低い": "スコアが低い",
    "無能": "（個人への否定的評価は禁止）",
    "低品質": "（個人・作品への否定的評価は慎重に）",
    "劣っている": "スコアが低い",
    "優れている": "スコアが高い",  # context-dependent; may need to be exempted
    "スキル評価": "スコア / ネットワーク指標",
    "技量評価": "スコア / ネットワーク指標",
    "能力評価": "スコア / ネットワーク指標",
    "能力測定": "スコア算出",
    "実力評価": "スコア / ネットワーク指標",
}


#: Phrases that contain forbidden substrings but are explicit *refusals* or
#: disclaimers. The lint will not flag these when they appear verbatim.
#:
#: This is the escape hatch for the DISCLAIMER text and similar blocks.
ALLOWED_PHRASES: frozenset[str] = frozenset(
    {
        "能力を測るものではない",
        "能力や技量を評価するものではありません",
        "能力・技量を評価するものではありません",
        "能力・技量・芸術性を評価・測定・示唆するものではありません",
        "個人の能力・技量・芸術性を評価・測定・示唆するものではありません",
        "実力の不足を意味するものではありません",
        "能力判断として用いてはいけない",
        "能力の優劣を示すものではない",
        "スキル評価ではありません",
        "能力評価ではありません",
        "技量評価ではありません",
        # The DISCLAIMER constant from html_templates.py is a superset of
        # the substrings above, so matching on any of them exempts the
        # full DISCLAIMER block.
    }
)


def find_forbidden(text: str) -> list[tuple[str, str]]:
    """Scan ``text`` for forbidden phrases, respecting exemptions.

    Returns a list of ``(forbidden_substring, suggestion)`` pairs. Empty if
    nothing is flagged. Any ``ALLOWED_PHRASES`` match masks its overlapping
    forbidden substrings within that allowed region.
    """
    if not text:
        return []

    # Mark allowed ranges first so we can ignore forbidden substrings that
    # fall entirely inside them.
    allowed_ranges: list[tuple[int, int]] = []
    for allowed in ALLOWED_PHRASES:
        start = 0
        while True:
            idx = text.find(allowed, start)
            if idx < 0:
                break
            allowed_ranges.append((idx, idx + len(allowed)))
            start = idx + 1

    def _in_allowed(span_start: int, span_end: int) -> bool:
        for a_start, a_end in allowed_ranges:
            if a_start <= span_start and span_end <= a_end:
                return True
        return False

    hits: list[tuple[str, str]] = []
    for phrase, suggestion in FORBIDDEN_PHRASES.items():
        start = 0
        while True:
            idx = text.find(phrase, start)
            if idx < 0:
                break
            span_end = idx + len(phrase)
            if not _in_allowed(idx, span_end):
                hits.append((phrase, suggestion))
                break  # one hit per phrase is enough
            start = span_end
    return hits
