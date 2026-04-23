"""Native name script detection and nationality inference.

Handles Japanese, Korean, and Chinese names from AniList and other sources.
"""

from __future__ import annotations

_CHINESE_HOMETOWN_TOKENS = frozenset({
    "china", "prc", "mainland", "beijing", "shanghai", "guangzhou",
    "shenzhen", "chengdu", "wuhan", "hangzhou", "nanjing", "tianjin",
    "xian", "xi'an", "chongqing", "中国", "北京", "上海", "广州", "深圳",
    "成都", "武汉", "重庆", "台湾", "台北", "taipei", "taiwan",
    "hong kong", "hongkong", "hk", "香港",
})

_KOREAN_HOMETOWN_TOKENS = frozenset({
    "korea", "south korea", "seoul", "busan", "incheon", "daegu",
    "daejeon", "gwangju", "ulsan", "suwon", "창원",
    "한국", "서울", "부산", "인천", "대구",
})


def detect_name_script(name: str) -> str:
    """Detect the primary writing script of a name string.

    Returns:
        'ko'       — contains Hangul
        'ja'       — contains Hiragana or Katakana (no Hangul)
        'zh_or_ja' — pure CJK without kana (ambiguous; needs hometown hint)
        'en'       — Latin / unrecognised
    """
    if not name:
        return "en"
    for ch in name:
        cp = ord(ch)
        if (0xAC00 <= cp <= 0xD7AF       # Hangul syllables
                or 0x1100 <= cp <= 0x11FF  # Hangul Jamo
                or 0x3130 <= cp <= 0x318F):  # Hangul Compatibility Jamo
            return "ko"
        if (0x3040 <= cp <= 0x309F        # Hiragana
                or 0x30A0 <= cp <= 0x30FF):  # Katakana
            return "ja"
    if any(0x4E00 <= ord(c) <= 0x9FFF for c in name):
        return "zh_or_ja"
    return "en"


def lang_of_alias(alias: str) -> str | None:
    """Return BCP-47-like lang tag for an alias string, or None if unknown.

    Used to populate person_aliases.lang for display and cross-source matching.
    """
    script = detect_name_script(alias)
    return {"ko": "ko", "ja": "ja", "zh_or_ja": "zh"}.get(script)


def infer_nationalities(name_native: str, hometown: str | None) -> list[str]:
    """Infer ISO 3166-1 alpha-2 nationality codes from native script and hometown.

    Returns a list because dual nationality is possible (e.g. ["JP", "KR"]).
    Returns [] when nationality cannot be determined (e.g. Latin name only).
    """
    script = detect_name_script(name_native)

    if script == "ko":
        return ["KR"]
    if script == "ja":
        return ["JP"]
    if script == "zh_or_ja":
        if hometown:
            low = hometown.lower()
            if any(tok in low for tok in _CHINESE_HOMETOWN_TOKENS):
                return ["CN"]
            if any(tok in low for tok in _KOREAN_HOMETOWN_TOKENS):
                return ["KR"]
        return ["JP"]  # default for unresolved CJK without hometown hint
    return []


def assign_native_name_fields(
    name_native: str, nationalities: list[str]
) -> tuple[str, str, str]:
    """Map a native-script name to (name_ja, name_ko, name_zh).

    Script detection takes precedence over the nationality list so that
    a Japanese-Korean dual national whose AniList native name is in Hangul
    still goes into name_ko, not name_ja.
    """
    if not name_native:
        return ("", "", "")
    script = detect_name_script(name_native)
    if script == "ko":
        return ("", name_native, "")
    if script == "zh_or_ja":
        primary = nationalities[0] if nationalities else "JP"
        if primary in ("CN", "TW", "HK"):
            return ("", "", name_native)
        return (name_native, "", "")
    # 'ja' or 'en'/unknown → name_ja
    return (name_native, "", "")
