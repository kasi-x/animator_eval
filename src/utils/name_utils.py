"""Native name script detection and nationality inference.

Handles Japanese, Korean, Chinese, and other scripts from AniList and other
sources. Name columns (name_ja / name_ko / name_zh) are JA/KO/ZH only;
other scripts stay in name_en (romanised form) and are distinguished via
person_aliases.lang.
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

# Arabic hometown → ISO 3166-1 alpha-2 country
_ARABIC_HOMETOWN_TOKENS: dict[str, str] = {
    # Egypt
    "egypt": "EG", "cairo": "EG", "alexandria": "EG", "مصر": "EG", "القاهرة": "EG",
    # Saudi Arabia
    "saudi": "SA", "riyadh": "SA", "jeddah": "SA", "السعودية": "SA", "الرياض": "SA",
    # UAE
    "uae": "AE", "dubai": "AE", "abu dhabi": "AE", "الإمارات": "AE", "دبي": "AE",
    # Jordan
    "jordan": "JO", "amman": "JO", "الأردن": "JO",
    # Kuwait
    "kuwait": "KW", "الكويت": "KW",
    # Qatar
    "qatar": "QA", "doha": "QA", "قطر": "QA",
    # Lebanon
    "lebanon": "LB", "beirut": "LB", "لبنان": "LB",
    # Morocco
    "morocco": "MA", "casablanca": "MA", "المغرب": "MA",
    # Tunisia
    "tunisia": "TN", "tunis": "TN", "تونس": "TN",
    # Algeria
    "algeria": "DZ", "algiers": "DZ", "الجزائر": "DZ",
    # Iraq
    "iraq": "IQ", "baghdad": "IQ", "العراق": "IQ",
    # Syria
    "syria": "SY", "damascus": "SY", "سوريا": "SY",
}


def detect_name_script(name: str) -> str:
    """Detect the primary writing script of a name string.

    Returns:
        'ko'       — contains Hangul
        'ja'       — contains Hiragana or Katakana (no Hangul)
        'zh_or_ja' — pure CJK without kana (ambiguous; needs hometown hint)
        'ar'       — contains Arabic script characters
        'th'       — contains Thai script characters
        'hi'       — contains Devanagari (Hindi/Sanskrit/Nepali)
        'ru'       — contains Cyrillic (Russian/Ukrainian/Bulgarian …)
        'el'       — contains Greek script
        'en'       — Latin / unrecognised
    """
    if not name:
        return "en"
    has_cjk = False
    for ch in name:
        cp = ord(ch)
        # Hangul — highest priority (Korean names may mix with CJK)
        if (0xAC00 <= cp <= 0xD7AF       # Hangul syllables
                or 0x1100 <= cp <= 0x11FF  # Hangul Jamo
                or 0x3130 <= cp <= 0x318F):  # Hangul Compatibility Jamo
            return "ko"
        # Kana — definitively Japanese
        if (0x3040 <= cp <= 0x309F        # Hiragana
                or 0x30A0 <= cp <= 0x30FF):  # Katakana
            return "ja"
        # Arabic block (excludes Persian/Urdu extensions — close enough)
        if 0x0600 <= cp <= 0x06FF:
            return "ar"
        # Thai
        if 0x0E00 <= cp <= 0x0E7F:
            return "th"
        # Devanagari
        if 0x0900 <= cp <= 0x097F:
            return "hi"
        # Cyrillic
        if 0x0400 <= cp <= 0x04FF:
            return "ru"
        # Greek
        if 0x0370 <= cp <= 0x03FF:
            return "el"
        # CJK Unified Ideographs
        if 0x4E00 <= cp <= 0x9FFF:
            has_cjk = True
    if has_cjk:
        return "zh_or_ja"
    return "en"


# Script tag → BCP-47-like lang code for person_aliases.lang
_SCRIPT_TO_LANG: dict[str, str] = {
    "ko": "ko",
    "ja": "ja",
    "zh_or_ja": "zh",
    "ar": "ar",
    "th": "th",
    "hi": "hi",
    "ru": "ru",
    "el": "el",
}


def lang_of_alias(alias: str) -> str | None:
    """Return BCP-47-like lang tag for an alias string, or None if unknown.

    Used to populate person_aliases.lang for display and cross-source matching.
    """
    return _SCRIPT_TO_LANG.get(detect_name_script(alias))


def infer_nationalities(name_native: str, hometown: str | None) -> list[str]:
    """Infer ISO 3166-1 alpha-2 nationality codes from native script and hometown.

    Returns a list because dual nationality is possible (e.g. ["JP", "KR"]).
    Returns [] when nationality cannot be determined.

    Conservative by design: prefers returning [] over a wrong guess.
    The only confident single-script → single-country mappings are:
      Hangul → KR, Thai → TH.
    All others require a hometown hint or remain [].
    """
    script = detect_name_script(name_native)

    if script == "ko":
        return ["KR"]
    if script == "ja":
        return ["JP"]
    if script == "th":
        return ["TH"]

    if script == "zh_or_ja":
        if hometown:
            low = hometown.lower()
            if any(tok in low for tok in _CHINESE_HOMETOWN_TOKENS):
                return ["CN"]
            if any(tok in low for tok in _KOREAN_HOMETOWN_TOKENS):
                return ["KR"]
        # Do not assume JP — return [] to avoid misclassifying Chinese staff.
        return []

    if script == "ar":
        if hometown:
            low = hometown.lower()
            for tok, country in _ARABIC_HOMETOWN_TOKENS.items():
                if tok in low:
                    return [country]
        return []

    # hi / ru / el / en — nationality not inferable from script alone
    return []


def assign_native_name_fields(
    name_native: str, nationalities: list[str]
) -> tuple[str, str, str]:
    """Map a native-script name to (name_ja, name_ko, name_zh).

    Non-CJK/KO scripts are not stored in these three columns (caller should
    keep the romanised form in name_en).  Script detection takes precedence
    over the nationality list.
    """
    if not name_native:
        return ("", "", "")
    script = detect_name_script(name_native)
    if script == "ko":
        return ("", name_native, "")
    if script == "zh_or_ja":
        primary = nationalities[0] if nationalities else ""
        if primary in ("CN", "TW", "HK"):
            return ("", "", name_native)
        # Unknown CJK (nationality=[]) or JP: store in name_ja.
        # Japanese CJK names go to name_ja; truly ambiguous ones also land
        # here until hometown data arrives and can correct them via re-scrape.
        return (name_native, "", "")
    if script == "ja":
        return (name_native, "", "")
    # ar / th / hi / ru / el / en — no dedicated column; caller keeps name_en
    return ("", "", "")
