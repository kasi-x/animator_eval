"""Native name script detection and nationality inference.

Handles Japanese, Korean, Chinese, and other scripts from AniList and other
sources. Name columns (name_ja / name_ko / name_zh) are JA/KO/ZH only;
other scripts stay in name_en (romanised form) and are distinguished via
person_aliases.lang.
"""

from __future__ import annotations

import re

# ---------------------------------------------------------------------------
# Script detection — table-driven Unicode range lookup
# ---------------------------------------------------------------------------

# Each entry: (range_start, range_end_inclusive, script_tag)
# "cjk" is handled specially (continues loop to check for overriding kana).
# Sort order matters: checked linearly per codepoint.
_SCRIPT_RANGES: list[tuple[int, int, str]] = [
    (0x0370, 0x03FF, "el"),    # Greek
    (0x0400, 0x04FF, "ru"),    # Cyrillic
    (0x0600, 0x06FF, "ar"),    # Arabic
    (0x0900, 0x097F, "hi"),    # Devanagari
    (0x0E00, 0x0E7F, "th"),    # Thai
    (0x1100, 0x11FF, "ko"),    # Hangul Jamo
    (0x3040, 0x309F, "ja"),    # Hiragana
    (0x30A0, 0x30FF, "ja"),    # Katakana
    (0x3130, 0x318F, "ko"),    # Hangul Compatibility Jamo
    (0x4E00, 0x9FFF, "cjk"),   # CJK Unified Ideographs
    (0xAC00, 0xD7AF, "ko"),    # Hangul Syllables
]

# Script tag → BCP-47-like lang code for person_aliases.lang
# zh_or_ja → None (ambiguous; resolved only when nationality is known)
_SCRIPT_TO_LANG: dict[str, str] = {
    "ko": "ko",
    "ja": "ja",
    "ar": "ar",
    "th": "th",
    "hi": "hi",
    "ru": "ru",
    "el": "el",
}


def detect_name_script(name: str) -> str:
    """Detect the primary writing script of a name string.

    Returns:
        'ko'       — Hangul
        'ja'       — Hiragana or Katakana (no Hangul)
        'zh_or_ja' — pure CJK without kana (ambiguous; needs hometown hint)
        'ar'       — Arabic script
        'th'       — Thai script
        'hi'       — Devanagari (Hindi/Sanskrit/Nepali)
        'ru'       — Cyrillic
        'el'       — Greek
        'en'       — Latin / unrecognised
    """
    if not name:
        return "en"
    has_cjk = False
    for ch in name:
        cp = ord(ch)
        for lo, hi, tag in _SCRIPT_RANGES:
            if cp < lo:
                break
            if cp <= hi:
                if tag == "cjk":
                    has_cjk = True
                else:
                    return tag
                break
    return "zh_or_ja" if has_cjk else "en"


# ---------------------------------------------------------------------------
# Hometown token sets and compiled regexes for nationality inference
# ---------------------------------------------------------------------------

_CHINESE_HOMETOWN_TOKENS: frozenset[str] = frozenset({
    "china", "prc", "mainland", "beijing", "shanghai", "guangzhou",
    "shenzhen", "chengdu", "wuhan", "hangzhou", "nanjing", "tianjin",
    "xian", "xi'an", "chongqing", "中国", "北京", "上海", "广州", "深圳",
    "成都", "武汉", "重庆", "台湾", "台北", "taipei", "taiwan",
    "hong kong", "hongkong", "hk", "香港",
})

_KOREAN_HOMETOWN_TOKENS: frozenset[str] = frozenset({
    "korea", "south korea", "seoul", "busan", "incheon", "daegu",
    "daejeon", "gwangju", "ulsan", "suwon", "창원",
    "한국", "서울", "부산", "인천", "대구",
})

_JAPANESE_HOMETOWN_TOKENS: frozenset[str] = frozenset({
    "japan", "tokyo", "osaka", "kyoto", "yokohama", "nagoya", "sapporo",
    "fukuoka", "kobe", "kawasaki", "saitama", "hiroshima", "sendai",
    "kitakyushu", "chiba", "sakai", "niigata", "hamamatsu", "sagamihara",
    "shizuoka", "okayama", "kumamoto", "kagoshima", "kanazawa", "naha",
    "matsuyama", "nagasaki", "oita", "miyazaki", "akita", "aomori",
    "morioka", "yamagata", "fukushima", "utsunomiya", "maebashi",
    "nagano", "kofu", "tottori", "matsue", "takamatsu", "kochi",
    "佐賀", "日本", "東京", "大阪", "京都", "横浜", "名古屋", "札幌",
    "福岡", "神戸", "埼玉", "千葉", "広島", "仙台", "新潟",
})

# Arabic hometown → ISO 3166-1 alpha-2 country
_ARABIC_HOMETOWN_TOKENS: dict[str, str] = {
    "egypt": "EG", "cairo": "EG", "alexandria": "EG", "مصر": "EG", "القاهرة": "EG",
    "saudi": "SA", "riyadh": "SA", "jeddah": "SA", "السعودية": "SA", "الرياض": "SA",
    "uae": "AE", "dubai": "AE", "abu dhabi": "AE", "الإمارات": "AE", "دبي": "AE",
    "jordan": "JO", "amman": "JO", "الأردن": "JO",
    "kuwait": "KW", "الكويت": "KW",
    "qatar": "QA", "doha": "QA", "قطر": "QA",
    "lebanon": "LB", "beirut": "LB", "لبنان": "LB",
    "morocco": "MA", "casablanca": "MA", "المغرب": "MA",
    "tunisia": "TN", "tunis": "TN", "تونس": "TN",
    "algeria": "DZ", "algiers": "DZ", "الجزائر": "DZ",
    "iraq": "IQ", "baghdad": "IQ", "العراق": "IQ",
    "syria": "SY", "damascus": "SY", "سوريا": "SY",
}

def _build_hometown_re(tokens: frozenset[str]) -> re.Pattern[str]:
    """Compile tokens into a single regex (longest match first to avoid prefix shadowing)."""
    alts = sorted(map(re.escape, tokens), key=len, reverse=True)
    return re.compile("|".join(alts), re.IGNORECASE)

_CHINESE_RE   = _build_hometown_re(_CHINESE_HOMETOWN_TOKENS)
_KOREAN_RE    = _build_hometown_re(_KOREAN_HOMETOWN_TOKENS)
_JAPANESE_RE  = _build_hometown_re(_JAPANESE_HOMETOWN_TOKENS)

# Arabic: sorted longest-first so "hong kong" matches before "hong"
_arabic_keys_sorted = sorted(_ARABIC_HOMETOWN_TOKENS.keys(), key=len, reverse=True)
_ARABIC_RE = re.compile("|".join(map(re.escape, _arabic_keys_sorted)), re.IGNORECASE)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def lang_of_alias(alias: str) -> str | None:
    """Return BCP-47-like lang tag for an alias string, or None if unknown.

    Used to populate person_aliases.lang for display and cross-source matching.
    """
    return _SCRIPT_TO_LANG.get(detect_name_script(alias))


def infer_nationalities(name_native: str, hometown: str | None) -> list[str]:
    """Infer ISO 3166-1 alpha-2 nationality codes from native script and hometown.

    Returns a list because dual nationality is possible (e.g. ["JP", "KR"]).
    Returns [] when nationality cannot be determined.

    Conservative by design: prefers [] over a wrong guess.
    Confident single-script → single-country mappings: Hangul→KR, Thai→TH.
    All others require a hometown hint or remain [].
    """
    script = detect_name_script(name_native)

    if script == "ko":
        return ["KR"]
    if script == "ja":
        return ["JP"]
    if script == "th":
        return ["TH"]

    if not hometown:
        return []

    low = hometown.lower()

    if script == "zh_or_ja":
        if _JAPANESE_RE.search(low):
            return ["JP"]
        if _CHINESE_RE.search(low):
            return ["CN"]
        if _KOREAN_RE.search(low):
            return ["KR"]
        return []

    if script == "ar":
        m = _ARABIC_RE.search(low)
        if m:
            return [_ARABIC_HOMETOWN_TOKENS[m.group().lower()]]
        return []

    # hi / ru / el / en — not inferable from script alone
    return []


def assign_native_name_fields(
    name_native: str, nationalities: list[str]
) -> tuple[str, str, str]:
    """Map a native-script name to (name_ja, name_ko, name_zh).

    Non-CJK/KO scripts are not stored in these three columns (caller should
    keep the romanised form in name_en). Script detection takes precedence
    over the nationality list.

    For zh_or_ja with nationality=[] (unknown), returns ("","","") so that
    ambiguous CJK names (could be Chinese or Japanese) are not silently filed
    under name_ja. The raw value is preserved in Person.name_native_raw.
    """
    if not name_native:
        return ("", "", "")
    script = detect_name_script(name_native)
    if script == "ko":
        return ("", name_native, "")
    if script == "ja":
        return (name_native, "", "")
    if script == "zh_or_ja":
        primary = nationalities[0] if nationalities else ""
        if primary in ("CN", "TW", "HK"):
            return ("", "", name_native)
        if primary == "JP":
            return (name_native, "", "")
        if primary == "KR":
            return ("", name_native, "")
        # nationality=[] — cannot determine; preserve via name_native_raw instead
        return ("", "", "")
    # ar / th / hi / ru / el / en — no dedicated column
    return ("", "", "")


def parse_anilist_native_name(
    name_dict: dict, hometown: str | None
) -> tuple[str, str, str, str, list[str]]:
    """Parse an AniList name dict into resolved name fields + raw native.

    Returns (name_ja, name_ko, name_zh, name_native_raw, nationality).
    One-liner replacement for the repeated 4-line pattern in the scraper.
    """
    native = name_dict.get("native") or ""
    nationality = infer_nationalities(native, hometown)
    name_ja, name_ko, name_zh = assign_native_name_fields(native, nationality)
    return name_ja, name_ko, name_zh, native, nationality


def format_person_name(person: object, report_lang: str = "ja") -> str:
    """Return the best display name for a person given the report language.

    Priority orders:
      'ja' — JA → ZH → EN → id   (Japanese report: kanji first, then CJK, then romaji)
      'en' — EN → JA → ZH → id   (English report: romaji first)
      'ko' — KO → JA → ZH → EN → id
      other — same as 'en'

    name_ko is omitted from 'ja' priority: Hangul is not easily readable
    for Japanese readers, so romaji (name_en) is preferred.
    """
    ja  = getattr(person, "name_ja",  "") or ""
    ko  = getattr(person, "name_ko",  "") or ""
    zh  = getattr(person, "name_zh",  "") or ""
    en  = getattr(person, "name_en",  "") or ""
    pid = getattr(person, "id",       "") or ""

    if report_lang == "ja":
        return ja or zh or en or pid
    if report_lang == "ko":
        return ko or ja or zh or en or pid
    return en or ja or zh or ko or pid
