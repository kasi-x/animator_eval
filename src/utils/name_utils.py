"""Native name script detection and nationality inference.

Handles Japanese, Korean, Chinese, and other scripts from AniList and other
sources. Name columns (name_ja / name_ko / name_zh) are JA/KO/ZH only;
other scripts stay in name_en (romanised form) and are distinguished via
person_aliases.lang.
"""

from __future__ import annotations

import json
import os
import re
from pathlib import Path

import httpx
from src.utils.config import LLM_BASE_URL, LLM_MODEL_NAME, LLM_TIMEOUT

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
# Hometown token sets — loaded from JSON, with LLM fallback + cache
# ---------------------------------------------------------------------------

_TOKENS_JSON_PATH = Path(__file__).parent / "hometown_tokens.json"


def _load_tokens_json() -> dict:
    try:
        return json.loads(_TOKENS_JSON_PATH.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {"tokens": {}, "arabic_tokens": {}, "_cache": {}}


_tokens_data = _load_tokens_json()

_JAPANESE_HOMETOWN_TOKENS: frozenset[str] = frozenset(_tokens_data["tokens"].get("JP", []))
_CHINESE_HOMETOWN_TOKENS:  frozenset[str] = frozenset(_tokens_data["tokens"].get("CN", []))
_KOREAN_HOMETOWN_TOKENS:   frozenset[str] = frozenset(_tokens_data["tokens"].get("KR", []))
_ARABIC_HOMETOWN_TOKENS:   dict[str, str] = _tokens_data.get("arabic_tokens", {})
# Mutable dict; updated at runtime by LLM cache writes
_HOMETOWN_CACHE: dict[str, str | None] = _tokens_data.get("_cache", {})


def _build_hometown_re(tokens: frozenset[str]) -> re.Pattern[str]:
    """Compile tokens into a single regex (longest match first to avoid prefix shadowing)."""
    if not tokens:
        return re.compile(r"(?!)")  # never-matches sentinel
    alts = sorted(map(re.escape, tokens), key=len, reverse=True)
    return re.compile("|".join(alts), re.IGNORECASE)


_JAPANESE_RE = _build_hometown_re(_JAPANESE_HOMETOWN_TOKENS)
_CHINESE_RE  = _build_hometown_re(_CHINESE_HOMETOWN_TOKENS)
_KOREAN_RE   = _build_hometown_re(_KOREAN_HOMETOWN_TOKENS)

# Arabic: sorted longest-first so "hong kong" matches before "hong"
_arabic_keys_sorted = sorted(_ARABIC_HOMETOWN_TOKENS.keys(), key=len, reverse=True)
_ARABIC_RE = (
    re.compile("|".join(map(re.escape, _arabic_keys_sorted)), re.IGNORECASE)
    if _arabic_keys_sorted else re.compile(r"(?!)")
)


def _atomic_write_json(path: Path, data: dict) -> None:
    """Write *data* to *path* atomically via a sibling tmp file."""
    tmp = path.parent / f".{path.stem}_{os.getpid()}.tmp"
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)


def _save_hometown_cache(hometown_key: str, code: str | None) -> None:
    """Persist an LLM-inferred nationality to the JSON cache (atomic write)."""
    _HOMETOWN_CACHE[hometown_key] = code
    try:
        data = _load_tokens_json()
        data["_cache"] = _HOMETOWN_CACHE
        _atomic_write_json(_TOKENS_JSON_PATH, data)
    except OSError:
        pass  # cache persistence failure is non-fatal


# ---------------------------------------------------------------------------
# LLM nationality inference — decomposed helpers
# ---------------------------------------------------------------------------

def _build_llm_prompt(hometown: str) -> str:
    return (
        "You are a geography expert. Given a hometown/city/location string, "
        "output ONLY the ISO 3166-1 alpha-2 country code (e.g. JP, CN, KR, US). "
        "If you cannot determine the country, output NULL.\n\n"
        f"Location: {hometown}\nCountry code:"
    )


def _call_ollama_generate(prompt: str) -> str | None:
    """POST to Ollama /api/generate and return the raw response text, or None on error."""
    try:
        ollama_base = LLM_BASE_URL.replace("/v1", "")
        resp = httpx.post(
            f"{ollama_base}/api/generate",
            json={
                "model": LLM_MODEL_NAME,
                "prompt": prompt,
                "stream": False,
                "options": {"temperature": 0.0, "num_predict": 10},
            },
            timeout=LLM_TIMEOUT,
        )
        resp.raise_for_status()
        payload = resp.json()
        return (payload.get("response") or payload.get("thinking") or "").strip()
    except (httpx.HTTPError, ValueError):
        return None


def _parse_country_code(raw: str) -> str | None:
    """Extract an ISO 3166-1 alpha-2 code from a raw LLM response string."""
    if raw.upper().startswith("NULL"):
        return None
    m = re.search(r"\b([A-Z]{2})\b", raw)
    return m.group(1) if m else None


def _llm_infer_nationality(hometown: str) -> str | None:
    """Ask the local LLM to infer the ISO 3166-1 alpha-2 country code for a hometown.

    Returns a 2-letter code (e.g. "JP") or None when unknown/unavailable.
    Gracefully degrades: returns None if Ollama is not reachable.
    """
    prompt = _build_llm_prompt(hometown)
    raw = _call_ollama_generate(prompt)
    if raw is None:
        return None
    return _parse_country_code(raw)


# ---------------------------------------------------------------------------
# infer_nationalities — decomposed helpers
# ---------------------------------------------------------------------------

def _from_script_direct(script: str) -> list[str] | None:
    """Return a single-country list for unambiguous scripts, else None."""
    if script == "ko":
        return ["KR"]
    if script == "ja":
        return ["JP"]
    if script == "th":
        return ["TH"]
    return None


def _resolve_zh_or_ja(hometown: str, *, use_llm: bool) -> list[str]:
    """Resolve nationality for a zh_or_ja script name using hometown tokens, cache, or LLM."""
    low = hometown.lower()
    if _JAPANESE_RE.search(low):
        return ["JP"]
    if _CHINESE_RE.search(low):
        return ["CN"]
    if _KOREAN_RE.search(low):
        return ["KR"]
    cache_key = hometown.strip()
    if cache_key in _HOMETOWN_CACHE:
        cached = _HOMETOWN_CACHE[cache_key]
        return [cached] if cached else []
    if use_llm:
        code = _llm_infer_nationality(hometown)
        _save_hometown_cache(cache_key, code)
        return [code] if code else []
    return []


def _resolve_arabic(hometown_lower: str) -> list[str]:
    """Resolve nationality for an Arabic-script name using Arabic hometown tokens."""
    m = _ARABIC_RE.search(hometown_lower)
    if m:
        return [_ARABIC_HOMETOWN_TOKENS[m.group().lower()]]
    return []


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def lang_of_alias(alias: str) -> str | None:
    """Return BCP-47-like lang tag for an alias string, or None if unknown.

    Used to populate person_aliases.lang for display and cross-source matching.
    """
    return _SCRIPT_TO_LANG.get(detect_name_script(alias))


def infer_nationalities(
    name_native: str,
    hometown: str | None,
    *,
    use_llm: bool = False,
) -> list[str]:
    """Infer ISO 3166-1 alpha-2 nationality codes from native script and hometown.

    Returns a list because dual nationality is possible (e.g. ["JP", "KR"]).
    Returns [] when nationality cannot be determined.

    Conservative by design: prefers [] over a wrong guess.
    Confident single-script → single-country mappings: Hangul→KR, Thai→TH.
    All others require a hometown hint or remain [].

    Args:
        use_llm: When True, fall back to local Ollama LLM for unknown hometowns
                 and cache the result in hometown_tokens.json._cache.
    """
    script = detect_name_script(name_native)

    direct = _from_script_direct(script)
    if direct is not None:
        return direct

    if not hometown:
        return []

    if script == "zh_or_ja":
        return _resolve_zh_or_ja(hometown, use_llm=use_llm)

    if script == "ar":
        return _resolve_arabic(hometown.lower())

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
