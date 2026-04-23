"""i18n (internationalization) API endpoint.

Endpoints:
  GET /api/i18n/{language} — translation dictionary for specified language
"""

from __future__ import annotations

from fastapi import APIRouter, HTTPException

router = APIRouter(tags=["i18n"])


@router.get("/api/i18n/{language}")
def get_translations(language: str):
    """Get translations for specified language.

    Args:
        language: Language code ("en" or "ja")

    Returns:
        Translation dictionary for the specified language
    """
    from src.i18n import get_i18n

    i18n = get_i18n()

    if language not in i18n.supported_languages:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported language: {language}. Supported: {', '.join(i18n.supported_languages)}",
        )

    translations = i18n.get_all_translations(language=language)
    return {
        "language": language,
        "translations": translations,
    }
