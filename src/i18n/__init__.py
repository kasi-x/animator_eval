"""国際化 (i18n) モジュール — 多言語対応.

Provides internationalization support for CLI, API, and frontend.

Supported languages:
- en: English
- ja: Japanese (日本語)

Usage:
    from src.i18n import i18n

    # Get translation
    message = i18n.t("cli.stats.title")  # "Database Statistics" or "データベース統計"

    # With placeholders
    message = i18n.t("cli.export.success", count=100, path="/tmp/output.json")

    # Change language
    i18n.set_language("ja")

    # Get current language
    lang = i18n.get_language()  # "en" or "ja"
"""

import json
import os
from pathlib import Path
from typing import Any

import structlog

logger = structlog.get_logger()


class I18n:
    """国際化マネージャー.

    Manages translation dictionaries and provides API for localized messages.
    """

    def __init__(self, default_language: str = "en"):
        """Initialize i18n manager.

        Args:
            default_language: Default language code ("en" or "ja")
        """
        self.locales_dir = Path(__file__).parent / "locales"
        self.supported_languages = ["en", "ja"]
        self.default_language = default_language
        self._current_language = self._detect_language()
        self._translations: dict[str, dict[str, Any]] = {}

        # Load translations for current language
        self._load_translations(self._current_language)

    def _detect_language(self) -> str:
        """Detect language from environment or use default.

        Returns:
            Language code ("en" or "ja")
        """
        # Check environment variable
        env_lang = os.environ.get("ANIMETOR_LANG", "").lower()
        if env_lang in self.supported_languages:
            return env_lang

        # Check system locale
        system_locale = os.environ.get("LANG", "").lower()
        if "ja" in system_locale:
            return "ja"

        # Default
        return self.default_language

    def _load_translations(self, language: str) -> None:
        """Load translation dictionary for specified language.

        Args:
            language: Language code ("en" or "ja")
        """
        locale_file = self.locales_dir / f"{language}.json"

        if not locale_file.exists():
            logger.warning(
                "locale_file_not_found",
                language=language,
                path=str(locale_file),
                fallback=self.default_language,
            )
            # Fallback to default language
            if language != self.default_language:
                locale_file = self.locales_dir / f"{self.default_language}.json"

        if not locale_file.exists():
            logger.error("default_locale_not_found", path=str(locale_file))
            self._translations[language] = {}
            return

        try:
            with open(locale_file, encoding="utf-8") as f:
                self._translations[language] = json.load(f)
            logger.debug("locale_loaded", language=language, keys=len(self._translations[language]))
        except json.JSONDecodeError as e:
            logger.error("locale_parse_error", language=language, error=str(e))
            self._translations[language] = {}
        except Exception as e:
            logger.error("locale_load_error", language=language, error=str(e))
            self._translations[language] = {}

    def set_language(self, language: str) -> None:
        """Change current language.

        Args:
            language: Language code ("en" or "ja")
        """
        if language not in self.supported_languages:
            logger.warning("unsupported_language", language=language, supported=self.supported_languages)
            return

        self._current_language = language

        # Load if not already loaded
        if language not in self._translations:
            self._load_translations(language)

        logger.debug("language_changed", language=language)

    def get_language(self) -> str:
        """Get current language code.

        Returns:
            Current language code ("en" or "ja")
        """
        return self._current_language

    def t(self, key: str, language: str | None = None, **kwargs: Any) -> str:
        """Translate a key to localized message.

        Args:
            key: Translation key (dot-separated path like "cli.stats.title")
            language: Optional language override (uses current if not specified)
            **kwargs: Placeholder values for string interpolation

        Returns:
            Translated message with placeholders replaced

        Examples:
            >>> i18n.t("cli.stats.title")
            "Database Statistics"

            >>> i18n.t("cli.export.success", count=100, path="/tmp/out.json")
            "Successfully exported 100 records to /tmp/out.json"

            >>> i18n.t("pipeline.messages.complete", total_persons=42, duration=1.23)
            "Pipeline completed! (42 persons, 1.23s)"
        """
        lang = language or self._current_language

        # Load language if not loaded
        if lang not in self._translations:
            self._load_translations(lang)

        # Navigate nested dictionary
        translations = self._translations.get(lang, {})
        keys = key.split(".")
        value = translations

        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                value = None
                break

        # Fallback to key if translation not found
        if value is None:
            logger.debug("translation_missing", key=key, language=lang)
            return key

        # Convert to string
        message = str(value)

        # Replace placeholders
        if kwargs:
            try:
                message = message.format(**kwargs)
            except KeyError as e:
                logger.warning("placeholder_missing", key=key, placeholder=str(e))

        return message

    def has_key(self, key: str, language: str | None = None) -> bool:
        """Check if translation key exists.

        Args:
            key: Translation key to check
            language: Optional language override

        Returns:
            True if key exists, False otherwise
        """
        lang = language or self._current_language

        if lang not in self._translations:
            self._load_translations(lang)

        translations = self._translations.get(lang, {})
        keys = key.split(".")
        value = translations

        for k in keys:
            if isinstance(value, dict):
                value = value.get(k)
            else:
                return False

        return value is not None

    def get_all_translations(self, language: str | None = None) -> dict[str, Any]:
        """Get entire translation dictionary for a language.

        Args:
            language: Language code (uses current if not specified)

        Returns:
            Translation dictionary
        """
        lang = language or self._current_language

        if lang not in self._translations:
            self._load_translations(lang)

        return self._translations.get(lang, {})


# Global i18n instance
_i18n_instance: I18n | None = None


def get_i18n() -> I18n:
    """Get global i18n instance (singleton).

    Returns:
        Global I18n instance
    """
    global _i18n_instance
    if _i18n_instance is None:
        _i18n_instance = I18n()
    return _i18n_instance


# Convenience alias for easy imports
i18n = get_i18n()


# Convenience function
def t(key: str, **kwargs: Any) -> str:
    """Translate a key (convenience wrapper around i18n.t()).

    Args:
        key: Translation key
        **kwargs: Placeholder values

    Returns:
        Translated message
    """
    return i18n.t(key, **kwargs)


def set_language(language: str) -> None:
    """Set current language (convenience wrapper).

    Args:
        language: Language code ("en" or "ja")
    """
    i18n.set_language(language)


def get_language() -> str:
    """Get current language (convenience wrapper).

    Returns:
        Current language code
    """
    return i18n.get_language()
