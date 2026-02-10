"""Tests for i18n (internationalization) module."""



from src.i18n import I18n, get_i18n, get_language, set_language, t


def test_i18n_initialization():
    """I18n initializes with default language."""
    instance = I18n(default_language="en")
    assert instance.get_language() in ["en", "ja"]
    assert instance.supported_languages == ["en", "ja"]


def test_i18n_set_language():
    """I18n can change language."""
    instance = I18n(default_language="en")
    instance.set_language("ja")
    assert instance.get_language() == "ja"
    instance.set_language("en")
    assert instance.get_language() == "en"


def test_i18n_unsupported_language():
    """I18n handles unsupported language gracefully."""
    instance = I18n(default_language="en")
    current = instance.get_language()
    instance.set_language("fr")  # Not supported
    assert instance.get_language() == current  # Should not change


def test_i18n_translate_simple():
    """I18n translates simple keys."""
    instance = I18n(default_language="en")
    instance.set_language("en")

    # Test English
    assert instance.t("app.name") == "Animetor Eval"
    assert instance.t("cli.stats.title") == "Database Statistics"

    # Test Japanese
    instance.set_language("ja")
    assert instance.t("app.name") == "アニメーター評価"
    assert instance.t("cli.stats.title") == "データベース統計"


def test_i18n_translate_with_placeholders():
    """I18n supports placeholder interpolation."""
    instance = I18n(default_language="en")
    instance.set_language("en")

    message = instance.t("cli.export.success", count=100, path="/tmp/output.json")
    assert "100" in message
    assert "/tmp/output.json" in message

    instance.set_language("ja")
    message = instance.t("cli.export.success", count=50, path="/data/result.json")
    assert "50" in message
    assert "/data/result.json" in message


def test_i18n_missing_key():
    """I18n returns key itself when translation missing."""
    instance = I18n(default_language="en")
    result = instance.t("nonexistent.key.path")
    assert result == "nonexistent.key.path"


def test_i18n_has_key():
    """I18n can check if key exists."""
    instance = I18n(default_language="en")
    instance.set_language("en")

    assert instance.has_key("app.name") is True
    assert instance.has_key("cli.stats.title") is True
    assert instance.has_key("nonexistent.key") is False


def test_i18n_get_all_translations():
    """I18n can return entire translation dictionary."""
    instance = I18n(default_language="en")
    instance.set_language("en")

    translations = instance.get_all_translations()
    assert isinstance(translations, dict)
    assert "app" in translations
    assert "cli" in translations
    assert "pipeline" in translations


def test_global_i18n_instance():
    """Global i18n instance works correctly."""
    instance = get_i18n()
    assert isinstance(instance, I18n)

    # Same instance returned
    instance2 = get_i18n()
    assert instance is instance2


def test_convenience_functions():
    """Convenience functions (t, set_language, get_language) work."""
    set_language("en")
    assert get_language() == "en"
    assert t("app.name") == "Animetor Eval"

    set_language("ja")
    assert get_language() == "ja"
    assert t("app.name") == "アニメーター評価"


def test_i18n_nested_keys():
    """I18n handles deeply nested keys."""
    instance = I18n(default_language="en")
    instance.set_language("en")

    # 3-level nested
    assert instance.t("pipeline.phases.data_loading") == "Data Loading"
    assert instance.t("frontend.monitor.connection.connected") == "Connected"

    instance.set_language("ja")
    assert instance.t("pipeline.phases.data_loading") == "データ読み込み"
    assert instance.t("frontend.monitor.connection.connected") == "✓ 接続中"


def test_i18n_language_override():
    """I18n allows language override per translation."""
    instance = I18n(default_language="en")
    instance.set_language("ja")  # Current language is Japanese

    # Override to English
    assert instance.t("app.name", language="en") == "Animetor Eval"
    # Current language still Japanese
    assert instance.t("app.name") == "アニメーター評価"


def test_i18n_env_detection(monkeypatch):
    """I18n detects language from environment."""
    # Test ANIMETOR_LANG
    monkeypatch.setenv("ANIMETOR_LANG", "ja")
    instance = I18n(default_language="en")
    assert instance.get_language() == "ja"

    # Test LANG fallback
    monkeypatch.delenv("ANIMETOR_LANG", raising=False)
    monkeypatch.setenv("LANG", "ja_JP.UTF-8")
    instance = I18n(default_language="en")
    assert instance.get_language() == "ja"


def test_i18n_pipeline_messages():
    """I18n provides correct pipeline messages."""
    instance = I18n(default_language="en")

    # English
    instance.set_language("en")
    assert instance.t("pipeline.status.waiting") == "Waiting"
    assert instance.t("pipeline.status.running") == "Running"
    assert instance.t("pipeline.status.completed") == "Completed"

    # Japanese
    instance.set_language("ja")
    assert instance.t("pipeline.status.waiting") == "待機中"
    assert instance.t("pipeline.status.running") == "実行中..."
    assert instance.t("pipeline.status.completed") == "完了"


def test_i18n_error_messages():
    """I18n provides localized error messages."""
    instance = I18n(default_language="en")

    # English
    instance.set_language("en")
    assert "connection failed" in instance.t("errors.database.connection_failed").lower()

    # Japanese
    instance.set_language("ja")
    assert "接続" in instance.t("errors.database.connection_failed")
    assert "失敗" in instance.t("errors.database.connection_failed")


def test_i18n_report_disclaimer():
    """I18n provides localized report disclaimers."""
    instance = I18n(default_language="en")

    # English
    instance.set_language("en")
    disclaimer = instance.t("reports.disclaimer.content")
    assert "network position" in disclaimer.lower()
    assert "collaboration density" in disclaimer.lower()

    # Japanese
    instance.set_language("ja")
    disclaimer = instance.t("reports.disclaimer.content")
    assert "ネットワーク" in disclaimer
    assert "コラボレーション" in disclaimer
