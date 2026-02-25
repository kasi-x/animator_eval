"""Tests for improved parse_role function."""

from src.models import Role, parse_role


def test_parse_role_with_episode_numbers():
    """Test that episode-specific roles are correctly parsed."""
    # エピソード番号付きの役職
    assert parse_role("Animation Director (ep 10)") == Role.ANIMATION_DIRECTOR
    assert parse_role("Key Animation (eps 21, 25)") == Role.KEY_ANIMATOR
    assert parse_role("Episode Director (eps 1, 37)") == Role.EPISODE_DIRECTOR
    assert parse_role("Storyboard (OP1, eps 1, 25)") == Role.STORYBOARD
    assert parse_role("Director (eps 1-278)") == Role.DIRECTOR


def test_parse_role_with_language_specifiers():
    """Test that language-specific roles are correctly parsed."""
    # 言語指定付きの役職
    assert parse_role("Script (German; eps 314-400)") == Role.SCREENPLAY
    assert parse_role("Producer (English)") == Role.PRODUCER


def test_parse_role_voice_actor():
    """Test that voice actors are correctly categorized."""
    assert parse_role("voice actor") == Role.VOICE_ACTOR
    assert parse_role("Voice Actor") == Role.VOICE_ACTOR
    assert parse_role("voice acting") == Role.VOICE_ACTOR


def test_parse_role_theme_song():
    """Test that theme song roles are correctly categorized."""
    assert parse_role("Theme Song Performance") == Role.THEME_SONG
    assert parse_role("Theme Song Performance (OP1)") == Role.THEME_SONG
    assert parse_role("Theme Song Performance (English; OP2)") == Role.THEME_SONG
    assert parse_role("Insert Song Performance") == Role.THEME_SONG
    assert parse_role("Theme Song Arrangement (OP2, ED2)") == Role.THEME_SONG


def test_parse_role_adr():
    """Test that ADR roles are correctly categorized."""
    assert parse_role("ADR Director (English)") == Role.ADR
    assert parse_role("ADR Director (Brazilian Portuguese; 1st dub)") == Role.ADR
    assert parse_role("ADR Script (English)") == Role.ADR
    assert (
        parse_role("ADR Director Assistant (Brazilian Portuguese; 2nd dub)") == Role.ADR
    )


def test_parse_role_core_staff():
    """Test that core staff roles still work correctly."""
    assert parse_role("Director") == Role.DIRECTOR
    assert parse_role("Animation Director") == Role.ANIMATION_DIRECTOR
    assert parse_role("Key Animation") == Role.KEY_ANIMATOR
    assert parse_role("Character Design") == Role.CHARACTER_DESIGNER
    assert parse_role("Music") == Role.MUSIC
    assert parse_role("Producer") == Role.PRODUCER


def test_parse_role_other():
    """Test that unmapped roles fall back to OTHER."""
    assert parse_role("Endcard (ep 1)") == Role.OTHER
    assert parse_role("Photography (eps 2-6)") == Role.OTHER
    assert parse_role("Script Assistance (eps 24, 25)") == Role.OTHER
    assert parse_role("Unknown Role") == Role.OTHER


def test_parse_role_case_insensitive():
    """Test that role parsing is case insensitive."""
    assert parse_role("DIRECTOR") == Role.DIRECTOR
    assert parse_role("animation director") == Role.ANIMATION_DIRECTOR
    assert parse_role("Key Animation") == Role.KEY_ANIMATOR


def test_parse_role_whitespace():
    """Test that extra whitespace is handled."""
    assert parse_role("  Director  ") == Role.DIRECTOR
    assert parse_role("Animation Director   (ep 10)  ") == Role.ANIMATION_DIRECTOR
