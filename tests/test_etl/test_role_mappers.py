"""Unit tests for src/etl/role_mappers/.

Tests each source mapper independently:
  - Known role strings → expected Role.value
  - Unknown strings → Role.OTHER.value
  - Registry: map_role(source, raw) dispatches correctly
  - Bangumi: integer code strings → Role.value
"""
from __future__ import annotations

import pytest

from src.etl.role_mappers import MAPPERS, map_role
from src.runtime.models import Role


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

class TestRegistry:
    def test_all_expected_sources_registered(self) -> None:
        expected = {
            "seesaawiki", "anilist", "ann", "mal", "mediaarts", "bangumi",
            "keyframe", "sakuga_atwiki",
        }
        assert expected.issubset(MAPPERS.keys())

    def test_unknown_source_returns_identity(self) -> None:
        """Unknown source → raw string returned unchanged (identity)."""
        raw = "some_unknown_role_xyz"
        assert map_role("unknown_source_xyz", raw) == raw

    def test_map_role_dispatches_to_registered_mapper(self) -> None:
        result = map_role("anilist", "director")
        assert result == Role.DIRECTOR.value


# ---------------------------------------------------------------------------
# SeesaaWiki mapper
# ---------------------------------------------------------------------------

class TestSeesaawikiMapper:
    @pytest.mark.parametrize("raw,expected", [
        ("監督", Role.DIRECTOR.value),
        ("作画監督", Role.ANIMATION_DIRECTOR.value),
        ("総作画監督", Role.ANIMATION_DIRECTOR.value),
        ("原画", Role.KEY_ANIMATOR.value),
        ("第二原画", Role.SECOND_KEY_ANIMATOR.value),
        ("動画", Role.IN_BETWEEN.value),
        ("絵コンテ", Role.EPISODE_DIRECTOR.value),
        ("演出", Role.EPISODE_DIRECTOR.value),
        ("キャラクターデザイン", Role.CHARACTER_DESIGNER.value),
        ("音響監督", Role.SOUND_DIRECTOR.value),
        ("音楽", Role.MUSIC.value),
        ("脚本", Role.SCREENPLAY.value),
        ("撮影監督", Role.PHOTOGRAPHY_DIRECTOR.value),
        ("編集", Role.EDITING.value),
        ("仕上げ", Role.FINISHING.value),
        ("背景", Role.BACKGROUND_ART.value),
        ("プロデューサー", Role.PRODUCER.value),
        ("制作進行", Role.PRODUCTION_MANAGER.value),
    ])
    def test_known_ja_roles(self, raw: str, expected: str) -> None:
        assert map_role("seesaawiki", raw) == expected

    def test_unknown_role_returns_other(self) -> None:
        assert map_role("seesaawiki", "全く知らない役職名XYZ") == Role.OTHER.value

    def test_whitespace_trimmed(self) -> None:
        assert map_role("seesaawiki", "  監督  ") == Role.DIRECTOR.value

    def test_already_normalized_value_passthrough(self) -> None:
        """If bronze already ran _normalize_role and emits a Role.value string,
        seesaawiki mapper should still resolve it correctly via ROLE_MAP or
        return OTHER (since Role values are English, not in JA ROLE_MAP by default)."""
        # "director" is in ROLE_MAP, so it maps to Role.DIRECTOR
        result = map_role("seesaawiki", "director")
        assert result == Role.DIRECTOR.value


# ---------------------------------------------------------------------------
# AniList mapper
# ---------------------------------------------------------------------------

class TestAnilistMapper:
    @pytest.mark.parametrize("raw,expected", [
        ("director", Role.DIRECTOR.value),
        ("key animation", Role.KEY_ANIMATOR.value),
        ("animation director", Role.ANIMATION_DIRECTOR.value),
        ("chief animation director", Role.ANIMATION_DIRECTOR.value),
        ("episode director", Role.EPISODE_DIRECTOR.value),
        ("storyboard", Role.EPISODE_DIRECTOR.value),
        ("character design", Role.CHARACTER_DESIGNER.value),
        ("original creator", Role.ORIGINAL_CREATOR.value),
        ("series composition", Role.SCREENPLAY.value),
        ("sound director", Role.SOUND_DIRECTOR.value),
        ("music", Role.MUSIC.value),
        ("producer", Role.PRODUCER.value),
        ("2nd key animation", Role.SECOND_KEY_ANIMATOR.value),
        ("in-between animation", Role.IN_BETWEEN.value),
    ])
    def test_known_en_roles(self, raw: str, expected: str) -> None:
        assert map_role("anilist", raw) == expected

    def test_unknown_role_returns_other(self) -> None:
        assert map_role("anilist", "totally unknown job XYZ") == Role.OTHER.value

    def test_case_insensitive(self) -> None:
        assert map_role("anilist", "DIRECTOR") == Role.DIRECTOR.value
        assert map_role("anilist", "Key Animation") == Role.KEY_ANIMATOR.value


# ---------------------------------------------------------------------------
# ANN mapper
# ---------------------------------------------------------------------------

class TestAnnMapper:
    def test_known_role(self) -> None:
        assert map_role("ann", "director") == Role.DIRECTOR.value

    def test_unknown_role_returns_other(self) -> None:
        assert map_role("ann", "unknown_ann_role_XYZ") == Role.OTHER.value


# ---------------------------------------------------------------------------
# MAL mapper
# ---------------------------------------------------------------------------

class TestMalMapper:
    def test_known_role(self) -> None:
        assert map_role("mal", "key animation") == Role.KEY_ANIMATOR.value

    def test_unknown_role_returns_other(self) -> None:
        assert map_role("mal", "unrecognised mal role XYZ") == Role.OTHER.value


# ---------------------------------------------------------------------------
# MediaArts mapper
# ---------------------------------------------------------------------------

class TestMediaartsMapper:
    @pytest.mark.parametrize("raw,expected", [
        ("director", Role.DIRECTOR.value),
        ("key_animator", Role.KEY_ANIMATOR.value),
        ("animation_director", Role.ANIMATION_DIRECTOR.value),
        ("in_between", Role.IN_BETWEEN.value),
        ("finishing", Role.FINISHING.value),
        ("other", Role.OTHER.value),
    ])
    def test_valid_role_values_pass_through(self, raw: str, expected: str) -> None:
        """Values that are already Role.value strings are returned unchanged."""
        assert map_role("mediaarts", raw) == expected

    def test_japanese_fallback(self) -> None:
        """Japanese strings not yet normalized fall through to ROLE_MAP."""
        assert map_role("mediaarts", "監督") == Role.DIRECTOR.value

    def test_unknown_returns_other(self) -> None:
        assert map_role("mediaarts", "zzzunknown_mediaarts_role") == Role.OTHER.value


# ---------------------------------------------------------------------------
# Bangumi mapper
# ---------------------------------------------------------------------------

class TestBangumiMapper:
    @pytest.mark.parametrize("code,expected", [
        ("2", Role.DIRECTOR.value),        # Director/Direction
        ("1", Role.ORIGINAL_CREATOR.value), # Original Creator/Original Work
        ("3", Role.SCREENPLAY.value),       # Script/Screenplay
        ("4", Role.EPISODE_DIRECTOR.value), # Storyboard
        ("5", Role.EPISODE_DIRECTOR.value), # Episode Direction
        ("6", Role.MUSIC.value),            # Music
        ("8", Role.CHARACTER_DESIGNER.value), # Character Design
        ("10", Role.SCREENPLAY.value),      # Series Composition
        ("14", Role.ANIMATION_DIRECTOR.value), # Chief Animation Director
        ("15", Role.ANIMATION_DIRECTOR.value), # Animation Direction
        ("20", Role.KEY_ANIMATOR.value),    # Key Animation
        ("21", Role.SECOND_KEY_ANIMATOR.value), # 2nd Key Animation
        ("51", Role.IN_BETWEEN.value),      # In-Between Animation
        ("54", Role.PRODUCER.value),        # Producer
        ("44", Role.SOUND_DIRECTOR.value),  # Sound Director
        ("28", Role.EDITING.value),         # Editing
        ("13", Role.FINISHING.value),       # Color Design
        ("25", Role.BACKGROUND_ART.value),  # Background Art
        ("17", Role.PHOTOGRAPHY_DIRECTOR.value), # Director of Photography
        ("69", Role.CGI_DIRECTOR.value),    # CG Director
    ])
    def test_known_codes(self, code: str, expected: str) -> None:
        assert map_role("bangumi", code) == expected

    def test_integer_input_as_string(self) -> None:
        """Bangumi codes arrive as strings; ensure correct parsing."""
        assert map_role("bangumi", "2") == Role.DIRECTOR.value

    def test_unknown_code_returns_other(self) -> None:
        assert map_role("bangumi", "99999") == Role.OTHER.value

    def test_non_numeric_code_returns_other(self) -> None:
        assert map_role("bangumi", "not_a_number") == Role.OTHER.value


# ---------------------------------------------------------------------------
# Keyframe mapper
# ---------------------------------------------------------------------------

class TestKeyframeMapper:
    """Keyframe mapper delegates to SeesaaWiki — same ROLE_MAP covers both sources."""

    @pytest.mark.parametrize("raw,expected", [
        ("監督", Role.DIRECTOR.value),
        ("作画監督", Role.ANIMATION_DIRECTOR.value),
        ("原画", Role.KEY_ANIMATOR.value),
        ("動画", Role.IN_BETWEEN.value),
        ("絵コンテ", Role.EPISODE_DIRECTOR.value),
        ("演出", Role.EPISODE_DIRECTOR.value),
        ("キャラクターデザイン", Role.CHARACTER_DESIGNER.value),
        ("脚本", Role.SCREENPLAY.value),
        ("音響監督", Role.SOUND_DIRECTOR.value),
        ("音楽", Role.MUSIC.value),
        ("背景", Role.BACKGROUND_ART.value),
        ("仕上げ", Role.FINISHING.value),
        ("撮影監督", Role.PHOTOGRAPHY_DIRECTOR.value),
        ("プロデューサー", Role.PRODUCER.value),
    ])
    def test_known_ja_roles(self, raw: str, expected: str) -> None:
        assert map_role("keyframe", raw) == expected

    def test_normalized_value_passthrough(self) -> None:
        """Bronze role column already contains Role.value; mapper should resolve it."""
        assert map_role("keyframe", "director") == Role.DIRECTOR.value

    def test_unknown_role_returns_other(self) -> None:
        assert map_role("keyframe", "全く知らない役職XYZ") == Role.OTHER.value

    def test_whitespace_trimmed(self) -> None:
        assert map_role("keyframe", "  監督  ") == Role.DIRECTOR.value


# ---------------------------------------------------------------------------
# Sakuga@wiki mapper
# ---------------------------------------------------------------------------

class TestSakugaAtwikiMapper:
    """Sakuga@wiki mapper delegates to SeesaaWiki — same Japanese role vocabulary."""

    @pytest.mark.parametrize("raw,expected", [
        ("原画", Role.KEY_ANIMATOR.value),
        ("動画", Role.IN_BETWEEN.value),
        ("作画監督", Role.ANIMATION_DIRECTOR.value),
        ("監督", Role.DIRECTOR.value),
        ("背景", Role.BACKGROUND_ART.value),
        ("仕上げ", Role.FINISHING.value),
        ("演出", Role.EPISODE_DIRECTOR.value),
    ])
    def test_known_ja_roles(self, raw: str, expected: str) -> None:
        assert map_role("sakuga_atwiki", raw) == expected

    def test_unknown_role_returns_other(self) -> None:
        assert map_role("sakuga_atwiki", "未知の役職ZZZ") == Role.OTHER.value

    def test_whitespace_trimmed(self) -> None:
        assert map_role("sakuga_atwiki", "  原画  ") == Role.KEY_ANIMATOR.value
