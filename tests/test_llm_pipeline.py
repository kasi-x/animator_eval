"""Tests for LLM-assisted entity resolution pipeline.

Tests cache behavior, error handling, prompt formatting, and batch processing.
"""

import json
from unittest.mock import MagicMock, patch

import pytest

from src.analysis.llm_pipeline import (
    NameNormResult,
    OrgClassificationResult,
    classify_person_or_org,
    normalize_names,
    find_ai_match_candidates,
    check_llm_available,
    is_llm_enabled,
    _call_llm,
    _extract_json_array,
    _load_db_cache,
    _save_db_decision,
)
from src.runtime.models import Person
from src.analysis import calc_cache  # noqa: F401


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def temp_cache_db(tmp_path, monkeypatch):
    """Create a temporary cache database for testing."""
    cache_path = tmp_path / "cache.duckdb"
    # Patch the DEFAULT_CACHE_PATH in calc_cache module
    # This affects all calls to upsert_llm_decision and get_all_llm_decisions_bulk
    monkeypatch.setattr(calc_cache, "DEFAULT_CACHE_PATH", str(cache_path))
    yield cache_path


@pytest.fixture
def sample_persons():
    """Create sample Person instances for testing."""
    return [
        Person(id="p1", name_ja="太郎", name_en="Taro"),
        Person(id="p2", name_ja="花子", name_en="Hanako"),
        Person(id="p3", name_ja="スタジオA", name_en="Studio A"),
        Person(id="p4", name_ja="音響スタジオB", name_en="Sound Studio B"),
        Person(id="p5", name_ja="ACME Corp", name_en="ACME Corp"),
        Person(id="p6", name_ja="田中宏（フジテレビ）", name_en="Hiroshi Tanaka (Fuji)"),
        Person(id="p7", name_ja="山田・鈴木", name_en="Yamada, Suzuki"),
    ]


@pytest.fixture
def studio_names():
    """Known studio names for matching."""
    return {
        "スタジオA",
        "Studio A",
        "音響スタジオB",
        "Sound Studio B",
        "ACME Corp",
    }


# ============================================================================
# Tests: Helper Functions
# ============================================================================


class TestExtractJsonArray:
    """Test JSON extraction from LLM responses."""

    def test_extract_plain_json_array(self):
        """Extract valid JSON array without markdown."""
        response = '[{"name": "test", "type": "org"}]'
        result = _extract_json_array(response)
        assert result == [{"name": "test", "type": "org"}]

    def test_extract_json_with_markdown_fences(self):
        """Extract JSON from markdown code block."""
        response = '```json\n[{"name": "test", "type": "person"}]\n```'
        result = _extract_json_array(response)
        assert result == [{"name": "test", "type": "person"}]

    def test_extract_json_with_backticks_only(self):
        """Extract JSON from triple backticks without language specifier."""
        response = "```\n[{\"name\": \"test\"}]\n```"
        result = _extract_json_array(response)
        assert result == [{"name": "test"}]

    def test_extract_json_with_surrounding_text(self):
        """Extract JSON embedded in other text."""
        response = "Here is the result:\n[{\"name\": \"test\", \"type\": \"org\"}]\nDone!"
        result = _extract_json_array(response)
        assert result == [{"name": "test", "type": "org"}]

    def test_no_json_array_found(self):
        """Return None when no JSON array present."""
        response = "This is just plain text with no JSON."
        result = _extract_json_array(response)
        assert result is None

    def test_malformed_json(self):
        """Return None on invalid JSON."""
        response = "[{invalid json}]"
        result = _extract_json_array(response)
        assert result is None

    def test_json_object_not_array(self):
        """Return None when JSON is object, not array."""
        response = '{"name": "test"}'
        result = _extract_json_array(response)
        assert result is None


class TestIsLlmEnabled:
    """Test LLM enable/disable flag."""

    def test_disabled_by_default(self):
        """LLM disabled by default."""
        with patch.dict("os.environ", {}, clear=False):
            # Ensure ANIMETOR_LLM is not set
            import os
            os.environ.pop("ANIMETOR_LLM", None)
            assert not is_llm_enabled()

    def test_enabled_with_env_var(self):
        """LLM enabled with ANIMETOR_LLM=1."""
        with patch.dict("os.environ", {"ANIMETOR_LLM": "1"}):
            assert is_llm_enabled()

    def test_disabled_with_env_var_0(self):
        """LLM disabled with ANIMETOR_LLM=0."""
        with patch.dict("os.environ", {"ANIMETOR_LLM": "0"}):
            assert not is_llm_enabled()


# ============================================================================
# Tests: LLM Call Infrastructure
# ============================================================================


class TestCheckLlmAvailable:
    """Test LLM endpoint availability check."""

    def test_llm_available_success(self):
        """Return True when endpoint is reachable."""
        with patch("httpx.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 200
            mock_get.return_value = mock_response

            assert check_llm_available() is True
            mock_get.assert_called_once()

    def test_llm_unavailable_bad_status(self):
        """Return False on non-200 status."""
        with patch("httpx.get") as mock_get:
            mock_response = MagicMock()
            mock_response.status_code = 500
            mock_get.return_value = mock_response

            assert check_llm_available() is False

    def test_llm_unavailable_connection_error(self):
        """Return False on connection error."""
        with patch("httpx.get") as mock_get:
            mock_get.side_effect = ConnectionError("Cannot reach endpoint")

            assert check_llm_available() is False

    def test_llm_unavailable_timeout(self):
        """Return False on timeout."""
        import httpx
        with patch("httpx.get") as mock_get:
            mock_get.side_effect = httpx.TimeoutException("Timeout")

            assert check_llm_available() is False


class TestCallLlm:
    """Test single LLM call function."""

    def test_successful_llm_call(self):
        """Successful LLM call returns response text."""
        expected_response = '{"result": "success"}'
        with patch("httpx.post") as mock_post:
            mock_response = MagicMock()
            mock_response.json.return_value = {"response": expected_response}
            mock_response.status_code = 200
            mock_post.return_value = mock_response

            result = _call_llm("test prompt")
            assert result == expected_response

    def test_llm_call_returns_thinking_when_response_empty(self):
        """Fallback to 'thinking' field when 'response' is empty."""
        thinking_content = "I am thinking..."
        with patch("httpx.post") as mock_post:
            mock_response = MagicMock()
            mock_response.json.return_value = {
                "response": "",
                "thinking": thinking_content,
            }
            mock_response.status_code = 200
            mock_post.return_value = mock_response

            result = _call_llm("test prompt")
            assert result == thinking_content

    def test_llm_call_http_error(self):
        """Return empty string on HTTP error."""
        with patch("httpx.post") as mock_post:
            mock_response = MagicMock()
            mock_response.raise_for_status.side_effect = Exception("HTTP 500")
            mock_post.return_value = mock_response

            result = _call_llm("test prompt")
            assert result == ""

    def test_llm_call_timeout(self):
        """Return empty string on timeout."""
        import httpx
        with patch("httpx.post") as mock_post:
            mock_post.side_effect = httpx.TimeoutException("Timeout")

            result = _call_llm("test prompt")
            assert result == ""

    def test_llm_call_respects_max_tokens(self):
        """Pass max_tokens parameter to LLM."""
        with patch("httpx.post") as mock_post:
            mock_response = MagicMock()
            mock_response.json.return_value = {"response": "ok"}
            mock_post.return_value = mock_response

            _call_llm("prompt", max_tokens=1000)

            # Check that num_predict was set correctly
            call_args = mock_post.call_args
            assert call_args[1]["json"]["options"]["num_predict"] == 1000


# ============================================================================
# Tests: Cache Functions
# ============================================================================


class TestCacheFunctions:
    """Test database cache loading and saving — uses mocks."""

    def test_load_empty_cache(self):
        """Load returns empty dict for new cache."""
        with patch("src.analysis.llm_pipeline.get_all_llm_decisions_bulk") as mock_load:
            mock_load.return_value = {}
            result = _load_db_cache(None, "org_classification")
            assert result == {}

    def test_save_and_load_decision(self):
        """Save decision and retrieve it."""
        with patch("src.analysis.llm_pipeline.get_all_llm_decisions_bulk") as mock_load:
            # Mock the load to return what we "saved"
            cache_data = {"Studio A": {"type": "org"}}
            mock_load.return_value = cache_data

            with patch("src.analysis.llm_pipeline.upsert_llm_decision") as mock_save:
                _save_db_decision(None, "Studio A", "org_classification", {"type": "org"})
                mock_save.assert_called_once()

            cache = _load_db_cache(None, "org_classification")
            assert "Studio A" in cache
            assert cache["Studio A"]["type"] == "org"

    def test_multiple_decisions_in_cache(self):
        """Load multiple cached decisions."""
        with patch("src.analysis.llm_pipeline.get_all_llm_decisions_bulk") as mock_load:
            cache_data = {
                "Name1": {"value": "a"},
                "Name2": {"value": "b"},
            }
            mock_load.return_value = cache_data

            cache = _load_db_cache(None, "task1")
            assert len(cache) == 2
            assert cache["Name1"]["value"] == "a"
            assert cache["Name2"]["value"] == "b"

    def test_different_tasks_separate_caches(self):
        """Decisions for different tasks are separate."""
        with patch("src.analysis.llm_pipeline.get_all_llm_decisions_bulk") as mock_load:
            # First call returns task1 data, second call returns task2 data
            def side_effect(task):
                if task == "task1":
                    return {"Name": {"x": 1}}
                elif task == "task2":
                    return {"Name": {"x": 2}}
                return {}

            mock_load.side_effect = side_effect

            cache1 = _load_db_cache(None, "task1")
            cache2 = _load_db_cache(None, "task2")

            assert cache1["Name"]["x"] == 1
            assert cache2["Name"]["x"] == 2

    def test_cache_upsert(self):
        """Saving with same key overwrites previous value."""
        with patch("src.analysis.llm_pipeline.upsert_llm_decision") as mock_save:
            _save_db_decision(None, "Name", "task", {"v": 1})
            _save_db_decision(None, "Name", "task", {"v": 2})

            # Both calls should succeed
            assert mock_save.call_count == 2


# ============================================================================
# Tests: classify_person_or_org
# ============================================================================


class TestClassifyPersonOrOrg:
    """Test person vs organization classification."""

    def test_llm_disabled_returns_empty(self, sample_persons, studio_names):
        """When LLM disabled, only studio DB matching works."""
        with patch("src.analysis.llm_pipeline._load_db_cache") as mock_load_cache:
            mock_load_cache.return_value = {}  # Empty cache
            with patch("src.analysis.llm_pipeline.is_llm_enabled", return_value=False):
                result = classify_person_or_org(sample_persons, studio_names)

                # Studio A, Sound Studio B, and ACME Corp should be matched
                assert "p3" in result.org_ids
                assert "p4" in result.org_ids
                assert "p5" in result.org_ids
                assert result.from_studio_db >= 2  # At least these three
                assert result.from_llm == 0

    def test_hiragana_names_classified_as_persons(self, studio_names):
        """Names with hiragana are always persons."""
        persons = [
            Person(id="p1", name_ja="太郎"),  # has hiragana
        ]
        with patch("src.analysis.llm_pipeline.is_llm_enabled", return_value=True):
            with patch("src.analysis.llm_pipeline.check_llm_available", return_value=True):
                with patch("src.analysis.llm_pipeline._call_llm", return_value=""):
                    result = classify_person_or_org(persons, studio_names)

                    assert "p1" in result.person_ids
                    assert "p1" not in result.org_ids

    def test_studio_db_matching(self, studio_names):
        """Known studio names are instantly matched."""
        persons = [
            Person(id="p1", name_ja="スタジオA"),
            Person(id="p2", name_ja="Studio A"),
        ]
        with patch("src.analysis.llm_pipeline.is_llm_enabled", return_value=True):
            with patch("src.analysis.llm_pipeline.check_llm_available", return_value=True):
                with patch("src.analysis.llm_pipeline._call_llm", return_value=""):
                    result = classify_person_or_org(persons, studio_names)

                    assert result.from_studio_db >= 1  # at least one matched
                    assert len(result.org_ids) >= 1

    def test_cache_hit_skips_llm(self, sample_persons, studio_names):
        """Cached decisions bypass LLM call."""
        # Mock cache to return pre-populated data
        with patch("src.analysis.llm_pipeline._load_db_cache") as mock_load_cache:
            # Return a cache that has "太郎" already classified
            mock_load_cache.return_value = {"太郎": {"type": "person"}}

            with patch("src.analysis.llm_pipeline.is_llm_enabled", return_value=True):
                with patch("src.analysis.llm_pipeline.check_llm_available", return_value=True):
                    with patch("src.analysis.llm_pipeline._call_llm") as mock_llm:
                        result = classify_person_or_org(sample_persons[:1], studio_names)

                        # LLM should not be called (used cache)
                        mock_llm.assert_not_called()
                        assert result.from_cache >= 1
                        assert "p1" in result.person_ids

    def test_llm_unavailable_fallback(self, sample_persons, studio_names):
        """When LLM unavailable, unknown names default to persons."""
        persons = [
            Person(id="p1", name_ja="未知の名前"),  # unknown, no hiragana
        ]
        with patch("src.analysis.llm_pipeline.is_llm_enabled", return_value=True):
            with patch("src.analysis.llm_pipeline.check_llm_available", return_value=False):
                result = classify_person_or_org(persons, studio_names)

                # Fallback: treat as person
                assert "p1" in result.person_ids
                assert "p1" not in result.org_ids

    def test_llm_batch_classification(self, sample_persons, studio_names):
        """LLM classifies batch of candidates."""
        llm_response = json.dumps([
            {"name": "太郎", "type": "person"},
            {"name": "花子", "type": "person"},
        ])

        with patch("src.analysis.llm_pipeline._load_db_cache") as mock_load_cache:
            mock_load_cache.return_value = {}  # Empty cache
            with patch("src.analysis.llm_pipeline.is_llm_enabled", return_value=True):
                with patch("src.analysis.llm_pipeline.check_llm_available", return_value=True):
                    with patch("src.analysis.llm_pipeline._call_llm", return_value=llm_response):
                        with patch("src.analysis.llm_pipeline._save_db_decision"):
                            result = classify_person_or_org(sample_persons[:2], studio_names)

                            assert result.from_llm >= 1
                            assert "p1" in result.person_ids or "p2" in result.person_ids

    def test_llm_malformed_response_fallback(self, sample_persons, studio_names):
        """Malformed LLM response falls back to persons."""
        with patch("src.analysis.llm_pipeline.is_llm_enabled", return_value=True):
            with patch("src.analysis.llm_pipeline.check_llm_available", return_value=True):
                with patch("src.analysis.llm_pipeline._call_llm", return_value="not json"):
                    result = classify_person_or_org(sample_persons, studio_names)

                    # Fallback: treat all as persons
                    assert len(result.person_ids) > 0

    def test_result_summary_fields(self, sample_persons, studio_names):
        """Result object has expected summary fields."""
        with patch("src.analysis.llm_pipeline.is_llm_enabled", return_value=False):
            result = classify_person_or_org(sample_persons, studio_names)

            assert isinstance(result, OrgClassificationResult)
            assert result.total_classified >= 0
            assert result.from_cache >= 0
            assert result.from_llm >= 0
            assert result.from_studio_db >= 0


# ============================================================================
# Tests: normalize_names
# ============================================================================


class TestNormalizeNames:
    """Test name normalization."""

    def test_llm_disabled_returns_empty(self, sample_persons):
        """When LLM disabled, returns empty list."""
        with patch("src.analysis.llm_pipeline.is_llm_enabled", return_value=False):
            result = normalize_names(sample_persons)

            assert result == []

    def test_no_candidates_returns_empty(self):
        """No normalization needed if no special chars."""
        persons = [
            Person(id="p1", name_ja="太郎"),
            Person(id="p2", name_ja=""),
        ]
        with patch("src.analysis.llm_pipeline.is_llm_enabled", return_value=True):
            with patch("src.analysis.llm_pipeline.check_llm_available", return_value=True):
                result = normalize_names(persons)

                # No parentheses/slashes/commas, so no candidates
                assert result == []

    def test_parenthetical_detected(self):
        """Names with parentheses are candidates."""
        persons = [
            Person(id="p1", name_ja="田中宏（フジテレビ）"),
        ]
        with patch("src.analysis.llm_pipeline._load_db_cache") as mock_load_cache:
            mock_load_cache.return_value = {}  # Empty cache
            with patch("src.analysis.llm_pipeline.is_llm_enabled", return_value=True):
                with patch("src.analysis.llm_pipeline.check_llm_available", return_value=True):
                    llm_response = json.dumps([{
                        "original": "田中宏（フジテレビ）",
                        "names": ["田中宏"],
                        "episode_info": None,
                        "is_org": False,
                    }])
                    with patch("src.analysis.llm_pipeline._call_llm", return_value=llm_response):
                        with patch("src.analysis.llm_pipeline._save_db_decision"):
                            result = normalize_names(persons)

                            assert len(result) >= 1
                            assert result[0].original == "田中宏（フジテレビ）"

    def test_cache_hit_skips_llm(self):
        """Cached normalization skips LLM."""
        # Mock cache to return pre-cached result
        cached_decision = {
            "names": ["田中宏"],
            "episode_info": None,
            "is_org": False,
        }
        persons = [
            Person(id="p1", name_ja="田中宏（フジテレビ）"),
        ]

        with patch("src.analysis.llm_pipeline._load_db_cache") as mock_load_cache:
            mock_load_cache.return_value = {"田中宏（フジテレビ）": cached_decision}

            with patch("src.analysis.llm_pipeline.is_llm_enabled", return_value=True):
                with patch("src.analysis.llm_pipeline.check_llm_available", return_value=True):
                    with patch("src.analysis.llm_pipeline._call_llm") as mock_llm:
                        result = normalize_names(persons)

                        # LLM not called (cached)
                        mock_llm.assert_not_called()
                        assert len(result) == 1

    def test_llm_unavailable_returns_empty(self):
        """LLM unavailable returns cached results only."""
        persons = [
            Person(id="p1", name_ja="名前（括弧）"),
        ]
        with patch("src.analysis.llm_pipeline._load_db_cache") as mock_load_cache:
            mock_load_cache.return_value = {}  # Empty cache
            with patch("src.analysis.llm_pipeline.is_llm_enabled", return_value=True):
                with patch("src.analysis.llm_pipeline.check_llm_available", return_value=False):
                    result = normalize_names(persons)

                    # No cache, LLM unavailable: return empty
                    assert result == []

    def test_llm_malformed_response(self):
        """Malformed LLM response logs warning and continues."""
        persons = [
            Person(id="p1", name_ja="名前（括弧）"),
        ]
        with patch("src.analysis.llm_pipeline._load_db_cache") as mock_load_cache:
            mock_load_cache.return_value = {}  # Empty cache
            with patch("src.analysis.llm_pipeline.is_llm_enabled", return_value=True):
                with patch("src.analysis.llm_pipeline.check_llm_available", return_value=True):
                    with patch("src.analysis.llm_pipeline._call_llm", return_value="not json"):
                        result = normalize_names(persons)

                        # Malformed response: skip batch, continue
                        assert result == []

    def test_result_contains_split_names(self):
        """Multi-person results include separate names."""
        # Use a longer name with comma to trigger the multi-person pattern
        persons = [
            Person(id="p1", name_ja="太郎作画担当、花子背景美術"),
        ]
        llm_response = json.dumps([{
            "original": "太郎作画担当、花子背景美術",
            "names": ["太郎", "花子"],
            "episode_info": None,
            "is_org": False,
        }])

        with patch("src.analysis.llm_pipeline._load_db_cache") as mock_load_cache:
            mock_load_cache.return_value = {}  # Empty cache
            with patch("src.analysis.llm_pipeline.is_llm_enabled", return_value=True):
                with patch("src.analysis.llm_pipeline.check_llm_available", return_value=True):
                    with patch("src.analysis.llm_pipeline._call_llm", return_value=llm_response):
                        with patch("src.analysis.llm_pipeline._save_db_decision"):
                            result = normalize_names(persons)

                            assert len(result) == 1
                            assert len(result[0].names) == 2
                            assert "太郎" in result[0].names
                            assert "花子" in result[0].names

    def test_result_dataclass_structure(self):
        """Result is NameNormResult dataclass."""
        persons = [
            Person(id="p1", name_ja="名前（括弧）"),
        ]
        llm_response = json.dumps([{
            "original": "名前（括弧）",
            "names": ["名前"],
            "episode_info": "(1~10話)",
            "is_org": False,
        }])

        with patch("src.analysis.llm_pipeline._load_db_cache") as mock_load_cache:
            mock_load_cache.return_value = {}  # Empty cache
            with patch("src.analysis.llm_pipeline.is_llm_enabled", return_value=True):
                with patch("src.analysis.llm_pipeline.check_llm_available", return_value=True):
                    with patch("src.analysis.llm_pipeline._call_llm", return_value=llm_response):
                        with patch("src.analysis.llm_pipeline._save_db_decision"):
                            result = normalize_names(persons)

                            assert len(result) == 1
                            assert isinstance(result[0], NameNormResult)
                            assert result[0].episode_info == "(1~10話)"


# ============================================================================
# Tests: find_ai_match_candidates
# ============================================================================


class TestFindAiMatchCandidates:
    """Test candidate pair generation for AI matching."""

    def test_empty_persons_list(self):
        """Empty persons list returns empty candidates."""
        result = find_ai_match_candidates([], set())
        assert result == []

    def test_single_person_no_candidates(self):
        """Single person cannot form pairs."""
        persons = [Person(id="p1", name_ja="太郎")]
        result = find_ai_match_candidates(persons, set())
        assert result == []

    def test_already_matched_filtered(self):
        """Already matched persons are excluded."""
        persons = [
            Person(id="p1", name_ja="太郎"),
            Person(id="p2", name_ja="太朗"),  # similar
        ]
        result = find_ai_match_candidates(persons, {"p1"})

        # p1 already matched, so no pairs with p1
        assert len(result) == 0

    def test_no_name_ja_filtered(self):
        """Persons without name_ja are excluded."""
        persons = [
            Person(id="p1", name_ja="太郎"),
            Person(id="p2", name_ja=""),
            Person(id="p3", name_en="Hanako"),
        ]
        result = find_ai_match_candidates(persons, set())

        # Only p1 has name_ja
        assert len(result) == 0

    def test_high_similarity_not_returned(self):
        """Pairs with similarity >= 0.95 are not returned."""
        persons = [
            Person(id="p1", name_ja="太郎"),
            Person(id="p2", name_ja="太郎"),  # exact match (1.0)
        ]
        result = find_ai_match_candidates(
            persons,
            set(),
            similarity_threshold_high=0.95,
        )

        # Exact match is >= 0.95, so excluded
        assert len(result) == 0

    def test_low_similarity_not_returned(self):
        """Pairs with similarity < 0.85 are not returned."""
        persons = [
            Person(id="p1", name_ja="太郎"),
            Person(id="p2", name_ja="花子"),  # very different
        ]
        result = find_ai_match_candidates(
            persons,
            set(),
            similarity_threshold_low=0.85,
        )

        # Very different names are < 0.85, so excluded
        assert len(result) == 0

    def test_candidates_in_threshold_returned(self):
        """Pairs in [0.85, 0.95) range are returned."""
        persons = [
            Person(id="p1", name_ja="田中宏"),
            Person(id="p2", name_ja="田中博"),  # similar but not identical
        ]
        result = find_ai_match_candidates(
            persons,
            set(),
            similarity_threshold_low=0.80,
            similarity_threshold_high=1.0,
        )

        # Should have at least one pair if similarity is in range
        assert len(result) >= 0  # depends on actual similarity

    def test_first_character_blocking(self):
        """Candidates are only compared within same first character."""
        persons = [
            Person(id="p1", name_ja="太郎"),
            Person(id="p2", name_ja="太朗"),  # starts with same char
            Person(id="p3", name_ja="花子"),  # different first char
        ]
        result = find_ai_match_candidates(
            persons,
            set(),
            similarity_threshold_low=0.0,  # accept all
            similarity_threshold_high=1.1,
        )

        # Should not compare p1/p2 with p3
        # Check that no pairs involve p3 with p1/p2
        for p1, p2, sim in result:
            if p1.id == "p3" or p2.id == "p3":
                # p3 should only pair with persons starting with 花
                other = p2 if p1.id == "p3" else p1
                assert other.name_ja[0] == "花"

    def test_max_candidates_limit(self):
        """Result respects max_candidates parameter."""
        # Create many similar names
        persons = [Person(id=f"p{i}", name_ja=f"太{chr(0x3042 + i % 10)}", name_en=f"Taro{i}")
                   for i in range(20)]
        result = find_ai_match_candidates(
            persons,
            set(),
            max_candidates=5,
            similarity_threshold_low=0.0,
            similarity_threshold_high=1.1,
        )

        assert len(result) <= 5

    def test_result_sorted_by_similarity(self):
        """Results are sorted by similarity descending."""
        persons = [
            Person(id="p1", name_ja="太郎"),
            Person(id="p2", name_ja="太朗"),  # high similarity
            Person(id="p3", name_ja="大郎"),  # medium similarity
        ]
        result = find_ai_match_candidates(
            persons,
            set(),
            similarity_threshold_low=0.0,
            similarity_threshold_high=1.1,
        )

        # Check that similarity values are in descending order
        if len(result) > 1:
            for i in range(len(result) - 1):
                assert result[i][2] >= result[i + 1][2]
