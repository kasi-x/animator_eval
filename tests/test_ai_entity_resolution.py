"""AI entity resolution モジュールのテスト."""

from unittest.mock import MagicMock, patch

import pytest

from src.analysis.ai_entity_resolution import (
    LLMError,
    NameMatchDecision,
    ai_assisted_cluster,
    ask_llm_if_same_person,
    check_llm_available,
)
from src.models import Person


class TestCheckLLMAvailable:
    @patch("src.analysis.ai_entity_resolution.httpx.get")
    def test_llm_available(self, mock_get):
        """Ollama が利用可能な場合 True を返す."""
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_get.return_value = mock_response

        result = check_llm_available()
        assert result is True

    @patch("src.analysis.ai_entity_resolution.httpx.get")
    def test_llm_unavailable(self, mock_get):
        """Ollama が利用不可の場合 False を返す."""
        mock_get.side_effect = Exception("Connection refused")

        result = check_llm_available()
        assert result is False


class TestAskLLMIfSamePerson:
    @patch("src.analysis.ai_entity_resolution.httpx.post")
    def test_same_response(self, mock_post):
        """Ollama が SAME と回答した場合."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "thinking": "The names are identical. So the answer should be SAME.",
            "response": "",
        }
        mock_post.return_value = mock_response

        p1 = Person(id="mal:1", name_ja="渡辺信一郎")
        p2 = Person(id="mal:2", name_ja="渡邊信一郎")

        decision = ask_llm_if_same_person(p1, p2)

        assert decision.is_match is True
        assert decision.confidence == 0.85

    @patch("src.analysis.ai_entity_resolution.httpx.post")
    def test_different_response(self, mock_post):
        """Ollama が DIFFERENT と回答した場合."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "thinking": "These are clearly different people. Therefore DIFFERENT.",
            "response": "",
        }
        mock_post.return_value = mock_response

        p1 = Person(id="mal:1", name_ja="宮崎駿")
        p2 = Person(id="mal:2", name_ja="高畑勲")

        decision = ask_llm_if_same_person(p1, p2)

        assert decision.is_match is False
        assert decision.confidence == 0.9

    @patch("src.analysis.ai_entity_resolution.httpx.post")
    def test_uncertain_response(self, mock_post):
        """Ollama が UNCERTAIN と回答した場合."""
        mock_response = MagicMock()
        mock_response.json.return_value = {
            "thinking": "Not enough information. The answer is UNCERTAIN.",
            "response": "",
        }
        mock_post.return_value = mock_response

        p1 = Person(id="mal:1", name_ja="田中宏")
        p2 = Person(id="mal:2", name_ja="田中博")

        decision = ask_llm_if_same_person(p1, p2)

        assert decision.is_match is False
        assert decision.confidence == 0.5

    @patch("src.analysis.ai_entity_resolution.httpx.post")
    def test_api_error(self, mock_post):
        """API エラーが発生した場合."""
        import httpx

        mock_post.side_effect = httpx.HTTPError("Connection failed")

        p1 = Person(id="mal:1", name_ja="山田太郎")
        p2 = Person(id="mal:2", name_ja="山田次郎")

        with pytest.raises(LLMError):
            ask_llm_if_same_person(p1, p2)


class TestAIAssistedCluster:
    @patch("src.analysis.ai_entity_resolution.check_llm_available")
    def test_llm_unavailable_returns_empty(self, mock_check):
        """LLM が利用不可の場合は空の dict を返す."""
        mock_check.return_value = False

        persons = [
            Person(id="mal:1", name_ja="宮崎駿"),
            Person(id="mal:2", name_ja="宮﨑駿"),
        ]

        result = ai_assisted_cluster(persons)
        assert result == {}

    @patch("src.analysis.ai_entity_resolution.check_llm_available")
    @patch("src.analysis.ai_entity_resolution.ask_llm_if_same_person")
    def test_high_confidence_match(self, mock_ask, mock_check):
        """高信頼度でマッチした場合."""
        mock_check.return_value = True
        mock_ask.return_value = NameMatchDecision(
            is_match=True, confidence=0.9, reasoning="Variant kanji"
        )

        persons = [
            Person(id="mal:1", name_ja="渡辺信一郎"),
            Person(id="mal:2", name_ja="渡邊信一郎"),
        ]

        result = ai_assisted_cluster(persons, min_confidence=0.8)
        assert "mal:2" in result
        assert result["mal:2"] == "mal:1"

    @patch("src.analysis.ai_entity_resolution.check_llm_available")
    @patch("src.analysis.ai_entity_resolution.ask_llm_if_same_person")
    def test_low_confidence_no_match(self, mock_ask, mock_check):
        """信頼度が閾値未満の場合はマッチしない."""
        mock_check.return_value = True
        mock_ask.return_value = NameMatchDecision(
            is_match=True, confidence=0.7, reasoning="Maybe same"
        )

        persons = [
            Person(id="mal:1", name_ja="田中宏"),
            Person(id="mal:2", name_ja="田中博"),
        ]

        result = ai_assisted_cluster(persons, min_confidence=0.8)
        assert result == {}

    @patch("src.analysis.ai_entity_resolution.check_llm_available")
    @patch("src.analysis.ai_entity_resolution.ask_llm_if_same_person")
    def test_different_sources_not_matched_by_default(self, mock_ask, mock_check):
        """デフォルトでは異なるソース間でマッチしない."""
        mock_check.return_value = True
        mock_ask.return_value = NameMatchDecision(
            is_match=True, confidence=0.9, reasoning="Same person"
        )

        persons = [
            Person(id="mal:1", name_ja="宮崎駿"),
            Person(id="anilist:1", name_ja="宮崎駿"),
        ]

        result = ai_assisted_cluster(persons, same_source_only=True)
        # 異なるソースなのでマッチしない（ask_llm は呼ばれない）
        assert result == {}
        mock_ask.assert_not_called()

    @patch("src.analysis.ai_entity_resolution.check_llm_available")
    @patch("src.analysis.ai_entity_resolution.ask_llm_if_same_person")
    def test_cross_source_with_flag(self, mock_ask, mock_check):
        """same_source_only=False なら異なるソース間でもマッチ."""
        mock_check.return_value = True
        mock_ask.return_value = NameMatchDecision(
            is_match=True, confidence=0.9, reasoning="Same person"
        )

        persons = [
            Person(id="mal:1", name_ja="宮崎駿"),
            Person(id="anilist:1", name_ja="宮崎駿"),
        ]

        result = ai_assisted_cluster(
            persons, same_source_only=False, min_confidence=0.8
        )
        assert "anilist:1" in result
        assert result["anilist:1"] == "mal:1"

    @patch("src.analysis.ai_entity_resolution.check_llm_available")
    @patch("src.analysis.ai_entity_resolution.ask_llm_if_same_person")
    def test_already_mapped_skipped(self, mock_ask, mock_check):
        """既にマッピングされた人物はスキップ."""
        mock_check.return_value = True

        # 最初のペアのみマッチ、2回目はスキップされる
        mock_ask.return_value = NameMatchDecision(
            is_match=True, confidence=0.9, reasoning="Same"
        )

        persons = [
            Person(id="mal:1", name_ja="人物A"),
            Person(id="mal:2", name_ja="人物A"),
            Person(id="mal:3", name_ja="人物A"),
        ]

        result = ai_assisted_cluster(persons, min_confidence=0.8)

        # mal:2 が mal:1 にマッチしたら、mal:3 は比較されない
        assert len(result) == 1
        assert mock_ask.call_count == 1

    @patch("src.analysis.ai_entity_resolution.check_llm_available")
    @patch("src.analysis.ai_entity_resolution.ask_llm_if_same_person")
    def test_api_error_continues(self, mock_ask, mock_check):
        """API エラーが発生しても処理を続行."""
        mock_check.return_value = True

        # 最初のペアでエラー、2番目のペアは成功
        mock_ask.side_effect = [
            LLMError("API error"),
            NameMatchDecision(is_match=True, confidence=0.9, reasoning="Same"),
        ]

        persons = [
            Person(id="mal:1", name_ja="人物A"),
            Person(id="mal:2", name_ja="人物B"),
            Person(id="mal:3", name_ja="人物C"),
        ]

        ai_assisted_cluster(persons, min_confidence=0.8)

        # エラーが起きても次のペアを処理（2つのペアを試行）
        assert mock_ask.call_count == 2

    @patch("src.analysis.ai_entity_resolution.check_llm_available")
    def test_empty_list(self, mock_check):
        """空のリストでエラーが起きない."""
        mock_check.return_value = True
        result = ai_assisted_cluster([])
        assert result == {}

    @patch("src.analysis.ai_entity_resolution.check_llm_available")
    def test_single_person(self, mock_check):
        """1人だけの場合はマッチングなし."""
        mock_check.return_value = True
        persons = [Person(id="mal:1", name_ja="宮崎駿")]
        result = ai_assisted_cluster(persons)
        assert result == {}

    @patch("src.analysis.ai_entity_resolution.check_llm_available")
    @patch("src.analysis.ai_entity_resolution.ask_llm_if_same_person")
    def test_invalid_confidence_threshold(self, mock_ask, mock_check):
        """無効な信頼度閾値はデフォルト値にフォールバック."""
        mock_check.return_value = True
        mock_ask.return_value = NameMatchDecision(
            is_match=True, confidence=0.85, reasoning="Same"
        )

        persons = [
            Person(id="mal:1", name_ja="山田太郎"),
            Person(id="mal:2", name_ja="山田太朗"),
        ]

        # 無効な閾値 (1.5) → 0.8 にフォールバック
        result = ai_assisted_cluster(persons, min_confidence=1.5)
        # confidence=0.85 >= 0.8 なのでマッチ
        assert "mal:2" in result
