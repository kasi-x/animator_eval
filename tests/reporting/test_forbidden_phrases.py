"""Tests for the forbidden phrase lint."""

from __future__ import annotations

from src.reporting.specs.forbidden_phrases import (
    ALLOWED_PHRASES,
    FORBIDDEN_PHRASES,
    find_forbidden,
)


def test_clean_text_no_hits() -> None:
    text = (
        "本レポートは同役職・同キャリアバンド内の貢献分布を示すものである。"
        "結果は bootstrap CI 付きで表示する。"
    )
    assert find_forbidden(text) == []


def test_direct_hit() -> None:
    text = "この人物は能力が低い傾向にある。"
    hits = find_forbidden(text)
    assert any(phrase == "能力が低い" for phrase, _ in hits)


def test_disclaimer_phrase_is_allowed() -> None:
    text = (
        "本スコアは個人の能力・技量・芸術性を評価・測定・示唆するものではありません。"
    )
    assert find_forbidden(text) == []


def test_multiple_hits() -> None:
    text = "彼は才能がある一方、彼女は実力不足と言わざるを得ない。"
    hits = find_forbidden(text)
    phrases = {phrase for phrase, _ in hits}
    assert "才能がある" in phrases
    assert "実力不足" in phrases


def test_allowed_phrase_masks_substring() -> None:
    # "能力を測るものではない" is allowed; even though it contains "能力",
    # no forbidden phrase inside it should be flagged.
    text = "本スコアは能力を測るものではない指標である。"
    assert find_forbidden(text) == []


def test_all_forbidden_keys_present() -> None:
    # Regression: the dictionary should have no accidental empties.
    for phrase, suggestion in FORBIDDEN_PHRASES.items():
        assert phrase
        assert suggestion


def test_allowed_phrases_is_frozenset() -> None:
    assert isinstance(ALLOWED_PHRASES, frozenset)
    assert len(ALLOWED_PHRASES) > 0
