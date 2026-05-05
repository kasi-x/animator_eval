"""Field 別 value validator のテスト。

LLM 検証 (3周目) で残った wrong_value の各ケースを再現し、
validator が invalid と判定することを確認。
"""
from __future__ import annotations

import pytest

from src.etl.resolved._value_validators import (
    _has_multiple_persons,
    _has_role_suffix_brackets,
    _is_institution_name,
    _is_numeric_only,
    is_invalid_for_field,
)
from src.etl.resolved._select import select_representative_value


# ── helper-level tests ──────────────────────────────────────────────────────


@pytest.mark.parametrize("v,expected", [
    ("546456", True),
    ("123 456", True),
    ("12-34", True),
    ("ABC123", False),
    ("title", False),
    ("", False),  # 空文字は別 layer で
])
def test_is_numeric_only(v: str, expected: bool) -> None:
    assert _is_numeric_only(v) is expected


@pytest.mark.parametrize("v,expected", [
    ("日映科学映画製作所[製作]", True),
    ("田中太郎[作画]", True),
    ("田中太郎【監督】", True),
    ("田中太郎", False),
    ("[太郎]田中", False),  # 末尾でなければ False (先頭/中間)
    ("田中(太郎)別名", False),  # 丸括弧は対象外
])
def test_role_suffix(v: str, expected: bool) -> None:
    assert _has_role_suffix_brackets(v) is expected


@pytest.mark.parametrize("v,expected", [
    ("立命館大学政策科学研究科", True),
    ("東京大学", True),
    ("株式会社サンライズ", True),
    ("(株)タツノコ", True),
    ("田中太郎", False),
    ("山田花子", False),
])
def test_institution_name(v: str, expected: bool) -> None:
    assert _is_institution_name(v) is expected


@pytest.mark.parametrize("v,expected", [
    ("越智浩一 池口裕児 石野桂子 Adil Tahir", True),
    ("田中 太郎 山田", True),
    ("田中太郎", False),
    ("John Smith", False),
    ("田中 太郎", False),  # 2 token は単一人名 (姓 名)
])
def test_multiple_persons(v: str, expected: bool) -> None:
    assert _has_multiple_persons(v) is expected


# ── is_invalid_for_field ────────────────────────────────────────────────────


def test_title_en_numeric_invalid() -> None:
    assert is_invalid_for_field("title_en", "546456") is True
    assert is_invalid_for_field("title_en", "Attack on Titan") is False


def test_title_ja_no_japanese_invalid() -> None:
    """title_ja に日本語が含まれない → invalid。"""
    assert is_invalid_for_field("title_ja", "GUN HAZARD") is True
    assert is_invalid_for_field("title_ja", "TECMO SUPER BOWL") is True
    assert is_invalid_for_field("title_ja", "JUST DANCE WiiU") is True
    assert is_invalid_for_field("title_ja", "DigDug Digging Strike") is True
    # 1 文字でも日本語を含む → valid
    assert is_invalid_for_field("title_ja", "進撃の巨人") is False
    assert is_invalid_for_field("title_ja", "進撃の巨人 Season 1") is False
    assert is_invalid_for_field("title_ja", "シナぷしゅ") is False
    # title_en には日本語 check 適用しない
    assert is_invalid_for_field("title_en", "GUN HAZARD") is False


def test_title_ja_episode_token_invalid() -> None:
    """エピソード番号 suffix を含む title_ja は invalid。"""
    assert is_invalid_for_field("title_ja", "オーバーロードⅣ Episode6") is True
    assert is_invalid_for_field("title_ja", "Lesson 21") is True
    assert is_invalid_for_field("title_ja", "Track-12 Breathless") is True
    assert is_invalid_for_field("title_ja", "Chapter 5 はじまり") is True
    # episode 番号なし → valid
    assert is_invalid_for_field("title_ja", "進撃の巨人") is False


def test_name_ja_role_suffix_invalid() -> None:
    assert is_invalid_for_field("name_ja", "日映科学映画製作所[製作]") is True


def test_name_ja_institution_invalid() -> None:
    assert is_invalid_for_field("name_ja", "立命館大学政策科学研究科") is True


def test_name_ja_multiple_persons_invalid() -> None:
    assert is_invalid_for_field("name_ja", "越智浩一 池口裕児 石野桂子 Adil Tahir") is True


def test_name_ja_normal_valid() -> None:
    assert is_invalid_for_field("name_ja", "田中太郎") is False
    assert is_invalid_for_field("name_ja", "宮崎駿") is False


def test_unknown_field_passes() -> None:
    """未定義 field は常に valid (validator 範囲外)。"""
    assert is_invalid_for_field("nonexistent", "any value") is False


# ── select_representative_value 統合 ───────────────────────────────────────


def test_select_skips_invalid_value_and_falls_back() -> None:
    """tier 1 が invalid (機関名) なら、次 tier の正常値を採用。"""
    cands = [
        {"id": "madb:p_1", "name_ja": "立命館大学政策科学研究科"},
        {"id": "anilist:p_1", "name_ja": "山田花子"},
    ]
    val, src, rule = select_representative_value(
        "name_ja", cands, ["madb", "anilist"]
    )
    assert val == "山田花子"
    assert src == "anilist"
    assert rule == "priority_fallback"


def test_select_skips_role_suffix_and_falls_back() -> None:
    cands = [
        {"id": "madb:p_1", "name_ja": "日映科学映画製作所[製作]"},
        {"id": "seesaa:p_1", "name_ja": "田中太郎"},
    ]
    val, src, _ = select_representative_value(
        "name_ja", cands, ["madb", "seesaa"]
    )
    assert val == "田中太郎"
    assert src == "seesaa"


def test_select_skips_numeric_title_en() -> None:
    cands = [
        {"id": "tmdb:a_1", "title_en": "546456"},
        {"id": "anilist:a_1", "title_en": "Real Title"},
    ]
    val, src, _ = select_representative_value(
        "title_en", cands, ["tmdb", "anilist"]
    )
    assert val == "Real Title"
    assert src == "anilist"


def test_select_returns_no_value_when_all_invalid() -> None:
    """全 candidate invalid なら no_value。"""
    cands = [
        {"id": "madb:p_1", "name_ja": "東京大学"},
        {"id": "seesaa:p_1", "name_ja": "[製作]"},
    ]
    val, src, rule = select_representative_value(
        "name_ja", cands, ["madb", "seesaa"]
    )
    assert val is None
    assert rule == "no_value"
