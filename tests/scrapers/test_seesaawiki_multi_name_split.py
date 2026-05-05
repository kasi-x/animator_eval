"""seesaa _save_credit の複数名 split test。

LLM 検証 (3周目) で seesaa name_ja に '越智浩一 池口裕児 石野桂子 Adil Tahir' 等
24,124 件の複数名 1 cell 混在を発見。空白区切り 3+ tokens を個別 person + 個別
credit に split する parser fix の回帰防止。
"""
from __future__ import annotations

from src.runtime.models import Person
from src.scrapers.seesaawiki_scraper import _save_credit


class _FakeBW:
    def __init__(self) -> None:
        self.records: list[dict] = []

    def append(self, r: dict) -> None:
        self.records.append(r)


class _ParsedCredit:
    def __init__(self, name: str) -> None:
        self.name = name
        self.role = "原画"
        self.is_company = False
        self.episodes = []
        self.episode_from = None
        self.affiliation = None
        self.position = None


def _new_state():
    return {
        "persons_bw": _FakeBW(),
        "credits_bw": _FakeBW(),
        "studios_bw": _FakeBW(),
        "anime_studios_bw": _FakeBW(),
        "person_cache": {},
        "stats": {"persons_created": 0, "credits_created": 0},
    }


def test_single_name_no_split() -> None:
    s = _new_state()
    _save_credit(
        s["persons_bw"], s["credits_bw"], s["studios_bw"], s["anime_studios_bw"],
        s["person_cache"], s["stats"],
        anime_id="seesaa:s_1",
        parsed=_ParsedCredit("田中太郎"),
        episode=None,
    )
    assert s["stats"]["persons_created"] == 1
    assert s["stats"]["credits_created"] == 1
    assert s["persons_bw"].records[0]["name_ja"] == "田中太郎"


def test_two_token_name_no_split() -> None:
    """2 token (姓 名 / John Smith) は単一人名で split しない。"""
    s = _new_state()
    _save_credit(
        s["persons_bw"], s["credits_bw"], s["studios_bw"], s["anime_studios_bw"],
        s["person_cache"], s["stats"],
        anime_id="seesaa:s_1",
        parsed=_ParsedCredit("John Smith"),
        episode=None,
    )
    assert s["stats"]["persons_created"] == 1
    assert s["persons_bw"].records[0]["name_ja"] == "John Smith"


def test_three_token_name_splits() -> None:
    """3 token → 個別 person + 個別 credit に split。"""
    s = _new_state()
    _save_credit(
        s["persons_bw"], s["credits_bw"], s["studios_bw"], s["anime_studios_bw"],
        s["person_cache"], s["stats"],
        anime_id="seesaa:s_1",
        parsed=_ParsedCredit("越智浩一 池口裕児 石野桂子"),
        episode=None,
    )
    assert s["stats"]["persons_created"] == 3
    assert s["stats"]["credits_created"] == 3
    names = sorted(p["name_ja"] for p in s["persons_bw"].records)
    assert names == ["池口裕児", "石野桂子", "越智浩一"]


def test_four_token_with_english_name_splits() -> None:
    """LLM 検出例の元データ '越智浩一 池口裕児 石野桂子 Adil Tahir'。

    Limitation: 英字 2-word 名 (Adil Tahir) も空白で分割される。
    完全な解は単純な空白 split で得られない (姓名解析が必要)。
    機関名/グループ名混入の解消が主目的なのでこの limitation は許容。
    """
    s = _new_state()
    _save_credit(
        s["persons_bw"], s["credits_bw"], s["studios_bw"], s["anime_studios_bw"],
        s["person_cache"], s["stats"],
        anime_id="seesaa:s_1",
        parsed=_ParsedCredit("越智浩一 池口裕児 石野桂子 Adil Tahir"),
        episode=None,
    )
    # 実際: 5 token に split (越智浩一/池口裕児/石野桂子/Adil/Tahir)
    assert s["stats"]["persons_created"] == 5
    assert s["stats"]["credits_created"] == 5
    names = {p["name_ja"] for p in s["persons_bw"].records}
    assert "越智浩一" in names
    assert "池口裕児" in names


def test_full_width_space_split() -> None:
    """全角空白区切りでも split される。"""
    s = _new_state()
    _save_credit(
        s["persons_bw"], s["credits_bw"], s["studios_bw"], s["anime_studios_bw"],
        s["person_cache"], s["stats"],
        anime_id="seesaa:s_1",
        parsed=_ParsedCredit("田中　佐藤　鈴木"),
        episode=None,
    )
    assert s["stats"]["persons_created"] == 3


def test_split_uses_person_cache() -> None:
    """同じ token は cache 経由で重複 person を作らない。"""
    s = _new_state()
    # 1 回目: 3 names
    _save_credit(
        s["persons_bw"], s["credits_bw"], s["studios_bw"], s["anime_studios_bw"],
        s["person_cache"], s["stats"],
        anime_id="seesaa:s_1",
        parsed=_ParsedCredit("A B C"),
        episode=None,
    )
    # 2 回目: A 重複
    _save_credit(
        s["persons_bw"], s["credits_bw"], s["studios_bw"], s["anime_studios_bw"],
        s["person_cache"], s["stats"],
        anime_id="seesaa:s_1",
        parsed=_ParsedCredit("A D E"),
        episode=None,
    )
    # A は重複なので persons_created は 5 (A,B,C,D,E)、credits は 6 (3+3)
    assert s["stats"]["persons_created"] == 5
    assert s["stats"]["credits_created"] == 6
