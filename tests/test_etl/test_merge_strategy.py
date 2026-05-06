"""docs/merge_strategy.json + strategy_loader + decisions_log の test。"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.etl.resolved import strategy_loader
from src.etl.resolved._decisions_log import (
    DecisionsLogger,
    _edit_distance,
    _should_sample,
    build_decision_record,
)
from src.etl.resolved._select import select_representative_value


# ── strategy_loader ─────────────────────────────────────────────────────────


def test_strategy_json_valid() -> None:
    """JSON が parse でき必須 key を持つ。"""
    s = strategy_loader.load_strategy()
    assert isinstance(s.get("version"), str)
    assert "selection_rules" in s
    assert "entities" in s
    for ent in ("anime", "person", "studio"):
        assert ent in s["entities"]
        assert "fields" in s["entities"][ent]


def test_priority_for_returns_list() -> None:
    p = strategy_loader.priority_for("anime", "title_ja")
    assert isinstance(p, list) and len(p) > 0
    assert "seesaa" in p  # title_ja は seesaa 第1


def test_priority_for_unknown_returns_empty() -> None:
    assert strategy_loader.priority_for("anime", "nonexistent_field") == []


def test_majority_threshold_int() -> None:
    th = strategy_loader.majority_threshold()
    assert isinstance(th, int) and th >= 1


def test_source_ranking_compat() -> None:
    """旧 API (ANIME_RANKING 等) が JSON 由来の値で動作する。"""
    from src.etl.resolved.source_ranking import (
        ANIME_RANKING,
        PERSONS_RANKING,
        STUDIOS_RANKING,
        rank_for_field,
        source_prefix,
    )

    assert "title_ja" in ANIME_RANKING
    assert "name_ja" in PERSONS_RANKING
    assert "name" in STUDIOS_RANKING
    assert ANIME_RANKING["title_ja"][0] == "seesaa"
    assert source_prefix("anilist:p123") == "anilist"
    assert source_prefix("bareid") == "bareid"
    assert rank_for_field("title_ja", "anime") == ANIME_RANKING["title_ja"]
    assert rank_for_field("missing", "anime") == []


# ── _select with threshold override ────────────────────────────────────────


def test_select_priority_fallback() -> None:
    cands = [
        {"id": "anilist:1", "title_ja": "テスト"},
        {"id": "mal:1", "title_ja": "test"},
    ]
    val, src, rule = select_representative_value(
        "title_ja", cands, ["anilist", "mal"]
    )
    assert val == "テスト"
    assert src == "anilist"
    assert rule == "priority_fallback"


def test_select_no_value() -> None:
    cands = [{"id": "anilist:1", "title_ja": ""}]
    val, src, rule = select_representative_value(
        "title_ja", cands, ["anilist"]
    )
    assert val is None
    assert rule == "no_value"


def test_select_majority_vote_threshold_override() -> None:
    """threshold=2 で 2 件同値が majority_vote 発動。"""
    cands = [
        {"id": "anilist:1", "title_ja": "進撃の巨人"},
        {"id": "anilist:2", "title_ja": "進撃の巨人"},
        {"id": "anilist:3", "title_ja": "別作品"},
    ]
    val, src, rule = select_representative_value(
        "title_ja", cands, ["anilist"], majority_threshold_value=2
    )
    assert val == "進撃の巨人"
    assert rule == "majority_vote"


def test_select_tie_break_default_threshold_3() -> None:
    """デフォルト閾値 3 では 2:1 は tie_break。"""
    cands = [
        {"id": "anilist:1", "title_ja": "進撃の巨人"},
        {"id": "anilist:2", "title_ja": "進撃の巨人"},
        {"id": "anilist:3", "title_ja": "別作品"},
    ]
    val, src, rule = select_representative_value(
        "title_ja", cands, ["anilist"], majority_threshold_value=3
    )
    assert val == "進撃の巨人"  # insertion 順第1
    assert rule == "tie_break"


# ── decisions_log ───────────────────────────────────────────────────────────


def test_edit_distance_basic() -> None:
    assert _edit_distance("", "") == 0
    assert _edit_distance("abc", "abc") == 0
    assert _edit_distance("abc", "abd") == 1
    assert _edit_distance("abc", "") == 3
    assert _edit_distance("", "abc") == 3
    assert _edit_distance("kitten", "sitting") == 3


def test_should_sample_deterministic() -> None:
    """同じ key は常に同じ結果。"""
    a = _should_sample("resolved:anime:abc", "title_ja", sample_rate=0.5)
    b = _should_sample("resolved:anime:abc", "title_ja", sample_rate=0.5)
    assert a == b
    assert _should_sample("x", "y", sample_rate=1.0) is True
    assert _should_sample("x", "y", sample_rate=0.0) is False


def test_build_decision_record_shape() -> None:
    cands = [
        {"id": "anilist:a1", "title_ja": "進撃の巨人"},
        {"id": "seesaa:s1", "title_ja": "進撃の巨人"},
        {"id": "mal:m1", "title_ja": "Attack on Titan"},
    ]
    rec = build_decision_record(
        canonical_id="resolved:anime:test",
        entity_type="anime",
        field="title_ja",
        candidates=cands,
        selected_value="進撃の巨人",
        winning_source="seesaa",
        selection_rule="priority_fallback",
        priority=["seesaa", "anilist", "mal"],
    )
    assert rec["canonical_id"] == "resolved:anime:test"
    assert rec["entity_type"] == "anime"
    assert rec["field"] == "title_ja"
    assert len(rec["candidates"]) == 3
    assert rec["selected"] == {"value": "進撃の巨人", "src": "seesaa", "rule": "priority_fallback"}
    assert rec["context"]["cluster_size"] == 3
    assert rec["context"]["value_distinct_count"] == 2
    assert rec["context"]["string_edit_distance_max"] > 0
    assert rec["llm_review"] is None
    # tier 計算
    by_src = {c["src"]: c["tier"] for c in rec["candidates"]}
    assert by_src["seesaa"] == 0
    assert by_src["anilist"] == 1
    assert by_src["mal"] == 2


def test_decisions_logger_roundtrip(tmp_path: Path) -> None:
    log_path = tmp_path / "decisions.jsonl"
    with DecisionsLogger(log_path, sample_rate=1.0) as log:
        log.write(build_decision_record(
            canonical_id="x", entity_type="anime", field="title_ja",
            candidates=[{"id": "anilist:1", "title_ja": "A"}],
            selected_value="A", winning_source="anilist",
            selection_rule="priority_fallback",
            priority=["anilist"],
        ))
        log.write(build_decision_record(
            canonical_id="y", entity_type="anime", field="title_ja",
            candidates=[{"id": "mal:1", "title_ja": "B"}],
            selected_value="B", winning_source="mal",
            selection_rule="priority_fallback",
            priority=["mal"],
        ))
    lines = log_path.read_text(encoding="utf-8").strip().split("\n")
    assert len(lines) == 2
    rec0 = json.loads(lines[0])
    assert rec0["canonical_id"] == "x"


def test_decisions_logger_sampling(tmp_path: Path) -> None:
    """sample_rate=0.0 で全件 skip、書き込み行 0。"""
    log_path = tmp_path / "decisions.jsonl"
    with DecisionsLogger(log_path, sample_rate=0.0) as log:
        for i in range(10):
            log.write(build_decision_record(
                canonical_id=f"x{i}", entity_type="anime", field="title_ja",
                candidates=[{"id": "anilist:1", "title_ja": "A"}],
                selected_value="A", winning_source="anilist",
                selection_rule="priority_fallback",
                priority=["anilist"],
            ))
    assert log.skipped == 10
    assert log.written == 0


def test_decisions_logger_truncate(tmp_path: Path) -> None:
    log_path = tmp_path / "decisions.jsonl"
    log_path.write_text("OLD\n", encoding="utf-8")
    log = DecisionsLogger(log_path, sample_rate=1.0)
    log.truncate()
    assert not log_path.exists()


def test_strategy_threshold_used_by_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """select_representative_value() は明示指定なき時 strategy_loader 経由の threshold を使う。"""
    # threshold=2 を返すよう monkeypatch
    monkeypatch.setattr(
        "src.etl.resolved._select.majority_threshold", lambda: 2
    )
    cands = [
        {"id": "anilist:1", "title_ja": "進撃の巨人"},
        {"id": "anilist:2", "title_ja": "進撃の巨人"},
        {"id": "anilist:3", "title_ja": "別作品"},
    ]
    val, src, rule = select_representative_value("title_ja", cands, ["anilist"])
    assert rule == "majority_vote"
    assert val == "進撃の巨人"
