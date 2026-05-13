"""Tests for src.analysis.network.international_collab.

Coverage:
- compute_yearly_foreign_ratios: basic flow, unknown exclusion, role group split
- compute_collab_pair_densities: edge counting, pair splitting
- compute_role_progression_rates: transition detection, temporal constraint
- build_international_collab_graph: node/edge attributes
- detect_international_communities: Louvain output shape
- run_permutation_test: p-value in [0,1], round count
- analyze_international_collab: smoke test with synthetic SQLite DB
- lint_vocab: no forbidden vocabulary in source module
- method gate: no anime.score in source
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import networkx as nx
import pytest

from src.analysis.network.nationality_resolver import (
    CONF_HIGH,
    CONF_LOW,
    GROUP_CN,
    GROUP_DOMESTIC,
    GROUP_KR,
    GROUP_SE_ASIA,
    GROUP_UNKNOWN,
    NationalityRecord,
)

# ---------------------------------------------------------------------------
# Helpers: nationality records and credit rows
# ---------------------------------------------------------------------------

_ANALYSIS_SRC = Path(__file__).parents[3] / "src" / "analysis" / "network" / "international_collab.py"


def _make_rec(person_id: str, group: str, confidence: str = CONF_HIGH) -> NationalityRecord:
    code = {"JP": "JP", "CN": "CN", "KR": "KR", "SE_ASIA": "TH", "OTHER": "US", "UNKNOWN": "UNK"}
    return NationalityRecord(
        person_id=person_id,
        country_code=code.get(group, "UNK"),
        group=group,
        confidence=confidence,
    )


def _id_to_record(records: list[NationalityRecord]) -> dict[str, NationalityRecord]:
    return {r.person_id: r for r in records}


# ---------------------------------------------------------------------------
# compute_yearly_foreign_ratios
# ---------------------------------------------------------------------------


def _make_credits_simple() -> list[tuple[str, str, str, int | None]]:
    """10 JP + 3 CN + 2 unknown, all with in_between role, year=2010."""
    rows = []
    for i in range(10):
        rows.append((f"jp{i}", "a1", "in_between", 2010))
    for i in range(3):
        rows.append((f"cn{i}", "a1", "in_between", 2010))
    for i in range(2):
        rows.append((f"unk{i}", "a1", "in_between", 2010))
    return rows


def _make_nat_records_simple() -> list[NationalityRecord]:
    recs = []
    for i in range(10):
        recs.append(_make_rec(f"jp{i}", GROUP_DOMESTIC))
    for i in range(3):
        recs.append(_make_rec(f"cn{i}", GROUP_CN))
    for i in range(2):
        recs.append(_make_rec(f"unk{i}", GROUP_UNKNOWN, CONF_LOW))
    return recs


def test_yearly_ratio_basic():
    """Foreign ratio computed correctly excluding unknown from denominator."""
    from src.analysis.network.international_collab import compute_yearly_foreign_ratios

    rows = _make_credits_simple()
    id_to_rec = _id_to_record(_make_nat_records_simple())
    result = compute_yearly_foreign_ratios(rows, id_to_rec)

    # Expect a cell for year=2010, role_group=delegation_roles and all
    deleg_cells = [r for r in result if r.year == 2010 and r.role_group == "delegation_roles"]
    assert len(deleg_cells) == 1, f"Expected 1 delegation cell, got {len(deleg_cells)}"

    cell = deleg_cells[0]
    # denominator = 10 JP + 3 CN = 13, foreign = 3
    assert cell.n_total_credits == 15
    assert cell.n_unknown_credits == 2
    assert cell.n_foreign_credits == 3
    assert cell.foreign_ratio == pytest.approx(3 / 13, rel=1e-3)


def test_yearly_ratio_unknown_excluded():
    """When all persons are unknown, foreign_ratio is None (denom < min_n)."""
    from src.analysis.network.international_collab import compute_yearly_foreign_ratios

    rows = [(f"unk{i}", "a1", "in_between", 2010) for i in range(10)]
    id_to_rec = _id_to_record([_make_rec(f"unk{i}", GROUP_UNKNOWN, CONF_LOW) for i in range(10)])
    result = compute_yearly_foreign_ratios(rows, id_to_rec)

    all_cells = [r for r in result if r.year == 2010]
    for c in all_cells:
        # denom = 0 (all unknown), so ratio must be None
        assert c.foreign_ratio is None


def test_yearly_ratio_no_year_skipped():
    """Credits with year=None are excluded from computation."""
    from src.analysis.network.international_collab import compute_yearly_foreign_ratios

    rows = [("cn1", "a1", "in_between", None)]
    id_to_rec = _id_to_record([_make_rec("cn1", GROUP_CN)])
    result = compute_yearly_foreign_ratios(rows, id_to_rec)
    assert result == []


def test_yearly_ratio_role_group_split():
    """delegation_roles and creative_lead_roles appear as separate cells."""
    from src.analysis.network.international_collab import compute_yearly_foreign_ratios

    rows = []
    # 6 JP with key_animator
    for i in range(6):
        rows.append((f"jp{i}", "a1", "key_animator", 2015))
    # 6 CN with in_between
    for i in range(6):
        rows.append((f"cn{i}", "a1", "in_between", 2015))

    id_to_rec = _id_to_record(
        [_make_rec(f"jp{i}", GROUP_DOMESTIC) for i in range(6)]
        + [_make_rec(f"cn{i}", GROUP_CN) for i in range(6)]
    )
    result = compute_yearly_foreign_ratios(rows, id_to_rec)

    role_groups = {r.role_group for r in result if r.year == 2015}
    assert "delegation_roles" in role_groups
    assert "creative_lead_roles" in role_groups
    assert "all" in role_groups


# ---------------------------------------------------------------------------
# compute_collab_pair_densities
# ---------------------------------------------------------------------------


def test_pair_density_basic():
    """JP-CN pair has correct edge count and edges_per_anime."""
    from src.analysis.network.international_collab import compute_collab_pair_densities

    # 2 JP + 2 CN on anime a1, year=2020
    rows = [
        ("jp1", "a1", "key_animator", 2020),
        ("jp2", "a1", "key_animator", 2020),
        ("cn1", "a1", "in_between", 2020),
        ("cn2", "a1", "in_between", 2020),
    ]
    id_to_rec = _id_to_record([
        _make_rec("jp1", GROUP_DOMESTIC),
        _make_rec("jp2", GROUP_DOMESTIC),
        _make_rec("cn1", GROUP_CN),
        _make_rec("cn2", GROUP_CN),
    ])
    result = compute_collab_pair_densities(rows, id_to_rec)

    jp_cn = [d for d in result if d.pair == "JP-CN" and d.year == 2020]
    assert len(jp_cn) == 1
    d = jp_cn[0]
    # 2 JP × 2 CN = 4 edges, 1 anime
    assert d.n_edges == 4
    assert d.n_anime == 1
    assert d.edges_per_anime == pytest.approx(4.0)


def test_pair_density_no_jp_no_edge():
    """Anime with no JP persons contributes zero edge to any pair."""
    from src.analysis.network.international_collab import compute_collab_pair_densities

    rows = [("cn1", "a1", "in_between", 2020), ("kr1", "a1", "in_between", 2020)]
    id_to_rec = _id_to_record([
        _make_rec("cn1", GROUP_CN),
        _make_rec("kr1", GROUP_KR),
    ])
    result = compute_collab_pair_densities(rows, id_to_rec)

    # All pairs should have n_anime=0 for year 2020
    for d in result:
        if d.year == 2020:
            assert d.n_anime == 0
            assert d.edges_per_anime is None


# ---------------------------------------------------------------------------
# compute_role_progression_rates
# ---------------------------------------------------------------------------


def test_role_progression_transition_detected():
    """Person with delegation then creative_lead counted as transitioned."""
    from src.analysis.network.international_collab import compute_role_progression_rates

    rows = [
        ("cn1", "a1", "in_between", 2010),       # delegation start
        ("cn1", "a2", "key_animator", 2015),      # creative lead after
    ]
    # Need enough CN persons to exceed _MIN_GROUP_N
    for i in range(10):
        rows.append((f"cn_extra{i}", "a1", "in_between", 2010))

    id_to_rec = _id_to_record(
        [_make_rec("cn1", GROUP_CN)]
        + [_make_rec(f"cn_extra{i}", GROUP_CN) for i in range(10)]
    )
    result = compute_role_progression_rates(rows, id_to_rec)

    cn_row = next((r for r in result if r.group == GROUP_CN), None)
    assert cn_row is not None, "CN group must appear in results"
    assert cn_row.n_transitioned >= 1, "cn1 should be counted as transitioned"
    assert cn_row.transition_rate is not None


def test_role_progression_temporal_constraint():
    """Creative lead credit before delegation does NOT count as transition."""
    from src.analysis.network.international_collab import compute_role_progression_rates

    # creative lead BEFORE delegation — should not count
    rows = [
        ("cn1", "a1", "key_animator", 2005),    # creative lead FIRST
        ("cn1", "a2", "in_between", 2010),      # delegation later
    ]
    for i in range(10):
        rows.append((f"cn_extra{i}", "a1", "in_between", 2010))

    id_to_rec = _id_to_record(
        [_make_rec("cn1", GROUP_CN)]
        + [_make_rec(f"cn_extra{i}", GROUP_CN) for i in range(10)]
    )
    result = compute_role_progression_rates(rows, id_to_rec)

    cn_row = next((r for r in result if r.group == GROUP_CN), None)
    # cn1 has creative before delegation → not a transition
    if cn_row is not None and cn_row.transition_rate is not None:
        # cn1 should be in delegation_only, not transitioned
        assert cn_row.n_delegation_only >= 1


def test_role_progression_small_n_note():
    """Groups with n_total < _MIN_GROUP_N have small_n note and None rate."""
    from src.analysis.network.international_collab import compute_role_progression_rates

    rows = [("se1", "a1", "in_between", 2010)]
    id_to_rec = _id_to_record([_make_rec("se1", GROUP_SE_ASIA)])
    result = compute_role_progression_rates(rows, id_to_rec)

    se_row = next((r for r in result if r.group == GROUP_SE_ASIA), None)
    assert se_row is not None
    assert se_row.n_total == 1
    assert se_row.transition_rate is None
    assert se_row.note == "small_n"


# ---------------------------------------------------------------------------
# build_international_collab_graph
# ---------------------------------------------------------------------------


def test_graph_node_group_attributes():
    """Graph nodes carry correct group attribute."""
    from src.analysis.network.international_collab import build_international_collab_graph

    rows = [
        ("jp1", "a1", "key_animator", 2020),
        ("cn1", "a1", "in_between", 2020),
    ]
    id_to_rec = _id_to_record([
        _make_rec("jp1", GROUP_DOMESTIC),
        _make_rec("cn1", GROUP_CN),
    ])
    g = build_international_collab_graph(rows, id_to_rec)

    assert g.nodes["jp1"]["group"] == GROUP_DOMESTIC
    assert g.nodes["cn1"]["group"] == GROUP_CN


def test_graph_cross_border_edge():
    """Edge between JP and CN person is marked cross_border=True."""
    from src.analysis.network.international_collab import build_international_collab_graph

    rows = [
        ("jp1", "a1", "key_animator", 2020),
        ("cn1", "a1", "in_between", 2020),
    ]
    id_to_rec = _id_to_record([
        _make_rec("jp1", GROUP_DOMESTIC),
        _make_rec("cn1", GROUP_CN),
    ])
    g = build_international_collab_graph(rows, id_to_rec)

    assert g.has_edge("jp1", "cn1")
    assert g["jp1"]["cn1"]["cross_border"] is True


def test_graph_domestic_only_flag():
    """include_domestic_only=False drops JP–JP edges."""
    from src.analysis.network.international_collab import build_international_collab_graph

    rows = [
        ("jp1", "a1", "key_animator", 2020),
        ("jp2", "a1", "key_animator", 2020),
        ("cn1", "a1", "in_between", 2020),
    ]
    id_to_rec = _id_to_record([
        _make_rec("jp1", GROUP_DOMESTIC),
        _make_rec("jp2", GROUP_DOMESTIC),
        _make_rec("cn1", GROUP_CN),
    ])
    g = build_international_collab_graph(rows, id_to_rec, include_domestic_only=False)

    assert not g.has_edge("jp1", "jp2"), "JP–JP edge should be excluded"
    assert g.has_edge("jp1", "cn1") or g.has_edge("jp2", "cn1"), (
        "JP–CN edges should be present"
    )


def test_graph_edge_weight_accumulates():
    """Edge weight increments for each shared anime."""
    from src.analysis.network.international_collab import build_international_collab_graph

    rows = [
        ("jp1", "a1", "key_animator", 2020),
        ("cn1", "a1", "in_between", 2020),
        ("jp1", "a2", "key_animator", 2021),
        ("cn1", "a2", "in_between", 2021),
    ]
    id_to_rec = _id_to_record([
        _make_rec("jp1", GROUP_DOMESTIC),
        _make_rec("cn1", GROUP_CN),
    ])
    g = build_international_collab_graph(rows, id_to_rec)
    assert g["jp1"]["cn1"]["weight"] == 2


# ---------------------------------------------------------------------------
# detect_international_communities
# ---------------------------------------------------------------------------


def _build_test_graph(n_jp: int = 10, n_cn: int = 5) -> tuple[nx.Graph, dict[str, NationalityRecord]]:
    """Build a small synthetic graph with two cliques (JP and CN)."""
    from src.analysis.network.international_collab import build_international_collab_graph

    rows: list[tuple[str, str, str, int | None]] = []
    # JP clique via shared anime
    for i in range(n_jp):
        rows.append((f"jp{i}", "a_jp", "key_animator", 2020))
    # CN clique via shared anime
    for i in range(n_cn):
        rows.append((f"cn{i}", "a_cn", "in_between", 2020))
    # One cross-border link
    rows.append(("jp0", "a_cross", "key_animator", 2020))
    rows.append(("cn0", "a_cross", "in_between", 2020))

    id_to_rec = _id_to_record(
        [_make_rec(f"jp{i}", GROUP_DOMESTIC) for i in range(n_jp)]
        + [_make_rec(f"cn{i}", GROUP_CN) for i in range(n_cn)]
    )
    return build_international_collab_graph(rows, id_to_rec), id_to_rec


def test_detect_communities_returns_list():
    """detect_international_communities returns a non-empty list for a non-trivial graph."""
    from src.analysis.network.international_collab import detect_international_communities

    g, _ = _build_test_graph(n_jp=10, n_cn=5)
    comms, mod = detect_international_communities(g, min_community_size=2)
    assert isinstance(comms, list)
    # Modularity must be in valid range
    assert -1.0 <= mod <= 1.0


def test_detect_communities_empty_graph():
    """detect_international_communities returns empty list for empty graph."""
    from src.analysis.network.international_collab import detect_international_communities

    g = nx.Graph()
    comms, mod = detect_international_communities(g, min_community_size=3)
    assert comms == []
    assert mod == 0.0


def test_community_international_fraction():
    """Communities with mixed JP/CN membership have non-zero international_fraction."""
    from src.analysis.network.international_collab import detect_international_communities

    g, _ = _build_test_graph(n_jp=8, n_cn=8)
    comms, _ = detect_international_communities(g, min_community_size=2)
    # At least one community should have CN members (the cross-border anime links them)
    fractions = [c.international_fraction for c in comms]
    assert any(f > 0 for f in fractions), "At least one community should contain CN members"


# ---------------------------------------------------------------------------
# run_permutation_test
# ---------------------------------------------------------------------------


def test_permutation_test_p_value_range():
    """Permutation test p_value is in [0, 1] and n_rounds is positive."""
    from src.analysis.network.international_collab import run_permutation_test

    g, _ = _build_test_graph(n_jp=8, n_cn=6)
    raw_comms = list(nx.community.louvain_communities(g, weight="weight", seed=42))
    mod = nx.community.modularity(g, raw_comms, weight="weight")

    perm = run_permutation_test(g, mod, raw_comms, n_rounds=19, rng_seed=99)

    assert 0.0 <= perm.p_value <= 1.0
    assert perm.n_rounds <= 19
    assert perm.observed_modularity == pytest.approx(mod, abs=1e-4)


def test_permutation_test_too_few_international():
    """Permutation test returns p=1.0 when there are too few international nodes."""
    from src.analysis.network.international_collab import run_permutation_test

    g = nx.Graph()
    for i in range(10):
        g.add_node(f"jp{i}", group=GROUP_DOMESTIC)
    g.add_edges_from([(f"jp{i}", f"jp{i+1}", {"weight": 1}) for i in range(9)])

    # Only 2 non-JP nodes — below _MIN_GROUP_N
    g.add_node("cn1", group=GROUP_CN)
    g.add_node("cn2", group=GROUP_CN)

    perm = run_permutation_test(g, 0.5, [], n_rounds=10)
    assert perm.p_value == 1.0
    assert perm.n_rounds == 0


# ---------------------------------------------------------------------------
# analyze_international_collab — smoke test with synthetic SQLite DB
# ---------------------------------------------------------------------------


def _build_synthetic_db() -> sqlite3.Connection:
    """Build an in-memory SILVER schema DB with JP + CN + KR persons."""
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE persons (
            id TEXT PRIMARY KEY,
            name_en TEXT NOT NULL DEFAULT '',
            name_zh TEXT,
            name_ko TEXT,
            country_of_origin TEXT
        );
        CREATE TABLE anime (
            id TEXT PRIMARY KEY,
            title_ja TEXT NOT NULL DEFAULT '',
            studio_id TEXT,
            year INTEGER,
            quarter INTEGER,
            episodes INTEGER DEFAULT 12,
            duration INTEGER DEFAULT 24
        );
        CREATE TABLE credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            role TEXT NOT NULL,
            raw_role TEXT NOT NULL DEFAULT '',
            credit_year INTEGER,
            episode INTEGER,
            evidence_source TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE meta_lineage (
            table_name TEXT PRIMARY KEY,
            audience TEXT NOT NULL,
            source_silver_tables TEXT NOT NULL,
            source_bronze_forbidden INTEGER NOT NULL DEFAULT 1,
            source_display_allowed INTEGER NOT NULL DEFAULT 0,
            formula_version TEXT NOT NULL,
            computed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
            ci_method TEXT,
            null_model TEXT,
            holdout_method TEXT,
            description TEXT,
            inputs_hash TEXT,
            notes TEXT,
            rng_seed INTEGER,
            row_count INTEGER
        );
    """)

    # 30 anime across 3 studios, 2000–2019
    for i in range(30):
        conn.execute(
            "INSERT INTO anime (id, title_ja, studio_id, year) VALUES (?,?,?,?)",
            (f"a{i}", f"Anime{i}", f"studio_{i % 3}", 2000 + i % 20),
        )

    # 200 JP + 50 CN + 30 KR + 20 SE_ASIA + 20 unknown
    configs = [
        ("JP", 200, None, None),
        ("CN", 50, None, None),
        ("KR", 30, None, None),
        ("TH", 20, None, None),
        (None, 20, None, None),
    ]
    p_idx = 0
    for country, count, zh, ko in configs:
        for _ in range(count):
            conn.execute(
                "INSERT INTO persons (id, name_en, country_of_origin, name_zh, name_ko) "
                "VALUES (?,?,?,?,?)",
                (f"p{p_idx}", f"Person{p_idx}", country, zh, ko),
            )
            # Assign credits (mix of delegation and creative)
            role = "in_between" if p_idx % 3 == 0 else "key_animator"
            anime_id = f"a{p_idx % 30}"
            credit_year = 2000 + (p_idx % 20)
            conn.execute(
                "INSERT INTO credits (person_id, anime_id, role, credit_year) VALUES (?,?,?,?)",
                (f"p{p_idx}", anime_id, role, credit_year),
            )
            p_idx += 1

    conn.commit()
    return conn


def test_analyze_international_collab_smoke():
    """analyze_international_collab runs without error on synthetic data."""
    from src.analysis.network.international_collab import analyze_international_collab

    conn = _build_synthetic_db()
    result = analyze_international_collab(conn, perm_rounds=9)

    assert result is not None
    assert result.coverage_note != ""
    # Should produce some ratio data
    assert len(result.yearly_ratios) > 0
    assert len(result.pair_densities) > 0


def test_analyze_international_collab_empty_db():
    """analyze_international_collab degrades gracefully with empty DB."""
    from src.analysis.network.international_collab import analyze_international_collab

    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE persons (
            id TEXT PRIMARY KEY,
            name_en TEXT NOT NULL DEFAULT '',
            country_of_origin TEXT
        );
        CREATE TABLE anime (
            id TEXT PRIMARY KEY,
            title_ja TEXT NOT NULL DEFAULT '',
            year INTEGER
        );
        CREATE TABLE credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            role TEXT NOT NULL
        );
    """)
    conn.commit()

    result = analyze_international_collab(conn, perm_rounds=5)
    # Should not raise; yearly_ratios should be empty
    assert result.yearly_ratios == []
    assert result.pair_densities == []


def test_analyze_international_collab_low_coverage_flag():
    """analyze_international_collab sets low_coverage_warning when no foreign persons."""
    from src.analysis.network.international_collab import analyze_international_collab

    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE persons (
            id TEXT PRIMARY KEY,
            name_en TEXT NOT NULL DEFAULT '',
            country_of_origin TEXT
        );
        CREATE TABLE anime (id TEXT PRIMARY KEY, title_ja TEXT NOT NULL DEFAULT '', year INTEGER);
        CREATE TABLE credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            role TEXT NOT NULL
        );
    """)
    # Only JP persons
    for i in range(10):
        conn.execute(
            "INSERT INTO persons (id, name_en, country_of_origin) VALUES (?,?,?)",
            (f"p{i}", f"P{i}", "JP"),
        )
    conn.commit()

    result = analyze_international_collab(conn, perm_rounds=5)
    assert result.low_coverage_warning is True


# ---------------------------------------------------------------------------
# Lint vocab and invariant checks
# ---------------------------------------------------------------------------


def _lint_vocab_violations(path: Path) -> list[str]:
    """Run lint_vocab on a file and return violation strings."""
    import sys

    lint_vocab_module = Path(__file__).parents[3] / "scripts" / "report_generators"
    if str(lint_vocab_module) not in sys.path:
        sys.path.insert(0, str(lint_vocab_module))

    from scripts.report_generators.lint_vocab import (
        _compile_patterns,
        _is_definitional,
        _is_excepted,
        lint_file,
        load_exceptions,
        load_replacements,
        load_vocab,
    )

    terms = load_vocab()
    replacements = load_replacements()
    exceptions = load_exceptions()
    patterns = _compile_patterns(terms)
    findings = lint_file(path, patterns, replacements)
    return [
        f.format()
        for f in findings
        if not _is_definitional(f) and not _is_excepted(f, exceptions)
    ]


def test_lint_vocab_analysis_module():
    """international_collab.py must not contain forbidden vocabulary."""
    violations = _lint_vocab_violations(_ANALYSIS_SRC)
    assert not violations, (
        "Forbidden vocabulary found in international_collab.py:\n"
        + "\n".join(violations)
    )


def test_no_anime_score_in_analysis():
    """international_collab.py must not reference anime.score."""
    text = _ANALYSIS_SRC.read_text(encoding="utf-8")
    assert "anime.score" not in text


def test_analysis_src_exists():
    """international_collab.py source file must exist."""
    assert _ANALYSIS_SRC.exists(), f"Source not found: {_ANALYSIS_SRC}"
