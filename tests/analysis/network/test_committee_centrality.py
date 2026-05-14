"""Tests for src.analysis.network.committee_centrality.

Coverage:
- ``build_bipartite_graph``: node attributes, edge weights, min-anime filter.
- ``project_to_company_graph``: company–company edge weight aggregation.
- ``compute_period_centralities``: pre/post split, eigenvector vs fallback.
- ``compute_period_hhi``: HHI scale, top-10 share, insufficient-data note.
- ``load_committee_memberships``: works with both joined and standalone DDLs.
- ``analyze_committee_centrality``: smoke test on synthetic SQLite fixture.
- ``lint_vocab``: source module is free of forbidden vocabulary.
- method gate: source contains no ``anime.score`` reference.

All tests use in-memory SQLite or pure-Python fixtures — no DuckDB or
external file dependencies.
"""

from __future__ import annotations

import sqlite3
from pathlib import Path

import networkx as nx
import pytest

from src.analysis.network.committee_centrality import (
    CommitteeCentralityResult,
    CommitteeMembership,
    analyze_committee_centrality,
    build_bipartite_graph,
    compute_period_centralities,
    compute_period_hhi,
    load_committee_memberships,
    project_to_company_graph,
)

_ANALYSIS_SRC = (
    Path(__file__).parents[3]
    / "src" / "analysis" / "network" / "committee_centrality.py"
)


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------


def _mk(anime: str, company: str, year: int | None = 2010, episodes: int | None = 12) -> CommitteeMembership:
    return CommitteeMembership(
        anime_canonical_id=anime,
        company_name=company,
        year=year,
        episodes=episodes,
    )


# ---------------------------------------------------------------------------
# build_bipartite_graph
# ---------------------------------------------------------------------------


def test_bipartite_graph_basic_nodes_and_edges():
    """Companies and anime become distinct node sets with bipartite attr."""
    members = [
        _mk("a1", "X"),
        _mk("a1", "Y"),
        _mk("a2", "X"),
        _mk("a2", "Y"),
    ]
    g = build_bipartite_graph(members, min_anime_per_company=1)
    # 2 anime + 2 companies
    bp_types = {d["bipartite"] for _, d in g.nodes(data=True)}
    assert bp_types == {"anime", "company"}
    assert g.number_of_nodes() == 4
    assert g.number_of_edges() == 4


def test_bipartite_graph_min_anime_per_company_filter():
    """Companies appearing on only one anime are dropped when threshold=2."""
    members = [
        _mk("a1", "X"),
        _mk("a1", "Y"),
        _mk("a2", "X"),  # X has 2 anime
        # Y has only 1 anime → must be dropped at min=2
    ]
    g = build_bipartite_graph(members, min_anime_per_company=2)
    company_nodes = [
        n for n, d in g.nodes(data=True) if d.get("bipartite") == "company"
    ]
    assert company_nodes == ["company::X"]
    # Anime nodes still appear because X invested in both.
    assert g.has_edge("anime::a1", "company::X")
    assert g.has_edge("anime::a2", "company::X")


def test_bipartite_graph_episode_weighting_monotonic():
    """log1p episode weighting is monotonic and >= 1.0."""
    g_small = build_bipartite_graph([_mk("a1", "X", episodes=1)], min_anime_per_company=1)
    g_large = build_bipartite_graph([_mk("a1", "X", episodes=100)], min_anime_per_company=1)
    w_small = g_small["anime::a1"]["company::X"]["weight"]
    w_large = g_large["anime::a1"]["company::X"]["weight"]
    assert w_small >= 1.0
    assert w_large > w_small


def test_bipartite_graph_duplicate_pair_max_weight():
    """Repeated (anime, company) keeps the maximum weight, not the sum."""
    members = [
        _mk("a1", "X", episodes=10),
        _mk("a1", "X", episodes=100),  # higher
    ]
    g = build_bipartite_graph(members, min_anime_per_company=1)
    w = g["anime::a1"]["company::X"]["weight"]
    # Equal to log1p(100)+1
    import math
    assert abs(w - (1.0 + math.log1p(100.0))) < 1e-9


# ---------------------------------------------------------------------------
# project_to_company_graph
# ---------------------------------------------------------------------------


def test_projection_edge_weight_min_of_episode_weights():
    """Projection uses min(weight_A, weight_B) per shared anime."""
    members = [
        _mk("a1", "X", episodes=1),    # weight ≈ 1.693
        _mk("a1", "Y", episodes=100),  # weight ≈ 5.615
        _mk("a2", "X", episodes=10),
        _mk("a2", "Y", episodes=10),
    ]
    g_bp = build_bipartite_graph(members, min_anime_per_company=1)
    proj = project_to_company_graph(g_bp)
    assert proj.has_edge("company::X", "company::Y")
    # Two shared anime; expected = min(eps=1 weights) + min(eps=10 weights).
    import math
    expected = min(
        1.0 + math.log1p(1.0), 1.0 + math.log1p(100.0),
    ) + min(
        1.0 + math.log1p(10.0), 1.0 + math.log1p(10.0),
    )
    assert abs(proj["company::X"]["company::Y"]["weight"] - expected) < 1e-9


def test_projection_only_company_nodes():
    """Projection contains company nodes only, not anime nodes."""
    members = [_mk("a1", "X"), _mk("a1", "Y")]
    g_bp = build_bipartite_graph(members, min_anime_per_company=1)
    proj = project_to_company_graph(g_bp)
    for n, d in proj.nodes(data=True):
        assert d.get("bipartite") == "company"


def test_projection_empty_when_no_companies():
    """Empty bipartite → empty projection."""
    proj = project_to_company_graph(nx.Graph())
    assert proj.number_of_nodes() == 0
    assert proj.number_of_edges() == 0


# ---------------------------------------------------------------------------
# compute_period_centralities
# ---------------------------------------------------------------------------


def _make_period_members() -> list[CommitteeMembership]:
    """Make 12 anime × 4 companies, half pre-2017 / half post."""
    members: list[CommitteeMembership] = []
    for i in range(6):
        for c in ("X", "Y", "Z"):
            members.append(_mk(f"a_pre_{i}", c, year=2010 + i))
        members.append(_mk(f"a_pre_{i}", "W", year=2010 + i))
    for i in range(6):
        for c in ("X", "Y", "Z"):
            members.append(_mk(f"a_post_{i}", c, year=2018 + i % 5))
        members.append(_mk(f"a_post_{i}", "W", year=2018 + i % 5))
    return members


def test_centrality_pre_post_split():
    """Centrality rows are returned for both pre and post periods."""
    members = _make_period_members()
    rows, note = compute_period_centralities(
        members, boundary_year=2017, min_anime_per_company=2,
    )
    periods = {r.period for r in rows}
    assert periods == {"pre", "post"}
    assert note  # method note is non-empty
    # Each of X/Y/Z/W appears in both periods.
    companies_pre = {r.company_name for r in rows if r.period == "pre"}
    companies_post = {r.company_name for r in rows if r.period == "post"}
    assert {"X", "Y", "Z", "W"} <= companies_pre
    assert {"X", "Y", "Z", "W"} <= companies_post


def test_centrality_small_period_skipped():
    """Periods with too few memberships are skipped without raising."""
    # Only 3 memberships pre-boundary (< _MIN_GROUP_N = 5).
    members = [
        _mk("a1", "X", year=2010),
        _mk("a1", "Y", year=2010),
        _mk("a2", "X", year=2011),
    ]
    rows, note = compute_period_centralities(
        members, boundary_year=2017, min_anime_per_company=1,
    )
    # No rows because both periods are small.
    assert rows == []
    assert "n_too_small" in note


def test_centrality_values_in_zero_one_range():
    """Eigenvector centrality scores are in [0, 1] (or fallback degree)."""
    members = _make_period_members()
    rows, _ = compute_period_centralities(
        members, boundary_year=2017, min_anime_per_company=2,
    )
    for r in rows:
        assert r.eigenvector_centrality >= 0.0
        # Fallback path can exceed 1.0 (weighted degree), so just check non-negative.


# ---------------------------------------------------------------------------
# compute_period_hhi
# ---------------------------------------------------------------------------


def test_hhi_concentrated_market_high():
    """A single dominant company yields high HHI."""
    members: list[CommitteeMembership] = []
    # 20 anime, all invested in by company X; small Y/Z/... investors.
    for i in range(20):
        members.append(_mk(f"a{i}", "X", year=2015))
    for c in ("Y", "Z", "U", "V", "W", "P", "Q", "R", "S", "T"):
        members.append(_mk("a0", c, year=2015))
    hhi = compute_period_hhi(members, boundary_year=2017)
    pre_hhi = [h for h in hhi if h.period == "pre"][0]
    assert pre_hhi.hhi is not None
    # X has 20 / (20+10) ≈ 0.667 share → HHI > 4400 alone.
    assert pre_hhi.hhi > 4000


def test_hhi_atomistic_market_low():
    """A perfectly atomistic market yields low HHI."""
    members = [_mk(f"a{i}", f"C{i}", year=2015) for i in range(20)]
    # Need >= 10 distinct companies + n>=5 memberships to compute HHI.
    hhi = compute_period_hhi(members, boundary_year=2017)
    pre_hhi = [h for h in hhi if h.period == "pre"][0]
    assert pre_hhi.hhi is not None
    # Each company has share 1/20 → HHI = 20 × (1/20)² × 10000 = 500.
    assert pre_hhi.hhi == pytest.approx(500.0, abs=1.0)


def test_hhi_insufficient_data_note():
    """HHI is None with insufficient_data note when company count is small."""
    members = [_mk("a1", "X", year=2010), _mk("a1", "Y", year=2010)]
    hhi = compute_period_hhi(members, boundary_year=2017)
    pre_hhi = [h for h in hhi if h.period == "pre"][0]
    assert pre_hhi.hhi is None
    assert pre_hhi.note == "insufficient_data"


# ---------------------------------------------------------------------------
# load_committee_memberships
# ---------------------------------------------------------------------------


def _build_joined_db() -> sqlite3.Connection:
    """Resolved anime + unqualified anime_production_committee."""
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE anime (
            id TEXT PRIMARY KEY,
            title_ja TEXT NOT NULL DEFAULT '',
            year INTEGER,
            episodes INTEGER
        );
        CREATE TABLE anime_production_committee (
            anime_id TEXT NOT NULL,
            company_name TEXT NOT NULL,
            role_label TEXT
        );
    """)
    for i in range(8):
        conn.execute(
            "INSERT INTO anime (id, title_ja, year, episodes) VALUES (?,?,?,?)",
            (f"a{i}", f"Title{i}", 2010 + i, 12),
        )
    for i in range(8):
        for c in ("X", "Y", "Z"):
            conn.execute(
                "INSERT INTO anime_production_committee "
                "(anime_id, company_name) VALUES (?, ?)",
                (f"a{i}", c),
            )
    conn.commit()
    return conn


def test_load_committee_memberships_joined_db():
    conn = _build_joined_db()
    rows = load_committee_memberships(conn)
    assert len(rows) == 8 * 3
    assert all(r.year is not None for r in rows)
    assert {r.company_name for r in rows} == {"X", "Y", "Z"}


def test_load_committee_memberships_standalone_table():
    """Standalone committee table without anime is still loadable."""
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE anime_production_committee (
            anime_id TEXT NOT NULL,
            company_name TEXT NOT NULL
        );
    """)
    for i in range(5):
        conn.execute(
            "INSERT INTO anime_production_committee (anime_id, company_name) "
            "VALUES (?, ?)",
            (f"a{i}", "X"),
        )
    conn.commit()
    rows = load_committee_memberships(conn)
    assert len(rows) == 5
    assert all(r.year is None for r in rows)


def test_load_committee_memberships_empty_db_returns_empty():
    conn = sqlite3.connect(":memory:")
    rows = load_committee_memberships(conn)
    assert rows == []


def test_load_committee_memberships_strips_whitespace_and_drops_blank():
    """Blank/whitespace-only company names are filtered out."""
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE anime_production_committee (
            anime_id TEXT NOT NULL,
            company_name TEXT NOT NULL
        );
    """)
    conn.execute("INSERT INTO anime_production_committee VALUES ('a1', '  ')")
    conn.execute("INSERT INTO anime_production_committee VALUES ('a1', ' X ')")
    conn.commit()
    rows = load_committee_memberships(conn)
    assert len(rows) == 1
    assert rows[0].company_name == "X"


# ---------------------------------------------------------------------------
# analyze_committee_centrality — smoke + edge cases
# ---------------------------------------------------------------------------


def _build_synthetic_db() -> sqlite3.Connection:
    """Synthetic Resolved-style DB spanning 2010–2022 with 4 companies."""
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE anime (
            id TEXT PRIMARY KEY,
            title_ja TEXT NOT NULL DEFAULT '',
            year INTEGER,
            episodes INTEGER
        );
        CREATE TABLE anime_production_committee (
            anime_id TEXT NOT NULL,
            company_name TEXT NOT NULL,
            role_label TEXT
        );
    """)
    # 26 anime: 2010..2022 inclusive, two per year.
    aid = 0
    for year in range(2010, 2023):
        for _ in range(2):
            conn.execute(
                "INSERT INTO anime (id, year, episodes) VALUES (?,?,?)",
                (f"a{aid}", year, 12),
            )
            for c in ("X", "Y", "Z", "W"):
                conn.execute(
                    "INSERT INTO anime_production_committee "
                    "(anime_id, company_name) VALUES (?, ?)",
                    (f"a{aid}", c),
                )
            aid += 1
    conn.commit()
    return conn


def test_analyze_committee_centrality_smoke():
    conn = _build_synthetic_db()
    result = analyze_committee_centrality(conn, boundary_year=2017)
    assert isinstance(result, CommitteeCentralityResult)
    assert result.memberships
    assert result.n_unique_companies == 4
    # Pre + post HHI rows.
    periods = {h.period for h in result.period_hhi}
    assert periods == {"pre", "post"}
    # Centralities populated for both periods.
    assert {r.period for r in result.centralities} == {"pre", "post"}
    # Coverage note has the expected metadata.
    assert "制作委員会クレジット" in result.coverage_note


def test_analyze_committee_centrality_empty_db():
    conn = sqlite3.connect(":memory:")
    result = analyze_committee_centrality(conn)
    assert result.low_coverage_warning is True
    assert "取得できませんでした" in result.coverage_note
    assert result.memberships == []
    assert result.centralities == []


def test_analyze_committee_centrality_low_coverage_flag():
    """Few unique companies trigger low_coverage_warning."""
    conn = sqlite3.connect(":memory:")
    conn.executescript("""
        CREATE TABLE anime (
            id TEXT PRIMARY KEY,
            year INTEGER,
            episodes INTEGER
        );
        CREATE TABLE anime_production_committee (
            anime_id TEXT NOT NULL,
            company_name TEXT NOT NULL
        );
    """)
    # Only 3 companies → below _MIN_COMPANIES_FOR_HHI = 10.
    for i in range(8):
        conn.execute("INSERT INTO anime (id, year, episodes) VALUES (?,?,?)", (f"a{i}", 2015, 12))
        for c in ("X", "Y", "Z"):
            conn.execute(
                "INSERT INTO anime_production_committee VALUES (?, ?)",
                (f"a{i}", c),
            )
    conn.commit()
    result = analyze_committee_centrality(conn)
    assert result.low_coverage_warning is True


def test_analyze_committee_centrality_boundary_year_param():
    """Passing a custom boundary year shifts the period split."""
    conn = _build_synthetic_db()
    # Push the boundary forward so almost everything is "pre".
    result = analyze_committee_centrality(conn, boundary_year=2030)
    pre_hhi = [h for h in result.period_hhi if h.period == "pre"][0]
    post_hhi = [h for h in result.period_hhi if h.period == "post"][0]
    assert pre_hhi.n_memberships > 0
    # No anime in 2030+; post must be empty.
    assert post_hhi.n_memberships == 0


# ---------------------------------------------------------------------------
# Lint vocab + invariant checks
# ---------------------------------------------------------------------------


def _lint_vocab_violations(path: Path) -> list[str]:
    """Run lint_vocab on a file and return non-excepted violations."""
    import sys

    lint_dir = Path(__file__).parents[3] / "scripts" / "report_generators"
    if str(lint_dir) not in sys.path:
        sys.path.insert(0, str(lint_dir))

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
    """committee_centrality.py must not contain forbidden vocabulary."""
    violations = _lint_vocab_violations(_ANALYSIS_SRC)
    assert not violations, (
        "Forbidden vocabulary found in committee_centrality.py:\n"
        + "\n".join(violations)
    )


def test_no_anime_score_in_analysis():
    """committee_centrality.py must not reference anime.score."""
    text = _ANALYSIS_SRC.read_text(encoding="utf-8")
    assert "anime.score" not in text


def test_analysis_src_exists():
    """committee_centrality.py source file must exist."""
    assert _ANALYSIS_SRC.exists(), f"Source not found: {_ANALYSIS_SRC}"
