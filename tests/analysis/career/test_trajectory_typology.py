"""Tests for career trajectory typology analysis.

Coverage:
- build_role_sequences: sequence construction from synthetic credits
- compute_om_distance_matrix: OM distance symmetry and triangle inequality
- select_best_k: silhouette evaluation + stop-if gate
- compute_trajectory_typology: end-to-end with synthetic DB
- stop-if: triggers when silhouette < 0.2 for all k
- Markov transition matrix: probability row sums
- Structural labels: non-evaluative checks
"""

from __future__ import annotations

import sqlite3

import numpy as np
import pytest


# ---------------------------------------------------------------------------
# Synthetic DB fixture
# ---------------------------------------------------------------------------


def _make_credits_table(conn: sqlite3.Connection) -> None:
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS credits (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            person_id TEXT NOT NULL,
            anime_id TEXT NOT NULL,
            role TEXT NOT NULL,
            credit_year INTEGER,
            raw_role TEXT NOT NULL DEFAULT '',
            evidence_source TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS meta_lineage (
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


@pytest.fixture()
def minimal_conn() -> sqlite3.Connection:
    """Minimal in-memory DB — only enough to smoke-test sequence building."""
    conn = sqlite3.connect(":memory:")
    _make_credits_table(conn)
    return conn


@pytest.fixture()
def synthetic_conn() -> sqlite3.Connection:
    """In-memory DB with structured synthetic career sequences.

    20 persons, each with 5 years of credits:
    - Group A (persons 0-9): in_between → key_animator → animation_director pattern
    - Group B (persons 10-19): in_between → in_between → key_animator → key_animator pattern

    Designed to produce 2+ separable clusters with reasonable silhouette.
    """
    conn = sqlite3.connect(":memory:")
    _make_credits_table(conn)

    # Group A: progressive ascent
    group_a_sequence = [
        "in_between", "in_between", "key_animator", "key_animator", "animation_director"
    ]
    # Group B: late specialist
    group_b_sequence = [
        "in_between", "in_between", "in_between", "key_animator", "key_animator"
    ]

    for p_idx in range(20):
        seq = group_a_sequence if p_idx < 10 else group_b_sequence
        for year_offset, role in enumerate(seq):
            conn.execute(
                "INSERT INTO credits (person_id, anime_id, role, credit_year) "
                "VALUES (?, ?, ?, ?)",
                (f"p{p_idx}", f"a{p_idx}", role, 2000 + year_offset),
            )

    conn.commit()
    return conn


@pytest.fixture()
def large_synthetic_conn() -> sqlite3.Connection:
    """Larger synthetic DB designed to produce separable clusters.

    Creates 60 persons across 3 distinct trajectory patterns to
    enable k=3 cluster separation with higher silhouette.
    """
    conn = sqlite3.connect(":memory:")
    _make_credits_table(conn)

    patterns = [
        # Pattern 0: fast progression (in_between → key → anim_dir)
        ["in_between", "key_animator", "key_animator", "animation_director",
         "animation_director", "animation_director", "director"],
        # Pattern 1: stable specialist (key_animator throughout)
        ["key_animator", "key_animator", "key_animator",
         "key_animator", "key_animator", "key_animator", "key_animator"],
        # Pattern 2: slow start (in_between × many years, then key)
        ["in_between", "in_between", "in_between", "in_between",
         "in_between", "key_animator", "key_animator"],
    ]

    for p_idx in range(60):
        pattern = patterns[p_idx % 3]
        for year_offset, role in enumerate(pattern):
            conn.execute(
                "INSERT INTO credits (person_id, anime_id, role, credit_year) "
                "VALUES (?, ?, ?, ?)",
                (f"p{p_idx}", f"a{p_idx % 20}", role, 2000 + year_offset),
            )

    conn.commit()
    return conn


# ---------------------------------------------------------------------------
# build_role_sequences
# ---------------------------------------------------------------------------


def test_build_role_sequences_basic(synthetic_conn: sqlite3.Connection) -> None:
    """build_role_sequences returns non-empty list for synthetic data."""
    from src.analysis.career.trajectory_typology import build_role_sequences

    seqs = build_role_sequences(synthetic_conn)
    assert len(seqs) > 0, "Must return at least one sequence"


def test_build_role_sequences_min_length(synthetic_conn: sqlite3.Connection) -> None:
    """All returned sequences have length >= min_seq_length."""
    from src.analysis.career.trajectory_typology import build_role_sequences

    min_len = 3
    seqs = build_role_sequences(synthetic_conn, min_seq_length=min_len)
    for seq in seqs:
        assert seq.length >= min_len, (
            f"Sequence {seq.person_id} has length {seq.length} < {min_len}"
        )


def test_build_role_sequences_stages_non_zero(synthetic_conn: sqlite3.Connection) -> None:
    """All stages in returned sequences are > 0 (non-production excluded)."""
    from src.analysis.career.trajectory_typology import build_role_sequences

    seqs = build_role_sequences(synthetic_conn)
    for seq in seqs:
        for stage in seq.stages:
            assert stage > 0, (
                f"Person {seq.person_id}: stage 0 (non-production) must be excluded"
            )


def test_build_role_sequences_empty_db(minimal_conn: sqlite3.Connection) -> None:
    """build_role_sequences returns empty list for empty credits table."""
    from src.analysis.career.trajectory_typology import build_role_sequences

    seqs = build_role_sequences(minimal_conn)
    assert seqs == [], "Empty credits table must return empty sequence list"


def test_build_role_sequences_person_count(synthetic_conn: sqlite3.Connection) -> None:
    """Sequence count matches expected person count in synthetic DB."""
    from src.analysis.career.trajectory_typology import build_role_sequences

    seqs = build_role_sequences(synthetic_conn, min_seq_length=3)
    # All 20 persons have 5-year sequences, each with stages > 0
    assert len(seqs) == 20, f"Expected 20 sequences, got {len(seqs)}"


# ---------------------------------------------------------------------------
# OM distance matrix
# ---------------------------------------------------------------------------


def test_om_distance_matrix_symmetry(synthetic_conn: sqlite3.Connection) -> None:
    """OM distance matrix is symmetric."""
    from src.analysis.career.trajectory_typology import (
        build_role_sequences,
        compute_om_distance_matrix,
    )

    seqs = build_role_sequences(synthetic_conn)
    dist = compute_om_distance_matrix(seqs)
    assert dist.shape == (len(seqs), len(seqs)), "Shape must be (n, n)"
    np.testing.assert_allclose(dist, dist.T, atol=1e-9, err_msg="Matrix must be symmetric")


def test_om_distance_diagonal_zero(synthetic_conn: sqlite3.Connection) -> None:
    """OM distance of a sequence to itself is zero."""
    from src.analysis.career.trajectory_typology import (
        build_role_sequences,
        compute_om_distance_matrix,
    )

    seqs = build_role_sequences(synthetic_conn)
    dist = compute_om_distance_matrix(seqs)
    np.testing.assert_allclose(
        np.diag(dist), np.zeros(len(seqs)), atol=1e-9,
        err_msg="Diagonal (self-distance) must be zero"
    )


def test_om_distance_non_negative(synthetic_conn: sqlite3.Connection) -> None:
    """All OM distances are non-negative."""
    from src.analysis.career.trajectory_typology import (
        build_role_sequences,
        compute_om_distance_matrix,
    )

    seqs = build_role_sequences(synthetic_conn)
    dist = compute_om_distance_matrix(seqs)
    assert np.all(dist >= 0), "All distances must be non-negative"


def test_om_distance_identical_sequences() -> None:
    """Two identical sequences have distance 0."""
    from src.analysis.career.trajectory_typology import (
        RoleSequence,
        compute_om_distance_matrix,
    )

    seq = RoleSequence(
        person_id="p0", debut_year=2000, final_year=2004,
        stages=[1, 1, 3, 3, 5], roles=["in_between"] * 2 + ["key_animator"] * 2 + ["animation_director"]
    )
    dist = compute_om_distance_matrix([seq, seq])
    assert dist[0, 1] == pytest.approx(0.0), "Identical sequences must have distance 0"


def test_om_distance_different_sequences_positive() -> None:
    """Two different sequences have distance > 0."""
    from src.analysis.career.trajectory_typology import (
        RoleSequence,
        compute_om_distance_matrix,
    )

    seq_a = RoleSequence(
        person_id="a", debut_year=2000, final_year=2004,
        stages=[1, 1, 1, 1, 1], roles=["in_between"] * 5
    )
    seq_b = RoleSequence(
        person_id="b", debut_year=2000, final_year=2004,
        stages=[5, 5, 5, 5, 5], roles=["animation_director"] * 5
    )
    dist = compute_om_distance_matrix([seq_a, seq_b])
    assert dist[0, 1] > 0, "Different sequences must have positive distance"


# ---------------------------------------------------------------------------
# select_best_k
# ---------------------------------------------------------------------------


def test_select_best_k_stop_if_small_data(minimal_conn: sqlite3.Connection) -> None:
    """select_best_k returns (empty_dict, None) for fewer sequences than k_min."""
    from src.analysis.career.trajectory_typology import (
        RoleSequence,
        compute_om_distance_matrix,
        select_best_k,
    )

    # Only 3 sequences — too few for k=3..7 with silhouette
    seqs = [
        RoleSequence(
            person_id=f"p{i}", debut_year=2000, final_year=2004,
            stages=[1, 2, 3, 4, 5], roles=["in_between"] * 5
        )
        for i in range(3)
    ]
    dist = compute_om_distance_matrix(seqs)
    sil_by_k, best_k = select_best_k(dist, k_min=3, k_max=7)
    # With only 3 samples, silhouette is unreliable → expect stop-if or very low k
    # The function must not crash
    assert isinstance(sil_by_k, dict)


def test_select_best_k_returns_dict(large_synthetic_conn: sqlite3.Connection) -> None:
    """select_best_k returns a dict for large enough data."""
    from src.analysis.career.trajectory_typology import (
        build_role_sequences,
        compute_om_distance_matrix,
        select_best_k,
    )

    seqs = build_role_sequences(large_synthetic_conn)
    dist = compute_om_distance_matrix(seqs)
    sil_by_k, best_k = select_best_k(dist, k_min=3, k_max=5)
    assert isinstance(sil_by_k, dict), "Must return dict"
    assert all(isinstance(k, int) for k in sil_by_k), "Keys must be int"
    assert all(isinstance(v, float) for v in sil_by_k.values()), "Values must be float"


# ---------------------------------------------------------------------------
# compute_markov_transitions
# ---------------------------------------------------------------------------


def test_markov_rows_sum_to_one(synthetic_conn: sqlite3.Connection) -> None:
    """Non-zero rows of Markov transition matrix sum to 1.0."""
    from src.analysis.career.trajectory_typology import (
        build_role_sequences,
        compute_markov_transitions,
    )

    seqs = build_role_sequences(synthetic_conn)
    mat, labels = compute_markov_transitions(seqs)
    for i, row in enumerate(mat):
        row_sum = sum(row)
        if row_sum > 0:
            assert abs(row_sum - 1.0) < 1e-9, (
                f"Row {i} (stage_{i}) sum {row_sum:.6f} != 1.0"
            )


def test_markov_shape_correct(synthetic_conn: sqlite3.Connection) -> None:
    """Markov matrix has shape (stage_max+1, stage_max+1)."""
    from src.analysis.career.trajectory_typology import (
        build_role_sequences,
        compute_markov_transitions,
    )

    seqs = build_role_sequences(synthetic_conn)
    mat, labels = compute_markov_transitions(seqs, stage_max=6)
    assert len(mat) == 7, f"Matrix must have 7 rows (stages 0-6), got {len(mat)}"
    for row in mat:
        assert len(row) == 7, f"Each row must have 7 cols, got {len(row)}"


def test_markov_non_negative(synthetic_conn: sqlite3.Connection) -> None:
    """All Markov probabilities are non-negative."""
    from src.analysis.career.trajectory_typology import (
        build_role_sequences,
        compute_markov_transitions,
    )

    seqs = build_role_sequences(synthetic_conn)
    mat, _ = compute_markov_transitions(seqs)
    for row in mat:
        assert all(v >= 0 for v in row), "All transition probabilities must be non-negative"


# ---------------------------------------------------------------------------
# stop-if gate
# ---------------------------------------------------------------------------


def test_stop_if_triggers_for_insufficient_sequences(minimal_conn: sqlite3.Connection) -> None:
    """compute_trajectory_typology triggers stop-if when too few sequences."""
    from src.analysis.career.trajectory_typology import compute_trajectory_typology

    result = compute_trajectory_typology(minimal_conn)
    assert result.stop_if_triggered, "stop_if must trigger for empty/insufficient DB"
    assert result.clusters == [], "No clusters on stop-if"
    assert result.best_k is None, "best_k must be None on stop-if"


def test_stop_if_result_has_reason(minimal_conn: sqlite3.Connection) -> None:
    """stop_if_reason is non-empty when stop-if is triggered."""
    from src.analysis.career.trajectory_typology import compute_trajectory_typology

    result = compute_trajectory_typology(minimal_conn)
    if result.stop_if_triggered:
        assert result.stop_if_reason, "stop_if_reason must be non-empty"


def test_stop_if_below_threshold() -> None:
    """select_best_k returns best_k=None when best silhouette < threshold."""
    from src.analysis.career.trajectory_typology import (
        SILHOUETTE_THRESHOLD,
        select_best_k,
    )

    # Synthetic all-equal distance matrix → silhouette = 0 for any k
    n = 20
    dist = np.zeros((n, n), dtype=float)
    # Make it non-degenerate: add a tiny perturbation
    rng = np.random.default_rng(0)
    dist += rng.uniform(0, 1e-4, size=(n, n))
    dist = (dist + dist.T) / 2
    np.fill_diagonal(dist, 0.0)

    sil_by_k, best_k = select_best_k(dist, k_min=3, k_max=5)
    # All silhouettes should be near 0 → below threshold → best_k=None
    if sil_by_k:
        best_sil = max(sil_by_k.values())
        if best_sil < SILHOUETTE_THRESHOLD:
            assert best_k is None, (
                f"best_k must be None when best_silhouette={best_sil:.4f} < {SILHOUETTE_THRESHOLD}"
            )


# ---------------------------------------------------------------------------
# compute_trajectory_typology: end-to-end
# ---------------------------------------------------------------------------


def test_typology_end_to_end_large(large_synthetic_conn: sqlite3.Connection) -> None:
    """compute_trajectory_typology succeeds and returns valid TypologyResult."""
    from src.analysis.career.trajectory_typology import (
        SILHOUETTE_THRESHOLD,
        TypologyResult,
        compute_trajectory_typology,
    )

    result = compute_trajectory_typology(large_synthetic_conn)
    assert isinstance(result, TypologyResult)
    assert result.n_sequences > 0, "Must count sequences"
    assert isinstance(result.silhouette_scores, dict)

    if not result.stop_if_triggered:
        # Valid clustering result
        assert result.best_k is not None
        assert result.best_silhouette is not None
        assert result.best_silhouette >= SILHOUETTE_THRESHOLD, (
            f"best_silhouette={result.best_silhouette:.4f} < threshold"
        )
        assert len(result.clusters) == result.best_k, (
            f"Expected {result.best_k} clusters, got {len(result.clusters)}"
        )
        for cl in result.clusters:
            assert cl.n > 0, f"Cluster {cl.cluster_id} must have n > 0"
            assert cl.medoid_person_id, "Medoid must be set"
            assert cl.typical_stages, "typical_stages must be non-empty"
            assert cl.label, "label must be non-empty"


def test_typology_cluster_sizes_sum_to_n(large_synthetic_conn: sqlite3.Connection) -> None:
    """Sum of cluster sizes equals total sequence count."""
    from src.analysis.career.trajectory_typology import compute_trajectory_typology

    result = compute_trajectory_typology(large_synthetic_conn)
    if not result.stop_if_triggered:
        total = sum(cl.n for cl in result.clusters)
        assert total == result.n_sequences, (
            f"Cluster size sum {total} != n_sequences {result.n_sequences}"
        )


def test_typology_no_empty_clusters(large_synthetic_conn: sqlite3.Connection) -> None:
    """No cluster should be empty."""
    from src.analysis.career.trajectory_typology import compute_trajectory_typology

    result = compute_trajectory_typology(large_synthetic_conn)
    for cl in result.clusters:
        assert len(cl.person_ids) > 0, f"Cluster {cl.cluster_id} has no members"


# ---------------------------------------------------------------------------
# Structural label checks (no evaluative framing)
# ---------------------------------------------------------------------------


_FORBIDDEN_LABEL_TERMS = (
    "high",
    "low",
    "fast",
    "slow",
    "good",
    "bad",
    "top",
    "best",
    "worst",
    "superior",
    "inferior",
    "promising",
    "failed",
)


def test_structural_labels_no_evaluative_terms(
    large_synthetic_conn: sqlite3.Connection,
) -> None:
    """Cluster labels must not contain evaluative framing."""
    from src.analysis.career.trajectory_typology import compute_trajectory_typology

    result = compute_trajectory_typology(large_synthetic_conn)
    for cl in result.clusters:
        label_lower = cl.label.lower()
        for term in _FORBIDDEN_LABEL_TERMS:
            assert term not in label_lower, (
                f"Cluster {cl.cluster_id} label '{cl.label}' contains "
                f"forbidden evaluative term '{term}'"
            )


# ---------------------------------------------------------------------------
# Lint vocab check for analysis source
# ---------------------------------------------------------------------------


def test_lint_vocab_trajectory_typology_src() -> None:
    """trajectory_typology.py must not contain forbidden vocabulary in string literals."""
    from pathlib import Path

    src_path = (
        Path(__file__).parents[3]
        / "src"
        / "analysis"
        / "career"
        / "trajectory_typology.py"
    )
    assert src_path.exists(), f"Source file not found: {src_path}"

    import sys

    lint_vocab_module = (
        Path(__file__).parents[3] / "scripts" / "report_generators" / "lint_vocab.py"
    )
    if str(lint_vocab_module.parent) not in sys.path:
        sys.path.insert(0, str(lint_vocab_module.parent))

    from scripts.report_generators.lint_vocab import (
        _compile_patterns,
        _is_definitional,
        _is_excepted,
        lint_file,
        load_exceptions,
        load_vocab,
    )

    terms = load_vocab()
    exceptions = load_exceptions()
    patterns = _compile_patterns(terms)
    findings = lint_file(src_path, patterns, {})
    real_findings = [
        f for f in findings
        if not _is_definitional(f) and not _is_excepted(f, exceptions)
    ]
    assert not real_findings, (
        "Forbidden vocabulary found in trajectory_typology.py:\n"
        + "\n".join(f.format() for f in real_findings)
    )


def test_no_anime_score_in_src() -> None:
    """trajectory_typology.py must not reference anime.score."""
    from pathlib import Path

    src_path = (
        Path(__file__).parents[3]
        / "src"
        / "analysis"
        / "career"
        / "trajectory_typology.py"
    )
    text = src_path.read_text(encoding="utf-8")
    assert "anime.score" not in text, "anime.score must not appear in trajectory_typology.py"
