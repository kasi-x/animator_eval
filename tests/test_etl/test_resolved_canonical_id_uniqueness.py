"""Regression tests for canonical_id collision bug in build_cross_source_anime_clusters.

Bug summary:
    When two independent UF groups both have rep.year=None and the same title,
    the old cluster_key formula (f"{title}|{year}") produced identical strings,
    causing dict-overwrite (last-writer-wins silent drop).

    The fix replaces the key with a hash of sorted member IDs, which is
    guaranteed collision-free for distinct member sets.

Cases:
    1. サイボーグ009 1968 TV regression — concrete real-world symptom.
    2. Full row coverage — sum of all cluster members == input N.
    3. Idempotency — same canonical_id regardless of row ordering.
"""

from __future__ import annotations

import hashlib
import random
from typing import Any

from src.etl.resolved._cross_source_ids import (
    _compute_canonical_id,
    build_cross_source_anime_clusters,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _row(
    row_id: str,
    title_ja: str = "",
    year: int | None = None,
    fmt: str | None = None,
) -> dict[str, Any]:
    """Minimal conformed anime row dict."""
    return {
        "id": row_id,
        "title_ja": title_ja,
        "year": year,
        "format": fmt,
    }


# ---------------------------------------------------------------------------
# Case 1: サイボーグ009 1968 TV — regression for canonical_id collision
# ---------------------------------------------------------------------------


class TestSaiborg009Regression:
    """Regression for canonical_id collision when two independent UF groups
    share title=サイボーグ009 and year=None.

    Background:
        The production bug involved:
          UF group A — bgm:s13605 (year=None, fmt=NULL)
          UF group B — keyframe:... (year=None) + anilist/mal/tmdb (year=1968)
                       linked by BRONZE keyframe.anilist_id foreign key

        With bronze_root=None (no ID-link parquets), the two groups that are
        both year=None never merge via title+year fallback (correct behaviour
        since year=None rows are excluded from title+year clustering).

        In the old code, both year=None singletons produced the SAME cluster_key
        string "サイボーグ009|" and sha256 → same canonical_id → dict overwrite.

        This test class verifies that the new member-ID-hash approach produces
        distinct canonical_ids regardless of shared title/year.

    With bronze_root=None, expected clustering:
        - bgm:s13605              → singleton (year=None → no title+year merge)
        - keyframe:4837b9...      → singleton (year=None → no title+year merge)
        - anilist:8394 + mal:a8394 + tmdb:tv:56427  → 3-row cluster (year=1968, title+year merge)
        Total: 3 clusters, all 5 rows present.
    """

    TITLE = "サイボーグ009"

    def _build_rows(self) -> list[dict[str, Any]]:
        """Input rows that triggered the canonical_id collision bug."""
        return [
            # Group A — bgm singleton, year=None
            _row("bgm:s13605", title_ja=self.TITLE, year=None, fmt=None),
            # Group B — keyframe is year=None (no BRONZE parquet → no ID link)
            _row(
                "keyframe:4837b9269266b69e7c6186c96a006b9d7c3aede1e8fd06fe",
                title_ja=self.TITLE,
                year=None,
                fmt="TV",
            ),
            # These three share year=1968 → merged by title+year fallback
            _row("anilist:8394", title_ja=self.TITLE, year=1968, fmt="TV"),
            _row("mal:a8394", title_ja=self.TITLE, year=1968, fmt="TV"),
            _row("tmdb:tv:56427", title_ja=self.TITLE, year=1968, fmt="TV"),
        ]

    def test_produces_three_distinct_clusters(self):
        """Both year=None singletons and the year=1968 triple must be distinct."""
        rows = self._build_rows()
        clusters = build_cross_source_anime_clusters(rows, bronze_root=None)

        assert len(clusters) == 3, (
            f"Expected 3 clusters: bgm-singleton, keyframe-singleton, 1968-triple; "
            f"got {len(clusters)}: sizes={sorted(len(v) for v in clusters.values())}"
        )

    def test_three_row_1968_cluster_contains_all_members(self):
        """anilist + mal + tmdb (year=1968) must be in one cluster."""
        rows = self._build_rows()
        clusters = build_cross_source_anime_clusters(rows, bronze_root=None)

        three_row_cluster = [v for v in clusters.values() if len(v) == 3]
        assert len(three_row_cluster) == 1, (
            "Expected exactly one cluster with 3 members (anilist/mal/tmdb year=1968), "
            f"got sizes: {sorted(len(v) for v in clusters.values())}"
        )
        member_ids = {r["id"] for r in three_row_cluster[0]}
        expected = {"anilist:8394", "mal:a8394", "tmdb:tv:56427"}
        assert member_ids == expected

    def test_both_year_none_singletons_have_different_canonical_id(self):
        """The canonical_id collision: bgm and keyframe both year=None must NOT share an ID."""
        rows = self._build_rows()
        clusters = build_cross_source_anime_clusters(rows, bronze_root=None)

        singleton_clusters = {
            next(iter(v))["id"]: cid for cid, v in clusters.items() if len(v) == 1
        }
        assert "bgm:s13605" in singleton_clusters, "bgm singleton missing"
        kf_id = "keyframe:4837b9269266b69e7c6186c96a006b9d7c3aede1e8fd06fe"
        assert kf_id in singleton_clusters, "keyframe singleton missing"

        # Core regression assertion: they must NOT share canonical_id
        assert singleton_clusters["bgm:s13605"] != singleton_clusters[kf_id], (
            "canonical_id collision: bgm and keyframe year=None singletons "
            "have the same canonical_id — the bug is still present"
        )

    def test_bgm_singleton_cluster_present(self):
        rows = self._build_rows()
        clusters = build_cross_source_anime_clusters(rows, bronze_root=None)

        singleton_ids = {next(iter(v))["id"] for v in clusters.values() if len(v) == 1}
        assert "bgm:s13605" in singleton_ids

    def test_no_row_dropped(self):
        rows = self._build_rows()
        clusters = build_cross_source_anime_clusters(rows, bronze_root=None)
        total = sum(len(v) for v in clusters.values())
        assert total == len(rows), f"Expected {len(rows)} rows, got {total}"


# ---------------------------------------------------------------------------
# Case 2: Full row coverage — all rows must appear in exactly one cluster
# ---------------------------------------------------------------------------


class TestFullRowCoverage:
    """All input rows must flow into exactly one cluster (no silent drops)."""

    def test_all_rows_present_simple(self):
        rows = [
            _row("anilist:1", title_ja="進撃の巨人", year=2013, fmt="TV"),
            _row("mal:a16498", title_ja="進撃の巨人", year=2013, fmt="TV"),
            _row("anilist:2", title_ja="鬼滅の刃", year=2019, fmt="TV"),
            _row("mal:a38000", title_ja="鬼滅の刃", year=2019, fmt="TV"),
        ]
        clusters = build_cross_source_anime_clusters(rows, bronze_root=None)
        total = sum(len(v) for v in clusters.values())
        assert total == len(rows)

    def test_three_independent_groups_same_title_year_none(self):
        """Three independent UF groups with same title and year=None must NOT collide."""
        rows = [
            _row("src_a:001", title_ja="タイトルなし系列", year=None, fmt="TV"),
            _row("src_b:002", title_ja="タイトルなし系列", year=None, fmt="TV"),
            _row("src_c:003", title_ja="タイトルなし系列", year=None, fmt=None),
        ]
        clusters = build_cross_source_anime_clusters(rows, bronze_root=None)

        # year=None rows skip title+year fallback → 3 separate singleton clusters
        assert len(clusters) == 3, (
            f"year=None rows must stay separate (3 singletons), "
            f"got {len(clusters)} clusters"
        )
        total = sum(len(v) for v in clusters.values())
        assert total == 3

    def test_all_rows_present_large_mixed(self):
        """50-row mixed input: all rows must appear in clusters."""
        rows = []
        for i in range(10):
            rows.append(
                _row(f"src_x:{i}", title_ja=f"作品{i}", year=2020 + i, fmt="TV")
            )
            rows.append(
                _row(f"src_y:{i}", title_ja=f"作品{i}", year=2020 + i, fmt="TV")
            )
            rows.append(_row(f"src_z:{i}", title_ja=f"未知{i}", year=None, fmt=None))
            rows.append(_row(f"src_w:{i}", title_ja="", year=None, fmt=None))
            rows.append(
                _row(f"src_v:{i}", title_ja=f"映画{i}", year=2010 + i, fmt="MOVIE")
            )

        clusters = build_cross_source_anime_clusters(rows, bronze_root=None)
        total = sum(len(v) for v in clusters.values())
        assert total == len(rows), (
            f"Row count mismatch: input={len(rows)}, sum of clusters={total}"
        )

    def test_no_duplicate_rows_across_clusters(self):
        """Each row ID must appear in at most one cluster."""
        rows = [
            _row("a:1", title_ja="A", year=2020, fmt="TV"),
            _row("b:1", title_ja="A", year=2020, fmt="TV"),
            _row("a:2", title_ja="B", year=2021, fmt="MOVIE"),
            _row("b:2", title_ja="B", year=2021, fmt="MOVIE"),
            _row("a:3", title_ja="C", year=None, fmt=None),
            _row("b:3", title_ja="C", year=None, fmt=None),
        ]
        clusters = build_cross_source_anime_clusters(rows, bronze_root=None)
        seen_ids: set[str] = set()
        for members in clusters.values():
            for r in members:
                assert r["id"] not in seen_ids, f"Duplicate: {r['id']}"
                seen_ids.add(r["id"])


# ---------------------------------------------------------------------------
# Case 3: Idempotency — row order must not affect canonical_id
# ---------------------------------------------------------------------------


class TestIdempotency:
    """canonical_id must be stable regardless of input order."""

    def test_same_canonical_id_repeated_call(self):
        rows = [
            _row("anilist:100", title_ja="α作品", year=2000, fmt="TV"),
            _row("mal:a100", title_ja="α作品", year=2000, fmt="TV"),
            _row("bgm:200", title_ja="α作品", year=2000, fmt="TV"),
        ]
        c1 = build_cross_source_anime_clusters(rows, bronze_root=None)
        c2 = build_cross_source_anime_clusters(rows, bronze_root=None)
        assert set(c1.keys()) == set(c2.keys())

    def test_shuffle_preserves_canonical_id(self):
        rows = [
            _row("anilist:200", title_ja="β作品", year=2010, fmt="TV"),
            _row("mal:a200", title_ja="β作品", year=2010, fmt="TV"),
            _row("keyframe:abc123", title_ja="β作品", year=2010, fmt="TV"),
            _row("bgm:300", title_ja="β作品", year=2010, fmt="TV"),
        ]
        c_original = build_cross_source_anime_clusters(list(rows), bronze_root=None)

        rng = random.Random(42)
        for _ in range(5):
            shuffled = list(rows)
            rng.shuffle(shuffled)
            c_shuffled = build_cross_source_anime_clusters(shuffled, bronze_root=None)
            assert set(c_original.keys()) == set(c_shuffled.keys()), (
                "canonical_id changed after shuffle — sorted() not working"
            )

    def test_compute_canonical_id_deterministic(self):
        """Unit-test _compute_canonical_id directly."""
        rows_a = [
            {"id": "anilist:1"},
            {"id": "mal:a1"},
        ]
        rows_b = [
            {"id": "mal:a1"},
            {"id": "anilist:1"},
        ]
        assert _compute_canonical_id(rows_a, None) == _compute_canonical_id(
            rows_b, None
        )

    def test_compute_canonical_id_format_suffix_changes_id(self):
        """format_suffix must change the canonical_id to prevent collisions between
        sub-clusters of the same UF group split by format."""
        rows = [{"id": "anilist:999"}, {"id": "mal:a999"}]
        id_no_suffix = _compute_canonical_id(rows, None)
        id_with_tv = _compute_canonical_id(rows, "TV")
        id_with_movie = _compute_canonical_id(rows, "MOVIE")
        assert id_no_suffix != id_with_tv
        assert id_with_tv != id_with_movie
        assert id_no_suffix != id_with_movie

    def test_compute_canonical_id_expected_hash(self):
        """Verify the exact hash for a known input to guard against regressions."""
        # Single row — no format suffix
        rows = [{"id": "bgm:s13605"}]
        key = "bgm:s13605"  # single element, no unit separator
        expected_digest = hashlib.sha256(key.encode()).hexdigest()[:12]
        expected = f"resolved:anime:{expected_digest}"
        assert _compute_canonical_id(rows, None) == expected

    def test_different_member_sets_different_canonical_id(self):
        """Distinct member sets must never share a canonical_id."""
        set_a = [{"id": "src_a:001"}, {"id": "src_b:001"}]
        set_b = [{"id": "src_a:001"}, {"id": "src_b:002"}]  # different second ID
        set_c = [{"id": "src_c:999"}]  # completely different
        ids = {
            _compute_canonical_id(set_a, None),
            _compute_canonical_id(set_b, None),
            _compute_canonical_id(set_c, None),
        }
        assert len(ids) == 3, f"canonical_id collision detected: {ids}"
