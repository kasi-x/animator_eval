"""Tests for madb M-row → C-anchor cluster logic in build_cross_source_anime_clusters.

Verifies:
  1. M-rows with parent_madb_id starting with 'C' are grouped under a madb:C anchor.
  2. M-rows without parent_madb_id form independent singleton clusters.
  3. No cluster exceeds source_count=100 when M-rows are correctly anchored.
  4. _build_episode_rows extracts M-row metadata correctly.
"""
from __future__ import annotations

from typing import Any

from src.etl.resolved._cross_source_ids import build_cross_source_anime_clusters
from src.etl.resolved.resolve_anime import _build_episode_rows


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _m_row(madb_id: str, parent_id: str = "", title: str = "episode", year: int | None = 2020) -> dict[str, Any]:
    """Build a minimal conformed anime row for a madb M-item."""
    return {
        "id": f"madb:{madb_id}",
        "title_ja": title,
        "title_en": "",
        "title_zh": "",
        "year": year,
        "season": None,
        "quarter": None,
        "episodes": None,
        "format": "TV",
        "duration": None,
        "start_date": None,
        "end_date": None,
        "status": None,
        "source_mat": None,
        "work_type": None,
        "scale_class": None,
        "country_of_origin": None,
        "mal_id_int": None,
        "parent_madb_id": parent_id,
        "record_type": "AnimationTVProgram",
    }


def _other_row(source_id: str, title: str = "other anime", year: int | None = 2020) -> dict[str, Any]:
    """Build a minimal conformed anime row for a non-madb source."""
    return {
        "id": source_id,
        "title_ja": title,
        "title_en": "",
        "title_zh": "",
        "year": year,
        "season": None,
        "quarter": None,
        "episodes": None,
        "format": "TV",
        "duration": None,
        "start_date": None,
        "end_date": None,
        "status": None,
        "source_mat": None,
        "work_type": None,
        "scale_class": None,
        "country_of_origin": None,
        "mal_id_int": None,
        "parent_madb_id": None,
        "record_type": None,
    }


# ---------------------------------------------------------------------------
# Tests: M-row to C-anchor clustering
# ---------------------------------------------------------------------------


class TestMadbMRowClustering:
    def test_m_rows_with_same_parent_form_one_cluster(self):
        """Multiple M-rows sharing a parent_madb_id should end up in one cluster."""
        rows = [
            _m_row("M100", parent_id="C7207", title="ep1"),
            _m_row("M101", parent_id="C7207", title="ep2"),
            _m_row("M102", parent_id="C7207", title="ep3"),
        ]
        clusters = build_cross_source_anime_clusters(rows)
        # All 3 M-rows plus the anchor node form one cluster
        sizes = [len(v) for v in clusters.values()]
        assert max(sizes) >= 3, "All M-rows with same parent should be in one cluster"
        # The anchor cluster should contain all M-rows
        anchor_clusters = [
            v for v in clusters.values()
            if any(r["id"] in {"madb:M100", "madb:M101", "madb:M102"} for r in v)
        ]
        assert len(anchor_clusters) == 1, "All same-parent M-rows should share one cluster"
        assert len(anchor_clusters[0]) == 3

    def test_m_rows_with_different_parents_form_separate_clusters(self):
        """M-rows pointing to different C-series should not be merged."""
        rows = [
            _m_row("M200", parent_id="C1001", title="series A ep1"),
            _m_row("M201", parent_id="C1001", title="series A ep2"),
            _m_row("M300", parent_id="C2002", title="series B ep1"),
            _m_row("M301", parent_id="C2002", title="series B ep2"),
        ]
        clusters = build_cross_source_anime_clusters(rows)
        # Find clusters containing A-rows and B-rows
        a_cluster = next(
            (v for v in clusters.values() if any(r["id"] in {"madb:M200", "madb:M201"} for r in v)),
            None,
        )
        b_cluster = next(
            (v for v in clusters.values() if any(r["id"] in {"madb:M300", "madb:M301"} for r in v)),
            None,
        )
        assert a_cluster is not None
        assert b_cluster is not None
        # They should be different clusters
        a_ids = {r["id"] for r in a_cluster}
        b_ids = {r["id"] for r in b_cluster}
        assert a_ids.isdisjoint(b_ids), "Different-parent M-rows must not share a cluster"

    def test_orphan_m_row_forms_singleton(self):
        """M-rows without parent_madb_id form their own singleton cluster."""
        rows = [
            _m_row("M999", parent_id="", title="orphan", year=None),
        ]
        clusters = build_cross_source_anime_clusters(rows)
        # Orphan M-row should be in exactly one cluster of size 1
        m_clusters = [
            v for v in clusters.values()
            if any(r["id"] == "madb:M999" for r in v)
        ]
        assert len(m_clusters) == 1
        assert len(m_clusters[0]) == 1

    def test_cluster_size_does_not_exceed_100(self):
        """With correct parent linking, no cluster should exceed 100 members.

        Uses distinct titles per series to avoid title+year fallback conflating
        the clusters.  Each series gets 30 M-rows → 10 clusters of 30 rows each.
        """
        rows = []
        for series_idx in range(10):
            parent_id = f"C{10000 + series_idx}"
            series_title = f"シリーズ{series_idx:02d}"
            for ep_idx in range(30):
                rows.append(
                    _m_row(
                        f"M{series_idx * 100 + ep_idx}",
                        parent_id=parent_id,
                        title=f"{series_title} 第{ep_idx + 1}話",
                        year=2020 + series_idx,  # distinct year per series
                    )
                )
        clusters = build_cross_source_anime_clusters(rows)
        max_size = max(len(v) for v in clusters.values())
        assert max_size <= 100, (
            f"Largest cluster has {max_size} rows — expected ≤ 100 after parent linking"
        )

    def test_orphan_m_rows_not_merged_by_title_year(self):
        """M-rows without parent_madb_id must NOT be merged by title+year.

        Even if multiple M-rows share the same title and year, each should
        form its own singleton cluster (they are individual episodes).
        """
        rows = [
            _m_row("M1000", parent_id="", title="あおきいろ", year=2024),
            _m_row("M1001", parent_id="", title="あおきいろ", year=2024),
            _m_row("M1002", parent_id="", title="あおきいろ", year=2024),
        ]
        clusters = build_cross_source_anime_clusters(rows)
        # Each orphan M-row should be in its own singleton cluster
        cluster_sizes = [len(v) for v in clusters.values()]
        assert max(cluster_sizes) == 1, (
            "Orphan M-rows with same title+year must not be merged by title+year fallback"
        )

    def test_non_madb_rows_not_affected(self):
        """Non-madb source rows are unaffected by M→C linking logic."""
        rows = [
            _other_row("anilist:12345", title="My Anime", year=2020),
            _other_row("mal:12345", title="My Anime", year=2020),
        ]
        clusters = build_cross_source_anime_clusters(rows)
        # Two rows with same title+year should be merged via title+year fallback
        all_row_ids = set()
        for cluster_rows in clusters.values():
            for r in cluster_rows:
                all_row_ids.add(r["id"])
        assert "anilist:12345" in all_row_ids
        assert "mal:12345" in all_row_ids


# ---------------------------------------------------------------------------
# Tests: _build_episode_rows
# ---------------------------------------------------------------------------


class TestBuildEpisodeRows:
    def test_m_rows_become_episodes(self):
        """Each M-row in a cluster becomes one episode record."""
        clusters = {
            "resolved:anime:aabbcc001234": [
                _m_row("M100", parent_id="C7207", title="ep1", year=1969),
                _m_row("M101", parent_id="C7207", title="ep2", year=1969),
            ]
        }
        episodes = _build_episode_rows(clusters)
        assert len(episodes) == 2
        ep_ids = {e["episode_id"] for e in episodes}
        assert ep_ids == {"madb:M100", "madb:M101"}

    def test_episode_has_correct_parent_anime_id(self):
        """episode.parent_anime_id must equal the cluster's canonical_id."""
        canonical_id = "resolved:anime:aabbcc001234"
        clusters = {
            canonical_id: [
                _m_row("M200", parent_id="C1001"),
            ]
        }
        episodes = _build_episode_rows(clusters)
        assert len(episodes) == 1
        assert episodes[0]["parent_anime_id"] == canonical_id

    def test_non_m_rows_excluded(self):
        """Non-madb or C-prefix rows are excluded from episodes output."""
        clusters = {
            "resolved:anime:aabbcc001234": [
                _other_row("anilist:999", title="not an episode"),
                _m_row("M300", parent_id="C7207"),
            ]
        }
        episodes = _build_episode_rows(clusters)
        assert len(episodes) == 1
        assert episodes[0]["episode_id"] == "madb:M300"

    def test_episode_fields_present(self):
        """Each episode dict must have all required fields."""
        clusters = {
            "resolved:anime:abc123": [
                _m_row("M400", parent_id="C1001", title="テスト話", year=2020),
            ]
        }
        episodes = _build_episode_rows(clusters)
        assert len(episodes) == 1
        ep = episodes[0]
        assert ep["episode_id"] == "madb:M400"
        assert ep["parent_anime_id"] == "resolved:anime:abc123"
        assert ep["title_ja"] == "テスト話"
        assert ep["year"] == 2020
        assert ep["record_type"] == "AnimationTVProgram"

    def test_no_duplicate_episodes(self):
        """If a row appears in multiple clusters (edge case), dedup applies."""
        # Duplicate M-row in two clusters (shouldn't happen in practice, defensive test)
        row = _m_row("M500", parent_id="C9999", title="dup ep")
        clusters = {
            "resolved:anime:cluster_a": [row],
            "resolved:anime:cluster_b": [row],
        }
        episodes = _build_episode_rows(clusters)
        episode_ids = [e["episode_id"] for e in episodes]
        # At most one entry per episode_id
        assert len(episode_ids) == len(set(episode_ids))
