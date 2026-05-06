"""Cooccurrence Groups report — v2 compliant.

Production team cooccurrence cluster analysis with 14 sections:
- Section 1: Summary statistics
- Section 2: Group listing table (top 100)
- Section 3: Temporal stacked bar (active groups per period by size)
- Section 4: Group size distribution
- Section 5: Shared works distribution by size (raincloud)
- Section 6: Activity span vs shared works scatter
- Section 7: Dual-axis new/cumulative groups per year
- Section 8: Active vs dormant shared works (raincloud)
- Section 9: Role breakdown by shared-works bucket (stacked bar)
- Section 10: First year vs avg IV Score scatter
- Section 11: Collaboration power & uniqueness (K-Means cluster scatter + violin)
- Section 12: Collaboration power TOP 30
- Section 13: Uniqueness TOP 30
- Section 14: ML classification (reason bar, active/dormant stacked, reason violin)
"""

from __future__ import annotations

import math
from collections import Counter, defaultdict
from pathlib import Path

import numpy as np
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

from ..ci_utils import distribution_summary, format_ci, format_distribution_inline
from ..helpers import name_clusters_by_rank, person_link
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_GRP_COLORS = ["#E09BC2", "#E07532", "#FFB444", "#7CC8F2", "#3BC494"]
_SIZE_LABELS = {"3": "3人組", "4": "4人組", "5": "5人組"}
_SIZE_COLORS = {"3": "#E09BC2", "4": "#E07532", "5": "#FFB444"}

_ERA_WEIGHTS: dict[int, float] = {
    1960: 3.0,
    1970: 2.5,
    1980: 2.0,
    1990: 1.5,
    2000: 1.0,
    2010: 0.8,
    2020: 0.65,
}

_REASON_COLORS: dict[str, str] = {
    "レガシー長期確立チーム": "#a3e635",
    "長期確立チーム": "#3BC494",
    "集中型シリーズ": "#E09BC2",
    "新興高品質チーム": "#F8EC6A",
    "現代新興チーム": "#FFB444",
    "レガシーチーム": "#3593D2",
    "シリーズ継続型": "#E07532",
    "高評価チーム": "#EF476F",
    "標準コラボ": "#7CC8F2",
}

_ROLE_LABELS_JA: dict[str, str] = {
    "director": "監督",
    "screenplay": "脚本",
    "character_designer": "キャラデザ",
    "animation_director": "作画監督",
    "background_art": "背景美術",
    "finishing": "仕上げ",
    "sound_director": "音響監督",
    "photography_director": "撮影監督",
    "cgi_director": "CGI監督",
}

# Cooccurrence core-staff roles (must match src/analysis/cooccurrence_groups.py)
_COOCCURRENCE_ROLE_VALUES = frozenset(
    {
        "director",
        "screenplay",
        "character_designer",
        "animation_director",
        "background_art",
        "finishing",
        "sound_director",
        "photography_director",
        "cgi_director",
    }
)

_ACTIVE_THRESHOLD_YEAR = 2022

_PERIODS = [
    ("~1999", None, 1999),
    ("2000-2004", 2000, 2004),
    ("2005-2009", 2005, 2009),
    ("2010-2014", 2010, 2014),
    ("2015-2019", 2015, 2019),
    ("2020-", 2020, None),
]

_ROLE_PALETTE = [
    "#E09BC2",
    "#E07532",
    "#FFB444",
    "#7CC8F2",
    "#3BC494",
    "#F8EC6A",
    "#3593D2",
    "#EF476F",
]


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _era_weight(first_year: int | None) -> float:
    if not first_year:
        return 1.0
    decade = (int(first_year) // 10) * 10
    for d in sorted(_ERA_WEIGHTS, reverse=True):
        if decade >= d:
            return _ERA_WEIGHTS[d]
    return _ERA_WEIGHTS[1960]


def _collab_power(g: dict) -> float:
    comp = g.get("avg_iv_score", 0) or 0
    sw = g.get("shared_works", 0) or 0
    fy = g.get("first_year") or 0
    ly = g.get("last_year") or 0
    span = max(1, ly - fy + 1) if fy and ly else 1
    longevity = 1.0 + span / 30.0
    return float(comp * math.log1p(sw) * longevity)


def _uniqueness(g: dict) -> float:
    sw = g.get("shared_works", 0) or 0
    fy = g.get("first_year") or 0
    ly = g.get("last_year") or 0
    sz = g.get("size", 3) or 3
    span = max(1, ly - fy + 1) if fy and ly else 1
    intensity = sw / span
    size_bonus = 1.0 + (sz - 3) * 0.5
    era_bonus = _era_weight(fy or None)
    return float(intensity * size_bonus * era_bonus)


def _era_sw_threshold(fy: int, base_threshold: int) -> int:
    if fy < 1985:
        return max(3, int(base_threshold * 0.4))
    if fy < 1995:
        return max(3, int(base_threshold * 0.6))
    if fy < 2005:
        return max(3, int(base_threshold * 0.8))
    if fy >= 2015:
        return int(base_threshold * 1.2)
    return base_threshold


def _classify_group(g: dict) -> str:
    sw = g.get("shared_works", 0) or 0
    fy = int(g.get("first_year") or 2000)
    ly = g.get("last_year") or 0
    span = max(1, ly - fy + 1) if ly else 1
    is_active = g.get("is_active", False)
    intensity = sw / span
    comp = g.get("avg_iv_score", 0) or 0

    long_sw = _era_sw_threshold(fy, 15)
    series_sw = _era_sw_threshold(fy, 10)
    quality_threshold = 40

    if sw >= long_sw and span >= 15:
        if fy < 2000:
            return "レガシー長期確立チーム"
        return "長期確立チーム"
    if sw >= series_sw and span < 8:
        return "集中型シリーズ"
    if is_active and fy >= 2015 and comp >= quality_threshold:
        return "新興高品質チーム"
    if is_active and fy >= 2015:
        return "現代新興チーム"
    if not is_active and fy < 2000:
        return "レガシーチーム"
    if intensity >= 2.5:
        return "シリーズ継続型"
    if comp >= 50:
        return "高評価チーム"
    return "標準コラボ"


def _violin_raincloud(
    data: list[float],
    name: str,
    color: str,
) -> go.Box | go.Violin:
    """Return a raincloud-style Plotly trace (box or violin depending on n)."""
    clean = [float(x) for x in data if x is not None and x == x]
    n = len(clean)
    if n < 5:
        return go.Box(
            y=clean,
            name=name,
            marker_color=color,
            showlegend=True,
            boxmean=True,
        )
    if n < 40:
        return go.Box(
            y=clean,
            name=name,
            marker_color=color,
            showlegend=True,
            boxmean=True,
            boxpoints="all",
            jitter=0.35,
            pointpos=0,
        )
    arr = np.array(clean)
    q25, q75 = np.percentile(arr, [25, 75])
    iqr = max(float(q75 - q25), 0.01)
    bw = max(0.9 * min(float(arr.std()), iqr / 1.34) * n ** (-0.2), 0.3)
    return go.Violin(
        y=clean,
        name=name,
        side="positive",
        points="all",
        jitter=0.08,
        pointpos=-0.7,
        box_visible=True,
        meanline_visible=True,
        fillcolor=color,
        line_color=color,
        opacity=0.72,
        bandwidth=bw,
        spanmode="hard",
        showlegend=True,
        marker=dict(size=3, opacity=0.35, color=color),
    )


def _member_display(names: list[str], member_ids: list[str]) -> str:
    """Format member names as links."""
    if member_ids:
        return " / ".join(person_link(n, pid) for n, pid in zip(names, member_ids))
    return " / ".join(names)


# ---------------------------------------------------------------------------
# Report class
# ---------------------------------------------------------------------------


class CooccurrenceGroupsReport(BaseReportGenerator):
    name = "cooccurrence_groups"
    title = "共同制作集団分析"
    subtitle = "コアスタッフの繰り返し共同制作パターン検出・K-Means/ML分類"
    filename = "cooccurrence_groups.html"

    glossary_terms = {
        "コアスタッフ (Core Staff)": (
            "本分析で対象とする9職種: 監督・脚本・キャラクターデザイナー・"
            "作画監督・背景美術・仕上げ・音響監督・撮影監督・CGI監督。"
        ),
        "共同制作集団 (Co-occurrence Group)": (
            "同一コアスタッフが3回以上別々の作品のクレジットに"
            "同時登場する組み合わせ。公式組織とは無関係の非公式固定チーム。"
        ),
        "is_active (現役フラグ)": (
            "グループの最後の共参加作品が2022年以降の場合にTrue。"
        ),
        "collab_power": (
            "avg_iv_score x log1p(shared_works) x (1 + span/30), "
            "log-normalized to 0-99."
        ),
        "uniqueness_score": (
            "(shared_works/span) x size_bonus x era_weight, log-normalized to 0-99."
        ),
    }

    def generate(self) -> Path | None:
        groups = self._load_groups()
        if not groups:
            sb = SectionBuilder()
            section = sb.build_section(
                ReportSection(
                    title="データ不足",
                    findings_html="<p>共同制作集団データがありません。</p>",
                    section_id="no_data",
                )
            )
            return self.write_report(section)

        # Compute derived fields on all groups
        self._enrich_groups(groups)

        sb = SectionBuilder()
        sections: list[str] = []

        # Compute summary
        total_groups = len(groups)
        active_groups = sum(1 for g in groups if g.get("is_active"))
        by_size: dict[str, int] = defaultdict(int)
        for g in groups:
            by_size[str(g.get("size", 3))] += 1

        summary = {
            "total_groups": total_groups,
            "active_groups": active_groups,
            "by_size": dict(by_size),
        }

        # Build temporal_slices
        temporal_slices = self._build_temporal_slices(groups)

        # Section 1: Summary
        sections.append(
            sb.build_section(
                self._build_summary_section(sb, summary, groups),
            )
        )
        # Section 2: Group listing table
        sections.append(
            sb.build_section(
                self._build_group_table_section(sb, groups),
            )
        )
        # Section 3: Temporal stacked bar
        sections.append(
            sb.build_section(
                self._build_temporal_section(sb, groups, temporal_slices),
            )
        )
        # Section 4: Group size distribution
        sections.append(
            sb.build_section(
                self._build_size_distribution_section(sb, by_size),
            )
        )
        # Section 5: Shared works violin by size
        sections.append(
            sb.build_section(
                self._build_shared_works_violin_section(sb, groups),
            )
        )
        # Section 6: Activity span vs shared works scatter
        sections.append(
            sb.build_section(
                self._build_span_scatter_section(sb, groups),
            )
        )
        # Section 7: Dual-axis new/cumulative
        sections.append(
            sb.build_section(
                self._build_dual_axis_section(sb, groups),
            )
        )
        # Section 8: Active vs dormant violin
        sections.append(
            sb.build_section(
                self._build_active_dormant_violin_section(sb, groups),
            )
        )
        # Section 9: Role breakdown
        sections.append(
            sb.build_section(
                self._build_role_breakdown_section(sb, groups),
            )
        )
        # Section 10: First year vs avg IV Score
        sections.append(
            sb.build_section(
                self._build_year_score_scatter_section(sb, groups),
            )
        )
        # Section 11: K-Means clustering
        sections.append(
            sb.build_section(
                self._build_kmeans_section(sb, groups),
            )
        )
        # Section 12: Collab power TOP 30
        sections.append(
            sb.build_section(
                self._build_collab_power_top30_section(sb, groups),
            )
        )
        # Section 13: Uniqueness TOP 30
        sections.append(
            sb.build_section(
                self._build_uniqueness_top30_section(sb, groups),
            )
        )
        # Section 14: ML classification
        sections.append(
            sb.build_section(
                self._build_ml_classification_section(sb, groups),
            )
        )

        return self.write_report("\n".join(sections))

    # ------------------------------------------------------------------ #
    # Data loading
    # ------------------------------------------------------------------ #

    def _load_groups(self) -> list[dict]:
        """Reconstruct cooccurrence groups from DB tables.

        Uses feat_cluster_membership.cooccurrence_group_id to group persons,
        then enriches with credits, anime metadata, scores, and person names.
        """
        try:
            # Get persons with cooccurrence_group_id
            membership_rows = self.conn.execute("""
                SELECT person_id, cooccurrence_group_id
                FROM feat_cluster_membership
                WHERE cooccurrence_group_id IS NOT NULL
            """).fetchall()
        except Exception:
            membership_rows = []

        if not membership_rows:
            return []

        # Build group_id -> [person_ids]
        gid_to_pids: dict[int, list[str]] = defaultdict(list)
        for row in membership_rows:
            gid_to_pids[row["cooccurrence_group_id"]].append(row["person_id"])

        # Get all relevant person_ids
        all_pids = {row["person_id"] for row in membership_rows}

        # Fetch person names
        pid_name_map: dict[str, str] = {}
        try:
            for pid in all_pids:
                prow = self.conn.execute(
                    "SELECT name_ja, name_zh, name_en FROM conformed.persons WHERE id = ?",
                    (pid,),
                ).fetchone()
                if prow:
                    pid_name_map[pid] = prow["name_ja"] or prow["name_zh"] or prow["name_en"] or pid
        except Exception:
            pass

        # Fetch iv_scores
        iv_scores: dict[str, float] = {}
        try:
            for pid in all_pids:
                srow = self.conn.execute(
                    "SELECT iv_score FROM scores WHERE person_id = ?",
                    (pid,),
                ).fetchone()
                if srow:
                    iv_scores[pid] = float(srow["iv_score"])
        except Exception:
            pass

        # Fetch credits for core roles
        placeholders = ",".join("?" for _ in _COOCCURRENCE_ROLE_VALUES)
        pid_anime: dict[str, set[str]] = defaultdict(set)
        pid_roles: dict[str, set[str]] = defaultdict(set)
        anime_years_map: dict[str, int | None] = {}

        try:
            for pid in all_pids:
                crows = self.conn.execute(
                    f"""SELECT anime_id, role FROM conformed.credits
                        WHERE person_id = ? AND role IN ({placeholders})""",
                    (pid, *_COOCCURRENCE_ROLE_VALUES),
                ).fetchall()
                for cr in crows:
                    pid_anime[pid].add(cr["anime_id"])
                    pid_roles[pid].add(cr["role"])
        except Exception:
            pass

        # Fetch anime years (batch)
        all_anime_ids = set()
        for aids in pid_anime.values():
            all_anime_ids.update(aids)
        try:
            for aid in all_anime_ids:
                arow = self.conn.execute(
                    "SELECT year FROM conformed.anime WHERE id = ?",
                    (aid,),
                ).fetchone()
                if arow:
                    anime_years_map[aid] = arow["year"]
        except Exception:
            pass

        # Build groups
        groups: list[dict] = []
        for gid in sorted(gid_to_pids):
            members = sorted(gid_to_pids[gid])
            if len(members) < 2:
                continue

            member_names = [pid_name_map.get(pid, pid) for pid in members]

            # shared anime = intersection of all members' anime sets
            member_anime_sets = [pid_anime.get(pid, set()) for pid in members]
            if member_anime_sets:
                shared_anime = set.intersection(*member_anime_sets)
            else:
                shared_anime = set()

            # Collect roles per member across shared anime
            roles_per_member: dict[str, list[str]] = {}
            for pid in members:
                member_role_set: set[str] = set()
                for aid in shared_anime:
                    # Get roles for this person on this anime
                    member_role_set.update(pid_roles.get(pid, set()))
                roles_per_member[pid] = sorted(member_role_set)

            # Activity period from shared anime years
            years = [
                anime_years_map[aid]
                for aid in shared_anime
                if aid in anime_years_map and anime_years_map[aid] is not None
            ]
            first_year = min(years) if years else None
            last_year = max(years) if years else None
            is_active = last_year is not None and last_year >= _ACTIVE_THRESHOLD_YEAR

            # Average IV score
            scores_list = [iv_scores[pid] for pid in members if pid in iv_scores]
            avg_iv = (
                round(sum(scores_list) / len(scores_list), 1) if scores_list else 0.0
            )

            groups.append(
                {
                    "members": members,
                    "member_names": member_names,
                    "size": len(members),
                    "shared_works": len(shared_anime),
                    "shared_anime": sorted(shared_anime),
                    "roles": roles_per_member,
                    "first_year": first_year,
                    "last_year": last_year,
                    "is_active": is_active,
                    "avg_iv_score": avg_iv,
                }
            )

        # Sort by shared_works descending
        groups.sort(key=lambda g: (-g["shared_works"], -g["size"]))
        return groups

    def _enrich_groups(self, groups: list[dict]) -> None:
        """Compute collab_power, uniqueness, and grouping_reason for all groups."""
        raw_powers = [_collab_power(g) for g in groups]
        raw_uniques = [_uniqueness(g) for g in groups]
        max_power = max(raw_powers) if raw_powers else 1.0
        max_unique = max(raw_uniques) if raw_uniques else 1.0
        max_power = max_power or 1.0
        max_unique = max_unique or 1.0

        for i, g in enumerate(groups):
            g["collab_power"] = round(raw_powers[i] / max_power * 99, 1)
            g["uniqueness_score"] = round(raw_uniques[i] / max_unique * 99, 1)
            g["grouping_reason"] = _classify_group(g)

    def _build_temporal_slices(self, groups: list[dict]) -> list[dict]:
        """Build temporal slices for 5-year periods."""
        slices = []
        for label, year_from, year_to in _PERIODS:
            count = 0
            for g in groups:
                fy = g.get("first_year")
                ly = g.get("last_year")
                if fy is None or ly is None:
                    continue
                p_start = year_from if year_from is not None else 0
                p_end = year_to if year_to is not None else 9999
                if fy <= p_end and ly >= p_start:
                    count += 1
            slices.append({"period": label, "active_group_count": count})
        return slices

    # ------------------------------------------------------------------ #
    # Section builders
    # ------------------------------------------------------------------ #

    def _build_summary_section(
        self,
        sb: SectionBuilder,
        summary: dict,
        groups: list[dict],
    ) -> ReportSection:
        total = summary["total_groups"]
        active = summary["active_groups"]
        by_size = summary["by_size"]
        active_pct = 100 * active / max(total, 1)

        sw_vals = [g.get("shared_works", 0) for g in groups]
        sw_summ = distribution_summary(sw_vals, label="shared_works")

        findings = (
            f"<p>{total:,}件の共同制作集団を検出。"
            f"うち{active:,}件（{active_pct:.1f}%）が"
            f"{_ACTIVE_THRESHOLD_YEAR}年以降に活動実績あり。"
            f"サイズ別内訳: "
            + ", ".join(
                f"{_SIZE_LABELS.get(k, k + '人組')}={v:,}"
                for k, v in sorted(by_size.items())
            )
            + f"。</p>"
            f"<p>グループあたりの共参加作品数: {format_distribution_inline(sw_summ)}、"
            f"{format_ci((sw_summ['ci_lower'], sw_summ['ci_upper']))}。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="共同制作集団サマリー",
            findings_html=findings,
            method_note=(
                "グループ検出はアニメクレジットにおけるコアスタッフ役職の"
                "全k組合せ（k=3..5）を列挙し、共有作品数 >= 3 でフィルタリングして行う。"
                "is_active = 直近の共有作品年が 2022年以降。"
            ),
            section_id="cooc_summary",
        )

    def _build_group_table_section(
        self,
        sb: SectionBuilder,
        groups: list[dict],
    ) -> ReportSection:
        n_shown = min(100, len(groups))
        findings = (
            f"<p>shared_works数上位{n_shown}件のグループ。"
            f"全グループ数: {len(groups):,}。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        # Build HTML table
        rows_html: list[str] = []
        rows_html.append("<table><thead><tr>")
        rows_html.append(
            "<th>#</th><th>メンバー</th><th>役割</th>"
            "<th>共参加作品数</th><th>活動期間</th><th>現役</th>"
        )
        rows_html.append("</tr></thead><tbody>")

        for i, g in enumerate(groups[:100], 1):
            member_ids = g.get("members", [])
            names = g.get("member_names") or member_ids
            name_str = _member_display(names, member_ids)

            all_roles: list[str] = []
            for role_list in g.get("roles", {}).values():
                all_roles.extend(role_list)
            role_str = ", ".join(sorted(set(all_roles)))

            fy = g.get("first_year", "?")
            ly = g.get("last_year", "?")
            period = f"{fy}-{ly}" if fy and ly else "?"

            active_badge = (
                '<span style="color:#06D6A0;">現役</span>'
                if g.get("is_active")
                else '<span style="color:#EF476F;">休眠</span>'
            )
            rows_html.append(
                f"<tr><td>{i}</td>"
                f"<td>{name_str}</td>"
                f"<td style='font-size:0.8rem;color:#a0a0c0'>{role_str}</td>"
                f"<td>{g.get('shared_works', 0)}</td>"
                f"<td>{period}</td>"
                f"<td>{active_badge}</td></tr>"
            )
        rows_html.append("</tbody></table>")

        return ReportSection(
            title="グループ一覧 (上位100件)",
            findings_html=findings,
            visualization_html="\n".join(rows_html),
            method_note=(
                "コアスタッフ（9役職）で共有作品数 >= 3。"
                "shared_works 降順で並び替え。"
                "メンバー名は人物詳細ページへのリンク。"
            ),
            section_id="cooc_table",
        )

    def _build_temporal_section(
        self,
        sb: SectionBuilder,
        groups: list[dict],
        temporal_slices: list[dict],
    ) -> ReportSection:
        if not temporal_slices:
            return ReportSection(
                title="期間別アクティブグループ数",
                findings_html="<p>期間別データがありません。</p>",
                section_id="cooc_temporal",
            )

        periods = [ts["period"] for ts in temporal_slices]

        # Build per-size counts for each period
        period_size_counts: dict[str, dict[str, int]] = {
            p: {"3": 0, "4": 0, "5": 0} for p in periods
        }
        for g in groups:
            sz = str(g.get("size", 3))
            fy = g.get("first_year") or 9999
            ly = g.get("last_year") or 0
            for label, year_from, year_to in _PERIODS:
                p_start = year_from if year_from is not None else 0
                p_end = year_to if year_to is not None else 9999
                if (
                    fy <= p_end
                    and ly >= p_start
                    and sz in period_size_counts.get(label, {})
                ):
                    period_size_counts[label][sz] += 1

        total_active_counts = [ts["active_group_count"] for ts in temporal_slices]
        counts_summ = distribution_summary(
            total_active_counts, label="active_per_period"
        )

        findings = (
            f"<p>{len(periods)}期間にわたるアクティブグループ数: "
            f"{format_distribution_inline(counts_summ)}、"
            f"{format_ci((counts_summ['ci_lower'], counts_summ['ci_upper']))}。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        fig = go.Figure()
        for sz in ["3", "4", "5"]:
            fig.add_trace(
                go.Bar(
                    x=periods,
                    y=[period_size_counts[p][sz] for p in periods],
                    name=_SIZE_LABELS[sz],
                    marker_color=_SIZE_COLORS[sz],
                    hovertemplate="%{x}: %{y} groups<extra></extra>",
                )
            )
        fig.update_layout(
            barmode="stack",
            title="期間別アクティブグループ数（サイズ別内訳）",
            xaxis_title="期間",
            yaxis_title="グループ数",
        )

        return ReportSection(
            title="期間別アクティブグループ数（サイズ別内訳）",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_cooc_temporal", height=400),
            method_note=(
                "5-year periods. A group is 'active in period' if its "
                "[first_year, last_year] range overlaps the period."
            ),
            section_id="cooc_temporal",
        )

    def _build_size_distribution_section(
        self,
        sb: SectionBuilder,
        by_size: dict[str, int],
    ) -> ReportSection:
        size_keys = sorted(by_size.keys())
        size_vals = [by_size[k] for k in size_keys]

        if not size_keys:
            return ReportSection(
                title="グループサイズ別分布",
                findings_html="<p>サイズ分布データがありません。</p>",
                section_id="cooc_size_dist",
            )

        summ = distribution_summary(size_vals, label="groups_per_size")

        findings = (
            "<p>サイズ別グループ数: "
            + ", ".join(f"{k}人組 = {by_size[k]:,}" for k in size_keys)
            + f"。サイズ別分布: {format_distribution_inline(summ)}、"
            f"{format_ci((summ['ci_lower'], summ['ci_upper']))}。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        colors = [_SIZE_COLORS.get(k, "#7CC8F2") for k in size_keys]
        fig = go.Figure(
            go.Bar(
                x=[f"{k}人組" for k in size_keys],
                y=size_vals,
                marker_color=colors,
                hovertemplate="%{x}: %{y} groups<extra></extra>",
            )
        )
        fig.update_layout(
            title="グループサイズ別分布",
            xaxis_title="グループサイズ",
            yaxis_title="グループ数",
        )

        return ReportSection(
            title="グループサイズ別分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_cooc_size", height=350),
            method_note="メンバー数別（3、4、5）のグループ数。",
            section_id="cooc_size_dist",
        )

    def _build_shared_works_violin_section(
        self,
        sb: SectionBuilder,
        groups: list[dict],
    ) -> ReportSection:
        violin_data: dict[str, list[int]] = {}
        for g in groups:
            sz = str(g.get("size", 3))
            sw = g.get("shared_works", 0)
            violin_data.setdefault(sz, []).append(sw)

        if not violin_data:
            return ReportSection(
                title="グループサイズ別 共参加作品数分布",
                findings_html="<p>共参加作品数分布データがありません。</p>",
                section_id="cooc_violin_size",
            )

        all_sw = [g.get("shared_works", 0) for g in groups]
        summ = distribution_summary(all_sw, label="shared_works")

        findings_parts = [
            f"<p>共参加作品数の分布（全サイズ、n={summ['n']:,}）: "
            f"{format_distribution_inline(summ)}、"
            f"{format_ci((summ['ci_lower'], summ['ci_upper']))}。</p>"
            "<p>サイズ別:</p><ul>"
        ]
        for sz in sorted(violin_data):
            sz_summ = distribution_summary(violin_data[sz], label=f"size_{sz}")
            findings_parts.append(
                f"<li>{sz}人組（n={sz_summ['n']:,}）: "
                f"{format_distribution_inline(sz_summ)}</li>"
            )
        findings_parts.append("</ul>")
        findings = "\n".join(findings_parts)

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        fig = go.Figure()
        for sz in sorted(violin_data):
            fig.add_trace(
                _violin_raincloud(
                    [float(v) for v in violin_data[sz]],
                    f"{sz}人組",
                    _SIZE_COLORS.get(sz, "#7CC8F2"),
                )
            )
        fig.update_layout(
            title="グループサイズ別 共参加作品数分布 (Raincloud)",
            yaxis_title="共参加作品数",
            xaxis_title="グループサイズ",
            violinmode="overlay",
        )

        return ReportSection(
            title="グループサイズ別 共参加作品数分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_cooc_violin_size", height=450
            ),
            method_note=(
                "Raincloud plot: n < 5 = box, n < 40 = box + jitter, "
                "n >= 40 = half-violin + box + strip. IQR-based bandwidth."
            ),
            section_id="cooc_violin_size",
        )

    def _build_span_scatter_section(
        self,
        sb: SectionBuilder,
        groups: list[dict],
    ) -> ReportSection:
        scatter_groups = [
            g for g in groups if g.get("first_year") and g.get("last_year")
        ]
        if not scatter_groups:
            return ReportSection(
                title="活動期間 vs 共参加作品数",
                findings_html="<p>活動期間散布図のデータがありません。</p>",
                section_id="cooc_span_scatter",
            )

        spans = [g["last_year"] - g["first_year"] for g in scatter_groups]
        shared = [g.get("shared_works", 0) for g in scatter_groups]
        spans_summ = distribution_summary(spans, label="activity_span")
        shared_summ = distribution_summary(shared, label="shared_works")

        findings = (
            f"<p>有効な年範囲を持つ{len(scatter_groups):,}グループ。"
            f"活動期間: {format_distribution_inline(spans_summ)}、"
            f"{format_ci((spans_summ['ci_lower'], spans_summ['ci_upper']))}。"
            f"共参加作品数: {format_distribution_inline(shared_summ)}、"
            f"{format_ci((shared_summ['ci_lower'], shared_summ['ci_upper']))}。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        avg_scores = [g.get("avg_iv_score", 0) for g in scatter_groups]
        sizes_s = [g.get("size", 3) for g in scatter_groups]
        marker_sizes = [s * 4 for s in sizes_s]

        fig = go.Figure(
            go.Scatter(
                x=spans,
                y=shared,
                mode="markers",
                marker=dict(
                    size=marker_sizes,
                    color=avg_scores,
                    colorscale="Viridis",
                    showscale=True,
                    colorbar=dict(title="平均IV"),
                    opacity=0.7,
                ),
                text=[f"規模={s}" for s in sizes_s],
                hovertemplate=(
                    "期間: %{x}年<br>"
                    "共参加作品数: %{y}<br>"
                    "%{text}<br>"
                    "平均IV: %{marker.color:.1f}<extra></extra>"
                ),
            )
        )
        fig.update_layout(
            title="活動期間 vs 共参加作品数（平均IV色・サイズ別マーカー）",
            xaxis_title="活動期間（年）",
            yaxis_title="共参加作品数",
        )

        return ReportSection(
            title="活動期間 vs 共参加作品数",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_cooc_scatter", height=500),
            method_note=(
                "Each dot = one group. X = last_year - first_year, "
                "Y = shared_works, color = avg IV score, "
                "marker size = group member count."
            ),
            section_id="cooc_span_scatter",
        )

    def _build_dual_axis_section(
        self,
        sb: SectionBuilder,
        groups: list[dict],
    ) -> ReportSection:
        first_year_counts: dict[int, int] = {}
        for g in groups:
            fy = g.get("first_year")
            if fy:
                first_year_counts[fy] = first_year_counts.get(fy, 0) + 1

        if not first_year_counts:
            return ReportSection(
                title="年別グループ形成数（デュアル軸）",
                findings_html="<p>初出年データがありません。</p>",
                section_id="cooc_dual_axis",
            )

        sorted_years = sorted(first_year_counts)
        new_per_year = [first_year_counts[yr] for yr in sorted_years]
        cumulative: list[int] = []
        total = 0
        for n in new_per_year:
            total += n
            cumulative.append(total)

        new_summ = distribution_summary(
            [float(v) for v in new_per_year],
            label="new_groups_per_year",
        )

        findings = (
            f"<p>グループ形成期間: {sorted_years[0]}–{sorted_years[-1]}年。"
            f"年間新規グループ数: {format_distribution_inline(new_summ)}、"
            f"{format_ci((new_summ['ci_lower'], new_summ['ci_upper']))}。"
            f"累計: {cumulative[-1]:,}。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(
            go.Bar(
                x=sorted_years,
                y=new_per_year,
                name="年別新規グループ数",
                marker_color="rgba(240,147,251,0.6)",
                hovertemplate="%{x}: %{y} groups<extra></extra>",
            ),
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(
                x=sorted_years,
                y=cumulative,
                name="累積グループ数",
                line=dict(color="#3BC494", width=2),
                mode="lines",
                hovertemplate="%{x} cumulative: %{y}<extra></extra>",
            ),
            secondary_y=True,
        )
        fig.update_yaxes(title_text="新規グループ数", secondary_y=False)
        fig.update_yaxes(title_text="累積グループ数", secondary_y=True)
        fig.update_layout(
            title="年別新規グループ形成数（棒）+ 累積（折れ線）",
            xaxis_title="年",
        )

        return ReportSection(
            title="年別グループ形成数（デュアル軸）",
            findings_html=findings,
            visualization_html=plotly_div_safe(fig, "chart_cooc_dual_axis", height=450),
            method_note=(
                "Bar (left axis) = new groups formed that year "
                "(first_year of first shared work). "
                "Line (right axis) = running cumulative total."
            ),
            section_id="cooc_dual_axis",
        )

    def _build_active_dormant_violin_section(
        self,
        sb: SectionBuilder,
        groups: list[dict],
    ) -> ReportSection:
        active_sw = [g.get("shared_works", 0) for g in groups if g.get("is_active")]
        inactive_sw = [
            g.get("shared_works", 0) for g in groups if not g.get("is_active")
        ]

        if not active_sw and not inactive_sw:
            return ReportSection(
                title="現役 vs 休眠グループの共参加作品数分布",
                findings_html="<p>現役/休眠データがありません。</p>",
                section_id="cooc_active_dormant",
            )

        findings_parts = ["<p>ステータス別の共参加作品数分布:</p><ul>"]
        if active_sw:
            a_summ = distribution_summary(
                [float(v) for v in active_sw],
                label="active",
            )
            findings_parts.append(
                f"<li>現役（n={a_summ['n']:,}）: "
                f"{format_distribution_inline(a_summ)}、"
                f"{format_ci((a_summ['ci_lower'], a_summ['ci_upper']))}</li>"
            )
        if inactive_sw:
            i_summ = distribution_summary(
                [float(v) for v in inactive_sw],
                label="dormant",
            )
            findings_parts.append(
                f"<li>休眠（n={i_summ['n']:,}）: "
                f"{format_distribution_inline(i_summ)}、"
                f"{format_ci((i_summ['ci_lower'], i_summ['ci_upper']))}</li>"
            )
        findings_parts.append("</ul>")
        findings = "\n".join(findings_parts)

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        fig = go.Figure()
        if active_sw:
            fig.add_trace(
                _violin_raincloud(
                    [float(v) for v in active_sw],
                    "現役",
                    "#3BC494",
                )
            )
        if inactive_sw:
            fig.add_trace(
                _violin_raincloud(
                    [float(v) for v in inactive_sw],
                    "休眠",
                    "#EF476F",
                )
            )
        fig.update_layout(
            title="現役 vs 休眠グループ — 共参加作品数分布 (Raincloud)",
            yaxis_title="共参加作品数",
            violinmode="overlay",
        )

        return ReportSection(
            title="現役 vs 休眠グループの共参加作品数分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig,
                "chart_cooc_active_dormant",
                height=450,
            ),
            method_note=(
                "Active = last shared-work year >= 2022. "
                "Dormant = last shared-work year < 2022."
            ),
            section_id="cooc_active_dormant",
        )

    def _build_role_breakdown_section(
        self,
        sb: SectionBuilder,
        groups: list[dict],
    ) -> ReportSection:
        role_counts: Counter = Counter()
        for g in groups[:200]:
            for role_list in g.get("roles", {}).values():
                for r in role_list:
                    role_counts[r] += 1

        if not role_counts:
            return ReportSection(
                title="共参加作品数帯別 x 役割内訳",
                findings_html="<p>役割データがありません。</p>",
                section_id="cooc_role_breakdown",
            )

        bucket_labels = ["3-4作", "5-7作", "8-11作", "12-19作", "20作+"]
        role_bucket_counts: dict[str, list[int]] = {
            r: [0] * len(bucket_labels) for r in role_counts
        }

        for g in groups:
            sw = g.get("shared_works", 0)
            if sw < 3:
                continue
            if sw >= 20:
                bucket_idx = 4
            elif sw >= 12:
                bucket_idx = 3
            elif sw >= 8:
                bucket_idx = 2
            elif sw >= 5:
                bucket_idx = 1
            else:
                bucket_idx = 0

            for role_list in g.get("roles", {}).values():
                for r in role_list:
                    if r in role_bucket_counts:
                        role_bucket_counts[r][bucket_idx] += 1

        top_roles = sorted(role_counts, key=lambda r: -role_counts[r])[:8]
        n_total_roles = sum(role_counts.values())

        findings = (
            f"<p>上位200グループにおける役割出現回数の合計: {n_total_roles:,}回。"
            f"上位役割: "
            + ", ".join(
                f"{_ROLE_LABELS_JA.get(r, r)}（{role_counts[r]:,}）" for r in top_roles
            )
            + "。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        fig = go.Figure()
        for i, role in enumerate(top_roles):
            fig.add_trace(
                go.Bar(
                    name=_ROLE_LABELS_JA.get(role, role),
                    x=bucket_labels,
                    y=role_bucket_counts[role],
                    marker_color=_ROLE_PALETTE[i % len(_ROLE_PALETTE)],
                    hovertemplate=(
                        f"{_ROLE_LABELS_JA.get(role, role)}: "
                        "%{y} groups<extra></extra>"
                    ),
                )
            )
        fig.update_layout(
            barmode="stack",
            title="共参加作品数帯別 x 役割内訳（上位8役割）",
            xaxis_title="共参加作品数帯",
            yaxis_title="グループ数（役割別）",
        )

        return ReportSection(
            title="共参加作品数帯別 x 役割内訳",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig,
                "chart_cooc_role_breakdown",
                height=450,
            ),
            method_note=(
                "Top 8 roles by frequency across top-200 groups. "
                "Shared-works buckets: 3-4, 5-7, 8-11, 12-19, 20+."
            ),
            section_id="cooc_role_breakdown",
        )

    def _build_year_score_scatter_section(
        self,
        sb: SectionBuilder,
        groups: list[dict],
    ) -> ReportSection:
        scatter_groups = [
            g
            for g in groups
            if g.get("first_year") and (g.get("avg_iv_score") or 0) > 0
        ]
        if not scatter_groups:
            return ReportSection(
                title="初出年 x 平均スコア散布図",
                findings_html="<p>初出年×スコア散布図のデータがありません。</p>",
                section_id="cooc_year_score",
            )

        fy_vals = [float(g["first_year"]) for g in scatter_groups]
        iv_vals = [g["avg_iv_score"] for g in scatter_groups]
        fy_summ = distribution_summary(fy_vals, label="first_year")
        iv_summ = distribution_summary(iv_vals, label="avg_iv_score")

        findings = (
            f"<p>first_yearおよびavg_iv_score > 0を持つ{len(scatter_groups):,}グループ。"
            f"初出年: {format_distribution_inline(fy_summ)}。"
            f"平均IV score: {format_distribution_inline(iv_summ)}、"
            f"{format_ci((iv_summ['ci_lower'], iv_summ['ci_upper']))}。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        colors = ["#3BC494" if g["is_active"] else "#EF476F" for g in scatter_groups]
        sizes = [
            max(4, min(20, g.get("shared_works", 3) * 1.5)) for g in scatter_groups
        ]

        fig = go.Figure(
            go.Scatter(
                x=[g["first_year"] for g in scatter_groups],
                y=[g["avg_iv_score"] for g in scatter_groups],
                mode="markers",
                marker=dict(
                    color=colors,
                    size=sizes,
                    opacity=0.65,
                    line=dict(width=0.5, color="rgba(255,255,255,0.3)"),
                ),
                text=[
                    f"{'アクティブ' if g['is_active'] else '休眠'} | "
                    f"共参加{g.get('shared_works', 0)}作品 | "
                    f"規模={g.get('size', '?')}"
                    for g in scatter_groups
                ],
                hovertemplate=(
                    "初年: %{x}<br>平均IV: %{y:.1f}<br>%{text}<extra></extra>"
                ),
            )
        )
        fig.update_layout(
            title="初出年 x 平均IV Score（緑=現役, 赤=休眠, サイズ=共参加数）",
            xaxis_title="グループ初出年",
            yaxis_title="メンバー平均IV Score",
        )

        return ReportSection(
            title="初出年 x 平均スコア散布図",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig,
                "chart_cooc_year_score",
                height=500,
            ),
            method_note=(
                "X軸 = グループ初共同制作年、Y軸 = メンバー平均IVスコア。"
                "緑 = アクティブ（2022年以降）、赤 = 休眠。"
                "マーカーサイズは shared_works に比例。"
            ),
            section_id="cooc_year_score",
        )

    def _build_kmeans_section(
        self,
        sb: SectionBuilder,
        groups: list[dict],
    ) -> ReportSection:
        valid_grps = [g for g in groups if g.get("first_year") and g.get("last_year")]

        if len(valid_grps) < 5:
            return ReportSection(
                title="グループ K-Means クラスタリング",
                findings_html=(
                    f"<p>K-Meansクラスタリングに必要な有効グループが5件未満"
                    f"（{len(valid_grps)}件）です。</p>"
                ),
                section_id="cooc_kmeans",
            )

        # Build feature matrix (9 features)
        def _features(g: dict) -> list[float]:
            sw = g.get("shared_works", 0) or 0
            fy = float(g.get("first_year") or 2000)
            ly = g.get("last_year") or 0
            span = max(1.0, ly - fy + 1)
            return [
                float(g.get("size", 3)),
                float(sw),
                float(span),
                float(g.get("avg_iv_score", 0) or 0),
                float(g.get("is_active", False)),
                float(sw / span),
                float(g.get("collab_power", 0)),
                float(g.get("uniqueness_score", 0)),
                fy,
            ]

        feature_matrix = np.array(
            [_features(g) for g in valid_grps],
            dtype=float,
        )
        scaler = StandardScaler()
        scaled = scaler.fit_transform(feature_matrix)
        k = min(5, len(valid_grps))
        km = KMeans(n_clusters=k, n_init=20, random_state=42)
        labels = km.fit_predict(scaled)

        centers = scaler.inverse_transform(km.cluster_centers_)
        cluster_names = name_clusters_by_rank(
            centers,
            [
                (8, ["現代型", "過渡期型", "クラシック型"]),
                (1, ["多作", "少作"]),
                (3, ["高品質", "低品質"]),
            ],
        )

        for i, g in enumerate(valid_grps):
            g["group_cluster"] = int(labels[i])
            g["group_cluster_name"] = cluster_names[int(labels[i])]

        gc_groups: dict[int, list[dict]] = {}
        for g in valid_grps:
            gc_groups.setdefault(g["group_cluster"], []).append(g)

        # Findings: cluster sizes
        findings_parts = [
            f"<p>K-Meansクラスタリング（k={k}）: 9特徴量"
            f"（size, shared_works, span, avg_iv_score, is_active, "
            f"intensity, collab_power, uniqueness, first_year）を"
            f"{len(valid_grps):,}グループに適用。</p>"
            "<p>クラスタサイズ:</p><ul>"
        ]
        for cid in sorted(gc_groups):
            cp_vals = [g.get("collab_power", 0) for g in gc_groups[cid]]
            cp_summ = distribution_summary(cp_vals, label=cluster_names[cid])
            findings_parts.append(
                f"<li>{cluster_names[cid]}（n={len(gc_groups[cid]):,}）: "
                f"collab_power {format_distribution_inline(cp_summ)}</li>"
            )
        findings_parts.append("</ul>")
        findings = "\n".join(findings_parts)

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        # Chart 1: Cluster scatter (span vs shared_works)
        fig_scatter = go.Figure()
        for cid in sorted(gc_groups):
            members = gc_groups[cid]
            fig_scatter.add_trace(
                go.Scatter(
                    x=[g.get("last_year", 0) - g.get("first_year", 0) for g in members],
                    y=[g.get("shared_works", 0) for g in members],
                    mode="markers",
                    name=cluster_names[cid],
                    marker=dict(
                        size=8,
                        color=_GRP_COLORS[cid % len(_GRP_COLORS)],
                        opacity=0.65,
                    ),
                    hovertemplate=(
                        "期間: %{x}年<br>"
                        "共参加作品数: %{y}<br>"
                        f"クラスタ: {cluster_names[cid]}<extra></extra>"
                    ),
                )
            )
        fig_scatter.update_layout(
            title="グループクラスタ散布図（活動期間 x 共参加作品数）",
            xaxis_title="活動期間（年）",
            yaxis_title="共参加作品数",
        )

        # Chart 2: Cluster collab_power violin
        fig_violin = go.Figure()
        for cid in sorted(gc_groups):
            cp_vals = [g.get("collab_power", 0) for g in gc_groups[cid]]
            if cp_vals:
                fig_violin.add_trace(
                    _violin_raincloud(
                        cp_vals,
                        cluster_names[cid],
                        _GRP_COLORS[cid % len(_GRP_COLORS)],
                    )
                )
        fig_violin.update_layout(
            title="クラスタ別 コラボレーションパワー分布 (Raincloud)",
            yaxis_title="コラボレーションパワー (0-99)",
            violinmode="overlay",
        )

        viz = plotly_div_safe(
            fig_scatter, "chart_cooc_cluster_scatter", height=500
        ) + plotly_div_safe(fig_violin, "chart_cooc_cluster_violin", height=450)

        return ReportSection(
            title="グループ K-Means クラスタリング",
            findings_html=findings,
            visualization_html=viz,
            method_note=(
                "K-Means (k=5, n_init=20, random_state=42) on StandardScaler-"
                "normalized 9-feature matrix. Cluster names assigned by "
                "name_clusters_by_rank (relative centroid ranking on "
                "first_year, shared_works, avg_iv_score)."
            ),
            section_id="cooc_kmeans",
        )

    def _build_collab_power_top30_section(
        self,
        sb: SectionBuilder,
        groups: list[dict],
    ) -> ReportSection:
        top_power = sorted(
            groups,
            key=lambda g: g.get("collab_power", 0),
            reverse=True,
        )[:30]

        if not top_power:
            return ReportSection(
                title="コラボレーションパワー TOP30",
                findings_html="<p>collab_powerデータがありません。</p>",
                section_id="cooc_power_top30",
            )

        vals = [g.get("collab_power", 0) for g in top_power]
        summ = distribution_summary(vals, label="top30_collab_power")

        findings = (
            f"<p>コラボレーションパワー上位30グループ。"
            f"上位30件の分布: {format_distribution_inline(summ)}、"
            f"{format_ci((summ['ci_lower'], summ['ci_upper']))}。"
            f"最大collab_power = {max(vals):.1f}/99。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        fig = go.Figure(
            go.Bar(
                x=[
                    " / ".join((g.get("member_names") or g.get("members", []))[:2])
                    for g in top_power
                ],
                y=[g.get("collab_power", 0) for g in top_power],
                marker_color=[
                    "#3BC494" if g.get("is_active") else "#EF476F" for g in top_power
                ],
                text=[f"{g.get('shared_works', 0)}作品" for g in top_power],
                hovertemplate=(
                    "%{x}<br>Collab power: %{y:.1f}<br>%{text}<extra></extra>"
                ),
            )
        )
        fig.update_layout(
            title="コラボレーションパワー TOP30（緑=現役 / 赤=休眠）",
            yaxis_title="コラボパワー",
            xaxis_tickangle=-35,
            height=480,
        )

        return ReportSection(
            title="コラボレーションパワー TOP30",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig,
                "chart_cooc_power_top30",
                height=480,
            ),
            method_note=(
                "collab_power = avg_iv_score x log1p(shared_works) "
                "x (1 + span/30), normalized 0-99. "
                "Green = active (2022+), red = dormant."
            ),
            section_id="cooc_power_top30",
        )

    def _build_uniqueness_top30_section(
        self,
        sb: SectionBuilder,
        groups: list[dict],
    ) -> ReportSection:
        top_unique = sorted(
            groups,
            key=lambda g: g.get("uniqueness_score", 0),
            reverse=True,
        )[:30]

        if not top_unique:
            return ReportSection(
                title="ユニークさ TOP30",
                findings_html="<p>uniquenessデータがありません。</p>",
                section_id="cooc_unique_top30",
            )

        vals = [g.get("uniqueness_score", 0) for g in top_unique]
        summ = distribution_summary(vals, label="top30_uniqueness")

        findings = (
            f"<p>ユニークさスコア上位30グループ。"
            f"上位30件の分布: {format_distribution_inline(summ)}、"
            f"{format_ci((summ['ci_lower'], summ['ci_upper']))}。"
            f"最大uniqueness = {max(vals):.1f}/99。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        fig = go.Figure(
            go.Bar(
                x=[
                    " / ".join((g.get("member_names") or g.get("members", []))[:2])
                    for g in top_unique
                ],
                y=[g.get("uniqueness_score", 0) for g in top_unique],
                marker_color=[
                    f"rgba(160,210,219,{0.4 + 0.6 * g.get('uniqueness_score', 0) / 99})"
                    for g in top_unique
                ],
                text=[
                    f"size={g.get('size', '?')}, "
                    f"{g.get('shared_works', 0)} works / "
                    f"{max(1, (g.get('last_year') or 0) - (g.get('first_year') or 0) + 1)} yrs"
                    for g in top_unique
                ],
                hovertemplate=(
                    "%{x}<br>Uniqueness: %{y:.1f}<br>%{text}<extra></extra>"
                ),
            )
        )
        fig.update_layout(
            title="ユニークさ（過剰結びつき度）TOP30",
            yaxis_title="ユニークさスコア",
            xaxis_tickangle=-35,
            height=480,
        )

        return ReportSection(
            title="ユニークさ TOP30",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig,
                "chart_cooc_unique_top30",
                height=480,
            ),
            method_note=(
                "uniqueness = (shared_works / span) x size_bonus x era_weight, "
                "normalized 0-99. "
                "era_weight: 1960s=3.0, 1970s=2.5, ..., 2020s=0.65."
            ),
            section_id="cooc_unique_top30",
        )

    def _build_ml_classification_section(
        self,
        sb: SectionBuilder,
        groups: list[dict],
    ) -> ReportSection:
        reason_counter: Counter = Counter(
            g.get("grouping_reason", "標準コラボ") for g in groups
        )

        if not reason_counter:
            return ReportSection(
                title="グルーピング理由 分類",
                findings_html="<p>分類データがありません。</p>",
                section_id="cooc_ml_class",
            )

        reasons_sorted = sorted(reason_counter, key=lambda r: -reason_counter[r])
        total_classified = sum(reason_counter.values())

        findings_parts = [
            f"<p>ルールベースMLにより{total_classified:,}グループを"
            f"{len(reasons_sorted)}カテゴリに分類。</p>"
            "<p>分類結果:</p><ul>"
        ]
        for r in reasons_sorted:
            pct = 100 * reason_counter[r] / max(total_classified, 1)
            findings_parts.append(f"<li>{r}: {reason_counter[r]:,}件（{pct:.1f}%）</li>")
        findings_parts.append("</ul>")
        findings = "\n".join(findings_parts)

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        # Chart 1: Horizontal bar — reason counts
        fig_bar_h = go.Figure(
            go.Bar(
                y=reasons_sorted,
                x=[reason_counter[r] for r in reasons_sorted],
                orientation="h",
                marker_color=[_REASON_COLORS.get(r, "#888") for r in reasons_sorted],
                hovertemplate="%{y}: %{x} groups<extra></extra>",
            )
        )
        fig_bar_h.update_layout(
            title="グルーピング理由 構成比",
            xaxis_title="グループ数",
            yaxis_title="分類",
        )

        # Chart 2: Stacked bar — reason x active/dormant
        fig_stacked = go.Figure()
        fig_stacked.add_trace(
            go.Bar(
                name="現役",
                x=reasons_sorted,
                y=[
                    sum(
                        1
                        for g in groups
                        if g.get("grouping_reason") == r and g.get("is_active")
                    )
                    for r in reasons_sorted
                ],
                marker_color="#3BC494",
            )
        )
        fig_stacked.add_trace(
            go.Bar(
                name="休眠",
                x=reasons_sorted,
                y=[
                    sum(
                        1
                        for g in groups
                        if g.get("grouping_reason") == r and not g.get("is_active")
                    )
                    for r in reasons_sorted
                ],
                marker_color="#EF476F",
            )
        )
        fig_stacked.update_layout(
            barmode="stack",
            title="グルーピング理由別 現役/休眠内訳",
            xaxis_tickangle=-25,
            yaxis_title="グループ数",
        )

        # Chart 3: Violin — collab_power per reason
        fig_violin = go.Figure()
        for r in reasons_sorted:
            r_vals = [
                g.get("collab_power", 0)
                for g in groups
                if g.get("grouping_reason") == r
            ]
            if r_vals:
                fig_violin.add_trace(
                    _violin_raincloud(
                        r_vals,
                        r,
                        _REASON_COLORS.get(r, "#888"),
                    )
                )
        fig_violin.update_layout(
            title="分類別 コラボレーションパワー分布 (Raincloud)",
            yaxis_title="コラボパワー",
            xaxis_tickangle=-20,
            violinmode="overlay",
        )

        viz = (
            plotly_div_safe(fig_bar_h, "chart_cooc_reason_bar", height=420)
            + plotly_div_safe(fig_stacked, "chart_cooc_reason_active", height=420)
            + plotly_div_safe(fig_violin, "chart_cooc_reason_violin", height=450)
        )

        return ReportSection(
            title="グルーピング理由 分類（ルールベースML）",
            findings_html=findings,
            visualization_html=viz,
            method_note=(
                "Rule-based classification using shared_works, span, "
                "is_active, first_year, intensity, and avg_iv_score. "
                "Era-adjusted thresholds: _era_sw_threshold scales "
                "shared-works thresholds by decade "
                "(pre-1985: 0.4x, 2015+: 1.2x base). "
                "Categories: long-established, legacy, series-intensive, "
                "emerging high-quality, modern emerging, legacy dormant, "
                "series-continuation, high-rated, standard."
            ),
            section_id="cooc_ml_class",
        )


# v3 minimal SPEC — generated by scripts/maintenance/add_default_specs.py.
# Replace ``claim`` / ``identifying_assumption`` / ``null_model`` with
# report-specific values when curating this module.
from .._spec import make_default_spec  # noqa: E402

SPEC = make_default_spec(
    name='cooccurrence_groups',
    audience='technical_appendix',
    claim=(
        '共クレジット 3 作以上のコアスタッフ集合 (size 3-5) を K-Means + ML 分類で '
        '識別し、集団内の役職構成 / スケール / ジャンル で 5-7 typology が抽出される'
    ),
    identifying_assumption=(
        '共クレジット 3 作以上 = 「集団」 を operational に定義。'
        '実際のチーム / circle / 派閥としての結束度は本指標で直接測らない。'
        'typology は K-means cluster の解釈ラベルであり、客観的分類ではない。'
    ),
    null_model=['N1', 'N2'],
    sources=['credits', 'persons', 'anime'],
    meta_table='meta_cooccurrence_groups',
    estimator='K-Means + ML classifier on (role_mix, scale, genre, era)',
    ci_estimator='bootstrap', n_resamples=500,
    extra_limitations=[
        '共クレジット 3 作閾値は事前固定 — 2 作 / 5 作で集団数が桁単位変動',
        'cluster typology は label switching、実行間で安定しない',
        '時代別の集団サイズ分布は credit-record 粒度差を含む',
    ],
)
