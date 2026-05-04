"""O8 ソフトパワー指標 (soft_power_index) — v2 compliant.

AniList の external_links_json から国際配信プラットフォームへの
リンクを抽出し、以下を計算・可視化する:

1. 配信プラットフォーム別の anime 数と関与人材分布
2. 国際展開 anime 関与人材 vs 国内専 anime 関与人材の
   ネットワーク位置 (theta_i 代理値) 比較 — Mann-Whitney U + 効果量 r
3. 構造的 soft_power_index =
       配信プラットフォーム数 × person FE 平均 (配信作品の関与人材)
   視聴者評価は算出パスから除外 (H1 invariant)

Tier2 (国際賞 / 海外売上) は別カード (Card 16) で実施予定。

Method note 宣言:
- 重み固定: platform_weight は各プラットフォームへの固定事前重み (1.0)。
  優位性・評判による可変重みは使用しない。
- theta_i 代理値: person FE (AKM theta_i) が SILVER にない場合は
  log(1 + total_credits) を代理として使用する。
- bootstrap CI: Mann-Whitney U の効果量 r に対して bootstrap n=1000。
"""

from __future__ import annotations

import json
import math
import random
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import plotly.graph_objects as go
import structlog

from ..helpers import insert_lineage
from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator, append_validation_warnings

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# 国際配信プラットフォームのホワイトリスト
# サービス名サブストリングで判定 (case-insensitive)
# ---------------------------------------------------------------------------

_STREAMING_PLATFORMS: dict[str, str] = {
    "netflix": "Netflix",
    "crunchyroll": "Crunchyroll",
    "funimation": "Funimation",
    "hidive": "HIDIVE",
    "disney": "Disney+",
    "amazon": "Amazon Prime Video",
    "hulu": "Hulu",
    "vrv": "VRV",
    "bilibili": "Bilibili",
    "iqiyi": "iQIYI",
    "wakanim": "Wakanim",
    "animelab": "AnimeLab",
}

# Bootstrap iterations
_N_BOOTSTRAP = 1000

# Minimum number of anime in the "international" group to run comparison
_MIN_INTL_ANIME = 5


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass
class PlatformCount:
    """Number of anime linked to a specific platform."""

    platform: str
    label: str
    anime_count: int


@dataclass
class AnimeDistributionProfile:
    """International distribution profile for a single anime."""

    anime_id: str
    title: str
    platform_count: int
    platforms: list[str] = field(default_factory=list)


@dataclass
class PersonNetworkRow:
    """Network position proxy for a person."""

    person_id: str
    name: str
    theta_proxy: float   # log(1 + total_credits) or AKM theta_i if available
    is_international: bool  # contributed to >= 1 intl-distributed anime


@dataclass
class MannWhitneyResult:
    """Mann-Whitney U test result + bootstrap effect size CI."""

    n_intl: int
    n_domestic: int
    u_stat: float
    p_value_approx: float  # normal approximation
    effect_r: float        # rank-biserial correlation r
    ci_lower: float        # bootstrap 95% CI on r
    ci_upper: float        # bootstrap 95% CI on r


@dataclass
class SoftPowerIndexRow:
    """Per-platform soft_power_index entry."""

    platform: str
    label: str
    anime_count: int
    mean_theta_proxy: float   # mean theta_proxy of involved persons
    soft_power_index: float   # anime_count × mean_theta_proxy


# ---------------------------------------------------------------------------
# Step 1: Extract platforms from external_links_json
# ---------------------------------------------------------------------------


def _extract_platform(url: str) -> str | None:
    """Return platform key if the URL matches a known streaming service."""
    url_lower = url.lower()
    for key in _STREAMING_PLATFORMS:
        if key in url_lower:
            return key
    return None


def extract_anime_distribution_profiles(
    conn: Any,
) -> tuple[list[AnimeDistributionProfile], dict[str, PlatformCount]]:
    """Query anime table and extract international distribution profiles.

    Parses external_links_json to detect streaming platform links.
    Returns (profiles, platform_counts).

    Args:
        conn: DB connection.

    Returns:
        Tuple of (list of AnimeDistributionProfile, dict platform_key -> PlatformCount).
    """
    try:
        rows = conn.execute(
            "SELECT id, title_romaji, external_links_json FROM conformed.anime"
        ).fetchall()
    except Exception as exc:
        log.warning("anime_query_failed", error=str(exc))
        return [], {}

    platform_anime: dict[str, set[str]] = defaultdict(set)
    profiles: list[AnimeDistributionProfile] = []

    for row in rows:
        anime_id = str(row[0])
        title = str(row[1] or "")
        links_raw = row[2]

        if not links_raw:
            continue

        try:
            links = json.loads(links_raw)
        except (json.JSONDecodeError, TypeError):
            continue

        if not isinstance(links, list):
            continue

        found_platforms: list[str] = []
        for link in links:
            url = link.get("url", "") if isinstance(link, dict) else ""
            if not url:
                continue
            pkey = _extract_platform(url)
            if pkey and pkey not in found_platforms:
                found_platforms.append(pkey)
                platform_anime[pkey].add(anime_id)

        if found_platforms:
            profiles.append(AnimeDistributionProfile(
                anime_id=anime_id,
                title=title,
                platform_count=len(found_platforms),
                platforms=found_platforms,
            ))

    platform_counts: dict[str, PlatformCount] = {
        pkey: PlatformCount(
            platform=pkey,
            label=_STREAMING_PLATFORMS[pkey],
            anime_count=len(anime_ids),
        )
        for pkey, anime_ids in platform_anime.items()
    }

    return profiles, platform_counts


# ---------------------------------------------------------------------------
# Step 2: Person network position (theta_i proxy)
# ---------------------------------------------------------------------------


def fetch_person_network_rows(
    conn: Any,
    intl_anime_ids: set[str],
) -> list[PersonNetworkRow]:
    """Fetch person theta_proxy and international-distribution flag.

    theta_proxy = AKM theta_i if available, else log(1 + total_credits).
    A person is "international" if they have at least one credit in an
    internationally distributed anime.

    Args:
        conn: DB connection.
        intl_anime_ids: set of anime_id values that have >= 1 platform link.

    Returns:
        List of PersonNetworkRow.
    """
    # Try to fetch theta_i from scores table
    theta_map: dict[str, float] = {}
    try:
        score_rows = conn.execute(
            "SELECT person_id, theta_i FROM scores WHERE theta_i IS NOT NULL"
        ).fetchall()
        for r in score_rows:
            theta_map[str(r[0])] = float(r[1])
    except Exception:
        pass  # scores table may not exist — use credit count proxy

    # Fetch credits per person + which anime they worked on
    try:
        credit_rows = conn.execute(
            "SELECT person_id, anime_id FROM conformed.credits"
        ).fetchall()
    except Exception as exc:
        log.warning("credits_query_failed", error=str(exc))
        return []

    # Fetch person names
    name_map: dict[str, str] = {}
    try:
        person_rows = conn.execute("SELECT id, name_romaji FROM conformed.persons").fetchall()
        for r in person_rows:
            name_map[str(r[0])] = str(r[1] or "")
    except Exception:
        pass

    # Aggregate
    person_credits: dict[str, int] = defaultdict(int)
    person_intl: dict[str, bool] = defaultdict(bool)

    for row in credit_rows:
        pid = str(row[0])
        aid = str(row[1])
        person_credits[pid] += 1
        if aid in intl_anime_ids:
            person_intl[pid] = True

    result: list[PersonNetworkRow] = []
    for pid, total in person_credits.items():
        if pid in theta_map:
            theta = theta_map[pid]
        else:
            theta = math.log1p(total)
        result.append(PersonNetworkRow(
            person_id=pid,
            name=name_map.get(pid, pid),
            theta_proxy=theta,
            is_international=person_intl.get(pid, False),
        ))

    return result


# ---------------------------------------------------------------------------
# Step 3: Mann-Whitney U + bootstrap CI
# ---------------------------------------------------------------------------


def compute_mann_whitney(
    rows: list[PersonNetworkRow],
    rng: random.Random,
    n_bootstrap: int = _N_BOOTSTRAP,
) -> MannWhitneyResult | None:
    """Compute Mann-Whitney U test comparing intl vs domestic theta_proxy.

    Effect size: rank-biserial correlation r = 1 - 2U / (n1 * n2).
    P-value: normal approximation (large sample).
    Bootstrap CI on r: n_bootstrap resamples.

    Args:
        rows: all PersonNetworkRow.
        rng: seeded random.
        n_bootstrap: number of bootstrap iterations for CI on r.

    Returns:
        MannWhitneyResult or None if insufficient data.
    """
    intl = [r.theta_proxy for r in rows if r.is_international]
    domestic = [r.theta_proxy for r in rows if not r.is_international]

    if len(intl) < _MIN_INTL_ANIME or len(domestic) < 2:
        return None

    n1, n2 = len(intl), len(domestic)

    # Mann-Whitney U: count concordant pairs
    u_stat = _compute_u_stat(intl, domestic)

    # Normal approximation
    mean_u = n1 * n2 / 2.0
    var_u = n1 * n2 * (n1 + n2 + 1) / 12.0
    z = (u_stat - mean_u) / math.sqrt(var_u) if var_u > 0 else 0.0
    p_approx = _normal_sf(abs(z)) * 2  # two-sided

    # Effect size r = 1 - 2U / (n1 * n2)
    r = 1.0 - 2.0 * u_stat / (n1 * n2)

    # Bootstrap CI on r
    combined = intl + domestic
    labels = [True] * n1 + [False] * n2

    r_samples: list[float] = []
    for _ in range(n_bootstrap):
        indices = rng.choices(range(n1 + n2), k=n1 + n2)
        boot_vals = [combined[i] for i in indices]
        boot_intl = [v for v, i in zip(boot_vals, indices) if labels[i]]
        boot_dom = [v for v, i in zip(boot_vals, indices) if not labels[i]]
        if not boot_intl or not boot_dom:
            r_samples.append(r)
            continue
        u_b = _compute_u_stat(boot_intl, boot_dom)
        r_b = 1.0 - 2.0 * u_b / (len(boot_intl) * len(boot_dom))
        r_samples.append(r_b)

    r_samples.sort()
    lo_idx = int(0.025 * n_bootstrap)
    hi_idx = int(0.975 * n_bootstrap)
    ci_lo = r_samples[lo_idx]
    ci_hi = r_samples[min(hi_idx, n_bootstrap - 1)]

    return MannWhitneyResult(
        n_intl=n1,
        n_domestic=n2,
        u_stat=u_stat,
        p_value_approx=p_approx,
        effect_r=r,
        ci_lower=ci_lo,
        ci_upper=ci_hi,
    )


def _compute_u_stat(group_a: list[float], group_b: list[float]) -> float:
    """Compute Mann-Whitney U statistic for group_a vs group_b.

    U = sum over (a in group_a, b in group_b) of I(a > b) + 0.5 * I(a == b).
    O(n1 * n2) — acceptable for typical sizes.
    """
    u = 0.0
    for a in group_a:
        for b in group_b:
            if a > b:
                u += 1.0
            elif a == b:
                u += 0.5
    return u


def _normal_sf(z: float) -> float:
    """Survival function of standard normal (one-tailed p-value).

    Uses math.erfc approximation.
    """
    return 0.5 * math.erfc(z / math.sqrt(2))


# ---------------------------------------------------------------------------
# Step 4: soft_power_index per platform
# ---------------------------------------------------------------------------


def compute_soft_power_index(
    platform_counts: dict[str, PlatformCount],
    profiles: list[AnimeDistributionProfile],
    person_rows: list[PersonNetworkRow],
) -> list[SoftPowerIndexRow]:
    """Compute structural soft_power_index per platform.

    soft_power_index[platform] =
        anime_count[platform] × mean_theta_proxy(persons involved in platform anime)

    Weights are fixed (platform_weight = 1.0 for all platforms).
    Viewer ratings are excluded from computation (H1 invariant).

    Args:
        platform_counts: dict of platform_key -> PlatformCount.
        profiles: list of AnimeDistributionProfile (one per anime with links).
        person_rows: list of PersonNetworkRow.

    Returns:
        List of SoftPowerIndexRow sorted by soft_power_index desc.
    """
    # Build: platform_key -> set of anime_ids
    platform_anime_map: dict[str, set[str]] = defaultdict(set)
    for p in profiles:
        for pkey in p.platforms:
            platform_anime_map[pkey].add(p.anime_id)

    # Build: anime_id -> list of theta_proxy values
    anime_theta: dict[str, list[float]] = defaultdict(list)
    for pr in person_rows:
        if pr.is_international:
            # We need per-anime mapping — use person rows as aggregate
            pass  # handled below via credits join (fallback: use global mean)

    # Fallback: use global mean theta_proxy per platform
    # (full per-anime-person join is deferred to Tier2 with scores table)
    intl_persons = [pr.theta_proxy for pr in person_rows if pr.is_international]
    global_mean = sum(intl_persons) / len(intl_persons) if intl_persons else 0.0

    result: list[SoftPowerIndexRow] = []
    for pkey, pc in platform_counts.items():
        # Use global_mean as first-order approximation for mean_theta_proxy
        # (platform-specific theta requires per-anime-person join, Tier2)
        mean_theta = global_mean
        spi = pc.anime_count * mean_theta
        result.append(SoftPowerIndexRow(
            platform=pkey,
            label=pc.label,
            anime_count=pc.anime_count,
            mean_theta_proxy=mean_theta,
            soft_power_index=spi,
        ))

    result.sort(key=lambda r: r.soft_power_index, reverse=True)
    return result


# ---------------------------------------------------------------------------
# Report class
# ---------------------------------------------------------------------------


class O8SoftPowerReport(BaseReportGenerator):
    """Business brief: ソフトパワー指標 (Tier1 — 配信プラットフォーム分析).

    Extracts international streaming platform links from AniList
    external_links_json and computes structural soft_power_index.
    Viewer ratings are excluded from all computations (H1 invariant).
    """

    name = "o8_soft_power"
    title = "ソフトパワー指標 — 国際配信プラットフォーム分析"
    subtitle = (
        "配信プラットフォーム別 anime 分布 × 関与人材ネットワーク位置 / "
        "Soft Power Index — International Streaming Distribution Analysis"
    )
    filename = "o8_soft_power.html"
    doc_type = "brief"

    def generate(self) -> Path | None:
        sb = SectionBuilder()
        rng = random.Random(42)

        profiles, platform_counts = extract_anime_distribution_profiles(self.conn)

        if not profiles:
            body = self._build_no_data_body(sb)
            return self.write_report(body, intro_html=self._build_intro())

        intl_anime_ids = {p.anime_id for p in profiles}
        person_rows = fetch_person_network_rows(self.conn, intl_anime_ids)
        mw_result = compute_mann_whitney(person_rows, rng)
        spi_rows = compute_soft_power_index(platform_counts, profiles, person_rows)

        sections = [
            sb.build_section(
                self._build_platform_section(sb, platform_counts, profiles)
            ),
            sb.build_section(
                self._build_distribution_comparison_section(sb, person_rows, mw_result)
            ),
            sb.build_section(
                self._build_spi_section(sb, spi_rows)
            ),
        ]

        insert_lineage(
            self.conn,
            table_name="meta_o8_soft_power",
            audience="biz",
            source_silver_tables=["anime", "credits", "persons"],
            formula_version="v1.0-tier1",
            ci_method=(
                "Bootstrap CI (n=1000, seed=42) on rank-biserial correlation r "
                "from Mann-Whitney U test (international vs domestic person groups); "
                "percentile method (2.5th/97.5th)"
            ),
            null_model=(
                "Mann-Whitney U normal approximation (two-sided); "
                "null hypothesis: theta_proxy distributions are identical "
                "for international and domestic groups"
            ),
            holdout_method="Not applicable (descriptive cross-sectional analysis)",
            description=(
                "Tier1 soft_power_index: structural measurement of international "
                "streaming distribution using AniList external_links_json. "
                "soft_power_index[platform] = anime_count × mean_theta_proxy "
                "(platform_weight fixed at 1.0 for all platforms). "
                "theta_proxy = AKM theta_i if available, else log(1+total_credits). "
                "Viewer ratings excluded from computation (H1 invariant)."
            ),
            rng_seed=42,
        )

        interpretation_html = self._build_interpretation(mw_result)

        return self.write_report(
            "\n".join(sections),
            intro_html=self._build_intro(),
            extra_glossary={
                "soft_power_index": (
                    "プラットフォーム別の構造的ソフトパワー指標。"
                    "soft_power_index = anime_count × mean_theta_proxy。"
                    "platform_weight は全プラットフォームで 1.0 に固定。"
                    "視聴者評価は算出パスから除外 (H1)。"
                ),
                "theta_proxy": (
                    "AKM person fixed effect theta_i の代理値。"
                    "SILVER scores テーブルに theta_i が存在する場合はその値を使用し、"
                    "存在しない場合は log(1 + total_credits) を代理とする。"
                    "クレジット記録に基づくネットワーク位置の構造的指標。"
                ),
                "international_distribution": (
                    "AniList external_links_json に Netflix / Crunchyroll / Funimation "
                    "等の国際配信プラットフォームへのリンクを持つ anime を指す。"
                    "リンクの存在は配信の事実を示すが、配信国数や視聴者数は含まない (Tier1)。"
                ),
                "rank_biserial_r": (
                    "Mann-Whitney U 検定の効果量。"
                    "r = 1 - 2U / (n1 × n2)。"
                    "r > 0 は国際展開群の theta_proxy が高い方向に分布する傾向を示す。"
                    "効果量の解釈: |r| < 0.1 = 小, 0.1-0.3 = 中, > 0.3 = 大 (Cohen の慣例)。"
                ),
            },
        )

    # ------------------------------------------------------------------
    # No-data fallback
    # ------------------------------------------------------------------

    def _build_no_data_body(self, sb: SectionBuilder) -> str:
        sec = ReportSection(
            title="データ不足 / No Distribution Data",
            findings_html=(
                "<p>anime テーブルの external_links_json に国際配信プラットフォームへの"
                "リンクが検出されませんでした。"
                "AniList scraper による external_links_json の充填が完了していることを"
                "確認してください。</p>"
                "<p>No international streaming platform links were detected in "
                "anime.external_links_json. "
                "Ensure the AniList scraper has populated this column.</p>"
            ),
            method_note=(
                "前提条件未充足: anime.external_links_json に Netflix / Crunchyroll 等の "
                "国際配信プラットフォームリンクが 1 件以上必要。"
            ),
            section_id="o8_no_data",
        )
        return sb.build_section(sec)

    # ------------------------------------------------------------------
    # Section 1: Platform distribution bar chart
    # ------------------------------------------------------------------

    def _build_platform_section(
        self,
        sb: SectionBuilder,
        platform_counts: dict[str, PlatformCount],
        profiles: list[AnimeDistributionProfile],
    ) -> ReportSection:
        sorted_platforms = sorted(
            platform_counts.values(), key=lambda pc: pc.anime_count, reverse=True
        )

        labels = [pc.label for pc in sorted_platforms]
        counts = [pc.anime_count for pc in sorted_platforms]

        fig = go.Figure(
            go.Bar(
                x=labels,
                y=counts,
                marker_color="#667eea",
                text=[f"{c:,}" for c in counts],
                textposition="outside",
                hovertemplate="%{x}<br>anime 数=%{y:,}<extra></extra>",
            )
        )
        fig.update_layout(
            title="国際配信プラットフォーム別 anime 数 (external_links_json 集計)",
            xaxis_title="プラットフォーム",
            yaxis_title="anime 数",
            height=420,
        )

        n_intl = len(profiles)
        n_platforms = len(sorted_platforms)
        top_label = sorted_platforms[0].label if sorted_platforms else "–"
        top_count = sorted_platforms[0].anime_count if sorted_platforms else 0
        multi_platform = sum(1 for p in profiles if p.platform_count > 1)

        findings_html = (
            f"<p>国際配信プラットフォームへのリンクを持つ anime: {n_intl:,} 件。"
            f"検出プラットフォーム数: {n_platforms}。"
            f"最多: {top_label} ({top_count:,} anime)。"
            f"複数プラットフォーム掲載 anime: {multi_platform:,} 件。</p>"
            f"<p>各バーは AniList の external_links_json に当該プラットフォームへの"
            f"リンクを持つ anime の件数を示す。"
            f"リンクの有無は配信事実を示すが、配信国数・視聴者数は含まない (Tier1)。</p>"
        )
        findings_html = append_validation_warnings(findings_html, sb)

        return ReportSection(
            title="国際配信プラットフォーム別 anime 分布",
            findings_html=findings_html,
            visualization_html=plotly_div_safe(fig, "chart_o8_platform_dist", height=420),
            method_note=(
                "データソース: SILVER anime テーブルの external_links_json 列 (AniList)。"
                "プラットフォーム判定: URL サブストリングマッチング (大文字小文字不区別)。"
                "対象プラットフォーム: Netflix / Crunchyroll / Funimation / HIDIVE / "
                "Disney+ / Amazon Prime Video / Hulu / VRV / Bilibili / iQIYI / "
                "Wakanim / AnimeLab。"
                "external_links_json が NULL の anime は除外。"
                "Tier2 追加指標 (配信国数 / 視聴者数) は Card 16 で実施予定。"
            ),
            section_id="o8_platform_dist",
        )

    # ------------------------------------------------------------------
    # Section 2: International vs domestic person distribution
    # ------------------------------------------------------------------

    def _build_distribution_comparison_section(
        self,
        sb: SectionBuilder,
        person_rows: list[PersonNetworkRow],
        mw_result: MannWhitneyResult | None,
    ) -> ReportSection:
        intl_theta = [r.theta_proxy for r in person_rows if r.is_international]
        dom_theta = [r.theta_proxy for r in person_rows if not r.is_international]

        if not intl_theta or not dom_theta:
            findings_html = (
                "<p>国際展開 anime と国内専 anime の関与人材を比較するための"
                "クレジットデータが取得できませんでした。</p>"
            )
            findings_html = append_validation_warnings(findings_html, sb)
            return ReportSection(
                title="国際展開 vs 国内専 — 関与人材 theta_proxy 分布",
                findings_html=findings_html,
                method_note=(
                    "Mann-Whitney U 検定: 国際展開 anime 関与人材 vs 国内専 anime 関与人材の"
                    "theta_proxy 分布比較。bootstrap CI n=1000。"
                ),
                section_id="o8_distribution_comparison",
            )

        # Box / violin comparison
        fig = go.Figure()
        fig.add_trace(go.Violin(
            y=intl_theta,
            name="国際展開",
            side="negative",
            line_color="#06D6A0",
            fillcolor="rgba(6,214,160,0.3)",
            meanline_visible=True,
            scalemode="width",
            spanmode="soft",
            hoverinfo="y",
        ))
        fig.add_trace(go.Violin(
            y=dom_theta,
            name="国内専",
            side="positive",
            line_color="#667eea",
            fillcolor="rgba(102,126,234,0.3)",
            meanline_visible=True,
            scalemode="width",
            spanmode="soft",
            hoverinfo="y",
        ))
        fig.update_layout(
            title="関与人材 theta_proxy 分布: 国際展開 vs 国内専 (violin, split)",
            yaxis_title="theta_proxy (AKM theta_i or log(1+credits))",
            violingap=0,
            violinmode="overlay",
            height=480,
            legend=dict(orientation="h", y=1.05),
        )

        n_intl = len(intl_theta)
        n_dom = len(dom_theta)
        med_intl = sorted(intl_theta)[n_intl // 2] if intl_theta else 0.0
        med_dom = sorted(dom_theta)[n_dom // 2] if dom_theta else 0.0

        if mw_result:
            mw_text = (
                f"Mann-Whitney U={mw_result.u_stat:,.0f}, "
                f"p≈{mw_result.p_value_approx:.4f} (正規近似, 両側), "
                f"効果量 r={mw_result.effect_r:.3f} "
                f"(95% CI bootstrap [{mw_result.ci_lower:.3f}, {mw_result.ci_upper:.3f}])。"
            )
        else:
            mw_text = (
                f"国際展開 anime が {_MIN_INTL_ANIME} 件未満のため Mann-Whitney U を省略した。"
            )

        findings_html = (
            f"<p>国際展開関与人材: {n_intl:,} 人, 中央値 theta_proxy={med_intl:.3f}。"
            f"国内専関与人材: {n_dom:,} 人, 中央値 theta_proxy={med_dom:.3f}。</p>"
            f"<p>{mw_text}</p>"
            f"<p>violin の幅は各 theta_proxy 値の分布密度を示す。"
            f"theta_proxy はクレジット記録に基づくネットワーク位置の構造的指標であり、"
            f"個人への主観的評価を含まない。</p>"
        )
        findings_html = append_validation_warnings(findings_html, sb)

        return ReportSection(
            title="国際展開 vs 国内専 — 関与人材 theta_proxy 分布",
            findings_html=findings_html,
            visualization_html=plotly_div_safe(
                fig, "chart_o8_dist_comparison", height=480
            ),
            method_note=(
                "theta_proxy = AKM theta_i (SILVER scores テーブルに存在する場合) "
                "または log(1 + total_credits) (代理値)。"
                "AKM 定式化: log(production_scale_ij) = theta_i + psi_j + epsilon_ij、"
                "production_scale = staff_count × episodes × duration_mult。"
                "国際展開フラグ: external_links_json にプラットフォームリンクを持つ "
                "anime に 1 件以上クレジットを持つ person = True。"
                "Mann-Whitney U: 2 群独立、非パラメトリック、分布位置の比較。"
                "効果量 r = 1 - 2U / (n1 × n2)。"
                "bootstrap CI: n=1000 (seed=42)、復元抽出、百分位法。"
                "制限: theta_proxy は AKM theta_i の完全代替ではなく、"
                "観測されない交絡の除去は行わない。"
            ),
            section_id="o8_distribution_comparison",
        )

    # ------------------------------------------------------------------
    # Section 3: soft_power_index bar chart
    # ------------------------------------------------------------------

    def _build_spi_section(
        self,
        sb: SectionBuilder,
        spi_rows: list[SoftPowerIndexRow],
    ) -> ReportSection:
        if not spi_rows:
            findings_html = (
                "<p>soft_power_index の算出に必要なデータが取得できませんでした。</p>"
            )
            findings_html = append_validation_warnings(findings_html, sb)
            return ReportSection(
                title="プラットフォーム別 soft_power_index",
                findings_html=findings_html,
                method_note=(
                    "soft_power_index = anime_count × mean_theta_proxy。"
                    "視聴者評価は算出パスから除外 (H1)。"
                ),
                section_id="o8_spi",
            )

        labels = [r.label for r in spi_rows]
        values = [r.soft_power_index for r in spi_rows]
        counts = [r.anime_count for r in spi_rows]
        theta_means = [r.mean_theta_proxy for r in spi_rows]

        fig = go.Figure(
            go.Bar(
                x=labels,
                y=values,
                marker_color="#f5576c",
                customdata=list(zip(counts, theta_means)),
                hovertemplate=(
                    "<b>%{x}</b><br>"
                    "soft_power_index=%{y:.2f}<br>"
                    "anime_count=%{customdata[0]}<br>"
                    "mean_theta_proxy=%{customdata[1]:.3f}<extra></extra>"
                ),
                text=[f"{v:.1f}" for v in values],
                textposition="outside",
            )
        )
        fig.update_layout(
            title=(
                "プラットフォーム別 soft_power_index "
                "(= anime_count × mean_theta_proxy; 視聴者評価除外)"
            ),
            xaxis_title="プラットフォーム",
            yaxis_title="soft_power_index",
            height=420,
        )

        top_label = spi_rows[0].label if spi_rows else "–"
        top_spi = spi_rows[0].soft_power_index if spi_rows else 0.0
        total_spi = sum(r.soft_power_index for r in spi_rows)
        mean_theta = spi_rows[0].mean_theta_proxy if spi_rows else 0.0

        findings_html = (
            f"<p>プラットフォーム別 soft_power_index の最大値: "
            f"{top_spi:.2f} ({top_label})。"
            f"全プラットフォーム合計: {total_spi:.2f}。"
            f"関与人材 mean_theta_proxy: {mean_theta:.3f}。</p>"
            f"<p>soft_power_index は anime_count と mean_theta_proxy の積であり、"
            f"配信規模とネットワーク位置の構造的積算値を示す。"
            f"視聴者評価は算出パスから除外 (H1)。</p>"
        )
        findings_html = append_validation_warnings(findings_html, sb)

        return ReportSection(
            title="プラットフォーム別 soft_power_index",
            findings_html=findings_html,
            visualization_html=plotly_div_safe(fig, "chart_o8_spi", height=420),
            method_note=(
                "soft_power_index[platform] = anime_count[platform] × mean_theta_proxy。"
                "anime_count: external_links_json にプラットフォームリンクを持つ anime 数。"
                "mean_theta_proxy: 国際展開 anime 全体に関与した人材の theta_proxy 平均 "
                "(Tier1 近似 — プラットフォーム固有の per-anime 集計は Tier2 で実施)。"
                "platform_weight: 全プラットフォームで 1.0 に固定 (事前宣言)。"
                "視聴者評価・外部人気指標は算出に含まない (H1 invariant)。"
                "Tier2 追加指標: プラットフォーム固有 mean_theta / 配信国数 / "
                "国際賞受賞重み (Card 16 実施予定)。"
            ),
            section_id="o8_spi",
        )

    # ------------------------------------------------------------------
    # Interpretation (labeled, with alternative)
    # ------------------------------------------------------------------

    def _build_interpretation(
        self, mw_result: MannWhitneyResult | None
    ) -> str:
        if mw_result and abs(mw_result.effect_r) >= 0.1:
            direction = (
                "国際展開 anime に関与した人材は国内専 anime 関与人材より"
                "高い theta_proxy 分布と共起する傾向"
                if mw_result.effect_r > 0
                else "国際展開 anime に関与した人材は国内専 anime 関与人材より"
                "低い theta_proxy 分布と共起する傾向"
            )
            r_str = f"r={mw_result.effect_r:.3f}"
            ci_str = f"[{mw_result.ci_lower:.3f}, {mw_result.ci_upper:.3f}]"
            return (
                f"<p>本レポートの分析者は、{direction}を観察する "
                f"({r_str}, bootstrap 95% CI {ci_str})。</p>"
                "<p>代替解釈: theta_proxy の差は国際展開の構造的特性ではなく、"
                "制作スケールの大きい作品 (多クレジット) が"
                "国際配信に選ばれやすいというセレクション効果を反映する可能性がある。"
                "また、theta_proxy として log(1+credits) を用いる場合、"
                "クレジット数の多さが theta_i の高さを直接意味するわけではないため、"
                "解釈には限界がある。</p>"
                "<p>この解釈が依拠する前提: "
                "external_links_json のプラットフォームリンクが配信事実を近似すること、"
                "および theta_proxy がネットワーク位置の有効な代理指標であること。"
                "これらが成立しない場合、本解釈は修正される。</p>"
            )
        return (
            "<p>国際展開 anime の件数が不十分、または効果量が小さいため、"
            "分布間の差異について解釈を提示しない。</p>"
            "<p>代替解釈: 差異の不在は国際展開と国内専の関与人材構造が類似することを示す可能性、"
            "または現在のデータ充填率では判別が困難であることを示す可能性がある。</p>"
        )

    def _build_intro(self) -> str:
        return (
            "<p>本レポートは AniList の external_links_json を用いて"
            "国際配信プラットフォームへのリンクを持つ anime を特定し、"
            "配信プラットフォーム別の分布と関与人材のネットワーク位置を構造的に測定する。</p>"
            "<p>ソフトパワー指標は海外展開の規模と関与人材の構造的位置の積算値であり、"
            "anime の文化的価値・視聴者評価・人気は含まない (H1 invariant)。"
            "本指標を海外市場戦略立案・人材配置判断の参照情報として提供する。</p>"
            "<p>本レポートは Tier1 (配信プラットフォームリンクのみ) に限定する。"
            "Tier2 (国際賞受賞 / 配信国数 / 海外売上比率) は Card 16 で実施予定。</p>"
        )
