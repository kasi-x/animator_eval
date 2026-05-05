"""Cohort Animation report -- v2 compliant.

Ports ALL 10 original charts from generate_all_reports.py
``generate_cohort_animation_report`` plus gender/tier enrichment.

Sections (14 total):
  1. Gapminder career-stage animation (JS-driven, base64 frames)
  2. PCA ecosystem map (ML cluster colours, IV-based bubble size)
  3. Supply vs workforce (dual-axis: anime count / active persons / demand density)
  4. Cohort debut counts (stacked bar by decade)
  5. Career span boxplots by cohort
  6. Director promotion heatmap (stage >= 4)
  7. Active persons stacked area by cohort
  8. Major-work bubble chart (avg stage x broadcast year)
  9. Major-work involvement vs normal IV comparison
 10. Top-star cumulative credit trajectories
 11. Top-star profile table
 12. ML cluster x cohort heatmap
 13. Inter-generational collaboration matrix

Gender / Tier enrichment woven into Sections 1, 4, 5, 9:
 - 4b: cohort composition by gender
 - 5b: attainment rates by gender
 - 9b: IV distribution by gender x cohort
 - 7b: tier dimension -- cohort directors' tier distribution
"""

from __future__ import annotations

import base64
import hashlib
import json as _json
import math
from collections import Counter
from pathlib import Path

import plotly.graph_objects as go
from plotly.subplots import make_subplots

from ..ci_utils import (
    distribution_summary,
    format_ci,
    format_distribution_inline,
)
from ..color_utils import TIER_PALETTE as _TIER_COLORS, hex_to_rgba as _hex_to_rgba
from ..helpers import (
    get_agg_milestones,
    get_feat_career,
    get_feat_person_scores,
    load_json,
    person_link,
)
from ..html_templates import plotly_div_safe, stratification_tabs, strat_panel
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator

# ── Cohort constants ──────────────────────────────────────────
COHORT_DECADES = [1960, 1970, 1980, 1990, 2000, 2010, 2020]
COHORT_LABELS: dict[int, str] = {
    1960: "1960s",
    1970: "1970s",
    1980: "1980s",
    1990: "1990s",
    2000: "2000s",
    2010: "2010s",
    2020: "2020s",
}
COHORT_COLORS: dict[int, str] = {
    1960: "#7CC8F2",
    1970: "#E09BC2",
    1980: "#E09BC2",
    1990: "#E07532",
    2000: "#3BC494",
    2010: "#F8EC6A",
    2020: "#aaaaaa",
}
STAGE_LABELS: dict[int, str] = {
    1: "動画",
    2: "第二原画",
    3: "原画",
    4: "キャラデ",
    5: "作監・演出",
    6: "監督",
}

# v3: CB-safe gender + decade palette (cross-report consistent)
from src.viz import palettes as _v3_pal  # noqa: E402

_GENDER_COLORS = {
    "Male": _v3_pal.GENDER["M"],
    "Female": _v3_pal.GENDER["F"],
    "unknown": _v3_pal.GENDER["unknown"],
}
_MILESTONE_YEARS = [2000, 2015, 2020]
_DECADE_COLORS = list(_v3_pal.OKABE_ITO_DARK[1:8])


# ── Helpers ───────────────────────────────────────────────────
def _get_cohort_decade(first_year: int | None) -> int:
    if not first_year:
        return 2000
    decade = (first_year // 10) * 10
    if decade <= 1960:
        return 1960
    if decade >= 2020:
        return 2020
    return decade


def _build_stage_timeline(person_ids: set, milestones_data: dict) -> dict:
    result: dict = {}
    for pid in person_ids:
        events = milestones_data.get(pid, [])
        promotions: list[tuple[int, int]] = []
        for e in events:
            if e.get("type") == "promotion" and "to_stage" in e and "year" in e:
                promotions.append((int(e["year"]), int(e["to_stage"])))
        if promotions:
            promotions.sort(key=lambda x: x[0])
            result[pid] = {yr: stg for yr, stg in promotions}
    return result


def _build_cumulative_credits_by_year(
    person_ids: set,
    growth_data: dict,
    scores_by_pid: dict,
    frame_years: list,
) -> dict:
    result: dict = {}
    growth_persons = growth_data.get("persons", {}) if growth_data else {}
    for pid in person_ids:
        score_entry = scores_by_pid.get(pid, {})
        career = score_entry.get("career", {})
        first_year = career.get("first_year") or 1990
        latest_year = career.get("latest_year") or 2025
        total_credits = score_entry.get("total_credits", 0) or 0
        cum: dict[int, int] = {}
        if pid in growth_persons:
            yearly = growth_persons[pid].get("yearly_credits", {})
            running = 0
            for yr in frame_years:
                if yr < first_year:
                    cum[yr] = 0
                    continue
                running += int(yearly.get(str(yr), 0))
                cum[yr] = running
        else:
            span = max(latest_year - first_year + 1, 1)
            credits_per_yr = total_credits / span
            for yr in frame_years:
                elapsed = max(0, min(yr - first_year + 1, span))
                cum[yr] = int(credits_per_yr * elapsed)
        result[pid] = cum
    return result


def _build_major_works_by_year(anime_stats: dict, min_score: float = 7.5) -> dict:
    year_works: dict[int, list] = {}
    for _aid, stats in anime_stats.items():
        yr = stats.get("year")
        sc = stats.get("score") or 0
        if not yr or sc < min_score:
            continue
        yr_int = int(yr)
        year_works.setdefault(yr_int, []).append(
            {
                "title": stats.get("title", "?"),
                "score": float(sc),
                "top_persons": [
                    tp["person_id"] for tp in stats.get("top_persons", [])
                ],
            }
        )
    return {
        yr: sorted(works, key=lambda w: -w["score"])[:3]
        for yr, works in year_works.items()
    }


def _add_milestone_vlines(fig: go.Figure) -> None:
    """Add vertical dashed lines for milestone years (2000, 2015, 2020)."""
    annotations = {
        2000: ("2000年代", "rgba(255,100,100,0.3)", "#E07532"),
        2015: ("配信時代", "rgba(255,255,100,0.4)", "#F8EC6A"),
        2020: ("2020年代", "rgba(170,170,170,0.3)", "#aaaaaa"),
    }
    for yr, (text, line_clr, font_clr) in annotations.items():
        fig.add_vline(
            x=yr,
            line_dash="dash",
            line_color=line_clr,
            annotation_text=text,
            annotation_position="top right",
            annotation_font_color=font_clr,
        )


# ══════════════════════════════════════════════════════════════
# Report class
# ══════════════════════════════════════════════════════════════


class CohortAnimationReport(BaseReportGenerator):
    name = "cohort_animation"
    title = "デビューコホート分析"
    subtitle = (
        "コホート別Gapminder・世代間コラボ・供給需要比較・"
        "性別構成・Tier到達を含む14セクション"
    )
    filename = "cohort_animation.html"

    glossary_terms = {
        "コホート (Cohort)": (
            "デビュー年代で分けた世代グループ"
            "（1970年代デビュー、1980年代デビューなど）。"
        ),
        "キャリアステージ": (
            "1=動画、2=第二原画、3=原画・レイアウト、"
            "4=キャラデサ・脚本、5=作監・演出・部門監督、6=監督 の6段階。"
        ),
        "需要密度": (
            "年間アニメ本数 / アクティブ人材数 x 100。"
            "値が上昇するほど1人あたりの担当作品が増加。"
        ),
        "Gapminder": (
            "GDP/人口/寿命の世界比較で有名なバブルアニメーション可視化手法。"
            "本レポートではキャリア成長に応用。"
        ),
    }

    def generate(self) -> Path | None:
        # ── Load shared data ─────────────────────────────────
        self._scores_data = get_feat_person_scores()
        if not self._scores_data:
            return None

        self._growth_data = get_feat_career() or {}
        self._milestones_data = get_agg_milestones() or {}
        self._anime_stats = load_json("anime_stats.json") or {}
        self._ml_data = load_json("ml_clusters.json") or {}
        self._time_series_data = load_json("time_series.json") or {}
        self._collaborations_data = load_json("collaborations.json") or []

        self._scores_by_pid: dict = {
            p["person_id"]: p for p in self._scores_data
        }

        # Top 600 by iv_score for animation
        top_persons = sorted(
            [
                p
                for p in self._scores_data
                if p.get("career", {}).get("first_year")
            ],
            key=lambda p: -(p.get("iv_score") or 0),
        )[:600]
        self._top_persons = top_persons
        self._top_pids = {p["person_id"] for p in top_persons}
        self._frame_years = list(range(1970, 2026))

        self._stage_tl = _build_stage_timeline(
            self._top_pids, self._milestones_data
        )
        self._cum_credits = _build_cumulative_credits_by_year(
            self._top_pids,
            self._growth_data,
            self._scores_by_pid,
            self._frame_years,
        )
        self._major_works_by_year = _build_major_works_by_year(
            self._anime_stats, min_score=7.5
        )

        # Deterministic Y-jitter per pid
        self._pid_jitter: dict[str, float] = {}
        for p in top_persons:
            pid = p["person_id"]
            h = int(hashlib.md5(pid.encode()).hexdigest()[:4], 16)
            self._pid_jitter[pid] = (h % 100 - 50) / 300.0

        # ── Build sections ───────────────────────────────────
        sb = SectionBuilder()
        sections: list[str] = []

        builders = [
            self._build_gapminder_section,
            self._build_pca_section,
            self._build_supply_demand_section,
            self._build_cohort_debut_section,
            self._build_cohort_debut_gender_section,
            self._build_career_span_section,
            self._build_career_span_gender_section,
            self._build_promotion_heatmap_section,
            self._build_active_area_section,
            self._build_cohort_director_tier_section,
            self._build_major_works_bubble_section,
            self._build_major_vs_normal_section,
            self._build_iv_gender_cohort_section,
            self._build_top_star_trajectory_section,
            self._build_top_star_table_section,
            self._build_cluster_cohort_section,
            self._build_collab_matrix_section,
        ]
        for builder in builders:
            sections.append(sb.build_section(builder(sb)))

        return self.write_report(
            "\n".join(sections),
            extra_glossary=self.glossary_terms,
        )

    # ══════════════════════════════════════════════════════════
    # Section 1: Gapminder animation (Chart 1)
    # ══════════════════════════════════════════════════════════

    def _build_gapminder_section(self, sb: SectionBuilder) -> ReportSection:
        major_top_pids_by_year: dict[int, set] = {}
        for yr, works in self._major_works_by_year.items():
            for w in works:
                if w["score"] >= 8.5:
                    major_top_pids_by_year.setdefault(yr, set()).update(
                        w["top_persons"]
                    )

        gapminder_frames: dict[str, dict] = {}
        for yr in self._frame_years:
            xs: list = []
            ys: list = []
            ss: list = []
            cs: list = []
            ts_txt: list = []
            hls: list = []

            for p in self._top_persons:
                pid = p["person_id"]
                career = p.get("career", {})
                first_year = career.get("first_year") or 1990
                latest_year = career.get("latest_year") or 2025
                if first_year > yr or yr > latest_year + 5:
                    continue

                career_age = yr - first_year

                stl = self._stage_tl.get(pid, {})
                stage = 1
                for ev_yr in sorted(stl.keys()):
                    if ev_yr <= yr:
                        stage = stl[ev_yr]
                    else:
                        break
                if not stl and career_age >= 10:
                    stage = min(career.get("highest_stage") or 1, 4)
                stage = max(1, min(6, stage))

                cum_at_yr = self._cum_credits.get(pid, {}).get(yr, 0)
                size = max(4.0, min(28.0, math.sqrt(max(cum_at_yr, 1)) * 1.8))

                cohort = _get_cohort_decade(first_year)
                color = COHORT_COLORS.get(cohort, "#888888")
                highlight = (
                    1 if pid in major_top_pids_by_year.get(yr, set()) else 0
                )

                name = p.get("name") or p.get("name_ja") or pid
                role = p.get("primary_role", "")
                text = (
                    f"{name}<br>役割: {role}<br>デビュー: {first_year}年"
                    f"<br>年功: {career_age}年<br>累積: {cum_at_yr}作品"
                    f"<br>ステージ: {STAGE_LABELS.get(stage, stage)}"
                )

                xs.append(career_age)
                ys.append(round(stage + self._pid_jitter.get(pid, 0.0), 3))
                ss.append(round(size, 1))
                cs.append(color)
                ts_txt.append(text)
                hls.append(highlight)

            gapminder_frames[str(yr)] = {
                "x": xs,
                "y": ys,
                "s": ss,
                "c": cs,
                "t": ts_txt,
                "h": hls,
            }

        frames_json = _json.dumps(
            gapminder_frames, ensure_ascii=False, separators=(",", ":")
        )
        frames_b64 = base64.b64encode(frames_json.encode("utf-8")).decode(
            "ascii"
        )

        stage_tickvals = list(range(1, 7))
        stage_ticktext = [STAGE_LABELS[s] for s in stage_tickvals]

        legend_html = ""
        for dec in COHORT_DECADES:
            legend_html += (
                '<span style="display:inline-flex;align-items:center;'
                'margin:0 8px 4px 0;">'
                f'<span style="width:12px;height:12px;border-radius:50%;'
                f"background:{COHORT_COLORS[dec]};"
                f'display:inline-block;margin-right:4px;"></span>'
                '<span style="font-size:0.82rem;color:#c0c0d0;">'
                f"{COHORT_LABELS[dec]}</span></span>"
            )

        init_yr = "1980"

        major_works_json = _json.dumps(
            {
                str(yr): works
                for yr, works in self._major_works_by_year.items()
                if 1970 <= yr <= 2025
            },
            ensure_ascii=False,
            separators=(",", ":"),
        )

        # Total persons in the animation
        n_persons = len(self._top_persons)

        findings = (
            f"<p>IV score上位{n_persons}人をプロット。 "
            f"X = 年功（デビューからの経過年数）、"
            f"Y = キャリアステージ（1=動画〜6=監督）、"
            f"バブルサイズ = 累積クレジット数、"
            f"色 = デビュー年代コホート。 "
            f"フレーム範囲: {self._frame_years[0]}--{self._frame_years[-1]}。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        # Build the JS-driven gapminder chart as raw HTML
        vis_html = f"""
<div style="margin:1rem 0;">
  <div style="display:flex;align-items:center;gap:1rem;flex-wrap:wrap;
       margin-bottom:0.8rem;">
    <button id="gap-play" onclick="gapTogglePlay()"
      style="background:linear-gradient(135deg,#f093fb,#f5576c);border:none;
      color:white;padding:0.5rem 1.2rem;border-radius:20px;font-size:0.9rem;
      cursor:pointer;font-weight:600;">
      &#9654; 再生
    </button>
    <span style="color:#f093fb;font-size:1.4rem;font-weight:700;"
      id="gap-year-label">{init_yr}年</span>
    <span style="color:#a0a0c0;font-size:0.85rem;" id="gap-top-work"></span>
  </div>
  <input type="range" id="gap-slider" min="1970" max="2025" value="{init_yr}"
    style="width:100%;accent-color:#f093fb;" oninput="gapSetYear(this.value)">
  <div style="display:flex;justify-content:space-between;font-size:0.75rem;
       color:#606080;margin-top:2px;">
    <span>1970</span><span>1980</span><span>1990</span><span>2000</span>
    <span>2010</span><span>2020</span><span>2025</span>
  </div>
  <div style="margin:0.5rem 0;">{legend_html}</div>
  <div id="gap-chart"
    style="width:100%;height:520px;background:rgba(0,0,0,0.2);
    border-radius:8px;"></div>
</div>
<script>
(function() {{
  var FRAMES = JSON.parse(atob("{frames_b64}"));
  var STAGE_LABELS = {_json.dumps(STAGE_LABELS, ensure_ascii=False)};
  var YEAR_WORKS = {major_works_json};
  var initFrame = FRAMES["{init_yr}"]
                  || {{x:[],y:[],s:[],c:[],t:[],h:[]}};
  var trace = {{
    type: 'scattergl', mode: 'markers',
    x: initFrame.x, y: initFrame.y,
    text: initFrame.t,
    hovertemplate: '%{{text}}<extra></extra>',
    marker: {{
      size: initFrame.s, color: initFrame.c,
      opacity: 0.8, line: {{width: 0}},
    }},
  }};
  var layout = {{
    template: 'plotly_dark',
    paper_bgcolor: 'rgba(0,0,0,0)',
    plot_bgcolor: 'rgba(0,0,0,0.2)',
    font: {{color: '#c0c0d0'}},
    xaxis: {{
      title: '年功（デビューからの経過年数）',
      range: [-1, 46],
      gridcolor: 'rgba(255,255,255,0.07)',
    }},
    yaxis: {{
      title: 'キャリアステージ',
      tickvals: {stage_tickvals},
      ticktext: {_json.dumps(stage_ticktext, ensure_ascii=False)},
      range: [0.5, 6.5],
      gridcolor: 'rgba(255,255,255,0.07)',
    }},
    margin: {{l:70, r:20, t:30, b:60}},
    hovermode: 'closest',
  }};
  Plotly.newPlot('gap-chart', [trace], layout,
    {{responsive:true, displayModeBar:false}});

  var playing = false;
  var currentYear = {int(init_yr)};
  var timer = null;

  function gapUpdate(yr) {{
    currentYear = parseInt(yr);
    var f = FRAMES[String(yr)] || {{x:[],y:[],s:[],c:[],t:[],h:[]}};
    Plotly.restyle('gap-chart', {{
      x: [f.x], y: [f.y], text: [f.t],
      'marker.size': [f.s], 'marker.color': [f.c],
    }}, [0]);
    document.getElementById('gap-year-label').textContent = yr + '年';
    document.getElementById('gap-slider').value = yr;
    var works = YEAR_WORKS[String(yr)] || [];
    var top = works[0];
    if (top && top.score >= 8.5) {{
      document.getElementById('gap-top-work').textContent =
        '大作: ' + top.title + ' ★' + top.score.toFixed(1);
    }} else if (top) {{
      document.getElementById('gap-top-work').textContent =
        '話題作: ' + top.title + ' ★' + top.score.toFixed(1);
    }} else {{
      document.getElementById('gap-top-work').textContent = '';
    }}
  }}

  window.gapSetYear = function(val) {{ gapUpdate(parseInt(val)); }};
  window.gapTogglePlay = function() {{
    playing = !playing;
    var btn = document.getElementById('gap-play');
    if (playing) {{
      btn.textContent = '⏸ 停止';
      if (currentYear >= 2025) currentYear = 1969;
      timer = setInterval(function() {{
        if (currentYear >= 2025) {{
          playing = false;
          btn.textContent = '▶ 再生';
          clearInterval(timer);
          return;
        }}
        currentYear++;
        gapUpdate(currentYear);
      }}, 120);
    }} else {{
      btn.textContent = '▶ 再生';
      clearInterval(timer);
    }}
  }};
  gapUpdate({int(init_yr)});
}})();
</script>
"""
        return ReportSection(
            title="Gapminder: キャリアステージ進化アニメーション",
            findings_html=findings,
            visualization_html=vis_html,
            method_note=(
                f"対象: IVスコア上位{n_persons}人のうち first_year が有効なもの。"
                "ステージは milestones.json の昇進イベントから導出し、"
                "career_age ≥ 10 で昇進データがない場合は highest_stage にフォールバック。"
                "累積クレジット数は growth.json の yearly_credits、"
                "もしくは total_credits からの均等配分。"
                "Yジッターは決定論的（person_id の MD5 ハッシュ）。"
            ),
            section_id="gapminder",
        )

    # ══════════════════════════════════════════════════════════
    # Section 2: PCA ecosystem (Chart 2)
    # ══════════════════════════════════════════════════════════

    def _build_pca_section(self, sb: SectionBuilder) -> ReportSection:
        ml_persons = self._ml_data.get("persons", [])
        if not ml_persons:
            return ReportSection(
                title="エコシステム内ポジション (PCA)",
                findings_html="<p>ml_clusters.jsonのデータが利用できません。</p>",
                section_id="pca_ecosystem",
            )

        pca_sample = [
            p
            for p in ml_persons
            if p.get("pca_2d") and len(p["pca_2d"]) == 2
        ][:2000]

        if not pca_sample:
            return ReportSection(
                title="エコシステム内ポジション (PCA)",
                findings_html="<p>PCA座標を持つ人物が見つかりません。</p>",
                section_id="pca_ecosystem",
            )

        cluster_names = sorted(
            set(p.get("cluster_name", "?") for p in pca_sample)
        )
        cluster_colors_auto = [
            "#E09BC2", "#E07532", "#FFB444", "#3593D2", "#3BC494",
            "#F8EC6A", "#7CC8F2", "#E09BC2", "#E07532", "#7CC8F2",
        ]
        cn_color = {
            cn: cluster_colors_auto[i % len(cluster_colors_auto)]
            for i, cn in enumerate(cluster_names)
        }

        cluster_to_pts: dict = {}
        for p in pca_sample:
            cn = p.get("cluster_name", "?")
            cluster_to_pts.setdefault(cn, []).append(p)

        fig = go.Figure()
        for cn in cluster_names:
            pts = cluster_to_pts[cn]
            pid_list = [q["person_id"] for q in pts]
            iv_scores = [
                self._scores_by_pid.get(pid, {}).get("iv_score", 10) or 10
                for pid in pid_list
            ]
            fig.add_trace(
                go.Scattergl(
                    x=[q["pca_2d"][0] for q in pts],
                    y=[q["pca_2d"][1] for q in pts],
                    mode="markers",
                    name=cn,
                    text=[q.get("name", q["person_id"]) for q in pts],
                    hovertemplate="%{text}<extra>" + cn + "</extra>",
                    marker=dict(
                        color=cn_color.get(cn, "#888"),
                        size=[
                            max(4, min(16, abs(c or 1) ** 0.4))
                            for c in iv_scores
                        ],
                        opacity=0.7,
                        line=dict(width=0),
                    ),
                )
            )

        n_pts = len(pca_sample)
        n_clusters = len(cluster_names)
        findings = (
            f"<p>{n_pts:,}人を{n_clusters}個のMLクラスタにわたってPCA投影。"
            f"バブルサイズはIV score^0.4に比例。"
            f"クラスタ割当はml_clusters.jsonによる。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="エコシステム内ポジション (PCA)",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "pca-ecosystem-chart", height=520
            ),
            method_note=(
                "20次元の特徴量ベクトルをPCAで2次元に縮約。"
                "クラスタは完全な特徴量空間に対するK-Means由来。"
                f"サンプル上限2,000人（実際: {n_pts:,}人）。"
            ),
            section_id="pca_ecosystem",
        )

    # ══════════════════════════════════════════════════════════
    # Section 3: Supply vs workforce (Chart 13 in original)
    # ══════════════════════════════════════════════════════════

    def _build_supply_demand_section(
        self, sb: SectionBuilder
    ) -> ReportSection:
        ts_series = self._time_series_data.get("series", {})
        unique_anime_raw = ts_series.get("unique_anime", {})
        active_persons_raw = ts_series.get("active_persons", {})
        new_entrants_raw = ts_series.get("new_entrants", {})

        if not ts_series or not unique_anime_raw or not active_persons_raw:
            return ReportSection(
                title="仕事の供給と担い手の関係",
                findings_html="<p>time_series.jsonのデータが利用できません。</p>",
                section_id="supply_demand",
            )

        sup_years: list[int] = []
        sup_anime: list[int] = []
        sup_persons: list[int] = []
        sup_new: list[int] = []
        sup_ratio: list[float] = []

        for yr in range(1970, 2025):
            yr_s = str(yr)
            anime_cnt = int(unique_anime_raw.get(yr_s, 0) or 0)
            persons_cnt = int(active_persons_raw.get(yr_s, 0) or 0)
            new_cnt = int(new_entrants_raw.get(yr_s, 0) or 0)
            if anime_cnt == 0 and persons_cnt == 0:
                continue
            sup_years.append(yr)
            sup_anime.append(anime_cnt)
            sup_persons.append(persons_cnt)
            sup_new.append(new_cnt)
            ratio = anime_cnt / max(persons_cnt, 1) * 100
            sup_ratio.append(round(ratio, 3))

        if not sup_years:
            return ReportSection(
                title="仕事の供給と担い手の関係",
                findings_html="<p>時系列データが不足しています。</p>",
                section_id="supply_demand",
            )

        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(
            go.Bar(
                x=sup_years,
                y=sup_anime,
                name="年間アニメ本数（供給）",
                marker_color="rgba(76, 201, 240, 0.6)",
                hovertemplate="%{x}年: %{y}本<extra></extra>",
            ),
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(
                x=sup_years,
                y=sup_persons,
                mode="lines",
                name="アクティブ人数（担い手）",
                line=dict(color="#3BC494", width=2),
                hovertemplate="%{x}年: %{y}人<extra></extra>",
            ),
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(
                x=sup_years,
                y=sup_new,
                mode="lines",
                name="新規参入者数",
                line=dict(color="#F8EC6A", width=1.5, dash="dot"),
                hovertemplate="%{x}年: %{y}人<extra></extra>",
            ),
            secondary_y=False,
        )
        fig.add_trace(
            go.Scatter(
                x=sup_years,
                y=sup_ratio,
                mode="lines",
                name="需要密度 (本/100人)",
                line=dict(color="#EF476F", width=2.5),
                hovertemplate="%{x}年: %{y:.1f}本/100人<extra></extra>",
            ),
            secondary_y=True,
        )

        _add_milestone_vlines(fig)

        fig.update_layout(
            title="アニメ本数 vs アクティブ人材数（1970--2024）",
            xaxis_title="年",
            barmode="overlay",
        )
        fig.update_yaxes(title_text="本数 / 人数", secondary_y=False)
        fig.update_yaxes(title_text="需要密度 (本/100人)", secondary_y=True)

        max_ratio_yr = sup_years[sup_ratio.index(max(sup_ratio))]
        min_ratio_yr = sup_years[sup_ratio.index(min(sup_ratio))]

        findings = (
            f"<p>対象期間: {sup_years[0]}--{sup_years[-1]}"
            f"（データのある{len(sup_years)}年間）。 "
            f"需要密度ピーク: {max_ratio_yr}年"
            f"（100人あたり{max(sup_ratio):.1f}本）。 "
            f"需要密度最低: {min_ratio_yr}年"
            f"（100人あたり{min(sup_ratio):.1f}本）。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="仕事の供給と担い手の関係",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "supply-demand-chart", height=500
            ),
            method_note=(
                "作品数 = time_series.json の unique_anime。"
                "稼働人数 = その年に少なくとも1件クレジットがある人数。"
                "新規参入 = その年に初クレジットが現れた人数。"
                "需要密度 = 作品数 / 稼働人数 × 100。"
                "縦線は2000年、2015年（配信時代）、2020年を示す。"
            ),
            section_id="supply_demand",
        )

    # ══════════════════════════════════════════════════════════
    # Section 4: Cohort debut counts (Chart 3)
    # ══════════════════════════════════════════════════════════

    def _build_cohort_debut_section(
        self, sb: SectionBuilder
    ) -> ReportSection:
        cohort_by_year: dict[int, dict[int, int]] = {}
        for p in self._scores_data:
            fy = p.get("career", {}).get("first_year")
            if not fy or not (1960 <= fy <= 2025):
                continue
            dec = _get_cohort_decade(fy)
            cohort_by_year.setdefault(fy, {}).setdefault(dec, 0)
            cohort_by_year[fy][dec] += 1

        debut_years = sorted(y for y in cohort_by_year if 1970 <= y <= 2025)

        if not debut_years:
            return ReportSection(
                title="世代別デビュー数（累積）",
                findings_html="<p>デビューデータが利用できません。</p>",
                section_id="cohort_debut",
            )

        fig = go.Figure()
        for dec in COHORT_DECADES:
            counts = [
                cohort_by_year.get(yr, {}).get(dec, 0) for yr in debut_years
            ]
            if sum(counts) == 0:
                continue
            fig.add_trace(
                go.Bar(
                    x=debut_years,
                    y=counts,
                    name=COHORT_LABELS[dec],
                    marker_color=COHORT_COLORS[dec],
                    hovertemplate=(
                        "%{x}年: %{y}人<extra>"
                        + COHORT_LABELS[dec]
                        + "</extra>"
                    ),
                )
            )
        fig.update_layout(
            barmode="stack",
            title="年別デビュー人数（コホート別）",
            xaxis_title="年",
            yaxis_title="人数",
        )
        _add_milestone_vlines(fig)

        total_n = sum(
            sum(d.values())
            for y, d in cohort_by_year.items()
            if 1970 <= y <= 2025
        )
        cohort_totals = {}
        for y, d in cohort_by_year.items():
            if 1970 <= y <= 2025:
                for dec, cnt in d.items():
                    cohort_totals[dec] = cohort_totals.get(dec, 0) + cnt

        findings = (
            f"<p>デビュー総数: {total_n:,}人"
            f"（{debut_years[0]}--{debut_years[-1]}）。 "
            "コホート別: "
            + ", ".join(
                f"{COHORT_LABELS[dec]}: {cohort_totals.get(dec, 0):,}人"
                for dec in COHORT_DECADES
                if cohort_totals.get(dec, 0) > 0
            )
            + "。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="世代別デビュー数（累積）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "cohort-debut-chart", height=420
            ),
            method_note=(
                "Debut year = first_year from scores.json career object. "
                "Cohort decade = floor(first_year / 10) * 10, "
                "clamped to [1960, 2020]."
            ),
            section_id="cohort_debut",
        )

    # ══════════════════════════════════════════════════════════
    # Section 4b: Cohort composition by gender (enrichment)
    # ══════════════════════════════════════════════════════════

    def _build_cohort_debut_gender_section(
        self, sb: SectionBuilder
    ) -> ReportSection:
        try:
            rows = self.conn.execute(
                """
                SELECT
                    (fc.first_year / 10) * 10 AS debut_decade,
                    p.gender,
                    COUNT(*) AS n
                FROM feat_career fc
                JOIN conformed.persons p ON fc.person_id = p.id
                WHERE fc.first_year BETWEEN 1960 AND 2025
                GROUP BY debut_decade, p.gender
                ORDER BY debut_decade, p.gender
            """
            ).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="コホート構成: 性別内訳",
                findings_html="<p>DBから性別データを取得できません。</p>",
                section_id="cohort_gender",
            )

        decade_gender: dict[int, dict[str, int]] = {}
        for r in rows:
            decade_gender.setdefault(r["debut_decade"], {})[
                r["gender"] or "unknown"
            ] = r["n"]

        decades = sorted(decade_gender.keys())
        all_genders = sorted({g for d in decade_gender.values() for g in d})

        fig = go.Figure()
        for g in all_genders:
            fig.add_trace(
                go.Bar(
                    x=[str(d) for d in decades],
                    y=[decade_gender[d].get(g, 0) for d in decades],
                    name=g,
                    marker_color=_GENDER_COLORS.get(g, "#a0a0c0"),
                )
            )
        fig.update_layout(
            title="デビューコホート構成（性別別）",
            barmode="stack",
            xaxis_title="デビュー年代",
            yaxis_title="人数",
        )

        findings = "<p>コホート構成（性別別）:</p><ul>"
        for d in decades:
            total = sum(decade_gender[d].values())
            g_parts = []
            for g in sorted(decade_gender[d].keys()):
                n = decade_gender[d][g]
                pct = 100 * n / max(total, 1)
                g_parts.append(f"{g}: {n:,} ({pct:.0f}%)")
            findings += (
                f"<li><strong>{d}s</strong> (n={total:,}): "
                f"{', '.join(g_parts)}</li>"
            )
        findings += "</ul>"

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="コホート構成: 性別内訳",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_cohort_gender", height=420
            ),
            method_note=(
                "性別は persons.gender 由来（NULL は 'unknown' にマッピング）。"
                "年代 = floor(feat_career.first_year / 10) * 10。"
                "feat_career に登録されている人物のみ含む。"
            ),
            section_id="cohort_gender",
        )

    # ══════════════════════════════════════════════════════════
    # Section 5: Career span boxplot (Chart 4)
    # ══════════════════════════════════════════════════════════

    def _build_career_span_section(
        self, sb: SectionBuilder
    ) -> ReportSection:
        fig = go.Figure()
        findings_parts: list[str] = []

        for dec in COHORT_DECADES:
            spans = []
            for p in self._scores_data:
                fy = p.get("career", {}).get("first_year")
                ly = p.get("career", {}).get("latest_year")
                if fy and ly and _get_cohort_decade(fy) == dec:
                    sp = ly - fy
                    if 0 <= sp <= 60:
                        spans.append(sp)
            if len(spans) < 5:
                continue
            fig.add_trace(
                go.Box(
                    y=spans,
                    name=COHORT_LABELS[dec],
                    marker_color=COHORT_COLORS[dec],
                    boxmean=True,
                    hovertemplate=(
                        "活動年数: %{y}年<extra>"
                        + COHORT_LABELS[dec]
                        + "</extra>"
                    ),
                )
            )
            ds = distribution_summary(spans, label=COHORT_LABELS[dec])
            findings_parts.append(
                f"<li>{COHORT_LABELS[dec]} (n={ds['n']:,}): "
                f"{format_distribution_inline(ds)}</li>"
            )

        fig.update_layout(
            title="コホート別キャリアスパン分布",
            yaxis_title="活動年数",
        )
        # Add median line
        fig.add_hline(
            y=10,
            line_dash="dot",
            line_color="rgba(255,255,255,0.2)",
            annotation_text="10yr",
            annotation_font_color="#808090",
        )

        findings = (
            "<p>キャリアスパン（latest_year - first_year）の"
            "デビュー年代コホート別分布:</p><ul>"
            + "".join(findings_parts)
            + "</ul>"
            "<p>右側打ち切り: 最近のコホートはキャリア継続中のため、"
            "観測されるスパンが短くなる。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="コホート別キャリアスパン",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "career-span-chart", height=420
            ),
            method_note=(
                "Career span = latest_year - first_year from scores.json. "
                "Capped to [0, 60]. Cohorts with n < 5 are excluded. "
                "Right-censoring is not corrected."
            ),
            section_id="career_span",
        )

    # ══════════════════════════════════════════════════════════
    # Section 5b: Attainment rates by gender (enrichment)
    # ══════════════════════════════════════════════════════════

    def _build_career_span_gender_section(
        self, sb: SectionBuilder
    ) -> ReportSection:
        try:
            rows = self.conn.execute(
                """
                SELECT
                    (fc.first_year / 10) * 10 AS debut_decade,
                    p.gender,
                    fc.highest_stage,
                    COUNT(*) AS n
                FROM feat_career fc
                JOIN conformed.persons p ON fc.person_id = p.id
                WHERE fc.first_year BETWEEN 1960 AND 2019
                  AND fc.highest_stage IS NOT NULL
                GROUP BY debut_decade, p.gender, fc.highest_stage
                ORDER BY debut_decade, p.gender, fc.highest_stage
            """
            ).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="性別別最高ステージ到達率",
                findings_html=(
                    "<p>性別×ステージ到達データが利用できません。</p>"
                ),
                section_id="gender_attainment",
            )

        # Organize: {gender: {decade: {stage: count}}}
        data: dict[str, dict[int, dict[int, int]]] = {}
        for r in rows:
            g = r["gender"] or "unknown"
            data.setdefault(g, {}).setdefault(r["debut_decade"], {})[
                r["highest_stage"]
            ] = r["n"]

        # Create tabs by gender
        genders = sorted(data.keys())
        tab_axes = {g: g for g in genders}
        tabs_html = stratification_tabs("gender_stage_tabs", tab_axes)

        panels = ""
        for gi, g in enumerate(genders):
            fig = go.Figure()
            decades = sorted(data[g].keys())
            for stage in sorted(
                {s for dd in data[g].values() for s in dd}
            ):
                totals = {
                    d: sum(data[g][d].values()) for d in decades
                }
                fig.add_trace(
                    go.Bar(
                        x=[str(d) for d in decades],
                        y=[
                            100
                            * data[g][d].get(stage, 0)
                            / max(totals[d], 1)
                            for d in decades
                        ],
                        name=STAGE_LABELS.get(stage, f"Stage {stage}"),
                    )
                )
            fig.update_layout(
                barmode="stack",
                title=f"コホート別 最高ステージ到達状況 ({g})",
                xaxis_title="デビュー年代",
                yaxis_title="性別-コホート内の割合 (%)",
            )
            chart_html = plotly_div_safe(
                fig, f"gender-stage-{gi}", height=400
            )
            panels += strat_panel(
                "gender_stage_tabs", g, chart_html, active=(gi == 0)
            )

        # Findings with counts per gender
        findings = (
            "<p>性別およびデビュー年代別の最高キャリアステージ到達状況:</p><ul>"
        )
        for g in genders:
            total_g = sum(
                sum(dd.values()) for dd in data[g].values()
            )
            stage4plus = sum(
                cnt
                for dd in data[g].values()
                for s, cnt in dd.items()
                if s >= 4
            )
            pct = 100 * stage4plus / max(total_g, 1)
            findings += (
                f"<li>{g}（n={total_g:,}）: "
                f"ステージ4以上到達率 = {pct:.1f}%</li>"
            )
        findings += "</ul>"

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="性別別最高ステージ到達率",
            findings_html=findings,
            visualization_html=tabs_html + panels,
            method_note=(
                "最高ステージは feat_career.highest_stage 由来。"
                "性別は persons.gender 由来。"
                "割合は性別×年代コホート内で算出。"
                "右打ち切り: 直近コホートは上位ステージ到達までの"
                "時間が短いことに注意。"
            ),
            section_id="gender_attainment",
        )

    # ══════════════════════════════════════════════════════════
    # Section 6: Director promotion heatmap (Chart 5)
    # ══════════════════════════════════════════════════════════

    def _build_promotion_heatmap_section(
        self, sb: SectionBuilder
    ) -> ReportSection:
        promo_heatmap: dict[int, dict[int, int]] = {}
        for pid, events in self._milestones_data.items():
            p_data = self._scores_by_pid.get(pid, {})
            fy = p_data.get("career", {}).get("first_year")
            if not fy:
                continue
            dec = _get_cohort_decade(fy)
            for e in events:
                if (
                    e.get("type") == "promotion"
                    and e.get("to_stage", 0) >= 4
                ):
                    yr = e.get("year")
                    if yr and 1970 <= yr <= 2025:
                        promo_heatmap.setdefault(yr, {}).setdefault(dec, 0)
                        promo_heatmap[yr][dec] += 1

        if not promo_heatmap:
            return ReportSection(
                title="監督昇進ヒートマップ",
                findings_html="<p>昇進データが利用できません。</p>",
                section_id="promotion_heatmap",
            )

        ph_years = sorted(y for y in promo_heatmap if 1970 <= y <= 2025)
        z_data = [
            [promo_heatmap.get(yr, {}).get(dec, 0) for yr in ph_years]
            for dec in COHORT_DECADES
        ]

        fig = go.Figure(
            go.Heatmap(
                x=ph_years,
                y=[COHORT_LABELS[d] for d in COHORT_DECADES],
                z=z_data,
                colorscale="Plasma",
                hovertemplate=(
                    "%{x}年 / %{y}: %{z}人昇進<extra></extra>"
                ),
                colorbar=dict(title="昇進人数"),
            )
        )
        fig.update_layout(
            title="作監以上への昇進数ヒートマップ",
            xaxis_title="年",
            yaxis_title="コホート",
        )

        total_promos = sum(
            sum(d.values()) for d in promo_heatmap.values()
        )
        peak_yr = max(ph_years, key=lambda y: sum(promo_heatmap[y].values()))
        peak_cnt = sum(promo_heatmap[peak_yr].values())

        findings = (
            f"<p>ステージ4以上への昇進総数: {total_promos:,}件"
            f"（{len(ph_years)}年間にわたる）。 "
            f"ピーク年: {peak_yr}年（{peak_cnt}件の昇進）。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="監督昇進ヒートマップ",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "promo-heatmap", height=400
            ),
            method_note=(
                "昇進イベントは milestones.json の"
                "type='promotion' かつ to_stage ≥ 4 のもの。"
                "人物は scores.json の first_year でコホートに割り当て。"
                "同一人物の複数回昇進（例: stage 4 ののち stage 5）は"
                "個別に計上。"
            ),
            section_id="promotion_heatmap",
        )

    # ══════════════════════════════════════════════════════════
    # Section 7: Active persons stacked area (Chart 6)
    # ══════════════════════════════════════════════════════════

    def _build_active_area_section(
        self, sb: SectionBuilder
    ) -> ReportSection:
        active_by_yr_cohort: dict[int, dict[int, int]] = {}
        for p in self._scores_data:
            fy = p.get("career", {}).get("first_year")
            ly = p.get("career", {}).get("latest_year") or 2020
            if not fy or not (1960 <= fy <= 2025):
                continue
            dec = _get_cohort_decade(fy)
            for yr in range(max(fy, 1970), min(ly + 1, 2026)):
                active_by_yr_cohort.setdefault(yr, {}).setdefault(dec, 0)
                active_by_yr_cohort[yr][dec] += 1

        act_years = sorted(active_by_yr_cohort.keys())
        if not act_years:
            return ReportSection(
                title="世代別アクティブ人数推移",
                findings_html="<p>アクティブ人数データがありません。</p>",
                section_id="active_area",
            )

        fig = go.Figure()
        for dec in COHORT_DECADES:
            counts = [
                active_by_yr_cohort.get(yr, {}).get(dec, 0)
                for yr in act_years
            ]
            if sum(counts) == 0:
                continue
            fill_rgba = _hex_to_rgba(COHORT_COLORS[dec], 0.4)
            fig.add_trace(
                go.Scatter(
                    x=act_years,
                    y=counts,
                    mode="lines",
                    name=COHORT_LABELS[dec],
                    fill="tonexty",
                    line=dict(color=COHORT_COLORS[dec], width=0.5),
                    fillcolor=fill_rgba,
                    stackgroup="one",
                    hovertemplate=(
                        "%{x}年: %{y}人<extra>"
                        + COHORT_LABELS[dec]
                        + "</extra>"
                    ),
                )
            )
        fig.update_layout(
            title="世代別アクティブ人数（積み上げ面グラフ）",
            xaxis_title="年",
            yaxis_title="アクティブ人数",
        )
        _add_milestone_vlines(fig)

        # Peak active year
        peak_yr = max(
            act_years,
            key=lambda y: sum(active_by_yr_cohort[y].values()),
        )
        peak_total = sum(active_by_yr_cohort[peak_yr].values())

        findings = (
            f"<p>コホート別アクティブ人数の積み上げ面グラフ、"
            f"{act_years[0]}--{act_years[-1]}。 "
            f"アクティブ人数ピーク年: {peak_yr}年（{peak_total:,}人）。 "
            f"アクティブの定義: first_year <= 年 <= latest_year。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="世代別アクティブ人数推移",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "active-area-chart", height=420
            ),
            method_note=(
                "A person is active in year Y if first_year <= Y <= "
                "latest_year. This is a generous definition -- "
                "gaps within the career window are not detected."
            ),
            section_id="active_area",
        )

    # ══════════════════════════════════════════════════════════
    # Section 7b: Cohort directors' tier distribution (enrichment)
    # ══════════════════════════════════════════════════════════

    def _build_cohort_director_tier_section(
        self, sb: SectionBuilder
    ) -> ReportSection:
        try:
            rows = self.conn.execute(
                """
                SELECT
                    (fc.first_year / 10) * 10 AS debut_decade,
                    fwc.scale_tier AS tier,
                    COUNT(DISTINCT fcc.person_id) AS n
                FROM feat_credit_contribution fcc
                JOIN feat_career fc ON fcc.person_id = fc.person_id
                JOIN feat_work_context fwc ON fcc.anime_id = fwc.anime_id
                WHERE fcc.role_category = 'direction'
                  AND fwc.scale_tier IS NOT NULL
                  AND fc.first_year BETWEEN 1960 AND 2025
                GROUP BY debut_decade, fwc.scale_tier
                ORDER BY debut_decade, fwc.scale_tier
            """
            ).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="コホート別監督: Tier分布",
                findings_html=(
                    "<p>監督Tier分布データが利用できません。</p>"
                ),
                section_id="cohort_dir_tier",
            )

        # {decade: {tier: n_directors}}
        decade_tier: dict[int, dict[int, int]] = {}
        for r in rows:
            decade_tier.setdefault(r["debut_decade"], {})[r["tier"]] = r["n"]

        decades = sorted(decade_tier.keys())
        all_tiers = sorted({t for dd in decade_tier.values() for t in dd})

        fig = go.Figure()
        for tier in all_tiers:
            totals = {
                d: sum(decade_tier[d].values()) for d in decades
            }
            fig.add_trace(
                go.Bar(
                    x=[str(d) for d in decades],
                    y=[
                        100
                        * decade_tier[d].get(tier, 0)
                        / max(totals[d], 1)
                        for d in decades
                    ],
                    name=f"Tier {tier}",
                    marker_color=_TIER_COLORS.get(tier, "#a0a0c0"),
                )
            )
        fig.update_layout(
            barmode="stack",
            title="監督クレジット: デビューコホート別Tier分布",
            xaxis_title="デビュー年代",
            yaxis_title="コホートの監督クレジットに占める割合 (%)",
        )

        findings = (
            "<p>各コホートの監督がクレジットされた作品のTier分布:</p><ul>"
        )
        for d in decades:
            total = sum(decade_tier[d].values())
            t_parts = ", ".join(
                f"T{t}: {100 * decade_tier[d].get(t, 0) / max(total, 1):.0f}%"
                for t in all_tiers
            )
            findings += (
                f"<li>{d}年代（n={total:,}人の監督）: {t_parts}</li>"
            )
        findings += "</ul>"

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="コホート別監督: Tier分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "cohort-dir-tier", height=420
            ),
            method_note=(
                "監督クレジットは feat_credit_contribution の"
                "role_category = 'direction'。"
                "Tier は feat_work_context.scale_tier 由来。"
                "同一人物が異なる規模の作品を監督した場合、"
                "複数Tierに出現し得る。"
            ),
            section_id="cohort_dir_tier",
        )

    # ══════════════════════════════════════════════════════════
    # Section 8: Major works bubble (Chart 7)
    # ══════════════════════════════════════════════════════════

    def _build_major_works_bubble_section(
        self, sb: SectionBuilder
    ) -> ReportSection:
        major_works_list = [
            (aid, stats)
            for aid, stats in self._anime_stats.items()
            if (stats.get("score") or 0) >= 8.0
            and stats.get("year")
            and stats.get("top_persons")
        ]
        major_works_list.sort(key=lambda x: -(x[1].get("score") or 0))
        major_works_list = major_works_list[:60]

        fig_x: list = []
        fig_y: list = []
        fig_s: list = []
        fig_c: list = []
        fig_t: list = []

        for _aid, stats in major_works_list:
            yr = int(stats.get("year") or 0)
            top_pids = [
                tp["person_id"] for tp in stats.get("top_persons", [])
            ]
            if not top_pids or yr < 1970:
                continue
            cohorts: list[int] = []
            stages: list[int] = []
            for pid in top_pids:
                p = self._scores_by_pid.get(pid, {})
                fy = p.get("career", {}).get("first_year")
                hs = p.get("career", {}).get("highest_stage")
                if fy:
                    cohorts.append(_get_cohort_decade(fy))
                if hs:
                    stages.append(int(hs))
            if not cohorts:
                continue
            avg_stage = sum(stages) / len(stages) if stages else 3.0
            main_cohort = Counter(cohorts).most_common(1)[0][0]
            top_names = [
                self._scores_by_pid.get(pid, {}).get("name") or pid
                for pid in top_pids[:3]
            ]
            title = stats.get("title", "?")
            hover = (
                f"{title}<br>年: {yr}年<br>"
                f"主要コホート: {COHORT_LABELS.get(main_cohort, '?')}<br>"
                f"スタッフ: {', '.join(top_names)}"
            )
            fig_x.append(yr)
            fig_y.append(round(avg_stage, 2))
            fig_s.append(max(6, min(40, len(top_pids) * 2.5)))
            fig_c.append(COHORT_COLORS.get(main_cohort, "#888"))
            fig_t.append(hover)

        if not fig_x:
            return ReportSection(
                title="大作アニメ x 関与世代バブルチャート",
                findings_html="<p>大作データが利用できません。</p>",
                section_id="major_works_bubble",
            )

        fig = go.Figure(
            go.Scatter(
                x=fig_x,
                y=fig_y,
                mode="markers",
                text=fig_t,
                hovertemplate="%{text}<extra></extra>",
                marker=dict(
                    size=fig_s,
                    color=fig_c,
                    opacity=0.8,
                    line=dict(color="white", width=0.5),
                ),
            )
        )
        fig.update_layout(
            title="大作アニメ (score >= 8.0) x 関与世代 x 平均ステージ",
            xaxis_title="放送年",
            yaxis_title="関与スタッフの平均キャリアステージ",
            yaxis=dict(
                tickvals=list(range(1, 7)),
                ticktext=[STAGE_LABELS[s] for s in range(1, 7)],
            ),
        )
        _add_milestone_vlines(fig)

        findings = (
            f"<p>score >= 8.0の{len(fig_x)}作品をプロット。 "
            f"X = 放送年、Y = クレジットされたスタッフの平均キャリアステージ、"
            f"バブルサイズ = スタッフ数、色 = 最頻デビューコホート。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="大作アニメ x 関与世代バブルチャート",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "major-works-chart", height=460
            ),
            method_note=(
                "anime_stats.json のスコア上位60作品（score ≥ 8.0）。"
                "平均ステージ = top_persons 全員の highest_stage の平均。"
                "最頻コホート = top_persons のデビュー年代で最頻のもの。"
                "ここでのスコアは情報メタデータとして保存されている anime.score であり、"
                "スコアリング式には一切使用していない。"
            ),
            section_id="major_works_bubble",
        )

    # ══════════════════════════════════════════════════════════
    # Section 9: Major vs normal IV comparison (Chart 8)
    # ══════════════════════════════════════════════════════════

    def _build_major_vs_normal_section(
        self, sb: SectionBuilder
    ) -> ReportSection:
        major_pids_set: set = set()
        for _aid, stats in self._anime_stats.items():
            if (stats.get("score") or 0) >= 8.5:
                for tp in stats.get("top_persons", []):
                    major_pids_set.add(tp["person_id"])

        major_iv: list[float] = []
        normal_iv: list[float] = []
        for p in self._scores_data:
            c = p.get("iv_score")
            if c is None or c <= 0:
                continue
            if p["person_id"] in major_pids_set:
                major_iv.append(float(c))
            else:
                normal_iv.append(float(c))

        fig = go.Figure()
        for label, data, color in [
            (f"大作関与 (n={len(major_iv):,})", major_iv, "#E07532"),
            (f"通常作品 (n={len(normal_iv):,})", normal_iv, "#7CC8F2"),
        ]:
            if data:
                fig.add_trace(
                    go.Box(
                        y=data,
                        name=label,
                        marker_color=color,
                        boxmean=True,
                        hovertemplate="IV Score: %{y:.1f}<extra></extra>",
                    )
                )
        fig.update_layout(
            title="大作関与 vs 通常作品のIV Score分布",
            yaxis_title="IVスコア",
        )

        ds_major = distribution_summary(major_iv, label="major")
        ds_normal = distribution_summary(normal_iv, label="normal")

        findings = (
            f"<p>IV score分布: "
            f"大作関与者（n={ds_major['n']:,}）: "
            f"{format_distribution_inline(ds_major)}, "
            f"{format_ci((ds_major['ci_lower'], ds_major['ci_upper']))}。 "
            f"非大作（n={ds_normal['n']:,}）: "
            f"{format_distribution_inline(ds_normal)}, "
            f"{format_ci((ds_normal['ci_lower'], ds_normal['ci_upper']))}。"
            "</p>"
            "<p>大作の定義: anime.score >= 8.5"
            "（情報メタデータであり、スコアリングへの入力ではない）。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="大作関与 vs 通常作品: IV Score分布",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "major-vs-normal-chart", height=420
            ),
            method_note=(
                "大作関与 = anime.score ≥ 8.5 の作品の top_persons に"
                "クレジットされた人物。"
                "IV スコア = パイプラインスコアリング由来の統合価値。"
                "本比較は観察的であり、大作に関与した人物は"
                "多くの交絡要因で異なる可能性がある。"
            ),
            section_id="major_vs_normal",
        )

    # ══════════════════════════════════════════════════════════
    # Section 9b: IV distribution by gender x cohort (enrichment)
    # ══════════════════════════════════════════════════════════

    def _build_iv_gender_cohort_section(
        self, sb: SectionBuilder
    ) -> ReportSection:
        try:
            rows = self.conn.execute(
                """
                SELECT
                    (fc.first_year / 10) * 10 AS debut_decade,
                    p.gender,
                    fps.iv_score
                FROM feat_person_scores fps
                JOIN conformed.persons p ON fps.person_id = p.id
                JOIN feat_career fc ON fps.person_id = fc.person_id
                WHERE fps.iv_score IS NOT NULL
                  AND fc.first_year BETWEEN 1960 AND 2025
            """
            ).fetchall()
        except Exception:
            rows = []

        if not rows:
            return ReportSection(
                title="IV分布: 性別 x コホート",
                findings_html=(
                    "<p>性別×コホートのIVデータが利用できません。</p>"
                ),
                section_id="iv_gender_cohort",
            )

        # {gender: {decade: [iv_scores]}}
        data: dict[str, dict[int, list[float]]] = {}
        for r in rows:
            g = r["gender"] or "unknown"
            data.setdefault(g, {}).setdefault(r["debut_decade"], []).append(
                float(r["iv_score"])
            )

        genders = sorted(data.keys())

        # Tabbed panels by gender
        tab_axes = {g: g for g in genders}
        tabs_html = stratification_tabs("iv_gender_tabs", tab_axes)

        panels = ""
        findings_parts: list[str] = []
        for gi, g in enumerate(genders):
            fig = go.Figure()
            decades = sorted(data[g].keys())
            for di, d in enumerate(decades):
                vals = data[g][d]
                if len(vals) < 3:
                    continue
                fig.add_trace(
                    go.Box(
                        y=vals[:500] if len(vals) > 500 else vals,
                        name=f"{d}s (n={len(vals):,})",
                        marker_color=_DECADE_COLORS[di % len(_DECADE_COLORS)],
                        boxpoints=False,
                    )
                )
                ds = distribution_summary(vals, label=f"{d}s")
                findings_parts.append(
                    f"<li>{g}, {d}s (n={ds['n']:,}): "
                    f"{format_distribution_inline(ds)}</li>"
                )
            fig.update_layout(
                title=f"デビューコホート別 IVスコア ({g})",
                xaxis_title="コホート",
                yaxis_title="IVスコア",
            )
            chart_html = plotly_div_safe(
                fig, f"iv-gender-{gi}", height=420
            )
            panels += strat_panel(
                "iv_gender_tabs", g, chart_html, active=(gi == 0)
            )

        findings = (
            "<p>性別およびデビュー年代別のIV score分布"
            f"（合計: {len(rows):,}件の観測）:</p><ul>"
            + "".join(findings_parts)
            + "</ul>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="IV分布: 性別 x コホート",
            findings_html=findings,
            visualization_html=tabs_html + panels,
            method_note=(
                "IV スコアは feat_person_scores.iv_score 由来。"
                "性別は persons.gender 由来。"
                "年代は feat_career.first_year 由来。"
                "ボックスプロットは描画のためグループあたり最大500点。"
            ),
            section_id="iv_gender_cohort",
        )

    # ══════════════════════════════════════════════════════════
    # Section 10: Top star trajectories (Chart 9)
    # ══════════════════════════════════════════════════════════

    def _build_top_star_trajectory_section(
        self, sb: SectionBuilder
    ) -> ReportSection:
        fig = go.Figure()
        n_stars = 0

        for dec in COHORT_DECADES:
            dec_persons = [
                p
                for p in self._scores_data
                if _get_cohort_decade(
                    p.get("career", {}).get("first_year")
                )
                == dec
            ]
            dec_top3 = sorted(
                dec_persons, key=lambda p: -(p.get("iv_score") or 0)
            )[:3]
            for p9 in dec_top3:
                pid = p9["person_id"]
                fy = p9.get("career", {}).get("first_year") or 1990
                ly = p9.get("career", {}).get("latest_year") or 2025
                traj_x: list[int] = []
                traj_y: list[int] = []
                prev_cum = 0
                for yr in self._frame_years:
                    if yr < fy or yr > ly + 2:
                        continue
                    age = yr - fy
                    cum = self._cum_credits.get(pid, {}).get(yr, 0)
                    if cum > prev_cum or not traj_x:
                        traj_x.append(age)
                        traj_y.append(cum)
                        prev_cum = cum
                if len(traj_x) < 2:
                    continue
                name = p9.get("name") or p9.get("name_ja") or pid
                fig.add_trace(
                    go.Scatter(
                        x=traj_x,
                        y=traj_y,
                        mode="lines+markers",
                        name=f"{COHORT_LABELS[dec]}: {name}",
                        line=dict(
                            color=COHORT_COLORS[dec], width=1.5
                        ),
                        marker=dict(size=4, color=COHORT_COLORS[dec]),
                        hovertemplate=(
                            f"{name}<br>年功: %{{x}}年<br>"
                            f"累積: %{{y}}作品<extra></extra>"
                        ),
                    )
                )
                n_stars += 1

        fig.update_layout(
            title="世代別トップスターの累積クレジット軌跡",
            xaxis_title="年功（デビューからの経過年数）",
            yaxis_title="累積クレジット数",
        )

        findings = (
            f"<p>コホートごとのIV score上位3人"
            f"（計{n_stars}人の軌跡）。 "
            f"X = 年功、Y = 累積クレジット数。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="世代別トップスターの累積クレジット軌跡",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "topstar-traj-chart", height=480
            ),
            method_note=(
                "コホートごとに IV スコア上位3名。"
                "累積クレジット数は growth.json の yearly_credits、"
                "もしくは total_credits からの均等配分。"
                "累積数が増加する点のみプロット。"
            ),
            section_id="top_star_trajectory",
        )

    # ══════════════════════════════════════════════════════════
    # Section 11: Top star table (Chart 10)
    # ══════════════════════════════════════════════════════════

    def _build_top_star_table_section(
        self, sb: SectionBuilder
    ) -> ReportSection:
        table_html = (
            "<table><thead><tr>"
            "<th>コホート</th><th>名前</th><th>デビュー年</th>"
            "<th>主役職</th><th>最高ステージ</th><th>IVスコア</th>"
            "</tr></thead><tbody>"
        )
        n_listed = 0
        for dec in COHORT_DECADES:
            dec_persons = [
                p
                for p in self._scores_data
                if _get_cohort_decade(
                    p.get("career", {}).get("first_year")
                )
                == dec
            ]
            dec_top5 = sorted(
                dec_persons, key=lambda p: -(p.get("iv_score") or 0)
            )[:5]
            for i, p10 in enumerate(dec_top5):
                cohort_cell = (
                    f'<span style="color:{COHORT_COLORS[dec]}">'
                    f"{COHORT_LABELS[dec]}</span>"
                    if i == 0
                    else ""
                )
                name = (
                    p10.get("name") or p10.get("name_ja") or p10["person_id"]
                )
                fy = p10.get("career", {}).get("first_year", "?")
                hs = p10.get("career", {}).get("highest_stage", 1)
                role = p10.get("primary_role", "?")
                iv = round(p10.get("iv_score") or 0, 1)
                table_html += (
                    f"<tr><td>{cohort_cell}</td>"
                    f"<td>{person_link(name, p10['person_id'])}</td>"
                    f"<td>{fy}</td><td>{role}</td>"
                    f"<td>{STAGE_LABELS.get(hs, hs)}</td>"
                    f"<td>{iv}</td></tr>"
                )
                n_listed += 1
        table_html += "</tbody></table>"

        findings = (
            f"<p>デビュー年代コホートごとのIV score上位5人"
            f"（{n_listed}人を掲載）。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="世代別トップスター一覧",
            findings_html=findings,
            visualization_html=table_html,
            method_note=(
                "コホートごとに IV スコア上位5名。"
                "最高ステージは scores.json の career.highest_stage 由来。"
            ),
            section_id="top_star_table",
        )

    # ══════════════════════════════════════════════════════════
    # Section 12: ML cluster x cohort heatmap (Chart 11)
    # ══════════════════════════════════════════════════════════

    def _build_cluster_cohort_section(
        self, sb: SectionBuilder
    ) -> ReportSection:
        ml_persons = self._ml_data.get("persons", [])
        if not ml_persons:
            return ReportSection(
                title="MLクラスタ x コホート分布ヒートマップ",
                findings_html="<p>ml_clusters.jsonのデータが利用できません。</p>",
                section_id="cluster_cohort",
            )

        cluster_cohort: dict[str, dict[int, int]] = {}
        for mp in ml_persons:
            pid_ml = mp["person_id"]
            cn_ml = mp.get("cluster_name", "?")
            p_sc = self._scores_by_pid.get(pid_ml, {})
            fy_ml = p_sc.get("career", {}).get("first_year")
            if not fy_ml:
                continue
            dec_ml = _get_cohort_decade(fy_ml)
            cluster_cohort.setdefault(cn_ml, {}).setdefault(dec_ml, 0)
            cluster_cohort[cn_ml][dec_ml] += 1

        cl_names = sorted(cluster_cohort.keys())
        z_data = [
            [cluster_cohort.get(cn, {}).get(dec, 0) for dec in COHORT_DECADES]
            for cn in cl_names
        ]

        fig = go.Figure(
            go.Heatmap(
                x=[COHORT_LABELS[d] for d in COHORT_DECADES],
                y=cl_names,
                z=z_data,
                colorscale="Viridis",
                hovertemplate=(
                    "クラスタ: %{y}<br>コホート: %{x}<br>"
                    "人数: %{z}<extra></extra>"
                ),
                colorbar=dict(title="人数"),
            )
        )
        fig.update_layout(
            title="MLクラスタ x コホート人数分布",
            xaxis_title="デビュー年代コホート",
            yaxis_title="MLクラスタ",
        )

        total_in_heatmap = sum(
            sum(cc.values()) for cc in cluster_cohort.values()
        )
        findings = (
            f"<p>{len(cl_names)}個のMLクラスタ x "
            f"{len(COHORT_DECADES)}個のコホート"
            f"（クラスタ割当とデビュー年の両方を持つn={total_in_heatmap:,}人）。 "
            f"セル値 = 人数。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="MLクラスタ x コホート分布ヒートマップ",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "cluster-cohort-heatmap", height=440
            ),
            method_note=(
                "クラスタは ml_clusters.json 由来。"
                "デビュー年代は scores.json の career.first_year 由来。"
                "デビュー年が欠損している人物は除外。"
            ),
            section_id="cluster_cohort",
        )

    # ══════════════════════════════════════════════════════════
    # Section 13: Inter-generational collab matrix (Chart 12)
    # ══════════════════════════════════════════════════════════

    def _build_collab_matrix_section(
        self, sb: SectionBuilder
    ) -> ReportSection:
        collaborations = self._collaborations_data
        if not collaborations or not isinstance(collaborations, list):
            return ReportSection(
                title="世代間コラボレーションマトリックス",
                findings_html=(
                    "<p>collaborations.jsonのデータが利用できません。</p>"
                ),
                section_id="collab_matrix",
            )

        collab_mat: dict[int, dict[int, float]] = {
            d: {d2: 0.0 for d2 in COHORT_DECADES} for d in COHORT_DECADES
        }
        n_edges = 0
        for col in collaborations:
            pid_a = col.get("person_a", "")
            pid_b = col.get("person_b", "")
            sw = float(col.get("shared_works", 0) or 0)
            if sw == 0:
                continue
            pa = self._scores_by_pid.get(pid_a, {})
            pb = self._scores_by_pid.get(pid_b, {})
            fya = pa.get("career", {}).get("first_year")
            fyb = pb.get("career", {}).get("first_year")
            if not fya or not fyb:
                continue
            da = _get_cohort_decade(fya)
            db = _get_cohort_decade(fyb)
            collab_mat[da][db] += sw
            if da != db:
                collab_mat[db][da] += sw
            n_edges += 1

        z_data = [
            [collab_mat[da].get(db, 0) for db in COHORT_DECADES]
            for da in COHORT_DECADES
        ]
        labels = [COHORT_LABELS[d] for d in COHORT_DECADES]

        fig = go.Figure(
            go.Heatmap(
                x=labels,
                y=labels,
                z=z_data,
                colorscale="Magma",
                hovertemplate=(
                    "縦: %{y}<br>横: %{x}<br>"
                    "コラボ強度: %{z:.0f}<extra></extra>"
                ),
                colorbar=dict(title="共作品数合計"),
            )
        )
        fig.update_layout(
            title="世代間コラボレーションマトリックス（共作品数ベース）",
            xaxis_title="コホートB",
            yaxis_title="コホートA",
        )

        # Diagonal dominance check
        diag_total = sum(
            collab_mat[d][d] for d in COHORT_DECADES
        )
        off_diag_total = sum(
            collab_mat[da][db]
            for da in COHORT_DECADES
            for db in COHORT_DECADES
            if da != db
        )
        grand_total = diag_total + off_diag_total
        diag_pct = (
            100 * diag_total / grand_total if grand_total > 0 else 0
        )

        findings = (
            f"<p>{n_edges:,}本のコラボレーションエッジからの"
            f"コラボレーション行列。対角線（同一コホート）は"
            f"共作品ウェイト全体の{diag_pct:.1f}%を占める"
            f"（合計ウェイト: {grand_total:,.0f}）。</p>"
        )

        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                '<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="世代間コラボレーションマトリックス",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "intgen-collab-matrix", height=440
            ),
            method_note=(
                "協業エッジは collaborations.json 由来。"
                "ウェイト = shared_works。行列は対称。"
                "first_year が欠損している人物は除外。"
            ),
            section_id="collab_matrix",
        )


# v3 minimal SPEC — generated by scripts/maintenance/add_default_specs.py.
# Replace ``claim`` / ``identifying_assumption`` / ``null_model`` with
# report-specific values when curating this module.
from .._spec import make_default_spec  # noqa: E402

SPEC = make_default_spec(
    name='cohort_animation',
    audience='technical_appendix',
    claim='デビューコホート分析 に関する記述的指標 (subtitle: コホート別Gapminder・世代間コラボ・供給需要比較・性別構成・Tier到達を含む14セクション)',
    sources=["credits", "persons", "anime"],
    meta_table='meta_cohort_animation',
)
