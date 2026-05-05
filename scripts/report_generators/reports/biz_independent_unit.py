"""独立ユニット形成可能性分析 — v2 compliant."""

from __future__ import annotations

import json
import math
from pathlib import Path

import plotly.graph_objects as go

from ..html_templates import plotly_div_safe
from ..section_builder import ReportSection, SectionBuilder
from ._base import BaseReportGenerator

_JSON_DIR = Path(__file__).parents[4] / "result" / "json"


def _load(name: str) -> dict | list:
    p = _JSON_DIR / f"{name}.json"
    if not p.exists():
        return {}
    try:
        return json.loads(p.read_text())
    except Exception:
        return {}


def _sf(v: object, default: float = 0.0) -> float:
    try:
        f = float(v)  # type: ignore[arg-type]
        return default if math.isnan(f) or math.isinf(f) else f
    except (TypeError, ValueError):
        return default


class BizIndependentUnitReport(BaseReportGenerator):
    name = "biz_independent_unit"
    title = "独立ユニット形成可能性"
    subtitle = "コミュニティ生存可能性スコア / ロールカバレッジ"
    filename = "biz_independent_unit.html"
    doc_type = "brief"

    def generate(self) -> Path | None:
        data = _load("independent_units")
        if not isinstance(data, dict):
            data = {}
        sb = SectionBuilder()
        sections = [
            sb.build_section(self._build_viability_distribution(sb, data)),
            sb.build_section(self._build_role_coverage(sb, data)),
        ]
        return self.write_report("\n".join(sections))

    # ── Section 1: Viability distribution scatter ─────────────────────────

    def _build_viability_distribution(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        cs = data.get("community_scores", {})
        if not isinstance(cs, dict):
            cs = {}
        n_communities = int(data.get("n_communities", len(cs)))
        n_viable = int(data.get("n_viable", 0))

        if not cs:
            findings = (
                "<p>コミュニティスコアデータが利用できません"
                "（independent_units.community_scores）。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="コミュニティ生存可能性スコア",
                findings_html=findings,
                method_note=(
                    "viability = coverage × 0.5 + density × 0.3 +"
                    " mean_fe_pct × 0.2 (正規化済み)"
                ),
                section_id="iu_viability",
            )

        cids = list(cs.keys())
        coverages = [_sf(cs[c].get("coverage", 0.0)) for c in cids]
        densities = [_sf(cs[c].get("density", 0.0)) for c in cids]
        sizes = [max(int(cs[c].get("size", 1)), 1) for c in cids]
        viabilities = [_sf(cs[c].get("viability", 0.0)) for c in cids]

        marker_sizes = [max(math.sqrt(s) * 4, 6) for s in sizes]

        fig = go.Figure(
            go.Scatter(
                x=coverages,
                y=densities,
                mode="markers+text",
                text=cids,
                textposition="top center",
                marker=dict(
                    size=marker_sizes,
                    color=viabilities,
                    colorscale="Plasma",
                    showscale=True,
                    colorbar=dict(title="viability"),
                ),
                hovertemplate=(
                    "コミュニティ=%{text}<br>"
                    "coverage=%{x:.3f}<br>"
                    "density=%{y:.3f}<br>"
                    "viability=%{marker.color:.3f}<extra></extra>"
                ),
            )
        )
        fig.update_layout(
            title="コミュニティ生存可能性: coverage × density（色=viability、サイズ=size）",
            xaxis_title="ロールカバレッジ率（coverage）",
            yaxis_title="コミュニティ密度（density）",
            height=480,
        )

        v_vals = viabilities
        v_min = min(v_vals) if v_vals else 0.0
        v_max = max(v_vals) if v_vals else 0.0

        findings = (
            f"<p>コミュニティ数: {n_communities:,}。"
            f"viability &gt; 0.5 のコミュニティ数: {n_viable:,}。"
            f"viabilityの範囲: {v_min:.3f} ～ {v_max:.3f}。"
            f"散布図の横軸=coverage、縦軸=density、"
            f"バブルサイズ=size^0.5、色=viabilityを示す。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="コミュニティ生存可能性スコア",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_iu_viability", height=480
            ),
            method_note=(
                "viability = coverage × 0.5 + density × 0.3 +"
                " mean_fe_pct × 0.2（各軸は0-1正規化済み）。"
                "coverage: コミュニティ内でカバーされる必須ロール数 / 全必須ロール数。"
                "density: コミュニティ内エッジ密度（NetworkX graph_density）。"
                "viability > 0.5 を「生存可能」と定義（固定事前設定値）。"
            ),
            section_id="iu_viability",
        )

    # ── Section 2: Role coverage bar chart ──────────────────────────

    def _build_role_coverage(
        self, sb: SectionBuilder, data: dict
    ) -> ReportSection:
        cs = data.get("community_scores", {})
        if not isinstance(cs, dict):
            cs = {}

        if not cs:
            findings = (
                "<p>ロールカバレッジデータが利用できません"
                "（independent_units.community_scores）。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="必須ロール別カバレッジ（冗長性チェック）",
                findings_html=findings,
                method_note="必須ロール: CORE_TEAM_ROLES 定義に基づく",
                section_id="iu_roles",
            )

        # Aggregate role_coverage_detail across communities
        role_coverage_count: dict[str, int] = {}
        n_communities = len(cs)
        for cdata in cs.values():
            if not isinstance(cdata, dict):
                continue
            detail = cdata.get("role_coverage_detail", {})
            if not isinstance(detail, dict):
                continue
            for role, covered in detail.items():
                if role not in role_coverage_count:
                    role_coverage_count[role] = 0
                if covered:
                    role_coverage_count[role] += 1

        if not role_coverage_count:
            findings = (
                "<p>ロール別カバレッジの詳細データが取得できません"
                "（role_coverage_detail が空）。</p>"
            )
            violations = sb.validate_findings(findings)
            if violations:
                findings += (
                    f'<p style="color:#e05080;font-size:0.8rem;">'
                    f"[v2: {'; '.join(violations)}]</p>"
                )
            return ReportSection(
                title="必須ロール別カバレッジ（冗長性チェック）",
                findings_html=findings,
                method_note="必須ロール: CORE_TEAM_ROLES 定義に基づく",
                section_id="iu_roles",
            )

        # Coverage rate per role
        roles = list(role_coverage_count.keys())
        coverage_rates = [
            role_coverage_count[r] / n_communities if n_communities > 0 else 0.0
            for r in roles
        ]

        # Sort by coverage rate desc
        paired = sorted(
            zip(roles, coverage_rates), key=lambda t: t[1], reverse=True
        )
        sorted_roles = [p[0] for p in paired]
        sorted_rates = [p[1] for p in paired]

        # Color: green for high coverage, red for low
        bar_colors = [
            f"rgba({int(255 * (1 - r))}, {int(200 * r)}, {int(80 * r)}, 0.8)"
            for r in sorted_rates
        ]

        fig = go.Figure(
            go.Bar(
                x=sorted_rates,
                y=sorted_roles,
                orientation="h",
                marker_color=bar_colors,
                text=[f"{r * 100:.0f}%" for r in sorted_rates],
                textposition="outside",
                hovertemplate="%{y}: カバレッジ率=%{x:.3f}<extra></extra>",
            )
        )
        fig.update_layout(
            title="必須ロール別 コミュニティカバレッジ率（全コミュニティに占める割合）",
            xaxis_title="カバレッジ率",
            xaxis=dict(range=[0, 1.15]),
            yaxis_title="ロール",
            height=max(380, len(sorted_roles) * 22 + 100),
            margin=dict(l=180, r=80),
        )

        # Fully covered and poorly covered roles
        full_roles = [r for r, rate in zip(sorted_roles, sorted_rates) if rate == 1.0]
        poor_roles = [r for r, rate in zip(sorted_roles, sorted_rates) if rate < 0.5]
        full_str = "、".join(full_roles) if full_roles else "なし"
        poor_str = "、".join(poor_roles) if poor_roles else "なし"

        findings = (
            f"<p>集計対象コミュニティ数: {n_communities:,}。"
            f"全コミュニティでカバーされるロール: {full_str}。"
            f"カバレッジ率 &lt; 50% のロール: {poor_str}。"
            f"バーの色は緑=高カバレッジ、赤=低カバレッジを示す。</p>"
        )
        violations = sb.validate_findings(findings)
        if violations:
            findings += (
                f'<p style="color:#e05080;font-size:0.8rem;">'
                f"[v2: {'; '.join(violations)}]</p>"
            )

        return ReportSection(
            title="必須ロール別カバレッジ（冗長性チェック）",
            findings_html=findings,
            visualization_html=plotly_div_safe(
                fig, "chart_iu_roles",
                height=max(380, len(sorted_roles) * 22 + 100)
            ),
            method_note=(
                "必須ロール: CORE_TEAM_ROLES 定義に基づく"
                "（director, series_director, character_design, "
                "animation_director, key_animator, in_betweener）。"
                "カバレッジ率 = 当該ロールを保有するコミュニティ数 / 全コミュニティ数。"
                "役割のカバレッジは人数冗長性（冗長数 ≥ 2）を含まない点に注意。"
            ),
            section_id="iu_roles",
        )


# v3 minimal SPEC — generated by scripts/maintenance/add_default_specs.py.
# Replace ``claim`` / ``identifying_assumption`` / ``null_model`` with
# report-specific values when curating this module.
from .._spec import make_default_spec  # noqa: E402

from .._spec import SensitivityAxis  # noqa: E402

SPEC = make_default_spec(
    name='biz_independent_unit',
    audience='biz',
    claim=(
        'Louvain コミュニティ単位で 7 役職グループを内部充足する coverage と '
        '密度 (intra-edge / total-edge) が両方高いコミュニティが、'
        'configuration model null の上位 5% に出現する'
    ),
    identifying_assumption=(
        'コミュニティ = Louvain modularity 最適化。実際の独立ユニット形成可能性は'
        'コミュニティの境界の安定性 (修正 modularity / 別アルゴリズム) に依存する。'
        '7 役職カバレッジは「最低限の制作体制」を仮定するが、実プロジェクトは'
        '外注や副業を含めて成立可能。'
    ),
    null_model=['N1', 'N2'],
    sources=['credits', 'persons', 'anime'],
    meta_table='meta_biz_independent_unit',
    estimator='V_G = role_coverage × density × scale_compatibility',
    ci_estimator='bootstrap', n_resamples=1000,
    sensitivity_grid=[
        SensitivityAxis(name='Louvain resolution', values=[0.5, 1.0, 2.0]),
        SensitivityAxis(name='role grouping', values=['7 groups', '24 roles', 'minimal 5']),
    ],
    extra_limitations=[
        'Louvain modularity は確率的 — seed ごとに ~10-15% 境界変動',
        '7 役職カバレッジは事前設定値、実態の最低制作体制とは異なる可能性',
        '外注 / 副業 / フリーランス の関係は捕捉外',
    ],
)
