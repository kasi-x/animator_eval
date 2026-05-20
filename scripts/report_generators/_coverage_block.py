"""Coverage caveat block builder for v2/v3 report templates.

Generates the mandatory coverage disclosure block (source × role_group × year)
that must appear in every report's Findings layer per:
  - TASK_CARDS/27_methodology/01_missingness_disclosure.md (Hard constraints §)
  - docs/REPORT_PHILOSOPHY.md §4 (Data Statement — Coverage & Known Biases)

Design:
- Pure HTML/string output — no DB access here. DB access happens in
  coverage_matrix.py (Resolved layer only).
- Called from wrap_html_v2() with the same pattern as STANCE_BLOCK.
- Two entry points:
    coverage_block_html(matrix)  → full HTML block for report injection
    coverage_summary_text(matrix) → plain-text summary for data statements
- No forbidden vocabulary (no evaluative adjectives, no causal verbs).
- structlog for warnings; no stdlib logging.
"""

from __future__ import annotations

import structlog

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# CSS class for the coverage block (appended to V2_CSS in html_templates.py)
# ---------------------------------------------------------------------------

COVERAGE_BLOCK_CSS = """
/* Coverage caveat block (27_methodology/01_missingness_disclosure) */
.coverage-block {
    border-left: 3px solid #5aafaf;
    background: rgba(90,175,175,0.06);
    border-radius: 0 12px 12px 0;
    padding: 1rem 1.4rem;
    margin: 1rem 0;
    font-size: 0.88rem;
    color: #c0d4d4;
    line-height: 1.7;
}
.coverage-block .cb-title {
    color: #5aafaf;
    font-weight: 700;
    font-size: 0.95rem;
    margin-bottom: 0.5rem;
    display: block;
}
.coverage-block .cb-under {
    color: #e0a050;
    font-weight: 600;
}
.coverage-block table {
    font-size: 0.82rem;
    width: 100%;
    border-collapse: collapse;
    margin-top: 0.6rem;
}
.coverage-block th {
    color: #5aafaf;
    text-align: left;
    padding: 0.3rem 0.5rem;
    border-bottom: 1px solid rgba(90,175,175,0.3);
    font-weight: 600;
}
.coverage-block td {
    padding: 0.25rem 0.5rem;
    color: #b0c4c4;
    border-bottom: 1px solid rgba(255,255,255,0.04);
}
.coverage-block .cb-ratio-low  { color: #e07060; }
.coverage-block .cb-ratio-mid  { color: #e0c060; }
.coverage-block .cb-ratio-high { color: #60d0a0; }
"""

# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

_ROLE_GROUP_LABELS_JA: dict[str, str] = {
    "direction": "演出・監督",
    "animation_supervision": "作画監督",
    "animation": "作画 (原画・動画・第二原画等)",
    "design": "キャラクターデザイン",
    "technical": "撮影・CG",
    "art": "美術",
    "sound": "音響・音楽",
    "writing": "脚本・原作",
    "production": "制作・プロデューサー",
    "production_management": "制作管理",
    "finishing": "仕上げ・色彩",
    "editing": "編集",
    "settings": "設定",
    "non_production": "非制作 (声優・楽曲等)",
}


def _ja_label(role_group: str) -> str:
    """Return Japanese display label for a role_group key."""
    return _ROLE_GROUP_LABELS_JA.get(role_group, role_group)


def _ratio_css_class(ratio: float) -> str:
    """Return CSS class for coverage ratio coloring."""
    if ratio < 0.40:
        return "cb-ratio-low"
    if ratio < 0.75:
        return "cb-ratio-mid"
    return "cb-ratio-high"


def _ratio_display(ratio: float) -> str:
    """Format coverage ratio as percentage string."""
    return f"{ratio * 100:.0f}%"


def _build_role_summary_rows(
    matrix: CoverageMatrix,  # noqa: F821  (avoid circular at module level)
    max_rows: int = 12,
) -> str:
    """Build HTML <tr> rows for role_group mean coverage per source.

    Groups cells by (source, role_group), computes mean coverage_ratio,
    and renders one row per combination (up to max_rows).
    """
    from collections import defaultdict

    sums: defaultdict[tuple[str, str], float] = defaultdict(float)
    counts: defaultdict[tuple[str, str], int] = defaultdict(int)

    for cell in matrix.cells:
        key = (cell.source, cell.role_group)
        sums[key] += cell.coverage_ratio
        counts[key] += 1

    rows_data = sorted(
        [
            (source, rg, sums[(source, rg)] / counts[(source, rg)])
            for (source, rg) in sums
        ],
        key=lambda t: (t[0], t[1]),
    )[:max_rows]

    if not rows_data:
        return "<tr><td colspan='3'>データなし</td></tr>"

    html_rows = []
    for source, role_group, mean_ratio in rows_data:
        css = _ratio_css_class(mean_ratio)
        html_rows.append(
            f"<tr>"
            f"<td>{source}</td>"
            f"<td>{_ja_label(role_group)}</td>"
            f'<td class="{css}">{_ratio_display(mean_ratio)}</td>'
            f"</tr>"
        )
    return "\n".join(html_rows)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def coverage_block_html(
    matrix: CoverageMatrix,  # noqa: F821
    *,
    show_table: bool = True,
    max_table_rows: int = 12,
) -> str:
    """Render a full HTML coverage caveat block suitable for report injection.

    This block discloses source × role_group × year coverage state per
    REPORT_PHILOSOPHY v2 §4 (Coverage & Known Biases). It is injected
    between STANCE_BLOCK and the body of each v3 report.

    Args:
        matrix: CoverageMatrix from compute_coverage_matrix().
            When matrix.is_empty(), a degraded notice is rendered instead
            of a table — the report still renders without DB data.
        show_table: Whether to include the role × source detail table.
            Set False for executive brief variants where space is tight.
        max_table_rows: Truncate table at this many rows (prevents huge blocks).

    Returns:
        HTML string starting with <div class="coverage-block">.
    """
    if matrix.is_empty():
        return (
            '<div class="coverage-block">'
            '<span class="cb-title">&#9432; データ coverage 開示</span>'
            "<p>resolved.duckdb が存在しないか credits テーブルが空のため、"
            "source × role × year coverage 行列を生成できなかった。"
            "本レポートのすべての推定値は <strong>過小推定の可能性</strong> がある。"
            "補正は行わない (source 透明性維持のため)。</p>"
            "</div>"
        )

    year_range = (
        f"{min(matrix.years)}–{max(matrix.years)}"
        if matrix.years else "年不明"
    )
    under_html = ""
    if matrix.under_credited_roles:
        labels = ", ".join(_ja_label(rg) for rg in matrix.under_credited_roles)
        under_html = (
            f'<p><span class="cb-under">&#9888; coverage 50% 未満の role:</span> '
            f"{labels}。"
            "これらの role に係る推定値はデータ不足のため過小推定となる可能性がある。"
            "補正は行わない。</p>"
        )

    table_html = ""
    if show_table:
        table_rows = _build_role_summary_rows(matrix, max_rows=max_table_rows)
        table_html = (
            "<details style='margin-top:0.6rem;'>"
            "<summary style='cursor:pointer;color:#5aafaf;font-size:0.82rem;'>"
            "source × role 別 coverage 詳細 (展開)</summary>"
            "<table>"
            "<thead><tr><th>source</th><th>role group</th><th>平均 coverage</th></tr></thead>"
            f"<tbody>{table_rows}</tbody>"
            "</table>"
            "</details>"
        )

    return (
        '<div class="coverage-block">'
        '<span class="cb-title">&#9432; データ coverage 開示</span>'
        f"<p>{len(matrix.sources)} source × "
        f"{len(matrix.role_groups)} role group × "
        f"{len(matrix.years)} 年 ({year_range}) の coverage 行列を生成した。"
        f"参照 upper bound: {_REFERENCE_SOURCE_LABEL}。"
        "「データ不足のため過小推定」の可能性があるクレジットは除外しない "
        "(推定値を歪めるより source の限界を明示することを優先する)。</p>"
        f"{under_html}"
        f"{table_html}"
        "</div>"
    )


_REFERENCE_SOURCE_LABEL = "ANN (最も網羅的な参照 source)"


def coverage_summary_text(
    matrix: CoverageMatrix,  # noqa: F821
) -> str:
    """Return a plain-text coverage summary for Data Statement injection.

    Suitable for section_builder.py's data_statement coverage_notes field.

    Args:
        matrix: CoverageMatrix from compute_coverage_matrix().

    Returns:
        Multi-sentence plain text (no HTML tags).
    """
    if matrix.is_empty():
        return (
            "coverage 行列: resolved.duckdb が存在しないか credits テーブルが空のため生成不可。"
            "全推定値は過小推定の可能性がある。"
        )

    year_range = (
        f"{min(matrix.years)}–{max(matrix.years)}"
        if matrix.years else "年不明"
    )
    under = (
        "、".join(_ja_label(rg) for rg in matrix.under_credited_roles)
        if matrix.under_credited_roles else "なし"
    )
    return (
        f"{len(matrix.sources)} source × "
        f"{len(matrix.role_groups)} role group × "
        f"{len(matrix.years)} 年 ({year_range})。"
        f"ANN を upper bound 参照とした coverage 比。"
        f"coverage 50% 未満の role group: {under}。"
        "補正は行わない (source 透明性維持)。"
    )


def inject_coverage_block_into_wrap_html_v2_args(
    matrix: CoverageMatrix,  # noqa: F821
    *,
    show_table: bool = True,
) -> str:
    """Convenience function: return coverage_block_html for direct use in wrap_html_v2.

    Usage in report generators:
        from scripts.report_generators._coverage_block import (
            inject_coverage_block_into_wrap_html_v2_args
        )
        coverage_html = inject_coverage_block_into_wrap_html_v2_args(matrix)
        # Then prepend to intro_html or body before calling wrap_html_v2().

    Args:
        matrix: CoverageMatrix instance (may be empty).
        show_table: Passed through to coverage_block_html().

    Returns:
        HTML string ready for injection.
    """
    html = coverage_block_html(matrix, show_table=show_table)
    logger.debug(
        "coverage_block_injected",
        is_empty=matrix.is_empty(),
        under_credited=matrix.under_credited_roles,
    )
    return html
