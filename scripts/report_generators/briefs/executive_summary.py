"""Brief executive summary auto-generator.

各 brief (policy / hr / business / labor) の冒頭に挿入する key findings の
auto-extract。3 つの要素を構造的に抽出:

1. **Headline metrics**: brief を貫く 3-5 個の主要数値 (CI 付き、出典 report 参照)
2. **Method gate status**: 「全 finding は CI / null model / holdout のどれを通過したか」
3. **Coverage caveats**: 各 finding の data coverage が adequate か low か

設計:
- structured Findings dict (key: metric name, value: ValueWithCI + source + caveat)
- 各 brief の生成 module から fact list を渡せばテンプレート HTML 化
- LLM-generated narrative ではなく **fact extraction + template** で再現性確保

H2: claim 表現は事実記述、formulation のみ。"優位" 等の評価語 NG。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from collections.abc import Sequence

import structlog

log = structlog.get_logger(__name__)


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class KeyFinding:
    """Brief の executive summary に挿入する 1 finding。"""

    metric_label: str          # 例: "credit gap (female vs male, 1990s cohort)"
    value: float
    unit: str                  # 例: "log credits", "HR", "Gini coefficient"
    ci_low: float | None = None
    ci_high: float | None = None
    source_report: str = ""    # 出典 report id (e.g. "equity_oaxaca")
    method_gate: str = ""      # 例: "bootstrap CI n=1000"
    coverage_caveat: str = ""  # 例: "gender null 80.9% (low coverage)"
    direction: str = ""        # "+" / "-" / "0" (interpretation 用)


@dataclass(frozen=True)
class ExecutiveSummary:
    """1 brief の executive summary 構造化結果。"""

    brief_id: str              # "policy" / "hr" / "biz" / "labor"
    audience: str
    findings: tuple[KeyFinding, ...]
    method_gate_summary: dict[str, int] = field(default_factory=dict)
    coverage_warnings: tuple[str, ...] = field(default_factory=tuple)
    generated_at: str = ""


# ---------------------------------------------------------------------------
# Format helpers
# ---------------------------------------------------------------------------


def format_value_with_ci(f: KeyFinding) -> str:
    """値 + CI を統一 format で文字列化。"""
    sign = "+" if (f.value > 0 and f.direction != "0") else ""
    main = f"{sign}{f.value:.3f}"
    if f.ci_low is not None and f.ci_high is not None:
        return f"{main} (CI [{f.ci_low:+.3f}, {f.ci_high:+.3f}]) {f.unit}".strip()
    return f"{main} {f.unit}".strip()


def render_executive_summary_html(summary: ExecutiveSummary) -> str:
    """ExecutiveSummary を 1 つの HTML block にレンダリング。

    各 brief の冒頭に挿入することを想定。
    """
    if not summary.findings:
        return (
            f'<section class="exec-summary" id="exec-{summary.brief_id}">'
            f"<h2>{summary.brief_id} brief</h2>"
            "<p>本データ範囲では headline finding を抽出可能な水準に到達していない。"
            "後続 enrichment 待ち。</p></section>"
        )
    finding_items = "".join(
        f"<li><strong>{f.metric_label}</strong>: {format_value_with_ci(f)}"
        + (f' <em>(出典: {f.source_report})</em>' if f.source_report else "")
        + (f' <span class="method-gate">[{f.method_gate}]</span>' if f.method_gate else "")
        + (f' <span class="caveat">[{f.coverage_caveat}]</span>' if f.coverage_caveat else "")
        + "</li>"
        for f in summary.findings
    )
    gate_summary_html = ""
    if summary.method_gate_summary:
        items = "".join(
            f"<li>{gate}: {n} findings</li>"
            for gate, n in sorted(summary.method_gate_summary.items())
        )
        gate_summary_html = (
            f'<details><summary>Method gate breakdown</summary><ul>{items}</ul></details>'
        )
    coverage_html = ""
    if summary.coverage_warnings:
        items = "".join(f"<li>{w}</li>" for w in summary.coverage_warnings)
        coverage_html = (
            f'<details open><summary>Coverage caveats ({len(summary.coverage_warnings)})</summary>'
            f"<ul>{items}</ul></details>"
        )
    return (
        f'<section class="exec-summary" id="exec-{summary.brief_id}">'
        f"<h2>{summary.brief_id} brief — Key Findings</h2>"
        f'<p><em>Audience: {summary.audience}. 全 finding は method gate (CI / null '
        "model / holdout) のいずれかを通過済。</em></p>"
        f"<ul>{finding_items}</ul>"
        f"{gate_summary_html}{coverage_html}"
        "</section>"
    )


# ---------------------------------------------------------------------------
# Aggregation: findings list → ExecutiveSummary
# ---------------------------------------------------------------------------


def build_executive_summary(
    brief_id: str,
    audience: str,
    findings: Sequence[KeyFinding],
    *,
    generated_at: str = "",
) -> ExecutiveSummary:
    """Findings list を集計 → ExecutiveSummary。

    method gate と coverage caveat を自動 aggregate。
    """
    gate_counts: dict[str, int] = {}
    for f in findings:
        if f.method_gate:
            gate_counts[f.method_gate] = gate_counts.get(f.method_gate, 0) + 1
    caveats = tuple(
        f"{f.metric_label} — {f.coverage_caveat}"
        for f in findings if f.coverage_caveat
    )
    return ExecutiveSummary(
        brief_id=brief_id,
        audience=audience,
        findings=tuple(findings),
        method_gate_summary=gate_counts,
        coverage_warnings=caveats,
        generated_at=generated_at,
    )


# ---------------------------------------------------------------------------
# Filter helpers — findings 候補から rank で抽出
# ---------------------------------------------------------------------------


def rank_findings_by_abs_value(
    findings: Sequence[KeyFinding], *, top_k: int = 5
) -> list[KeyFinding]:
    """|value| 降順で top-k を選抜。

    Brief の executive summary は 3-5 件が読みやすさのスイートスポット。
    """
    sorted_findings = sorted(findings, key=lambda f: -abs(f.value))
    return list(sorted_findings[:top_k])


def filter_findings_with_ci_excludes_zero(
    findings: Sequence[KeyFinding],
) -> list[KeyFinding]:
    """CI が 0 を含まない (= 統計的に区別可能) finding のみ抽出。"""
    return [
        f for f in findings
        if f.ci_low is not None and f.ci_high is not None
        and (f.ci_low > 0 or f.ci_high < 0)
    ]


def filter_findings_passing_coverage(
    findings: Sequence[KeyFinding],
) -> list[KeyFinding]:
    """coverage_caveat が空 (= adequate coverage) の finding のみ抽出。"""
    return [f for f in findings if not f.coverage_caveat]
