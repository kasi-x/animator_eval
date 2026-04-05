"""ExplanationMeta → HTML変換ウィジェット.

chart_guide, caveat_box, competing_interpretations 等の
HTMLフラグメントを生成する。
"""

from __future__ import annotations

from src.viz.chart_spec import ExplanationMeta


def chart_guide(text: str) -> str:
    """チャートの読み方ガイド（青ボーダー）."""
    return f'<div class="chart-guide"><strong>チャートの見方:</strong> {text}</div>'


def section_desc(text: str) -> str:
    """セクション説明文."""
    return f'<p class="section-desc">{text}</p>'


def caveat_box(text: str) -> str:
    """解釈上の注意・制約を強調する黄橙ボックス."""
    return f'<div class="caveat-box">&#9888; <strong>解釈上の注意:</strong> {text}</div>'


def competing_interpretations_html(claim: str, alternatives: tuple[str, ...]) -> str:
    """主張と競合する代替解釈を構造化して提示."""
    alts = "".join(f"<li>{a}</li>" for a in alternatives)
    return (
        '<div class="competing-interp">'
        f'<div class="ci-claim"><strong>主張:</strong> {claim}</div>'
        f'<div class="ci-alts"><strong>競合解釈:</strong><ol>{alts}</ol></div>'
        "</div>"
    )


def key_findings_html(items: tuple[str, ...]) -> str:
    """データ駆動の知見リスト."""
    if not items:
        return ""
    lis = "".join(f"<li>{item}</li>" for item in items)
    return (
        '<div class="insight-box">'
        f"<strong>主要な知見:</strong><ul>{lis}</ul>"
        "</div>"
    )


def significance_html(title: str, text: str) -> str:
    """分析の意義セクション."""
    return (
        '<div class="significance-section">'
        f"<h3>この分析の意義 — {title}</h3>"
        f"<p>{text}</p>"
        "</div>"
    )


def utilization_html(usecases: tuple[dict[str, str], ...]) -> str:
    """利用ガイド."""
    if not usecases:
        return ""
    items = ""
    for uc in usecases:
        role = uc.get("role", "")
        how = uc.get("how", "")
        items += (
            f'<li><span class="role-tag">{role}</span>'
            f'<span class="usecase-desc">{how}</span></li>'
        )
    return (
        '<div class="utilization-guide">'
        "<h3>活用ガイド</h3>"
        f'<ul class="usecase-list">{items}</ul>'
        "</div>"
    )



def render_explanation(meta: ExplanationMeta) -> tuple[str, str]:
    """ExplanationMeta → (before_chart_html, after_chart_html).

    チャートの前に配置するもの（reading_guide）と
    チャートの後に配置するもの（findings, caveats, interpretations等）を分離。
    """
    before = ""
    after = ""

    # Before chart
    if meta.reading_guide:
        before += chart_guide(meta.reading_guide)

    # After chart
    if meta.key_findings:
        after += key_findings_html(meta.key_findings)

    for cav in meta.caveats:
        after += caveat_box(cav)

    for claim, alts in meta.competing_interpretations:
        after += competing_interpretations_html(claim, alts)

    if meta.significance:
        after += significance_html("", meta.significance)

    if meta.utilization:
        after += utilization_html(meta.utilization)

    if meta.context:
        after += section_desc(meta.context)

    return before, after
