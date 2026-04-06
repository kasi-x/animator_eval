"""Thin wrappers around existing HTML primitives in ``scripts/report_generators``.

The goal is to reuse ``COMMON_CSS``, the ``DISCLAIMER`` constant, and the
rendering helpers (``report_intro``, ``chart_guide``, ``build_glossary``,
``plotly_div_safe``, …) without rewriting them. ``generate_all_reports.py``
imports them via a sys.path insertion; we do the same here so the new
declarative architecture stays visually consistent with existing reports.

The wrapper also provides ``wrap_html_with_katex()`` which augments the
existing ``wrap_html()`` with KaTeX CDN assets and the academic CSS
extensions needed to render ``finding-card`` blocks.
"""

from __future__ import annotations

import sys
from pathlib import Path

# --- sys.path bridge to scripts/report_generators ---------------------------
# ``scripts/`` is not a package; the legacy report generator adds it to
# ``sys.path`` at runtime. We replicate that here so the reporting package
# can import the existing primitives without refactoring 22,000+ lines.
_REPO_ROOT = Path(__file__).resolve().parents[3]
_SCRIPTS_DIR = _REPO_ROOT / "scripts"
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

# noqa: E402 — the imports below depend on the sys.path setup above.
from report_generators import helpers as _legacy_helpers  # noqa: E402
from report_generators import html_templates as _legacy_templates  # noqa: E402
from report_generators.html_templates import (  # noqa: E402
    COMMON_CSS,
    COMMON_GLOSSARY_TERMS,
    DISCLAIMER,
    METHODOLOGY_SUMMARY,
    build_glossary,
    caveat_box,
    chart_guide,
    competing_interpretations,
    future_possibilities,
    key_findings,
    plotly_div_safe,
    report_intro,
    section_desc,
    significance_section,
    utilization_guide,
    wrap_html,
)

__all__ = [
    "ACADEMIC_CSS",
    "COMMON_CSS",
    "COMMON_GLOSSARY_TERMS",
    "DISCLAIMER",
    "METHODOLOGY_SUMMARY",
    "build_glossary",
    "caveat_box",
    "chart_guide",
    "competing_interpretations",
    "future_possibilities",
    "key_findings",
    "plotly_div_safe",
    "report_intro",
    "section_desc",
    "significance_section",
    "utilization_guide",
    "wrap_html",
    "wrap_html_with_katex",
    "configure_legacy_dirs",
]


# ---------------------------------------------------------------------------
# Academic CSS extensions (Finding cards, data-scope flow, methods blocks)
# ---------------------------------------------------------------------------
ACADEMIC_CSS = """
/* ---- academic findings ---- */
.finding-card {
    background: rgba(255,255,255,0.04);
    border: 1px solid rgba(255,255,255,0.08);
    border-left: 4px solid #667eea;
    border-radius: 12px;
    padding: 1.2rem 1.5rem;
    margin: 1rem 0;
}
.finding-strength-strong { border-left-color: #06D6A0; }
.finding-strength-suggestive { border-left-color: #FFD166; }
.finding-strength-exploratory { border-left-color: #667eea; }
.finding-header {
    display: flex; align-items: center; gap: 0.8rem;
    font-size: 0.85rem; color: #a0a0c0; margin-bottom: 0.4rem;
}
.finding-slug {
    font-weight: 700; color: #f093fb;
    font-family: 'SFMono-Regular', Menlo, monospace;
}
.finding-strength { color: #d0d0e0; }
.finding-claim {
    font-size: 1.15rem; font-weight: 600;
    color: #e0e0ff; margin: 0.4rem 0 0.8rem;
    line-height: 1.5;
}
.finding-uncertainty {
    display: flex; flex-wrap: wrap; gap: 1.2rem;
    background: rgba(0,0,0,0.15); padding: 0.6rem 1rem;
    border-radius: 8px; font-size: 0.9rem;
    color: #c0c0d0; margin: 0.5rem 0;
}
.finding-uncertainty .uncertainty-label {
    color: #808090; font-size: 0.75rem; display: block;
}
.finding-uncertainty .uncertainty-value {
    color: #e0e0e0; font-weight: 600;
}
.finding-uncertainty .uncertainty-method {
    margin-left: auto; color: #606070; font-size: 0.8rem;
}
.finding-justification summary,
.finding-robustness summary {
    cursor: pointer; color: #a0d2db;
    font-size: 0.9rem; padding: 0.4rem 0;
}
.finding-justification .code-ref {
    font-family: 'SFMono-Regular', Menlo, monospace;
    font-size: 0.8rem; color: #808090;
}

/* ---- data scope flow ---- */
.data-scope .sample-flow {
    display: flex; flex-direction: column; align-items: center;
    margin: 1rem 0;
}
.data-scope .flow-step {
    background: rgba(102,126,234,0.1); padding: 0.5rem 1.2rem;
    border-radius: 8px; color: #e0e0e0; font-size: 0.95rem;
    min-width: 240px; text-align: center;
}
.data-scope .flow-step.final {
    background: rgba(240,147,251,0.2); color: #f0d0ff; font-weight: 600;
}
.data-scope .flow-arrow { color: #606070; line-height: 0.8; }
.data-scope dl.data-sources {
    display: grid; grid-template-columns: auto 1fr;
    gap: 0.4rem 1rem; margin: 0.8rem 0;
    font-size: 0.9rem;
}
.data-scope dl.data-sources dt { color: #808090; }
.data-scope dl.data-sources dd { color: #e0e0e0; }

/* ---- methods block ---- */
.methods-section .equation {
    background: rgba(0,0,0,0.15); padding: 0.7rem 1rem;
    border-radius: 6px; margin: 0.6rem 0;
}
.methods-section .eq-label {
    display: block; color: #a0d2db; font-size: 0.85rem;
    margin-bottom: 0.3rem;
}
.methods-section .eq-body { color: #e0e0e0; }
.methods-section ol, .methods-section dl {
    margin: 0.4rem 0 0.8rem 1.5rem;
    line-height: 1.7; color: #d0d0d8;
}
.methods-section dt { color: #c0c0d0; font-weight: 600; margin-top: 0.3rem; }
.methods-section dd { color: #a0a0b0; margin-left: 1rem; }

/* ---- reproducibility block ---- */
.reproducibility-section pre {
    background: rgba(0,0,0,0.3); padding: 0.8rem 1rem;
    border-radius: 6px; font-family: 'SFMono-Regular', Menlo, monospace;
    color: #c0c0d0; font-size: 0.85rem; overflow-x: auto;
}
.reproducibility-section dl {
    display: grid; grid-template-columns: auto 1fr;
    gap: 0.4rem 1rem; margin: 0.6rem 0;
}
.reproducibility-section dt { color: #808090; font-size: 0.85rem; }
.reproducibility-section dd { color: #c0c0d0; font-size: 0.9rem; }

/* ---- access layer badge ---- */
.access-layer-badge {
    display: inline-block; padding: 0.2rem 0.7rem;
    border-radius: 12px; font-size: 0.75rem; font-weight: 700;
    letter-spacing: 0.05em;
}
.access-layer-1 { background: rgba(6,214,160,0.2); color: #06D6A0; }
.access-layer-2 { background: rgba(255,209,102,0.2); color: #FFD166; }
.access-layer-3 { background: rgba(102,126,234,0.2); color: #a0b8ff; }
.access-layer-4 { background: rgba(239,71,111,0.2); color: #EF476F; }
"""


def configure_legacy_dirs(json_dir: Path, reports_dir: Path) -> None:
    """Point the legacy helpers / templates at the given directories.

    Mirrors the top-of-module configuration in ``generate_all_reports.py``.
    Callers should invoke this once at CLI startup so that
    ``helpers.get_footer_stats()`` and friends read from the correct paths.
    """
    _legacy_helpers.JSON_DIR = Path(json_dir)
    _legacy_templates.JSON_DIR = Path(json_dir)
    _legacy_templates.REPORTS_DIR = Path(reports_dir)


# ---------------------------------------------------------------------------
# wrap_html_with_katex
# ---------------------------------------------------------------------------
_KATEX_VERSION = "0.16.9"
_KATEX_BASE = f"https://cdn.jsdelivr.net/npm/katex@{_KATEX_VERSION}/dist"
_KATEX_HEAD = (
    f'<link rel="stylesheet" href="{_KATEX_BASE}/katex.min.css">\n'
    f'<script defer src="{_KATEX_BASE}/katex.min.js"></script>\n'
    f'<script defer src="{_KATEX_BASE}/contrib/auto-render.min.js" '
    'onload="renderMathInElement(document.body,{delimiters:['
    '{left:\'$$\',right:\'$$\',display:true},'
    '{left:\'$\',right:\'$\',display:false}]});"></script>'
)


def wrap_html_with_katex(
    title: str,
    subtitle: str,
    body: str,
    *,
    intro_html: str = "",
    glossary_terms: dict[str, str] | None = None,
    extra_css: str = "",
) -> str:
    """KaTeX-enabled variant of ``wrap_html()``.

    Renders the same layout as the legacy ``wrap_html`` but injects the
    KaTeX CDN assets and the academic CSS extensions into the ``<head>``.
    ``extra_css`` lets individual reports append report-specific CSS without
    editing the shared modules.
    """
    base = wrap_html(title, subtitle, body, intro_html=intro_html, glossary_terms=glossary_terms)
    # Inject KaTeX assets + ACADEMIC_CSS directly before ``</head>``.
    head_closing = "</head>"
    injection = (
        f"{_KATEX_HEAD}\n<style>{ACADEMIC_CSS}\n{extra_css}</style>\n{head_closing}"
    )
    return base.replace(head_closing, injection, 1)
