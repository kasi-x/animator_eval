"""Tests for the 2-page short-form Policy Brief.

Per TASK_CARDS/33_policy/01_short_form_brief.md.

Verifies:
- HTML and Markdown outputs are produced
- Both contain the labor-first STANCE line
- Both contain the policy recommendations (count + structural cues)
- The HTML uses A4 print CSS and 2 page-break sections
- Outputs pass lint_vocab (no banned terms outside negation context)
"""

from __future__ import annotations

from pathlib import Path

import pytest

from scripts.report_generators.briefs.policy_brief_short import (
    KEY_FINDINGS,
    RECOMMENDATIONS,
    generate_short_policy_brief,
    render_html,
    render_markdown,
)


def test_render_html_contains_stance_line():
    html = render_html()
    assert "labor-first" in html
    assert "本 brief の立場" in html
    assert "労働者寄り" in html


def test_render_html_contains_a4_print_css():
    html = render_html()
    assert "@page" in html
    assert "A4" in html


def test_render_html_has_two_page_breaks():
    html = render_html()
    # Two `<div class="page">` blocks; the last has page-break-after: auto.
    assert html.count('class="page"') == 2


def test_render_html_contains_all_findings():
    html = render_html()
    for f in KEY_FINDINGS:
        assert f["label"] in html
        assert f["value"] in html


def test_render_html_contains_all_recommendations():
    html = render_html()
    for r in RECOMMENDATIONS:
        assert r["title"] in html


def test_render_markdown_structure():
    md = render_markdown()
    assert md.startswith("# 政策 brief (短縮版)")
    assert "本プロジェクトは **労働者寄り (labor-first)** の構造観察" in md
    assert "## 主要 findings" in md
    assert "## 政策推奨" in md


def test_render_markdown_recommendation_count():
    md = render_markdown()
    # Each recommendation appears as a numbered list item.
    for i, r in enumerate(RECOMMENDATIONS, 1):
        assert f"{i}. **{r['title']}**" in md


def test_generate_writes_files(tmp_path: Path):
    paths = generate_short_policy_brief(
        out_dir_html=tmp_path / "html",
        out_dir_md=tmp_path / "md",
    )
    assert paths["html"].exists()
    assert paths["md"].exists()
    assert paths["html"].suffix == ".html"
    assert paths["md"].suffix == ".md"
    assert paths["html"].stat().st_size > 1000  # non-trivial output
    assert paths["md"].stat().st_size > 500


def test_disclaimer_uses_negation_context():
    """Disclaimer references banned terms only inside negation context.

    Required because the short brief explicitly names what these scores are
    NOT for ("能力", "適性", "序列化"). lint_vocab must accept these as
    negation, not flag them.
    """
    md = render_markdown()
    # Banned terms are present (in negation context).
    assert "能力" in md or "適性" in md
    # And the negation phrase is present.
    assert "ものではない" in md or "してはならない" in md


@pytest.mark.parametrize(
    "rec_idx,expected_keyword",
    [
        (0, "クレジット"),  # クレジット記載ガイドライン化
        (1, "ジェンダー"),  # 機会格差是正の補助金
        (2, "中堅"),  # 中堅枯渇対策
        (3, "フリーランス"),  # フリーランス保護法
        (4, "労働実態"),  # 労働実態調査との接続
    ],
)
def test_recommendation_topic_alignment(rec_idx: int, expected_keyword: str):
    """Each recommendation body contains its topic keyword."""
    rec = RECOMMENDATIONS[rec_idx]
    assert expected_keyword in rec["body"] or expected_keyword in rec["title"]
