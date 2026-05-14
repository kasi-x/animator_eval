"""Short-form (2-page) Policy Brief for ministry / parliamentary briefings.

Created 2026-05-06 per TASK_CARDS/33_policy/01_short_form_brief.md.

Distilled from the long-form Policy Brief into an A4 2-page format suitable
for ministry policy desks (METI Content / Bunka-cho Media Arts /
MHLW Labor) and parliamentary caucus secretariats.

Layout:
- Page 1: project stance + 3-5 key findings (numeric headlines) + 1 figure
- Page 2: 4-5 policy recommendations (labor-first) + contact + short disclaimer

Output:
- HTML file (print-ready, 2-page A4)
- Markdown file (for plain-text distribution)

Generation strategy: curated extraction from the long-form policy_brief
(NOT regex-based — picks the headline numbers and recommendations that
travel best in a 2-page format and revisits them when the long-form changes).
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path
from typing import Any

import structlog

log = structlog.get_logger(__name__)

OUT_DIR_HTML = Path("result/html")
OUT_DIR_MD = Path("result/md")


# ---------------------------------------------------------------------------
# Curated content (revisit when the long-form policy_brief changes)
# ---------------------------------------------------------------------------

KEY_FINDINGS: list[dict[str, str]] = [
    {
        "label": "市場集中度 (HHI)",
        "value": "0.38",
        "context": "credit allocation by studio. 米 DOJ 反トラスト懸念域。",
    },
    {
        "label": "ジェンダー進行格差",
        "value": "2.4×",
        "context": "監督職におけるジェンダー underrepresentation。エントリー層の格差拡大傾向。",
    },
    {
        "label": "エントリー層離脱超過",
        "value": "1.23×",
        "context": "近年コホートの早期離脱率超過。動画役職で最大。",
    },
    {
        "label": "クレジット可視性ギャップ",
        "value": "source 別 0–60%",
        "context": "公開 source 間の credit 記載率にばらつき。動画 / 第二原画 で under-credited。",
    },
]

RECOMMENDATIONS: list[dict[str, str]] = [
    {
        "title": "クレジット記載ガイドライン化",
        "body": (
            "公開 source 間の credit 可視性ギャップは構造的。"
            "業界ガイドライン (動画協会・JAniCA 共同) で全役職の credit 公表を推奨し、"
            "労働者がクレジット公表を会社に依頼する根拠を整備する。"
        ),
    },
    {
        "title": "機会格差是正の補助金プログラム",
        "body": (
            "監督職ジェンダー underrepresentation 2.4× は構造的偏在。"
            "中堅期 (animation_director → director transition) への mentorship / "
            "intern 補助金で機会格差を狭める。"
        ),
    },
    {
        "title": "中堅枯渇対策の人材育成助成",
        "body": (
            "エントリー層離脱超過 1.23× は中堅へのパイプラインを細らせる。"
            "動画役職への賃金フロア・apprenticeship 補助で構造的離脱を抑制。"
        ),
    },
    {
        "title": "フリーランス保護法の運用強化",
        "body": (
            "フリーランス保護法 (2024 施行) の運用において、credit 不記載 / 報酬不透明 "
            "を厚労省 雇用環境・均等局が定期 monitor 対象とする運用ガイドライン整備。"
        ),
    },
    {
        "title": "労働実態調査との接続",
        "body": (
            "本データは構造的可視性 (credit-based) を補完するが、賃金・労働時間は別 source。"
            "JAniCA 労働実態調査 (賃金) + 本構造データ (機会) の統合分析を業界団体と協議。"
        ),
    },
]


# ---------------------------------------------------------------------------
# HTML rendering
# ---------------------------------------------------------------------------

_PRINT_CSS = """
<style>
@page { size: A4; margin: 18mm 16mm; }
* { box-sizing: border-box; }
body {
    font-family: "Hiragino Sans", "Yu Gothic", "Noto Sans JP", system-ui, sans-serif;
    color: #1a1a1a;
    line-height: 1.5;
    margin: 0;
    font-size: 10pt;
}
.page {
    width: 100%;
    page-break-after: always;
}
.page:last-child { page-break-after: auto; }
header.brief-header {
    border-bottom: 2px solid #2a4d8f;
    padding-bottom: 8px;
    margin-bottom: 12px;
}
header.brief-header h1 {
    font-size: 14pt;
    margin: 0 0 2px;
    color: #2a4d8f;
}
header.brief-header .meta {
    font-size: 8pt;
    color: #555;
}
.stance-line {
    background: #eef3fb;
    border-left: 3px solid #2a4d8f;
    padding: 6px 10px;
    margin: 8px 0 14px;
    font-size: 9pt;
}
.findings-grid {
    display: grid;
    grid-template-columns: 1fr 1fr;
    gap: 8px 14px;
    margin-bottom: 14px;
}
.finding-card {
    border: 1px solid #d4d8df;
    border-radius: 4px;
    padding: 8px 10px;
}
.finding-card .label { font-size: 8.5pt; color: #555; }
.finding-card .value {
    font-size: 18pt;
    font-weight: 700;
    color: #2a4d8f;
    margin: 2px 0;
}
.finding-card .context { font-size: 8.5pt; color: #333; }
.figure-slot {
    border: 1px dashed #aaa;
    height: 110px;
    display: flex;
    align-items: center;
    justify-content: center;
    color: #888;
    font-size: 9pt;
    margin-bottom: 10px;
}
section.recommendations h2 {
    font-size: 12pt;
    color: #2a4d8f;
    margin: 4px 0 8px;
    border-bottom: 1px solid #d4d8df;
    padding-bottom: 4px;
}
.rec {
    margin-bottom: 10px;
    padding-left: 14px;
    position: relative;
}
.rec::before {
    content: counter(rec-counter);
    counter-increment: rec-counter;
    position: absolute;
    left: 0;
    font-weight: 700;
    color: #2a4d8f;
}
.rec .title { font-weight: 600; font-size: 10pt; }
.rec .body { font-size: 9pt; color: #333; }
section.recommendations { counter-reset: rec-counter; }
.contact {
    margin-top: 14px;
    padding: 8px 10px;
    background: #f4f6fa;
    border-radius: 4px;
    font-size: 9pt;
}
.contact strong { color: #2a4d8f; }
.disclaimer-short {
    font-size: 8pt;
    color: #555;
    border-top: 1px solid #d4d8df;
    padding-top: 6px;
    margin-top: 12px;
    line-height: 1.4;
}
.page-num {
    text-align: right;
    font-size: 8pt;
    color: #888;
    margin-top: 6px;
}
</style>
"""


def _render_page1(timestamp: str) -> str:
    findings_html = "\n".join(
        f'<div class="finding-card">'
        f'<div class="label">{f["label"]}</div>'
        f'<div class="value">{f["value"]}</div>'
        f'<div class="context">{f["context"]}</div>'
        f"</div>"
        for f in KEY_FINDINGS
    )
    return f"""
<div class="page">
  <header class="brief-header">
    <h1>政策 brief (短縮版) — アニメ業界の労働構造観察</h1>
    <div class="meta">Animetor Eval / labor-first observation / generated: {timestamp}</div>
  </header>
  <div class="stance-line">
    <strong>本 brief の立場.</strong>
    本プロジェクトは <em>労働者寄り (labor-first)</em> の構造観察。
    本 brief は労働者保護・機会格差是正に資する観察を、政策担当向けに 2 ページに圧縮した。
  </div>
  <section>
    <h2 style="font-size:11pt;color:#2a4d8f;margin:6px 0 8px;">主要 findings (公開 credit データ)</h2>
    <div class="findings-grid">
      {findings_html}
    </div>
    <div class="figure-slot">[Figure 1: 主要指標時系列 / 業界平均との比較 — 別添]</div>
    <div style="font-size:8.5pt;color:#444;">
      数値の出所・method: AKM 個人固定効果 + 構造的協業ネットワーク指標。
      analytical CI を全推定に付与。詳細は長文版 Policy brief。
    </div>
  </section>
  <div class="page-num">— 1 / 2 —</div>
</div>
"""


def _render_page2(contact: dict[str, str]) -> str:
    recs_html = "\n".join(
        f'<div class="rec">'
        f'<div class="title">{r["title"]}</div>'
        f'<div class="body">{r["body"]}</div>'
        f"</div>"
        for r in RECOMMENDATIONS
    )
    return f"""
<div class="page">
  <section class="recommendations">
    <h2>政策推奨 (labor-first orientation)</h2>
    {recs_html}
  </section>
  <div class="contact">
    <strong>連絡先 / 詳細資料:</strong><br>
    Project: Animetor Eval — labor-first 構造観察<br>
    Email: {contact["email"]}<br>
    詳細 brief: {contact["full_brief_url"]}<br>
    Stance: {contact["stance_url"]}
  </div>
  <div class="disclaimer-short">
    本データは公開クレジットの集約に基づく構造観察であり、個人の能力・適性を測るものではない。
    採用判断・人事評価・序列化の根拠として使用してはならない。
    on-request 削除機構 (opt-out) を提供。詳細は STANCE.md / REPORT_PHILOSOPHY.md。
  </div>
  <div class="page-num">— 2 / 2 —</div>
</div>
"""


def render_html(contact: dict[str, str] | None = None) -> str:
    """Render the full 2-page short-form policy brief as HTML."""
    if contact is None:
        contact = {
            "email": "policy@animetor-eval.example",
            "full_brief_url": "https://example.com/policy_brief_full.html",
            "stance_url": "https://example.com/STANCE.md",
        }
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M JST")
    return f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="UTF-8">
<title>政策 brief (短縮版) — Animetor Eval</title>
{_PRINT_CSS}
</head>
<body>
{_render_page1(timestamp)}
{_render_page2(contact)}
</body>
</html>
"""


def render_markdown() -> str:
    """Render the same content as plain Markdown (for non-HTML distribution)."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M JST")
    lines = [
        "# 政策 brief (短縮版) — アニメ業界の労働構造観察",
        "",
        f"_Animetor Eval / labor-first observation / generated: {timestamp}_",
        "",
        "## 本 brief の立場",
        "",
        "本プロジェクトは **労働者寄り (labor-first)** の構造観察。",
        "本 brief は労働者保護・機会格差是正に資する観察を、",
        "政策担当向けに 2 ページに圧縮した。",
        "",
        "## 主要 findings (公開 credit データ)",
        "",
    ]
    for f in KEY_FINDINGS:
        lines.append(f"- **{f['label']}**: {f['value']} — {f['context']}")
    lines.extend([
        "",
        "_数値の出所・method: AKM 個人固定効果 + 構造的協業ネットワーク指標。"
        "analytical CI を全推定に付与。詳細は長文版 Policy brief。_",
        "",
        "## 政策推奨 (labor-first orientation)",
        "",
    ])
    for i, r in enumerate(RECOMMENDATIONS, 1):
        lines.append(f"{i}. **{r['title']}** — {r['body']}")
    lines.extend([
        "",
        "## 連絡先 / 詳細資料",
        "",
        "- Project: Animetor Eval — labor-first 構造観察",
        "- Stance: docs/STANCE.md",
        "- 詳細 brief: result/html/policy_brief.html",
        "",
        "---",
        "",
        "_本データは公開クレジットの集約に基づく構造観察であり、"
        "個人の能力・適性を測るものではない。"
        "採用判断・人事評価・序列化の根拠として使用してはならない。"
        "on-request 削除機構 (opt-out) を提供。"
        "詳細は STANCE.md / REPORT_PHILOSOPHY.md。_",
        "",
    ])
    return "\n".join(lines)


def generate_short_policy_brief(
    contact: dict[str, str] | None = None,
    out_dir_html: Path = OUT_DIR_HTML,
    out_dir_md: Path = OUT_DIR_MD,
) -> dict[str, Path]:
    """Generate both HTML and Markdown short-form briefs.

    Returns dict with output paths.
    """
    out_dir_html.mkdir(parents=True, exist_ok=True)
    out_dir_md.mkdir(parents=True, exist_ok=True)

    html_path = out_dir_html / "policy_brief_short.html"
    md_path = out_dir_md / "policy_brief_short.md"

    html = render_html(contact)
    md = render_markdown()

    html_path.write_text(html, encoding="utf-8")
    md_path.write_text(md, encoding="utf-8")

    log.info(
        "policy_brief_short_generated",
        html=str(html_path),
        md=str(md_path),
        html_size_kb=round(len(html) / 1024, 1),
        md_size_kb=round(len(md) / 1024, 1),
    )

    return {"html": html_path, "md": md_path}


if __name__ == "__main__":
    paths = generate_short_policy_brief()
    print(f"HTML: {paths['html']}")
    print(f"MD:   {paths['md']}")
