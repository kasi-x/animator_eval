"""レポート生成 — JSON/テキスト形式の評価レポート出力.

スコアは「ネットワーク位置と密度の指標」であり、
「能力」の評価ではないことを明示する（法的要件）。
"""

import csv
import json
from datetime import datetime
from pathlib import Path

import structlog

from src.utils.config import JSON_DIR

logger = structlog.get_logger()

DISCLAIMER = (
    "本スコアはクレジットデータに基づくネットワーク位置・密度の定量指標であり、"
    "個人の能力や技量を評価するものではありません。"
    "個人の貢献を可視化し、適正な報酬と業界の健全化に資することを目的としています。"
)

DISCLAIMER_EN = (
    "These scores are quantitative indicators of network position and density "
    "based on credit data. They do NOT evaluate individual ability or skill. "
    "They aim to make individual contributions visible, supporting fair "
    "compensation and a healthier anime industry."
)


def generate_json_report(
    results: list[dict],
    output_path: Path | None = None,
) -> Path:
    """JSON形式のレポートを出力する."""
    report = {
        "metadata": {
            "generated_at": datetime.now().isoformat(),
            "disclaimer_ja": DISCLAIMER,
            "disclaimer_en": DISCLAIMER_EN,
            "total_persons": len(results),
            "scoring_axes": {
                "authority": "Weighted PageRank — コラボレーショングラフ上のネットワーク中心性",
                "trust": "Cumulative edge weight — 監督/演出からの継続起用度",
                "skill": "OpenSkill rating — 高評価作品への参加に基づくレーティング",
                "composite": "3軸の重み付き統合 (A:0.4, T:0.35, S:0.25)",
            },
        },
        "rankings": results,
    }

    if output_path is None:
        output_path = JSON_DIR / "report.json"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        json.dump(report, f, indent=2, ensure_ascii=False)

    logger.info("json_report_generated", path=str(output_path), persons=len(results))
    return output_path


def generate_text_report(
    results: list[dict],
    top_n: int = 50,
    output_path: Path | None = None,
) -> Path:
    """テキスト形式のレポートを出力する."""
    lines = [
        "=" * 80,
        "Animetor Eval — ネットワーク評価レポート",
        f"生成日時: {datetime.now().strftime('%Y-%m-%d %H:%M')}",
        "=" * 80,
        "",
        "【注意】" + DISCLAIMER,
        "",
        f"評価対象: {len(results)} 名",
        "",
        "-" * 80,
        f"{'Rank':<6}{'Name':<30}{'Auth':>8}{'Trust':>8}{'Skill':>8}{'Total':>8}",
        "-" * 80,
    ]

    for i, r in enumerate(results[:top_n], 1):
        name = r.get("name", r.get("person_id", ""))[:28]
        lines.append(
            f"{i:<6}{name:<30}{r['authority']:>8.1f}{r['trust']:>8.1f}"
            f"{r['skill']:>8.1f}{r['composite']:>8.1f}"
        )

    lines.extend([
        "-" * 80,
        "",
    ])

    # Career summary for top persons
    career_persons = [r for r in results[:top_n] if r.get("career")]
    if career_persons:
        lines.extend([
            "キャリアサマリー (上位):",
            "-" * 80,
            f"{'Name':<30}{'Years':>10}{'Stage':>8}{'Top Roles':<30}",
            "-" * 80,
        ])
        for r in career_persons[:20]:
            name = r.get("name", "")[:28]
            career = r["career"]
            year_range = f"{career.get('first_year', '?')}-{career.get('latest_year', '?')}"
            stage = str(career.get("highest_stage", "?"))
            top_roles = ", ".join(career.get("highest_roles", []))[:28]
            lines.append(f"{name:<30}{year_range:>10}{stage:>8}  {top_roles:<28}")
        lines.extend(["-" * 80, ""])

    lines.extend([
        "スコア凡例:",
        "  Authority : PageRank（高影響力の監督/作品との関係が多いほど高い）",
        "  Trust     : 継続起用度（同じ監督から繰り返し起用されるほど高い）",
        "  Skill     : OpenSkillレーティング（高評価作品への参加が多いほど高い）",
        "  Total     : 統合スコア (Auth×0.4 + Trust×0.35 + Skill×0.25)",
        "",
        "=" * 80,
    ])

    text = "\n".join(lines)

    if output_path is None:
        output_path = JSON_DIR.parent / "report.txt"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w") as f:
        f.write(text)

    logger.info("text_report_generated", path=str(output_path))
    return output_path


def generate_csv_report(
    results: list[dict],
    output_path: Path | None = None,
) -> Path:
    """CSV形式のレポートを出力する."""
    if output_path is None:
        output_path = JSON_DIR.parent / "scores.csv"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    fieldnames = ["rank", "person_id", "name", "name_ja", "name_en",
                  "authority", "trust", "skill", "composite",
                  "authority_pct", "trust_pct", "skill_pct", "composite_pct",
                  "primary_role", "total_credits",
                  "first_year", "latest_year", "active_years",
                  "highest_stage", "highest_roles"]

    with open(output_path, "w", newline="", encoding="utf-8-sig") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for i, r in enumerate(results, 1):
            career = r.get("career", {})
            writer.writerow({
                "rank": i,
                "person_id": r.get("person_id", ""),
                "name": r.get("name", ""),
                "name_ja": r.get("name_ja", ""),
                "name_en": r.get("name_en", ""),
                "authority": r.get("authority", 0),
                "trust": r.get("trust", 0),
                "skill": r.get("skill", 0),
                "composite": r.get("composite", 0),
                "authority_pct": r.get("authority_pct", ""),
                "trust_pct": r.get("trust_pct", ""),
                "skill_pct": r.get("skill_pct", ""),
                "composite_pct": r.get("composite_pct", ""),
                "primary_role": r.get("primary_role", ""),
                "total_credits": r.get("total_credits", ""),
                "first_year": career.get("first_year", ""),
                "latest_year": career.get("latest_year", ""),
                "active_years": career.get("active_years", ""),
                "highest_stage": career.get("highest_stage", ""),
                "highest_roles": "|".join(career.get("highest_roles", [])),
            })

    logger.info("csv_report_generated", path=str(output_path), persons=len(results))
    return output_path


def generate_html_report(
    results: list[dict],
    top_n: int = 50,
    output_path: Path | None = None,
) -> Path:
    """HTML形式の評価レポートを生成する.

    インライン SVG チャートを含むスタンドアロン HTML ファイル。
    """
    if output_path is None:
        output_path = JSON_DIR.parent / "report.html"
    output_path.parent.mkdir(parents=True, exist_ok=True)

    top = results[:top_n]
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    # Build ranking table rows
    table_rows = []
    for i, r in enumerate(top, 1):
        name = _html_escape(r.get("name", r.get("person_id", "")))
        role = r.get("primary_role", "")
        table_rows.append(
            f"<tr><td>{i}</td><td>{name}</td><td>{role}</td>"
            f"<td>{r['authority']:.1f}</td><td>{r['trust']:.1f}</td>"
            f"<td>{r['skill']:.1f}</td><td><strong>{r['composite']:.1f}</strong></td></tr>"
        )

    # Build SVG bar chart for score distribution
    max_composite = max((r["composite"] for r in top), default=1) or 1
    bars = []
    for i, r in enumerate(top[:20]):
        y = i * 28
        w_auth = r["authority"] / max_composite * 300
        w_trust = r["trust"] / max_composite * 300
        w_skill = r["skill"] / max_composite * 300
        name = _html_escape(r.get("name", "")[:20])
        bars.append(
            f'<text x="0" y="{y + 18}" font-size="11" fill="#333">{name}</text>'
            f'<rect x="160" y="{y + 4}" width="{w_auth:.0f}" height="7" fill="#2196F3" opacity="0.8"/>'
            f'<rect x="160" y="{y + 12}" width="{w_trust:.0f}" height="7" fill="#4CAF50" opacity="0.8"/>'
            f'<rect x="160" y="{y + 20}" width="{w_skill:.0f}" height="7" fill="#FF9800" opacity="0.8"/>'
        )
    chart_height = min(len(top), 20) * 28 + 10
    svg_chart = (
        f'<svg width="500" height="{chart_height}" xmlns="http://www.w3.org/2000/svg">'
        + "\n".join(bars)
        + "</svg>"
    )

    # Role distribution pie data
    role_counts: dict[str, int] = {}
    for r in results:
        role = r.get("primary_role", "other")
        role_counts[role] = role_counts.get(role, 0) + 1

    role_items = "".join(
        f"<li><strong>{role}</strong>: {count}</li>"
        for role, count in sorted(role_counts.items(), key=lambda x: -x[1])
    )

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Animetor Eval Report — {generated_at}</title>
<style>
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif; margin: 40px; color: #333; background: #fafafa; }}
  h1 {{ color: #1a237e; border-bottom: 3px solid #1a237e; padding-bottom: 8px; }}
  h2 {{ color: #283593; margin-top: 32px; }}
  .disclaimer {{ background: #fff3e0; border-left: 4px solid #ff9800; padding: 12px 16px; margin: 16px 0; font-size: 14px; }}
  .meta {{ color: #666; font-size: 13px; }}
  table {{ border-collapse: collapse; width: 100%; margin: 16px 0; }}
  th, td {{ border: 1px solid #ddd; padding: 8px 12px; text-align: right; }}
  th {{ background: #e8eaf6; color: #1a237e; }}
  td:nth-child(2) {{ text-align: left; }}
  td:nth-child(3) {{ text-align: left; }}
  tr:nth-child(even) {{ background: #f5f5f5; }}
  tr:hover {{ background: #e3f2fd; }}
  .chart-section {{ display: flex; gap: 40px; flex-wrap: wrap; }}
  .chart-box {{ background: #fff; border: 1px solid #ddd; border-radius: 8px; padding: 16px; }}
  .legend {{ display: flex; gap: 16px; margin: 8px 0; font-size: 13px; }}
  .legend span {{ display: inline-flex; align-items: center; gap: 4px; }}
  .legend .dot {{ width: 12px; height: 12px; border-radius: 2px; display: inline-block; }}
  footer {{ margin-top: 40px; padding-top: 16px; border-top: 1px solid #ddd; color: #999; font-size: 12px; }}
</style>
</head>
<body>
<h1>Animetor Eval Report</h1>
<p class="meta">Generated: {generated_at} | Persons evaluated: {len(results)}</p>

<div class="disclaimer">
<strong>【注意】</strong>{DISCLAIMER}<br>
<strong>[Notice]</strong> {DISCLAIMER_EN}
</div>

<h2>Top {top_n} Ranking</h2>
<table>
<thead>
<tr><th>#</th><th>Name</th><th>Role</th><th>Authority</th><th>Trust</th><th>Skill</th><th>Composite</th></tr>
</thead>
<tbody>
{"".join(table_rows)}
</tbody>
</table>

<h2>Score Distribution (Top 20)</h2>
<div class="chart-box">
<div class="legend">
  <span><span class="dot" style="background:#2196F3"></span> Authority</span>
  <span><span class="dot" style="background:#4CAF50"></span> Trust</span>
  <span><span class="dot" style="background:#FF9800"></span> Skill</span>
</div>
{svg_chart}
</div>

<h2>Role Distribution</h2>
<ul>{role_items}</ul>

<footer>
Animetor Eval v0.1 — Scores represent network position and density, not individual ability.
</footer>
</body>
</html>"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    logger.info("html_report_generated", path=str(output_path), persons=len(results))
    return output_path


def generate_visual_dashboard(
    results: list[dict],
    png_dir: Path | None = None,
    output_path: Path | None = None,
) -> Path:
    """PNG画像を埋め込んだビジュアルダッシュボードHTMLを生成する.

    パイプライン実行後に生成された全PNGファイルをbase64エンコードして
    1つのスタンドアロンHTMLにまとめる。

    Args:
        results: スコア結果リスト
        png_dir: PNG画像があるディレクトリ (default: JSON_DIR.parent)
        output_path: 出力先
    """
    import base64

    if output_path is None:
        output_path = JSON_DIR.parent / "dashboard.html"
    if png_dir is None:
        png_dir = JSON_DIR.parent

    output_path.parent.mkdir(parents=True, exist_ok=True)
    generated_at = datetime.now().strftime("%Y-%m-%d %H:%M")

    # Collect all PNGs
    chart_sections = []
    chart_order = [
        ("score_distribution.png", "Score Distribution"),
        ("top_radar.png", "Top Persons Radar"),
        ("collaboration_network.png", "Collaboration Network"),
        ("time_series.png", "Industry Activity Over Time"),
        ("growth_trends.png", "Career Growth Trends"),
        ("network_evolution.png", "Network Evolution"),
        ("decade_comparison.png", "Decade Comparison"),
        ("transitions.png", "Role Transitions"),
        ("role_flow.png", "Role Flow"),
        ("productivity.png", "Productivity Distribution"),
        ("collaborations.png", "Collaboration Strength"),
        ("influence_tree.png", "Influence Tree"),
        ("milestones.png", "Career Milestones"),
        ("bridges.png", "Bridge Analysis"),
        ("anime_stats.png", "Anime Statistics"),
        ("genre_affinity.png", "Genre Affinity"),
        ("tags.png", "Person Tags"),
        ("seasonal.png", "Seasonal Trends"),
        ("studios.png", "Studio Comparison"),
        ("outliers.png", "Outliers"),
        ("crossval.png", "Cross-Validation Stability"),
        ("performance.png", "Pipeline Performance Metrics"),
    ]

    for filename, title in chart_order:
        png_path = png_dir / filename
        if png_path.exists():
            with open(png_path, "rb") as f:
                b64 = base64.b64encode(f.read()).decode("ascii")
            chart_sections.append(
                f'<div class="chart-card">'
                f'<h3>{_html_escape(title)}</h3>'
                f'<img src="data:image/png;base64,{b64}" alt="{_html_escape(title)}" />'
                f'</div>'
            )

    # Top ranking table
    top = results[:30]
    table_rows = []
    for i, r in enumerate(top, 1):
        name = _html_escape(r.get("name", r.get("person_id", "")))
        role = r.get("primary_role", "")
        tags = ", ".join(r.get("tags", [])[:3])
        table_rows.append(
            f"<tr><td>{i}</td><td>{name}</td><td>{role}</td>"
            f"<td>{r['authority']:.1f}</td><td>{r['trust']:.1f}</td>"
            f"<td>{r['skill']:.1f}</td><td><strong>{r['composite']:.1f}</strong></td>"
            f"<td>{tags}</td></tr>"
        )

    html = f"""<!DOCTYPE html>
<html lang="ja">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Animetor Eval Dashboard — {generated_at}</title>
<style>
  * {{ box-sizing: border-box; }}
  body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
         margin: 0; padding: 20px 40px; color: #333; background: #f0f2f5; }}
  h1 {{ color: #1a237e; margin-bottom: 4px; }}
  h2 {{ color: #283593; margin-top: 32px; border-bottom: 2px solid #c5cae9; padding-bottom: 6px; }}
  h3 {{ color: #3949ab; margin: 0 0 12px 0; }}
  .meta {{ color: #666; font-size: 13px; margin-bottom: 16px; }}
  .disclaimer {{ background: #fff3e0; border-left: 4px solid #ff9800; padding: 12px 16px;
                 margin: 16px 0; font-size: 13px; border-radius: 0 4px 4px 0; }}
  .stats-row {{ display: flex; gap: 16px; flex-wrap: wrap; margin: 16px 0; }}
  .stat-card {{ background: #fff; border-radius: 8px; padding: 16px 24px; min-width: 150px;
                box-shadow: 0 1px 3px rgba(0,0,0,0.12); text-align: center; }}
  .stat-card .value {{ font-size: 28px; font-weight: bold; color: #1a237e; }}
  .stat-card .label {{ font-size: 12px; color: #666; margin-top: 4px; }}
  table {{ border-collapse: collapse; width: 100%; margin: 16px 0; background: #fff;
           border-radius: 8px; overflow: hidden; box-shadow: 0 1px 3px rgba(0,0,0,0.12); }}
  th, td {{ border: 1px solid #e0e0e0; padding: 8px 12px; text-align: right; }}
  th {{ background: #e8eaf6; color: #1a237e; font-size: 13px; }}
  td:nth-child(2), td:nth-child(3), td:nth-child(8) {{ text-align: left; }}
  tr:nth-child(even) {{ background: #fafafa; }}
  tr:hover {{ background: #e3f2fd; }}
  .chart-grid {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(600px, 1fr));
                 gap: 20px; margin: 16px 0; }}
  .chart-card {{ background: #fff; border-radius: 8px; padding: 16px;
                 box-shadow: 0 1px 3px rgba(0,0,0,0.12); }}
  .chart-card img {{ width: 100%; height: auto; border-radius: 4px; }}
  footer {{ margin-top: 40px; padding-top: 16px; border-top: 1px solid #ddd;
            color: #999; font-size: 12px; text-align: center; }}
</style>
</head>
<body>
<h1>Animetor Eval Dashboard</h1>
<p class="meta">Generated: {generated_at} | {len(results)} persons evaluated | {len(chart_sections)} charts</p>

<div class="disclaimer">
<strong>【注意】</strong>{DISCLAIMER}<br>
<strong>[Notice]</strong> {DISCLAIMER_EN}
</div>

<div class="stats-row">
  <div class="stat-card"><div class="value">{len(results)}</div><div class="label">Persons</div></div>
  <div class="stat-card"><div class="value">{results[0]['composite']:.1f}</div><div class="label">Top Score</div></div>
  <div class="stat-card"><div class="value">{results[len(results)//2]['composite']:.1f}</div><div class="label">Median Score</div></div>
  <div class="stat-card"><div class="value">{len(chart_sections)}</div><div class="label">Charts</div></div>
</div>

<h2>Top 30 Ranking</h2>
<table>
<thead>
<tr><th>#</th><th>Name</th><th>Role</th><th>Auth</th><th>Trust</th><th>Skill</th><th>Composite</th><th>Tags</th></tr>
</thead>
<tbody>
{"".join(table_rows)}
</tbody>
</table>

<h2>Visual Analytics</h2>
{"<div class='chart-grid'>" + "".join(chart_sections) + "</div>" if chart_sections else "<p>No visualization charts available. Run pipeline with <code>visualize=True</code> to generate charts.</p>"}

<footer>
Animetor Eval — Scores represent network position and density, not individual ability.
</footer>
</body>
</html>"""

    with open(output_path, "w", encoding="utf-8") as f:
        f.write(html)

    logger.info("visual_dashboard_generated", path=str(output_path), charts=len(chart_sections))
    return output_path


def _html_escape(text: str) -> str:
    """Basic HTML escaping."""
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;").replace('"', "&quot;")


def main() -> None:
    """DBからスコアを読み込みレポートを生成."""
    from src.log import setup_logging

    setup_logging()

    # まず pipeline を実行してスコアを計算
    from src.pipeline import run_scoring_pipeline

    results = run_scoring_pipeline()

    if not results:
        logger.warning("No results to report")
        return

    generate_json_report(results)
    generate_text_report(results)
    generate_csv_report(results)
    generate_html_report(results)


if __name__ == "__main__":
    main()
