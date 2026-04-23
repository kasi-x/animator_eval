#!/usr/bin/env python3
"""スタートアップコスト分析スクリプト.

アニメ制作における固定費（立ち上げコスト）と変動費（話数比例コスト）を
OLS回帰により定量化する再利用可能なスタンドアロンスクリプト。

使い方:
    pixi run python scripts/analyze_startup_costs.py
    pixi run python scripts/analyze_startup_costs.py --genre Action
    pixi run python scripts/analyze_startup_costs.py --output result/json/startup_costs.json

分析内容:
    - 固定費ロール（キャラデザ・監督・音楽等）のクール数別分布
    - 変動費ロール（原画・動画・演出等）のクール数別分布
    - OLS回帰: startup_persons ~ cour_count (by genre)
    - 年度別固定費トレンド（1クール作品限定）
    - ジャンル×年代クロス集計
    - スタジオ規模別固定費比較
"""

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.generate_all_reports import _build_startup_cost_data


def main():
    parser = argparse.ArgumentParser(description="アニメ制作コスト分析")
    parser.add_argument("--genre", help="特定ジャンルに絞り込み (例: Action, Drama)")
    parser.add_argument("--output", help="JSON出力パス (省略時はコンソール表示)")
    parser.add_argument(
        "--top-genres",
        type=int,
        default=10,
        help="表示するジャンル数 (デフォルト: 10)",
    )
    args = parser.parse_args()

    from src.db import get_connection

    conn = get_connection()
    print("データベースに接続しました。分析中...")
    data = _build_startup_cost_data(conn)
    conn.close()

    raw_rows = data["raw_rows"]
    ols_by_genre = data["ols_by_genre"]
    by_year_1cour = data["by_year_1cour"]
    genre_decade = data["genre_decade"]

    if args.genre:
        raw_rows = [r for r in raw_rows if args.genre in r["genres"]]
        print(f"\n[フィルタ: genre={args.genre}] 対象作品数: {len(raw_rows)}")

    # ── サマリー表示 ──────────────────────────────────────────────────────
    import statistics

    print(f"\n{'='*60}")
    print("固定費 vs 変動費 分析サマリー")
    print(f"{'='*60}")
    print(f"対象TVアニメ・ONA数: {len(raw_rows)}")

    # クール数別中央値
    by_cour: dict = {}
    for row in raw_rows:
        cc = min(row["cour_count"], 5)
        by_cour.setdefault(cc, {"startup": [], "variable": []})
        by_cour[cc]["startup"].append(row["startup_persons"])
        if row["variable_persons"] > 0:
            by_cour[cc]["variable"].append(row["variable_persons"])

    print("\n▼ クール数別 固定費/変動費 中央値")
    print(f"{'クール数':<12} {'固定費(n)':<18} {'変動費(n)':<18}")
    print("-" * 50)
    for cc in sorted(by_cour.keys()):
        sp = by_cour[cc]["startup"]
        vp = by_cour[cc]["variable"]
        lbl = f"{cc}+" if cc == 5 else str(cc)
        sp_med = f"{statistics.median(sp):.1f} (n={len(sp)})"
        vp_med = f"{statistics.median(vp):.1f} (n={len(vp)})" if vp else "N/A"
        print(f"{lbl:<12} {sp_med:<18} {vp_med:<18}")

    # OLS by genre
    top_n = args.top_genres
    sorted_genres = sorted(ols_by_genre.items(), key=lambda x: -x[1]["n"])[:top_n]
    print(f"\n▼ ジャンル別 OLS結果 (startup_persons ~ cour_count, 上位{top_n}ジャンル)")
    print(f"{'ジャンル':<20} {'intercept':>12} {'slope':>10} {'R²':>8} {'n':>6}")
    print("-" * 60)
    for g, res in sorted_genres:
        print(
            f"{g:<20} {res['intercept']:>12.2f} {res['slope']:>10.3f} "
            f"{res['r2']:>8.4f} {res['n']:>6}"
        )

    # Year trend (1-cour)
    recent_years = sorted(
        [yr for yr in by_year_1cour if yr >= 2010],
        key=lambda y: -by_year_1cour[y]["n"],
    )[:5]
    if recent_years:
        print("\n▼ 年度別固定費中央値 (1クール限定, n上位5年)")
        print(f"{'年':<8} {'固定費中央値':>14} {'変動費中央値':>14} {'n':>6}")
        print("-" * 46)
        for yr in sorted(recent_years):
            d = by_year_1cour[yr]
            print(
                f"{yr:<8} {d['startup_med']:>14.1f} {d['variable_med']:>14.1f} "
                f"{d['n']:>6}"
            )

    # Studio tier
    print("\n▼ スタジオ規模別固定費 (AniList人気度プロキシ)")
    tier_startup: dict = {}
    for row in raw_rows:
        t = row.get("studio_tier", "小規模 (<100 fav)")
        tier_startup.setdefault(t, []).append(row["startup_persons"])
    tier_order = ["大手 (1000+ fav)", "中規模 (100-999 fav)", "小規模 (<100 fav)"]
    print(f"{'スタジオ規模':<25} {'固定費中央値':>14} {'n':>6}")
    print("-" * 46)
    for t in tier_order:
        vals = tier_startup.get(t, [])
        if vals:
            print(f"{t:<25} {statistics.median(vals):>14.1f} {len(vals):>6}")

    # JSON output
    if args.output:
        # Convert tuple keys to strings for JSON serialization
        genre_decade_serializable = {
            f"{g}::{dec}": val for (g, dec), val in genre_decade.items()
        }
        output_data = {
            "summary": {
                "total_anime": len(raw_rows),
                "cour_breakdown": {
                    str(cc): {
                        "startup_median": statistics.median(d["startup"]) if d["startup"] else None,
                        "variable_median": statistics.median(d["variable"]) if d["variable"] else None,
                        "n": len(d["startup"]),
                    }
                    for cc, d in by_cour.items()
                },
            },
            "ols_by_genre": ols_by_genre,
            "by_year_1cour": {str(yr): v for yr, v in by_year_1cour.items()},
            "genre_decade": genre_decade_serializable,
        }
        out_path = Path(args.output)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        with out_path.open("w", encoding="utf-8") as f:
            json.dump(output_data, f, ensure_ascii=False, indent=2)
        print(f"\n→ JSON出力: {out_path}")

    print("\n完了。")


if __name__ == "__main__":
    main()
