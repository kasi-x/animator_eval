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
from collections import defaultdict as _dd
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

# Roles representing fixed/startup costs: same core team regardless of series length
_STARTUP_ROLES: frozenset = frozenset({
    "director",
    "series_composition",
    "character_designer",
    "art_director",
    "music",
    "original_creator",
    "chief_animation_director",
    "producer",
    "sound_director",
})

# Roles that scale with episode count (variable cost)
_VARIABLE_ROLES: frozenset = frozenset({
    "key_animator",
    "in_between",
    "animation_director",
    "episode_director",
    "storyboard",
    "second_key_animator",
    "photography_director",
    "background_art",
})


def _build_startup_cost_data(conn) -> dict:
    """固定費/変動費分析データを構築する再利用可能ヘルパー.

    TVアニメ・ONAを対象に、固定費ロール（キャラデザ・監督・音楽等）と
    変動費ロール（原画・動画・演出等）のユニーク人数をクール数で回帰分析。

    Args:
        conn: sqlite3.Connection to the animetor_eval database

    Returns:
        dict with keys:
            raw_rows: list of per-anime dicts (anime_id, format, genres, year,
                episodes, cour_count, startup_persons, variable_persons,
                studio_name, studio_favourites, studio_tier)
            ols_by_genre: {genre: {intercept, slope, r2, n}}
            by_year_1cour: {year: {startup_med, variable_med, n}}
            genre_decade: {(genre, decade_start): startup_median}
    """
    import statistics as _st

    cur = conn.cursor()

    # Per-anime × per-role: unique person counts
    cur.execute("""
        SELECT
            a.id,
            a.format,
            a.genres,
            a.year,
            a.episodes,
            c.role,
            COUNT(DISTINCT c.person_id) AS persons
        FROM credits c
        JOIN anime a ON c.anime_id = a.id
        WHERE a.format IN ('TV', 'ONA')
          AND a.episodes >= 4
          AND a.year BETWEEN 1985 AND 2025
          AND c.role IS NOT NULL
        GROUP BY a.id, c.role
    """)
    raw = cur.fetchall()

    # Main studio info (is_main = 1)
    cur.execute("""
        SELECT ast.anime_id, s.name, s.favourites
        FROM anime_studios ast
        JOIN studios s ON ast.studio_id = s.id
        WHERE ast.is_main = 1
    """)
    studio_by_anime: dict = {}
    for anime_id, sname, sfavs in cur.fetchall():
        studio_by_anime[anime_id] = {"name": sname or "不明", "favourites": sfavs or 0}

    # Aggregate per anime
    anime_data: dict = {}
    for anime_id, fmt, genres_json, year, eps, role, persons in raw:
        if anime_id not in anime_data:
            cour_count = max(1, round((eps or 12) / 12))
            try:
                genres = json.loads(genres_json or "[]")
            except Exception:
                genres = []
            anime_data[anime_id] = {
                "format": fmt,
                "genres": genres,
                "year": int(year or 0),
                "episodes": int(eps or 12),
                "cour_count": cour_count,
                "startup_persons": 0,
                "variable_persons": 0,
            }
        if role in _STARTUP_ROLES:
            anime_data[anime_id]["startup_persons"] += persons
        elif role in _VARIABLE_ROLES:
            anime_data[anime_id]["variable_persons"] += persons

    # Attach studio info + tier
    for anime_id, info in anime_data.items():
        st = studio_by_anime.get(anime_id, {})
        info["studio_name"] = st.get("name", "不明")
        favs = st.get("favourites", 0)
        info["studio_favourites"] = favs
        if favs >= 1000:
            info["studio_tier"] = "大手 (1000+ fav)"
        elif favs >= 100:
            info["studio_tier"] = "中規模 (100-999 fav)"
        else:
            info["studio_tier"] = "小規模 (<100 fav)"

    raw_rows = [
        {"anime_id": aid, **info}
        for aid, info in anime_data.items()
        if info["startup_persons"] > 0
    ]

    # OLS per genre: startup_persons ~ cour_count
    def _ols_sc(xs, ys):
        n = len(xs)
        if n < 5:
            return None
        mx = sum(xs) / n
        my = sum(ys) / n
        num = sum((x - mx) * (y - my) for x, y in zip(xs, ys))
        den = sum((x - mx) ** 2 for x in xs)
        if den == 0:
            return None
        slope = num / den
        intercept = my - slope * mx
        ss_res = sum((y - (intercept + slope * x)) ** 2 for x, y in zip(xs, ys))
        ss_tot = sum((y - my) ** 2 for y in ys)
        r2 = 1.0 - ss_res / ss_tot if ss_tot > 0 else 0.0
        return {
            "intercept": round(intercept, 2),
            "slope": round(slope, 2),
            "r2": round(r2, 3),
            "n": n,
        }

    genre_items: dict = _dd(list)
    for row in raw_rows:
        for g in row["genres"]:
            genre_items[g].append((row["cour_count"], row["startup_persons"]))

    ols_by_genre: dict = {}
    for g, items in genre_items.items():
        if len(items) >= 5:
            res = _ols_sc([x[0] for x in items], [x[1] for x in items])
            if res:
                ols_by_genre[g] = res

    # by_year_1cour: 1-cour TV anime, year → {startup_med, variable_med, n}
    year_items: dict = _dd(list)
    for row in raw_rows:
        if row["cour_count"] == 1:
            year_items[row["year"]].append(
                {"startup": row["startup_persons"], "variable": row["variable_persons"]}
            )

    by_year_1cour: dict = {}
    for yr, items in year_items.items():
        if len(items) >= 3:
            by_year_1cour[yr] = {
                "startup_med": _st.median([i["startup"] for i in items]),
                "variable_med": _st.median([i["variable"] for i in items]),
                "n": len(items),
            }

    # genre_decade: {(genre, decade_start): median startup persons}
    decade_items: dict = _dd(list)
    for row in raw_rows:
        if row["year"] >= 1990:
            dec = (row["year"] // 5) * 5  # 5-year bins
            for g in row["genres"]:
                decade_items[(g, dec)].append(row["startup_persons"])

    genre_decade: dict = {
        (g, dec): _st.median(vals)
        for (g, dec), vals in decade_items.items()
        if vals
    }

    return {
        "raw_rows": raw_rows,
        "ols_by_genre": ols_by_genre,
        "by_year_1cour": by_year_1cour,
        "genre_decade": genre_decade,
    }


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
