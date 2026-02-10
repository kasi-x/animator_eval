"""Temporal Influence Analysis — 時系列での影響力変化追跡.

クリエイターのAuthority/Trust/Skillスコアが時間とともにどう変化するかを分析。
キャリアの転換点、急上昇期、衰退期などを特定し、業界トレンドを可視化。
"""

from collections import defaultdict
from dataclasses import dataclass, field

import structlog

from src.models import Anime, Credit

logger = structlog.get_logger()


@dataclass
class TemporalSnapshot:
    """ある時点でのスコアスナップショット.

    Attributes:
        year: 年
        authority: Authority スコア
        trust: Trust スコア
        skill: Skill スコア
        composite: Composite スコア
        n_credits: その年のクレジット数
        n_collaborators: その年のコラボレーター数
        primary_role: その年の主要役職
    """

    year: int
    authority: float = 0.0
    trust: float = 0.0
    skill: float = 0.0
    composite: float = 0.0
    n_credits: int = 0
    n_collaborators: int = 0
    primary_role: str | None = None


@dataclass
class TemporalProfile:
    """クリエイターの時系列プロファイル.

    Attributes:
        person_id: person_id
        snapshots: 年ごとのスナップショット
        career_start: キャリア開始年
        career_end: 最新活動年
        peak_year: スコアがピークだった年
        peak_score: ピーク時のcompositeスコア
        growth_rate: 成長率（年平均）
        trend: トレンド（"rising", "stable", "declining"）
        turning_points: ターニングポイント（大きな変化があった年）
    """

    person_id: str
    snapshots: list[TemporalSnapshot] = field(default_factory=list)
    career_start: int | None = None
    career_end: int | None = None
    peak_year: int | None = None
    peak_score: float = 0.0
    growth_rate: float = 0.0
    trend: str = "stable"
    turning_points: list[tuple[int, str]] = field(default_factory=list)


def compute_temporal_profiles(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    current_scores: dict[str, dict] | None = None,
    window_years: int = 3,
) -> dict[str, TemporalProfile]:
    """時系列プロファイルを計算.

    Args:
        credits: 全クレジット
        anime_map: anime_id → Anime
        current_scores: 現在の最終スコア（オプション）
        window_years: 移動平均のウィンドウサイズ（年）

    Returns:
        person_id → TemporalProfile
    """
    # person_id → year → credits のマッピング
    person_year_credits: dict[str, dict[int, list[Credit]]] = defaultdict(lambda: defaultdict(list))

    for credit in credits:
        anime = anime_map.get(credit.anime_id)
        if anime and anime.year:
            person_year_credits[credit.person_id][anime.year].append(credit)

    profiles: dict[str, TemporalProfile] = {}

    for person_id, year_credits in person_year_credits.items():
        # 年ごとにスナップショット作成
        snapshots = []
        years = sorted(year_credits.keys())

        for year in years:
            year_creds = year_credits[year]

            # その年のコラボレーター数（概算）
            collaborators = set()
            for cred in year_creds:
                # 同じアニメに参加した他のスタッフをカウント（簡易版）
                # 本来は anime → staff のマッピングが必要
                pass

            # 主要役職
            role_counts = defaultdict(int)
            for cred in year_creds:
                role_counts[cred.role.value] += 1
            primary_role = max(role_counts.items(), key=lambda x: x[1])[0] if role_counts else None

            # スコアは現在の値を使用（時点ごとの再計算は重い）
            # 実際の実装では、その時点までのクレジットで再計算が望ましい
            current = current_scores.get(person_id, {}) if current_scores else {}

            snapshot = TemporalSnapshot(
                year=year,
                authority=current.get("authority", 0),
                trust=current.get("trust", 0),
                skill=current.get("skill", 0),
                composite=current.get("composite", 0),
                n_credits=len(year_creds),
                n_collaborators=len(collaborators),
                primary_role=primary_role,
            )
            snapshots.append(snapshot)

        # キャリア統計
        if snapshots:
            career_start = snapshots[0].year
            career_end = snapshots[-1].year

            # ピーク検出
            peak_snapshot = max(snapshots, key=lambda s: s.composite)
            peak_year = peak_snapshot.year
            peak_score = peak_snapshot.composite

            # 成長率計算（最初と最後のスコア比較）
            if len(snapshots) > 1 and snapshots[0].composite > 0:
                years_diff = career_end - career_start
                if years_diff > 0:
                    growth_rate = (
                        (snapshots[-1].composite - snapshots[0].composite) / snapshots[0].composite
                    ) / years_diff
                else:
                    growth_rate = 0.0
            else:
                growth_rate = 0.0

            # トレンド判定
            if len(snapshots) >= 3:
                recent_avg = sum(s.composite for s in snapshots[-3:]) / 3
                early_avg = sum(s.composite for s in snapshots[:3]) / 3
                if recent_avg > early_avg * 1.2:
                    trend = "rising"
                elif recent_avg < early_avg * 0.8:
                    trend = "declining"
                else:
                    trend = "stable"
            else:
                trend = "stable"

            # ターニングポイント検出（クレジット数が急増/急減した年）
            turning_points = []
            for i in range(1, len(snapshots)):
                prev = snapshots[i - 1]
                curr = snapshots[i]
                if prev.n_credits > 0:
                    change_rate = (curr.n_credits - prev.n_credits) / prev.n_credits
                    if change_rate > 1.0:  # 2倍以上増加
                        turning_points.append((curr.year, "surge"))
                    elif change_rate < -0.5:  # 半減以下
                        turning_points.append((curr.year, "decline"))

            profiles[person_id] = TemporalProfile(
                person_id=person_id,
                snapshots=snapshots,
                career_start=career_start,
                career_end=career_end,
                peak_year=peak_year,
                peak_score=round(peak_score, 2),
                growth_rate=round(growth_rate * 100, 2),  # パーセント表示
                trend=trend,
                turning_points=turning_points,
            )

    logger.info("temporal_profiles_computed", persons=len(profiles))
    return profiles


def analyze_cohort_trends(
    profiles: dict[str, TemporalProfile],
    cohort_window: int = 5,
) -> dict[str, dict]:
    """コホート別のトレンド分析.

    同じ時期にデビューしたクリエイターのグループ（コホート）ごとに
    平均的な成長パターンを分析。

    Args:
        profiles: 時系列プロファイル
        cohort_window: コホートの年幅

    Returns:
        cohort_key → 統計情報
    """
    # コホート分類
    cohorts: dict[str, list[TemporalProfile]] = defaultdict(list)

    for profile in profiles.values():
        if profile.career_start:
            cohort_start = (profile.career_start // cohort_window) * cohort_window
            cohort_key = f"{cohort_start}-{cohort_start + cohort_window - 1}"
            cohorts[cohort_key].append(profile)

    # 各コホートの統計
    cohort_stats = {}

    for cohort_key, cohort_profiles in cohorts.items():
        avg_peak_score = sum(p.peak_score for p in cohort_profiles) / len(cohort_profiles)
        avg_growth_rate = sum(p.growth_rate for p in cohort_profiles) / len(cohort_profiles)

        trend_counts = defaultdict(int)
        for p in cohort_profiles:
            trend_counts[p.trend] += 1

        cohort_stats[cohort_key] = {
            "size": len(cohort_profiles),
            "avg_peak_score": round(avg_peak_score, 2),
            "avg_growth_rate": round(avg_growth_rate, 2),
            "trend_distribution": dict(trend_counts),
            "median_career_length": round(
                sorted(
                    [(p.career_end - p.career_start) if p.career_start and p.career_end else 0
                     for p in cohort_profiles]
                )[len(cohort_profiles) // 2],
                1,
            ),
        }

    logger.info("cohort_trends_analyzed", cohorts=len(cohort_stats))
    return cohort_stats


def detect_industry_trends(
    profiles: dict[str, TemporalProfile],
    min_year: int | None = None,
    max_year: int | None = None,
) -> dict[int, dict]:
    """業界全体のトレンドを検出.

    各年の新規参入者数、平均スコア、役職分布などを集計。

    Args:
        profiles: 時系列プロファイル
        min_year: 開始年（Noneで自動）
        max_year: 終了年（Noneで自動）

    Returns:
        year → 統計情報
    """
    # 年ごとの集計
    year_stats: dict[int, dict] = defaultdict(lambda: {
        "new_entrants": 0,
        "active_persons": 0,
        "total_credits": 0,
        "avg_composite": 0.0,
        "role_distribution": defaultdict(int),
    })

    # 年範囲の決定
    all_years = set()
    for profile in profiles.values():
        for snapshot in profile.snapshots:
            all_years.add(snapshot.year)

    if not all_years:
        return {}

    if min_year is None:
        min_year = min(all_years)
    if max_year is None:
        max_year = max(all_years)

    # 新規参入者カウント
    for profile in profiles.values():
        if profile.career_start and min_year <= profile.career_start <= max_year:
            year_stats[profile.career_start]["new_entrants"] += 1

    # 各年のアクティビティ集計
    for profile in profiles.values():
        for snapshot in profile.snapshots:
            if min_year <= snapshot.year <= max_year:
                stats = year_stats[snapshot.year]
                stats["active_persons"] += 1
                stats["total_credits"] += snapshot.n_credits
                if snapshot.primary_role:
                    stats["role_distribution"][snapshot.primary_role] += 1

    # 平均スコア計算
    for year, stats in year_stats.items():
        active = stats["active_persons"]
        if active > 0:
            # その年にアクティブだった人のスコア平均
            year_composites = [
                snapshot.composite
                for profile in profiles.values()
                for snapshot in profile.snapshots
                if snapshot.year == year
            ]
            if year_composites:
                stats["avg_composite"] = round(sum(year_composites) / len(year_composites), 2)

    # defaultdictを通常のdictに変換
    result = {}
    for year in sorted(year_stats.keys()):
        stats = year_stats[year]
        result[year] = {
            "new_entrants": stats["new_entrants"],
            "active_persons": stats["active_persons"],
            "total_credits": stats["total_credits"],
            "avg_composite": stats["avg_composite"],
            "role_distribution": dict(stats["role_distribution"]),
        }

    logger.info("industry_trends_detected", years=len(result), min_year=min_year, max_year=max_year)
    return result


def main():
    """スタンドアロン実行用エントリーポイント."""
    from src.database import get_all_anime, get_all_credits, get_all_persons, get_all_scores, get_connection, init_db

    conn = get_connection()
    init_db(conn)

    persons = get_all_persons(conn)
    anime_list = get_all_anime(conn)
    credits = get_all_credits(conn)
    scores_list = get_all_scores(conn)

    # マップ作成
    anime_map = {a.id: a for a in anime_list}
    person_names = {p.id: p.name_ja or p.name_en or p.id for p in persons}
    scores_map = {
        s.person_id: {
            "authority": s.authority,
            "trust": s.trust,
            "skill": s.skill,
            "composite": s.composite,
        }
        for s in scores_list
    }

    # 時系列プロファイル計算
    profiles = compute_temporal_profiles(credits, anime_map, scores_map)

    # コホート分析
    cohort_stats = analyze_cohort_trends(profiles)

    print("\nコホート分析:")
    for cohort_key, stats in sorted(cohort_stats.items()):
        print(f"\n{cohort_key}年デビュー組 ({stats['size']}人):")
        print(f"  平均ピークスコア: {stats['avg_peak_score']}")
        print(f"  平均成長率: {stats['avg_growth_rate']}%/年")
        print(f"  トレンド分布: {stats['trend_distribution']}")

    # 業界トレンド
    industry_trends = detect_industry_trends(profiles)

    print("\n業界トレンド（直近5年）:")
    recent_years = sorted(industry_trends.keys())[-5:]
    for year in recent_years:
        stats = industry_trends[year]
        print(f"\n{year}年:")
        print(f"  新規参入: {stats['new_entrants']}人")
        print(f"  アクティブ: {stats['active_persons']}人")
        print(f"  総クレジット: {stats['total_credits']}")

    # トレンド分布
    trends = [p.trend for p in profiles.values()]
    trend_counts = {
        "rising": trends.count("rising"),
        "stable": trends.count("stable"),
        "declining": trends.count("declining"),
    }
    print("\n全体トレンド分布:")
    print(f"  Rising: {trend_counts['rising']} ({100*trend_counts['rising']/len(trends):.1f}%)")
    print(f"  Stable: {trend_counts['stable']} ({100*trend_counts['stable']/len(trends):.1f}%)")
    print(f"  Declining: {trend_counts['declining']} ({100*trend_counts['declining']/len(trends):.1f}%)")

    conn.close()


if __name__ == "__main__":
    main()
