"""成長・スコア分析レポート用データプロバイダ."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

from src.utils.json_io import load_json_file_or_return_default


@dataclass(frozen=True)
class GrowthPerson:
    """1人分の成長データ."""

    person_id: str
    name: str
    trend: str
    total_credits: int
    career_span: int
    activity_ratio: float
    yearly_credits: dict[int, int] = field(default_factory=dict)
    credits_per_year: float = 0.0
    debut_year: int = 0


@dataclass(frozen=True)
class GrowthData:
    """growth.json から抽出した構造化データ."""

    total_persons: int
    trend_summary: dict[str, int]  # {"rising": N, "stable": N, ...}
    persons: tuple[GrowthPerson, ...]

    # トレンド別総クレジット: {trend: (credits...)}
    trend_credits: dict[str, tuple[int, ...]]

    # 年別ローリングトレンド: {year: {trend: count}}
    yearly_rolling_trends: dict[int, dict[str, int]]

    # K-Means用 features (total_credits, credits_per_year, career_span, activity_ratio, debut_year)
    career_features: tuple[tuple[float, ...], ...] = ()
    career_feature_names: tuple[str, ...] = (
        "総クレジット", "年間クレジット", "キャリア期間", "活動率", "デビュー年",
    )

    # キャリア生存曲線用 (scores.json)
    career_durations: tuple[int, ...] = ()


def load_growth_data(json_dir: Path) -> GrowthData | None:
    """growth.json を読み込み GrowthData を返す."""
    raw = load_json_file_or_return_default(json_dir / "growth.json", {})
    if not raw or not isinstance(raw, dict):
        return None

    trend_summary = raw.get("trend_summary", {})
    total = raw.get("total_persons", 0)
    persons_raw = raw.get("persons", {})

    persons: list[GrowthPerson] = []
    trend_credits: dict[str, list[int]] = {}

    for pid, p in persons_raw.items():
        yc_raw = p.get("yearly_credits", {})
        yc = {}
        for yr_str, cnt in yc_raw.items():
            try:
                yc[int(yr_str)] = int(cnt)
            except (ValueError, TypeError):
                continue

        tc = int(p.get("total_credits", 0) or 0)
        active_years = int(p.get("total_years", 1) or 1)
        credits_per_year = tc / max(active_years, 1)
        debut_year = min((int(y) for y in yc), default=0) if yc else 0
        trend = p.get("trend", "unknown")

        gp = GrowthPerson(
            person_id=pid,
            name=p.get("name", pid),
            trend=trend,
            total_credits=tc,
            career_span=int(p.get("career_span", 0) or 0),
            activity_ratio=float(p.get("activity_ratio", 0) or 0),
            yearly_credits=yc,
            credits_per_year=credits_per_year,
            debut_year=debut_year,
        )
        persons.append(gp)
        trend_credits.setdefault(trend, []).append(tc)

    # Year-by-year rolling trends (3-year window)
    yearly_rolling: dict[int, dict[str, int]] = {}
    for gp in persons:
        yc = gp.yearly_credits
        if not yc:
            continue
        first_yr = min(yc)
        for yr in sorted(yc):
            if yc[yr] <= 0:
                continue
            yearly_rolling.setdefault(
                yr, {"rising": 0, "stable": 0, "declining": 0, "new": 0}
            )
            years_since = yr - first_yr
            if years_since <= 2:
                t = "new"
            else:
                recent = sum(yc.get(y, 0) for y in range(yr - 1, yr + 1))
                prior = sum(yc.get(y, 0) for y in range(yr - 3, yr - 1))
                if prior == 0:
                    t = "new" if recent > 0 else "stable"
                elif recent > prior * 1.3:
                    t = "rising"
                elif recent < prior * 0.5:
                    t = "declining"
                else:
                    t = "stable"
            yearly_rolling[yr][t] += 1

    # K-Means用 career features
    career_features = tuple(
        (float(gp.total_credits), gp.credits_per_year,
         float(gp.career_span), gp.activity_ratio, float(gp.debut_year))
        for gp in persons if gp.debut_year > 0
    )

    # キャリア生存曲線用 (scores.json)
    scores_raw = load_json_file_or_return_default(json_dir / "scores.json", [])
    career_durations: tuple[int, ...] = ()
    if scores_raw and isinstance(scores_raw, list):
        career_durations = tuple(
            p.get("career", {}).get("active_years", 0)
            for p in scores_raw
            if p.get("career", {}).get("active_years", 0) > 0
        )

    return GrowthData(
        total_persons=total,
        trend_summary=trend_summary,
        persons=tuple(persons),
        trend_credits={k: tuple(v) for k, v in trend_credits.items()},
        yearly_rolling_trends=yearly_rolling,
        career_features=career_features,
        career_durations=career_durations,
    )
