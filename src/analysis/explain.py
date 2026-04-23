"""Score explanation — explain why a person received their score.

各軸のスコアに対して、主要な寄与要因を抽出し、
個人が自身の評価根拠を理解できるようにする。
"""

from collections import defaultdict

import structlog

from src.models import AnimeAnalysis as Anime, Credit
from src.utils.role_groups import DIRECTOR_ROLES

logger = structlog.get_logger()


def explain_person_summary(
    person_id: str,
    credits: list[Credit],
    anime_map: dict[str, Anime],
    scores: dict | None = None,
    confidence: float | None = None,
    score_range: dict | None = None,
    potential_category: str | None = None,
    debiased_authority: float | None = None,
    growth_velocity: float | None = None,
    individual_profile: dict | None = None,
) -> dict:
    """Generate an evaluation summary for a specific person.

    スコアの根拠、キャリアの特徴、過小評価の有無を
    個人が理解できる形でまとめる。

    Args:
        person_id: 対象者ID
        credits: 全クレジットリスト
        anime_map: anime_id → Anime
        scores: {"birank": float, "patronage": float, "person_fe": float, "iv_score": float}
        confidence: 信頼度 (0-1)
        score_range: 各軸の信頼区間
        potential_category: ValueCategory の文字列 ("hidden_gem", "rising_star" 等)
        debiased_authority: スタジオバイアス補正後のAuthority
        growth_velocity: 成長速度
        individual_profile: 個人貢献プロファイル (peer_percentile, opportunity_residual 等)

    Returns:
        個人サマリー辞書
    """
    person_credits = [c for c in credits if c.person_id == person_id]
    if not person_credits:
        return {"person_id": person_id, "summary": "データなし"}

    # --- basic career information ---
    years = set()
    roles = defaultdict(int)
    studios: set[str] = set()
    anime_ids: set[str] = set()
    for c in person_credits:
        anime = anime_map.get(c.anime_id)
        if anime and anime.year:
            years.add(anime.year)
        roles[c.role.value] += 1
        anime_ids.add(c.anime_id)
        if anime and anime.studios:
            for s in anime.studios:
                studios.add(s)

    first_year = min(years) if years else None
    latest_year = max(years) if years else None
    career_span = (latest_year - first_year + 1) if first_year and latest_year else 0
    top_roles = sorted(roles.items(), key=lambda x: -x[1])[:3]

    # --- key works ---
    top_works = []
    seen_anime = set()
    for c in person_credits:
        if c.anime_id in seen_anime:
            continue
        seen_anime.add(c.anime_id)
        anime = anime_map.get(c.anime_id)
        _disp = getattr(anime, "score", None)  # display-only
        if anime and _disp:
            top_works.append(
                {
                    "title": anime.title_ja or anime.title_en or anime.id,
                    "year": anime.year,
                    "score": _disp,
                    "role": c.role.value,
                }
            )
    top_works.sort(key=lambda w: w["score"], reverse=True)

    # --- relationships with directors ---
    anime_directors: dict[str, set[str]] = defaultdict(set)
    for c in credits:
        if c.role in DIRECTOR_ROLES:
            anime_directors[c.anime_id].add(c.person_id)

    director_count: dict[str, int] = defaultdict(int)
    for c in person_credits:
        for dir_id in anime_directors.get(c.anime_id, set()):
            if dir_id != person_id:
                director_count[dir_id] += 1
    top_directors = sorted(director_count.items(), key=lambda x: -x[1])[:5]
    repeat_directors = sum(1 for _, cnt in director_count.items() if cnt >= 2)

    # --- undervaluation assessment ---
    undervaluation_gap = None
    if scores and debiased_authority is not None:
        gap = debiased_authority - scores.get("birank", 0)
        if gap > 0.05:
            undervaluation_gap = round(gap * 100, 1)

    # --- growth trend ---
    growth_trend = None
    if growth_velocity is not None:
        if growth_velocity > 1.0:
            growth_trend = "accelerating"
        elif growth_velocity > 0:
            growth_trend = "growing"
        elif growth_velocity > -0.5:
            growth_trend = "stable"
        else:
            growth_trend = "declining"

    # --- confidence interpretation ---
    confidence_label = None
    if confidence is not None:
        if confidence >= 0.8:
            confidence_label = "high"
        elif confidence >= 0.5:
            confidence_label = "moderate"
        else:
            confidence_label = "low"

    summary = {
        "person_id": person_id,
        "career": {
            "first_year": first_year,
            "latest_year": latest_year,
            "span_years": career_span,
            "total_credits": len(person_credits),
            "unique_works": len(anime_ids),
            "studios_worked": len(studios),
            "top_roles": [{"role": r, "count": c} for r, c in top_roles],
        },
        "top_works": top_works[:5],
        "collaboration": {
            "total_directors": len(director_count),
            "repeat_directors": repeat_directors,
            "top_directors": [
                {"director_id": d, "shared_works": c} for d, c in top_directors
            ],
        },
    }

    if scores:
        summary["scores"] = scores

    if confidence is not None:
        summary["confidence"] = {
            "value": confidence,
            "label": confidence_label,
        }

    if score_range:
        summary["score_range"] = score_range

    if potential_category:
        summary["potential_category"] = potential_category

    if undervaluation_gap is not None:
        summary["undervaluation"] = {
            "gap_points": undervaluation_gap,
            "debiased_authority": round(debiased_authority, 4),
        }

    if growth_trend:
        summary["growth"] = {
            "velocity": round(growth_velocity, 2) if growth_velocity else 0,
            "trend": growth_trend,
        }

    if individual_profile:
        summary["individual_profile"] = {
            k: v for k, v in individual_profile.items() if k != "person_id"
        }

    logger.debug(
        "person_summary_generated",
        person_id=person_id,
        credits=len(person_credits),
        confidence=confidence_label,
    )

    return summary


def explain_individual_profile(
    individual_profile: dict,
) -> dict:
    """Return interpretation of the Individual Contribution Profile (Layer 2).

    peer_percentile, opportunity_residual, consistency, independent_value
    の各指標を人間が理解しやすい形に翻訳する。

    Args:
        individual_profile: 個人貢献プロファイル辞書

    Returns:
        各指標の解釈を含む辞書
    """
    interpretations = {}

    # Peer percentile interpretation
    peer_pct = individual_profile.get("peer_percentile")
    if peer_pct is not None:
        if peer_pct >= 90:
            interpretations["peer_percentile"] = {
                "value": peer_pct,
                "label": "exceptional",
                "description": "同じキャリアバンド内で上位10% — 突出した活動量",
            }
        elif peer_pct >= 70:
            interpretations["peer_percentile"] = {
                "value": peer_pct,
                "label": "above_average",
                "description": "同じキャリアバンド内で上位30% — 平均以上の活動量",
            }
        elif peer_pct >= 30:
            interpretations["peer_percentile"] = {
                "value": peer_pct,
                "label": "average",
                "description": "同じキャリアバンド内で平均的な活動量",
            }
        else:
            interpretations["peer_percentile"] = {
                "value": peer_pct,
                "label": "below_average",
                "description": "同じキャリアバンド内で平均以下の活動量",
            }

    # Opportunity residual interpretation
    opp_res = individual_profile.get("opportunity_residual")
    if opp_res is not None:
        if opp_res > 0.1:
            interpretations["opportunity_residual"] = {
                "value": round(opp_res, 4),
                "label": "outperforming",
                "description": "環境（スタジオ・監督）から予測されるスコアを上回っている",
            }
        elif opp_res < -0.1:
            interpretations["opportunity_residual"] = {
                "value": round(opp_res, 4),
                "label": "underperforming",
                "description": "環境から予測されるスコアを下回っている — 機会が限られている可能性",
            }
        else:
            interpretations["opportunity_residual"] = {
                "value": round(opp_res, 4),
                "label": "expected",
                "description": "環境から予測されるスコアと概ね一致",
            }

    # Consistency interpretation
    consistency = individual_profile.get("consistency")
    if consistency is not None:
        if consistency >= 0.8:
            interpretations["consistency"] = {
                "value": round(consistency, 4),
                "label": "highly_consistent",
                "description": "作品間のパフォーマンスが非常に安定している",
            }
        elif consistency >= 0.5:
            interpretations["consistency"] = {
                "value": round(consistency, 4),
                "label": "consistent",
                "description": "作品間のパフォーマンスが安定している",
            }
        else:
            interpretations["consistency"] = {
                "value": round(consistency, 4),
                "label": "variable",
                "description": "作品によってパフォーマンスにばらつきがある",
            }

    # Independent value interpretation
    indep_val = individual_profile.get("independent_value")
    if indep_val is not None:
        if indep_val >= 0.7:
            interpretations["independent_value"] = {
                "value": round(indep_val, 4),
                "label": "high",
                "description": "環境によらない個人固有の貢献が大きい",
            }
        elif indep_val >= 0.4:
            interpretations["independent_value"] = {
                "value": round(indep_val, 4),
                "label": "moderate",
                "description": "個人固有の貢献と環境要因が同程度",
            }
        else:
            interpretations["independent_value"] = {
                "value": round(indep_val, 4),
                "label": "environment_dependent",
                "description": "スコアの多くが環境要因（スタジオ・監督）に依存している",
            }

    # Career band
    career_band = individual_profile.get("career_band")
    if career_band:
        interpretations["career_band"] = career_band

    return interpretations


def explain_authority(
    person_id: str,
    credits: list[Credit],
    anime_map: dict[str, Anime],
    *,
    _person_credits: list[Credit] | None = None,
) -> list[dict]:
    """Return the primary contributing factors for a BiRank score.

    高評価/高プロフィール作品への参加が BiRank に寄与する。
    """
    person_credits = (
        _person_credits
        if _person_credits is not None
        else [c for c in credits if c.person_id == person_id]
    )
    if not person_credits:
        return []

    works = []
    for c in person_credits:
        anime = anime_map.get(c.anime_id)
        if anime:
            works.append(
                {
                    "anime_id": anime.id,
                    "title": anime.title_ja or anime.title_en or anime.id,
                    "year": anime.year,
                    "score": getattr(anime, "score", None) or 0,  # display-only
                    "role": c.role.value,
                }
            )

    # sort by descending score
    works.sort(key=lambda w: w["score"], reverse=True)
    return works[:10]


def explain_trust(
    person_id: str,
    credits: list[Credit],
    anime_map: dict[str, Anime],
    *,
    _person_credits: list[Credit] | None = None,
    _anime_directors: dict[str, set[str]] | None = None,
) -> list[dict]:
    """Return the primary contributing factors for a Patronage score.

    同じ監督との繰り返し共演が Patronage に寄与する。
    """
    person_credits = (
        _person_credits
        if _person_credits is not None
        else [c for c in credits if c.person_id == person_id]
    )
    if not person_credits:
        return []

    # identify director IDs (use pre-built index if available)
    if _anime_directors is not None:
        anime_directors = _anime_directors
    else:
        anime_directors: dict[str, set[str]] = defaultdict(set)
        for c in credits:
            if c.role in DIRECTOR_ROLES:
                anime_directors[c.anime_id].add(c.person_id)

    # count co-credits with each director
    director_collabs: dict[str, list[str]] = defaultdict(list)
    for c in person_credits:
        for dir_id in anime_directors.get(c.anime_id, set()):
            if dir_id != person_id:
                anime = anime_map.get(c.anime_id)
                title = (
                    (anime.title_ja or anime.title_en or c.anime_id)
                    if anime
                    else c.anime_id
                )
                director_collabs[dir_id].append(title)

    result = []
    for dir_id, works in sorted(
        director_collabs.items(), key=lambda x: len(x[1]), reverse=True
    ):
        if len(works) >= 1:
            result.append(
                {
                    "director_id": dir_id,
                    "shared_works": len(works),
                    "works": works[:5],
                }
            )

    return result[:10]


def explain_skill(
    person_id: str,
    credits: list[Credit],
    anime_map: dict[str, Anime],
    *,
    _person_credits: list[Credit] | None = None,
) -> list[dict]:
    """Return the primary contributing factors for a Person FE score.

    高評価作品への参加が Person FE レーティングに影響する。
    年代順にソートして、最近の実績を強調する。
    """
    person_credits = (
        _person_credits
        if _person_credits is not None
        else [c for c in credits if c.person_id == person_id]
    )
    if not person_credits:
        return []

    works = []
    seen = set()
    for c in person_credits:
        if c.anime_id in seen:
            continue
        seen.add(c.anime_id)
        anime = anime_map.get(c.anime_id)
        _disp = getattr(anime, "score", None)  # display-only
        if anime and _disp:
            works.append(
                {
                    "anime_id": anime.id,
                    "title": anime.title_ja or anime.title_en or anime.id,
                    "year": anime.year,
                    "score": _disp,
                }
            )

    # most recent first
    works.sort(key=lambda w: w["year"] or 0, reverse=True)
    return works[:10]
