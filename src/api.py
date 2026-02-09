"""FastAPI サーバー — スコア照会 API.

エンドポイント:
  GET /api/v1/persons          — 全人物スコア一覧 (ページネーション対応)
  GET /api/v1/persons/search   — 人物検索
  GET /api/v1/persons/{id}     — 人物プロフィール
  GET /api/v1/persons/{id}/similar — 類似人物
  GET /api/v1/ranking          — ランキング (フィルタ対応)
  GET /api/v1/anime            — アニメ統計一覧
  GET /api/v1/anime/{id}       — アニメ詳細
  GET /api/v1/summary          — パイプラインサマリー
  GET /api/v1/health           — ヘルスチェック
"""

import structlog
import uvicorn
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from src.analysis.similarity import find_similar_persons
from src.database import get_connection, get_data_sources, get_db_stats, get_score_history, search_persons
from src.utils.config import JSON_DIR
from src.utils.json_io import (
    load_anime_statistics_from_json,
    load_bridge_analysis_from_json,
    load_career_milestones_from_json,
    load_collaboration_pairs_from_json,
    load_cross_validation_results_from_json,
    load_decade_analysis_from_json,
    load_genre_affinity_from_json,
    load_growth_trends_from_json,
    load_influence_tree_from_json,
    load_mentorship_relationships_from_json,
    load_network_evolution_from_json,
    load_outlier_analysis_from_json,
    load_person_scores_from_json,
    load_person_tags_from_json,
    load_pipeline_summary_from_json,
    load_productivity_metrics_from_json,
    load_role_flow_from_json,
    load_role_transitions_from_json,
    load_seasonal_trends_from_json,
    load_studio_analysis_from_json,
    load_team_patterns_from_json,
    load_time_series_from_json,
)

logger = structlog.get_logger()

app = FastAPI(
    title="Animetor Eval API",
    description="アニメ業界人物評価 API — ネットワーク密度・位置指標に基づくスコアリング",
    version="0.1.0",
)


# --- Response Models ---


class HealthResponse(BaseModel):
    status: str
    db_exists: bool
    scores_exist: bool


class PaginatedResponse(BaseModel):
    items: list[dict]
    total: int
    page: int
    per_page: int
    pages: int


# --- Endpoints ---


@app.get("/api/v1/health", response_model=HealthResponse)
def health():
    """ヘルスチェック."""
    from src.utils.config import DB_DIR

    db_path = DB_DIR / "animetor_eval.db"
    scores_path = JSON_DIR / "scores.json"
    return HealthResponse(
        status="ok",
        db_exists=db_path.exists(),
        scores_exist=scores_path.exists(),
    )


@app.get("/api/v1/summary")
def summary():
    """パイプラインサマリー."""
    data = load_pipeline_summary_from_json()
    if not data:
        raise HTTPException(status_code=404, detail="Summary not found. Run pipeline first.")
    return data


@app.get("/api/v1/persons")
def list_persons(
    page: int = Query(1, ge=1, description="ページ番号"),
    per_page: int = Query(50, ge=1, le=200, description="1ページあたりの件数"),
    sort: str = Query("composite", description="ソート軸 (composite, authority, trust, skill)"),
):
    """全人物スコア一覧（ページネーション対応）."""
    scores = load_person_scores_from_json()
    if not scores:
        return PaginatedResponse(items=[], total=0, page=page, per_page=per_page, pages=0)

    valid_sorts = {"composite", "authority", "trust", "skill"}
    if sort not in valid_sorts:
        raise HTTPException(status_code=400, detail=f"Invalid sort: {sort}. Use: {valid_sorts}")

    scores.sort(key=lambda x: x.get(sort, 0), reverse=True)
    total = len(scores)
    pages = (total + per_page - 1) // per_page
    start = (page - 1) * per_page
    items = scores[start : start + per_page]

    return PaginatedResponse(items=items, total=total, page=page, per_page=per_page, pages=pages)


@app.get("/api/v1/persons/search")
def search(
    q: str = Query(..., min_length=1, description="検索クエリ"),
    limit: int = Query(20, ge=1, le=100, description="最大件数"),
):
    """人物検索（名前・IDの部分一致）."""
    conn = get_connection()
    try:
        results = search_persons(conn, q, limit=limit)
    finally:
        conn.close()
    return {"query": q, "count": len(results), "results": results}


@app.get("/api/v1/persons/{person_id}")
def get_person(person_id: str):
    """人物プロフィール（スコア + ブレークダウン）."""
    scores = load_person_scores_from_json()
    for entry in scores:
        if entry["person_id"] == person_id:
            return entry
    raise HTTPException(status_code=404, detail=f"Person {person_id} not found")


@app.get("/api/v1/persons/{person_id}/similar")
def get_similar(
    person_id: str,
    top_n: int = Query(10, ge=1, le=50, description="類似人物の数"),
):
    """類似人物検索（コサイン類似度）."""
    scores = load_person_scores_from_json()
    if not scores:
        raise HTTPException(status_code=404, detail="No scores available")

    similar = find_similar_persons(person_id, scores, top_n=top_n)
    if not similar:
        raise HTTPException(status_code=404, detail=f"Person {person_id} not found")

    return {"person_id": person_id, "similar": similar}


@app.get("/api/v1/persons/{person_id}/history")
def get_person_history(
    person_id: str,
    limit: int = Query(50, ge=1, le=200, description="履歴件数"),
):
    """人物のスコア履歴."""
    conn = get_connection()
    try:
        history = get_score_history(conn, person_id, limit=limit)
    finally:
        conn.close()
    if not history:
        raise HTTPException(status_code=404, detail=f"No history for {person_id}")
    return {"person_id": person_id, "history": history}


@app.get("/api/v1/ranking")
def ranking(
    role: str | None = Query(None, description="役職フィルタ (director, animator, etc.)"),
    year_from: int | None = Query(None, description="開始年"),
    year_to: int | None = Query(None, description="終了年"),
    sort: str = Query("composite", description="ソート軸"),
    limit: int = Query(50, ge=1, le=500, description="件数"),
):
    """ランキング（フィルタ対応）."""
    scores = load_person_scores_from_json()
    if not scores:
        return {"items": [], "total": 0}

    filtered = scores

    if role:
        filtered = [s for s in filtered if s.get("primary_role") == role]

    if year_from:
        filtered = [
            s for s in filtered
            if s.get("career", {}).get("first_year") and s["career"]["first_year"] >= year_from
            or s.get("career", {}).get("latest_year") and s["career"]["latest_year"] >= year_from
        ]

    if year_to:
        filtered = [
            s for s in filtered
            if s.get("career", {}).get("first_year") and s["career"]["first_year"] <= year_to
        ]

    valid_sorts = {"composite", "authority", "trust", "skill"}
    if sort not in valid_sorts:
        raise HTTPException(status_code=400, detail=f"Invalid sort: {sort}")

    filtered.sort(key=lambda x: x.get(sort, 0), reverse=True)

    return {"items": filtered[:limit], "total": len(filtered), "sort": sort, "filters": {"role": role, "year_from": year_from, "year_to": year_to}}


@app.get("/api/v1/anime")
def list_anime(
    page: int = Query(1, ge=1),
    per_page: int = Query(50, ge=1, le=200),
    sort: str = Query("credit_count", description="ソート (credit_count, avg_person_score, year)"),
):
    """アニメ統計一覧."""
    stats = load_anime_statistics_from_json()
    if not stats:
        return PaginatedResponse(items=[], total=0, page=page, per_page=per_page, pages=0)

    items = [{"anime_id": k, **v} for k, v in stats.items()]

    sort_key_map = {
        "credit_count": lambda x: x.get("credit_count", 0),
        "avg_person_score": lambda x: x.get("avg_person_score", 0),
        "year": lambda x: x.get("year") or 0,
    }
    key_fn = sort_key_map.get(sort, sort_key_map["credit_count"])
    items.sort(key=key_fn, reverse=True)

    total = len(items)
    pages = (total + per_page - 1) // per_page
    start = (page - 1) * per_page

    return PaginatedResponse(
        items=items[start : start + per_page],
        total=total,
        page=page,
        per_page=per_page,
        pages=pages,
    )


@app.get("/api/v1/anime/{anime_id}")
def get_anime(anime_id: str):
    """アニメ詳細統計."""
    stats = load_anime_statistics_from_json()
    if anime_id not in stats:
        raise HTTPException(status_code=404, detail=f"Anime {anime_id} not found")
    return {"anime_id": anime_id, **stats[anime_id]}


@app.get("/api/v1/transitions")
def transitions():
    """役職遷移分析."""
    data = load_role_transitions_from_json()
    if not data:
        raise HTTPException(status_code=404, detail="Transitions not found. Run pipeline first.")
    return data


@app.get("/api/v1/crossval")
def crossval():
    """スコアクロスバリデーション結果."""
    data = load_cross_validation_results_from_json()
    if not data:
        raise HTTPException(status_code=404, detail="Cross-validation not found. Run pipeline first.")
    return data


@app.get("/api/v1/influence")
def influence():
    """影響ツリー（メンター・メンティー関係）."""
    data = load_influence_tree_from_json()
    if not data:
        raise HTTPException(status_code=404, detail="Influence data not found. Run pipeline first.")
    return data


@app.get("/api/v1/studios")
def studios():
    """スタジオ分析."""
    data = load_studio_analysis_from_json()
    if not data:
        raise HTTPException(status_code=404, detail="Studio data not found. Run pipeline first.")
    return data


@app.get("/api/v1/seasonal")
def seasonal():
    """シーズントレンド."""
    data = load_seasonal_trends_from_json()
    if not data:
        raise HTTPException(status_code=404, detail="Seasonal data not found. Run pipeline first.")
    return data


@app.get("/api/v1/collaborations")
def collaborations(
    limit: int = Query(50, ge=1, le=500, description="件数"),
    person_id: str | None = Query(None, description="人物IDでフィルタ"),
):
    """コラボレーション強度ペア."""
    data = load_collaboration_pairs_from_json()
    if not data:
        raise HTTPException(status_code=404, detail="Collaboration data not found. Run pipeline first.")
    if person_id:
        data = [d for d in data if d.get("person_a") == person_id or d.get("person_b") == person_id]
    return {"items": data[:limit], "total": len(data)}


@app.get("/api/v1/outliers")
def outliers():
    """スコア外れ値検出結果."""
    data = load_outlier_analysis_from_json()
    if not data:
        raise HTTPException(status_code=404, detail="Outlier data not found. Run pipeline first.")
    return data


@app.get("/api/v1/teams")
def teams():
    """チーム構成分析."""
    data = load_team_patterns_from_json()
    if not data:
        raise HTTPException(status_code=404, detail="Team data not found. Run pipeline first.")
    return data


@app.get("/api/v1/growth")
def growth(
    trend: str | None = Query(None, description="トレンドフィルタ (rising/stable/declining/inactive)"),
    limit: int = Query(50, ge=1, le=500),
):
    """成長トレンド."""
    data = load_growth_trends_from_json()
    if not data:
        raise HTTPException(status_code=404, detail="Growth data not found. Run pipeline first.")
    persons = data.get("persons", {})
    if trend:
        persons = {pid: d for pid, d in persons.items() if d.get("trend") == trend}
    items = [{"person_id": pid, **d} for pid, d in list(persons.items())[:limit]]
    return {
        "trend_summary": data.get("trend_summary", {}),
        "total": len(persons),
        "items": items,
    }


@app.get("/api/v1/time-series")
def time_series():
    """年次時系列データ."""
    data = load_time_series_from_json()
    if not data:
        raise HTTPException(status_code=404, detail="Time series data not found. Run pipeline first.")
    return data


@app.get("/api/v1/decades")
def decades():
    """年代別分析."""
    data = load_decade_analysis_from_json()
    if not data:
        raise HTTPException(status_code=404, detail="Decade data not found. Run pipeline first.")
    return data


@app.get("/api/v1/tags")
def tags(
    tag: str | None = Query(None, description="タグでフィルタ"),
):
    """人物タグ."""
    data = load_person_tags_from_json()
    if not data:
        raise HTTPException(status_code=404, detail="Tag data not found. Run pipeline first.")

    if tag:
        filtered = {
            pid: t_list for pid, t_list in data.get("person_tags", {}).items()
            if tag in t_list
        }
        return {
            "tag_filter": tag,
            "count": len(filtered),
            "persons": filtered,
            "tag_summary": data.get("tag_summary", {}),
        }
    return data


@app.get("/api/v1/role-flow")
def role_flow():
    """役職遷移フロー（Sankey diagram data）."""
    data = load_role_flow_from_json()
    if not data:
        raise HTTPException(status_code=404, detail="Role flow data not found. Run pipeline first.")
    return data


@app.get("/api/v1/compare")
def compare_persons(
    ids: str = Query(..., description="比較対象の人物ID (カンマ区切り)"),
):
    """複数人物の比較マトリクス."""
    from src.analysis.comparison_matrix import build_comparison_matrix

    person_ids = [pid.strip() for pid in ids.split(",") if pid.strip()]
    if len(person_ids) < 2:
        raise HTTPException(status_code=400, detail="At least 2 person IDs required")

    scores = load_person_scores_from_json()
    if not scores:
        raise HTTPException(status_code=404, detail="No scores available")

    result = build_comparison_matrix(person_ids, scores)
    if not result["persons"]:
        raise HTTPException(status_code=404, detail="None of the specified persons found")

    return result


@app.get("/api/v1/data-quality")
def data_quality():
    """データ品質スコア."""
    from src.analysis.data_quality import compute_data_quality_score

    conn = get_connection()
    try:
        stats = get_db_stats(conn)

        total_credits = stats.get("credits", 0)
        total_persons = stats.get("persons", 0)
        total_anime = stats.get("anime", 0)

        credits_with_source = conn.execute(
            "SELECT COUNT(*) FROM credits WHERE source != ''"
        ).fetchone()[0] if total_credits else 0

        persons_with_score = conn.execute(
            "SELECT COUNT(*) FROM scores"
        ).fetchone()[0] if total_persons else 0

        anime_with_year = conn.execute(
            "SELECT COUNT(*) FROM anime WHERE year IS NOT NULL"
        ).fetchone()[0] if total_anime else 0

        anime_with_score = conn.execute(
            "SELECT COUNT(*) FROM anime WHERE score IS NOT NULL"
        ).fetchone()[0] if total_anime else 0

        source_count = conn.execute(
            "SELECT COUNT(DISTINCT source) FROM credits WHERE source != ''"
        ).fetchone()[0]

        latest_year_row = conn.execute(
            "SELECT MAX(year) FROM anime WHERE year IS NOT NULL"
        ).fetchone()
        latest_year = latest_year_row[0] if latest_year_row else None
    finally:
        conn.close()

    return compute_data_quality_score(
        stats={"latest_year": latest_year},
        credits_with_source=credits_with_source,
        total_credits=total_credits,
        persons_with_score=persons_with_score,
        total_persons=total_persons,
        anime_with_year=anime_with_year,
        total_anime=total_anime,
        anime_with_score=anime_with_score,
        source_count=source_count,
    )


@app.get("/api/v1/persons/{person_id}/network")
def get_person_network(
    person_id: str,
    hops: int = Query(1, ge=1, le=3, description="ネットワーク深度"),
):
    """人物のエゴグラフ（ローカルネットワーク）."""
    from src.analysis.ego_graph import extract_ego_graph
    from src.database import load_all_anime, load_all_credits

    conn = get_connection()
    try:
        credits = load_all_credits(conn)
        anime_list = load_all_anime(conn)
    finally:
        conn.close()

    anime_map = {a.id: a for a in anime_list}
    scores = load_person_scores_from_json()
    person_scores = {r["person_id"]: r["composite"] for r in scores} if scores else None

    result = extract_ego_graph(person_id, credits, anime_map, hops=hops, person_scores=person_scores)
    if result["total_nodes"] == 0:
        raise HTTPException(status_code=404, detail=f"Person {person_id} not found in credits")
    return result


@app.get("/api/v1/recommend")
def recommend(
    team: str = Query(..., description="既存チームの人物ID (カンマ区切り)"),
    top_n: int = Query(10, ge=1, le=50),
):
    """チームへの人材推薦."""
    from src.analysis.recommendation import recommend_for_team
    from src.database import load_all_credits

    team_ids = [pid.strip() for pid in team.split(",") if pid.strip()]
    if not team_ids:
        raise HTTPException(status_code=400, detail="At least 1 team member required")

    scores = load_person_scores_from_json()
    if not scores:
        raise HTTPException(status_code=404, detail="No scores available")

    conn = get_connection()
    try:
        credits = load_all_credits(conn)
    finally:
        conn.close()

    recs = recommend_for_team(team_ids, scores, credits, top_n=top_n)
    return {"team": team_ids, "recommendations": recs}


@app.get("/api/v1/predict")
def predict(
    team: str = Query(..., description="チームの人物ID (カンマ区切り)"),
):
    """チーム構成からアニメスコアを予測."""
    from src.analysis.anime_prediction import predict_anime_score
    from src.database import load_all_anime, load_all_credits

    team_ids = [pid.strip() for pid in team.split(",") if pid.strip()]
    if not team_ids:
        raise HTTPException(status_code=400, detail="At least 1 team member required")

    conn = get_connection()
    try:
        credits = load_all_credits(conn)
        anime_list = load_all_anime(conn)
    finally:
        conn.close()

    anime_map = {a.id: a for a in anime_list}
    scores = load_person_scores_from_json()
    person_scores = {r["person_id"]: r["composite"] for r in scores} if scores else None

    result = predict_anime_score(team_ids, credits, anime_map, person_scores=person_scores)
    return {"team": team_ids, **result}


@app.get("/api/v1/bridges")
def bridges():
    """コミュニティ間ブリッジ人物."""
    data = load_bridge_analysis_from_json()
    if not data:
        raise HTTPException(status_code=404, detail="Bridge data not found. Run pipeline first.")
    return data


@app.get("/api/v1/mentorships")
def mentorships():
    """推定メンターシップ関係."""
    data = load_mentorship_relationships_from_json()
    if not data:
        raise HTTPException(status_code=404, detail="Mentorship data not found. Run pipeline first.")
    return data


@app.get("/api/v1/persons/{person_id}/milestones")
def get_person_milestones(person_id: str):
    """人物のキャリアマイルストーン."""
    data = load_career_milestones_from_json()
    if not data:
        raise HTTPException(status_code=404, detail="Milestone data not found. Run pipeline first.")
    if person_id not in data:
        raise HTTPException(status_code=404, detail=f"No milestones for {person_id}")
    return {"person_id": person_id, "milestones": data[person_id]}


@app.get("/api/v1/network-evolution")
def network_evolution():
    """ネットワーク進化の時系列データ."""
    data = load_network_evolution_from_json()
    if not data:
        raise HTTPException(status_code=404, detail="Network evolution data not found. Run pipeline first.")
    return data


@app.get("/api/v1/genre-affinity")
def genre_affinity(
    person_id: str | None = Query(None, description="人物IDでフィルタ"),
):
    """ジャンル親和性データ."""
    data = load_genre_affinity_from_json()
    if not data:
        raise HTTPException(status_code=404, detail="Genre affinity data not found. Run pipeline first.")
    if person_id:
        if person_id not in data:
            raise HTTPException(status_code=404, detail=f"No genre data for {person_id}")
        return {"person_id": person_id, **data[person_id]}
    return data


@app.get("/api/v1/productivity")
def productivity(
    limit: int = Query(50, ge=1, le=500),
):
    """生産性指標."""
    data = load_productivity_metrics_from_json()
    if not data:
        raise HTTPException(status_code=404, detail="Productivity data not found. Run pipeline first.")
    items = [{"person_id": pid, **d} for pid, d in list(data.items())[:limit]]
    return {"total": len(data), "items": items}


@app.get("/api/v1/stats")
def db_stats():
    """DB統計情報."""
    conn = get_connection()
    try:
        stats = get_db_stats(conn)
        sources = get_data_sources(conn)
    finally:
        conn.close()
    return {"stats": stats, "data_sources": sources}


def main() -> None:
    """API サーバーを起動する."""
    from src.log import setup_logging

    setup_logging()
    logger.info("starting_api_server", host="0.0.0.0", port=8000)
    uvicorn.run(app, host="0.0.0.0", port=8000)


if __name__ == "__main__":
    main()
