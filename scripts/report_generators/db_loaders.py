"""DB-backed data loaders for report generation (GOLD layer, DuckDB).

各関数はテーブル名に対応した命名規則を持つ:
  load_{table_name}(conn) → そのテーブルの全件を JSON 互換 dict/list で返す

性別・ロール等の軸別集計はレポート側で conn.execute(SQL) を直接使うこと。
例: conn.execute(
    "SELECT p.gender, AVG(fps.iv_score), COUNT(*)"
    " FROM feat_person_scores fps JOIN conformed.persons p ON fps.person_id=p.id"
    " WHERE p.gender IS NOT NULL GROUP BY p.gender"
).fetchall()
"""

from __future__ import annotations

import json as _json
from typing import Any

from .sql_fragments import person_display_name_sql


# ---------------------------------------------------------------------------
# feat_person_scores (+ feat_career + feat_network JOIN)
# ---------------------------------------------------------------------------

def load_feat_person_scores(conn: Any) -> list[dict]:
    """feat_person_scores JOIN feat_career JOIN feat_network JOIN conformed.persons から
    scores.json 互換のリストを全件返す。

    返却フィールド:
      person_id, name, name_ja, name_en, gender,
      iv_score, person_fe, studio_fe_exposure, birank, patronage,
      dormancy, awcc, ndi, career_friction, peer_boost,
      person_fe_se, person_fe_n_obs,
      iv_score_pct, person_fe_pct, birank_pct, patronage_pct,
      awcc_pct, dormancy_pct, confidence,
      score_range: {low, high},
      centrality: {degree, betweenness, closeness, eigenvector},
      career: {first_year, latest_year, active_years, highest_stage, peak_year, peak_credits},
      network: {collaborators, unique_anime, hub_score},
      growth: {trend, activity_ratio, recent_credits},
      primary_role, total_credits, career_track
    """
    sql = f"""
        SELECT
            fps.person_id,
            {person_display_name_sql('fps.person_id')},
            p.name_ja, p.name_zh, p.name_en, p.gender,
            fps.iv_score, fps.person_fe, fps.studio_fe_exposure,
            fps.birank, fps.patronage, fps.dormancy, fps.awcc,
            fps.ndi, fps.career_friction, fps.peer_boost,
            fps.person_fe_se, fps.person_fe_n_obs,
            fps.iv_score_pct, fps.person_fe_pct, fps.birank_pct,
            fps.patronage_pct, fps.awcc_pct, fps.dormancy_pct,
            fps.confidence, fps.score_range_low, fps.score_range_high,
            -- feat_career
            fc.first_year, fc.latest_year, fc.active_years,
            fc.total_credits, fc.highest_stage, fc.primary_role,
            fc.career_track, fc.peak_year, fc.peak_credits,
            fc.growth_trend, fc.activity_ratio, fc.recent_credits,
            -- feat_network
            fn.degree_centrality, fn.betweenness_centrality,
            fn.closeness_centrality, fn.eigenvector_centrality,
            fn.hub_score, fn.n_collaborators, fn.n_unique_anime,
            fn.bridge_score
        FROM feat_person_scores fps
        JOIN conformed.persons p ON fps.person_id = p.id
        LEFT JOIN feat_career fc ON fps.person_id = fc.person_id
        LEFT JOIN feat_network fn ON fps.person_id = fn.person_id
        ORDER BY fps.iv_score DESC
    """
    rows = conn.execute(sql).fetchall()
    result = []
    for r in rows:
        entry: dict = {
            "person_id": r["person_id"],
            "name": r["name"],
            "name_ja": r["name_ja"] or "",
            "name_en": r["name_en"] or "",
            "gender": r["gender"],
            "iv_score": r["iv_score"] or 0.0,
            "person_fe": r["person_fe"] or 0.0,
            "studio_fe_exposure": r["studio_fe_exposure"] or 0.0,
            "birank": r["birank"] or 0.0,
            "patronage": r["patronage"] or 0.0,
            "dormancy": r["dormancy"] or 1.0,
            "awcc": r["awcc"] or 0.0,
            "ndi": r["ndi"] or 0.0,
            "career_friction": r["career_friction"] or 0.0,
            "peer_boost": r["peer_boost"] or 0.0,
            "person_fe_se": r["person_fe_se"],
            "person_fe_n_obs": r["person_fe_n_obs"] or 0,
            "iv_score_pct": r["iv_score_pct"],
            "person_fe_pct": r["person_fe_pct"],
            "birank_pct": r["birank_pct"],
            "patronage_pct": r["patronage_pct"],
            "awcc_pct": r["awcc_pct"],
            "dormancy_pct": r["dormancy_pct"],
            "confidence": r["confidence"],
            "score_range": {
                "low": r["score_range_low"],
                "high": r["score_range_high"],
            },
            "centrality": {
                "degree": r["degree_centrality"],
                "betweenness": r["betweenness_centrality"],
                "closeness": r["closeness_centrality"],
                "eigenvector": r["eigenvector_centrality"],
            },
            "career": {
                "first_year": r["first_year"],
                "latest_year": r["latest_year"],
                "active_years": r["active_years"],
                "highest_stage": r["highest_stage"],
                "peak_year": r["peak_year"],
                "peak_credits": r["peak_credits"],
            },
            "network": {
                "collaborators": r["n_collaborators"],
                "unique_anime": r["n_unique_anime"],
                "hub_score": r["hub_score"],
            },
            "growth": {
                "trend": r["growth_trend"],
                "activity_ratio": r["activity_ratio"],
                "recent_credits": r["recent_credits"],
            },
            "primary_role": r["primary_role"],
            "total_credits": r["total_credits"] or 0,
            "career_track": r["career_track"],
            # JSON ファイルにのみ存在するフィールド (使用側は .get() で参照)
            "score_layers": None,
            "versatility": None,
            "breakdown": None,
            "tags": [],
        }
        result.append(entry)
    return result


# ---------------------------------------------------------------------------
# agg_milestones
# ---------------------------------------------------------------------------

def load_agg_milestones(conn: Any) -> dict[str, list[dict]]:
    """agg_milestones から milestones.json 互換の {person_id: [events]} を全件返す."""
    rows = conn.execute("""
        SELECT person_id, event_type, year, anime_id, anime_title, description
        FROM agg_milestones
        ORDER BY person_id, year
    """).fetchall()

    result: dict[str, list[dict]] = {}
    for r in rows:
        pid = r["person_id"]
        if pid not in result:
            result[pid] = []
        result[pid].append({
            "type": r["event_type"],
            "year": r["year"],
            "anime_id": r["anime_id"] or None,
            "anime_title": r["anime_title"],
            "description": r["description"],
        })
    return result


# ---------------------------------------------------------------------------
# agg_director_circles
# ---------------------------------------------------------------------------

def load_agg_director_circles(conn: Any) -> dict[str, dict]:
    """agg_director_circles から circles.json 互換の
    {director_id: {members: [...]}} を全件返す。
    """
    sql = f"""
        SELECT
            dc.person_id, dc.director_id,
            dc.shared_works, dc.hit_rate, dc.roles, dc.latest_year,
            {person_display_name_sql('dc.person_id', 'member_name')}
        FROM agg_director_circles dc
        LEFT JOIN conformed.persons p ON dc.person_id = p.id
        ORDER BY dc.director_id, dc.shared_works DESC
    """
    rows = conn.execute(sql).fetchall()

    result: dict[str, dict] = {}
    for r in rows:
        dir_id = r["director_id"]
        if dir_id not in result:
            result[dir_id] = {"members": []}
        roles = r["roles"] or "[]"
        try:
            roles_list = _json.loads(roles)
        except (ValueError, TypeError):
            roles_list = []
        result[dir_id]["members"].append({
            "person_id": r["person_id"],
            "name": r["member_name"],
            "shared_works": r["shared_works"],
            "hit_rate": r["hit_rate"],
            "roles": roles_list,
            "latest_year": r["latest_year"],
        })
    return result


# ---------------------------------------------------------------------------
# feat_mentorships
# ---------------------------------------------------------------------------

def load_feat_mentorships(conn: Any) -> list[dict]:
    """feat_mentorships から mentorships.json 互換のリストを全件返す."""
    rows = conn.execute("""
        SELECT
            fm.mentor_id, fm.mentee_id,
            fm.n_shared_works, fm.hit_rate,
            fm.mentor_stage, fm.mentee_stage,
            fm.first_year, fm.latest_year
        FROM feat_mentorships fm
        ORDER BY fm.n_shared_works DESC
    """).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# feat_career (growth.json 互換)
# ---------------------------------------------------------------------------

def load_feat_career(conn: Any) -> dict:
    """feat_career から growth.json の persons セクション互換データを全件返す."""
    rows = conn.execute(f"""
        SELECT fc.person_id,
               {person_display_name_sql('fc.person_id')},
               fc.growth_trend, fc.activity_ratio, fc.recent_credits, fc.total_credits
        FROM feat_career fc
        LEFT JOIN conformed.persons p ON fc.person_id = p.id
    """).fetchall()

    persons: dict[str, dict] = {}
    for r in rows:
        persons[r["person_id"]] = {
            "name": r["name"],
            "trend": r["growth_trend"],
            "activity_ratio": r["activity_ratio"],
            "recent_credits": r["recent_credits"],
            "total_credits": r["total_credits"],
        }
    return {
        "persons": persons,
        "total_persons": len(persons),
    }


# ---------------------------------------------------------------------------
# feat_genre_affinity
# ---------------------------------------------------------------------------

def load_feat_genre_affinity(conn: Any) -> dict[str, dict]:
    """feat_genre_affinity から {person_id: {genre: {score, count}}} を全件返す."""
    rows = conn.execute("""
        SELECT person_id, genre, affinity_score, work_count
        FROM feat_genre_affinity
        ORDER BY person_id, affinity_score DESC
    """).fetchall()

    result: dict[str, dict] = {}
    for r in rows:
        pid = r["person_id"]
        if pid not in result:
            result[pid] = {}
        result[pid][r["genre"]] = {
            "score": r["affinity_score"],
            "count": r["work_count"],
        }
    return result


# ---------------------------------------------------------------------------
# feat_network (bridge_persons)
# ---------------------------------------------------------------------------

def load_feat_network(conn: Any) -> dict:
    """feat_network から bridges.json の bridge_persons セクション互換データを返す.

    community レベルのデータ (cross_community_edges, community_connectivity, stats) は
    DB テーブルに存在しないため空を返す。
    """
    rows = conn.execute(f"""
        SELECT
            fn.person_id,
            {person_display_name_sql('fn.person_id')},
            fn.bridge_score,
            fn.n_bridge_communities AS communities_connected
        FROM feat_network fn
        JOIN conformed.persons p ON fn.person_id = p.id
        WHERE fn.bridge_score IS NOT NULL
        ORDER BY fn.bridge_score DESC
    """).fetchall()
    bridge_persons = [
        {
            "person_id": r["person_id"],
            "name": r["name"],
            "bridge_score": r["bridge_score"],
            "communities_connected": r["communities_connected"],
        }
        for r in rows
    ]
    return {
        "bridge_persons": bridge_persons,
        "cross_community_edges": [],
        "community_connectivity": {},
        "stats": {"total_bridge_persons": len(bridge_persons)},
    }


# ---------------------------------------------------------------------------
# feat_cluster_membership
# ---------------------------------------------------------------------------

def load_feat_cluster_membership(conn: Any) -> dict[str, dict]:
    """feat_cluster_membership から {person_id: {community_id, career_track, ...}} を全件返す."""
    rows = conn.execute("""
        SELECT
            fc.person_id,
            fc.community_id,
            fc.career_track,
            fc.growth_trend,
            fc.studio_cluster_id,
            fc.studio_cluster_name,
            fc.cooccurrence_group_id
        FROM feat_cluster_membership fc
        ORDER BY fc.person_id
    """).fetchall()
    return {
        r["person_id"]: {
            "community_id": r["community_id"],
            "career_track": r["career_track"],
            "growth_trend": r["growth_trend"],
            "studio_cluster_id": r["studio_cluster_id"],
            "studio_cluster_name": r["studio_cluster_name"],
            "cooccurrence_group_id": r["cooccurrence_group_id"],
        }
        for r in rows
    }


# ---------------------------------------------------------------------------
# feat_birank_annual (temporal_pagerank.json 互換)
# ---------------------------------------------------------------------------

def load_feat_birank_annual(conn: Any) -> dict:
    """feat_birank_annual から temporal_pagerank.json 互換データを全件返す.

    返却形式:
      {
        "birank_timelines": {
          person_id: {
            "person_id": ...,
            "snapshots": [{year, birank, raw_pagerank, graph_size, n_credits_cumulative}],
            "peak_year": int, "peak_birank": float,
            "career_start_year": int, "latest_year": int,
          }
        },
        "foresight_scores": {},   # DB に未格納
        "promotion_credits": {},  # DB に未格納
        "years_computed": [int],
        "total_persons": int,
      }
    """
    rows = conn.execute("""
        SELECT person_id, year, birank, raw_pagerank, graph_size, n_credits_cumulative
        FROM feat_birank_annual
        ORDER BY person_id, year
    """).fetchall()

    timelines: dict[str, dict] = {}
    years_set: set[int] = set()
    for r in rows:
        pid = r["person_id"]
        years_set.add(r["year"])
        snap = {
            "year": r["year"],
            "birank": r["birank"],
            "raw_pagerank": r["raw_pagerank"],
            "graph_size": r["graph_size"],
            "n_credits_cumulative": r["n_credits_cumulative"],
        }
        if pid not in timelines:
            timelines[pid] = {
                "person_id": pid,
                "snapshots": [],
                "peak_year": None,
                "peak_birank": None,
                "career_start_year": None,
                "latest_year": None,
                "trajectory": None,
            }
        tl = timelines[pid]
        tl["snapshots"].append(snap)
        if tl["peak_birank"] is None or (r["birank"] and r["birank"] > tl["peak_birank"]):
            tl["peak_birank"] = r["birank"]
            tl["peak_year"] = r["year"]
        if tl["career_start_year"] is None or r["year"] < tl["career_start_year"]:
            tl["career_start_year"] = r["year"]
        if tl["latest_year"] is None or r["year"] > tl["latest_year"]:
            tl["latest_year"] = r["year"]

    return {
        "birank_timelines": timelines,
        "foresight_scores": {},
        "promotion_credits": {},
        "years_computed": sorted(years_set),
        "total_persons": len(timelines),
    }


# ---------------------------------------------------------------------------
# feat_credit_activity
# ---------------------------------------------------------------------------

def load_feat_credit_activity(conn: Any) -> dict[str, dict]:
    """feat_credit_activity から {person_id: {...}} を全件返す."""
    rows = conn.execute("""
        SELECT person_id,
               first_abs_quarter, last_abs_quarter,
               activity_span_quarters, active_quarters, density,
               n_gaps, mean_gap_quarters, median_gap_quarters,
               min_gap_quarters, max_gap_quarters, std_gap_quarters,
               consecutive_quarters, consecutive_rate,
               n_hiatuses, longest_hiatus_quarters,
               quarters_since_last_credit,
               active_years, n_year_gaps, mean_year_gap, max_year_gap
        FROM feat_credit_activity
    """).fetchall()
    return {r["person_id"]: dict(r) for r in rows}


# ---------------------------------------------------------------------------
# feat_career_annual
# ---------------------------------------------------------------------------

def load_feat_career_annual(
    conn: Any,
    person_id: str | None = None,
) -> dict[str, list[dict]]:
    """feat_career_annual から {person_id: [year_rows]} を返す.

    person_id 指定時はその人物のみ。省略時は全件。
    """
    if person_id:
        rows = conn.execute(
            "SELECT * FROM feat_career_annual WHERE person_id = ? ORDER BY career_year",
            (person_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM feat_career_annual ORDER BY person_id, career_year"
        ).fetchall()

    result: dict[str, list[dict]] = {}
    for r in rows:
        pid = r["person_id"]
        if pid not in result:
            result[pid] = []
        result[pid].append(dict(r))
    return result


# ---------------------------------------------------------------------------
# feat_studio_affiliation
# ---------------------------------------------------------------------------

def load_feat_studio_affiliation(
    conn: Any,
    person_id: str | None = None,
) -> dict[str, list[dict]]:
    """feat_studio_affiliation から {person_id: [year_studio_rows]} を返す."""
    if person_id:
        rows = conn.execute(
            "SELECT * FROM feat_studio_affiliation WHERE person_id = ? "
            "ORDER BY credit_year, n_works DESC",
            (person_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM feat_studio_affiliation "
            "ORDER BY person_id, credit_year, n_works DESC"
        ).fetchall()

    result: dict[str, list[dict]] = {}
    for r in rows:
        pid = r["person_id"]
        if pid not in result:
            result[pid] = []
        result[pid].append(dict(r))
    return result


# ---------------------------------------------------------------------------
# feat_credit_contribution
# ---------------------------------------------------------------------------

def load_feat_credit_contribution(
    conn: Any,
    *,
    person_id: str | None = None,
    anime_id: str | None = None,
) -> list[dict]:
    """feat_credit_contribution から credit-level 行を返す.

    person_id / anime_id でフィルタ可能。省略時は全件（注意: 大量）。
    """
    conditions: list[str] = []
    params: list[str] = []
    if person_id:
        conditions.append("person_id = ?")
        params.append(person_id)
    if anime_id:
        conditions.append("anime_id = ?")
        params.append(anime_id)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    rows = conn.execute(
        f"SELECT * FROM feat_credit_contribution {where} "
        "ORDER BY person_id, credit_year, anime_id",
        tuple(params),
    ).fetchall()
    return [dict(r) for r in rows]


# ---------------------------------------------------------------------------
# feat_person_work_summary
# ---------------------------------------------------------------------------

def load_feat_person_work_summary(conn: Any) -> dict[str, dict]:
    """feat_person_work_summary から {person_id: {...}} を全件返す."""
    rows = conn.execute("""
        SELECT person_id,
               n_distinct_works, total_production_scale,
               mean_production_scale, max_production_scale,
               best_work_anime_id,
               total_edge_weight, mean_edge_weight_per_work,
               max_edge_weight, top_contrib_anime_id,
               total_iv_contrib_est
        FROM feat_person_work_summary
    """).fetchall()
    return {r["person_id"]: dict(r) for r in rows}


# ---------------------------------------------------------------------------
# feat_work_context
# ---------------------------------------------------------------------------

def load_feat_work_context(
    conn: Any,
    anime_id: str | None = None,
) -> dict[str, dict]:
    """feat_work_context から {anime_id: {...}} を返す.

    anime_id 指定時はその作品のみ。省略時は全件。
    """
    if anime_id:
        rows = conn.execute(
            "SELECT * FROM feat_work_context WHERE anime_id = ?",
            (anime_id,),
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM feat_work_context").fetchall()

    return {r["anime_id"]: dict(r) for r in rows}


# ---------------------------------------------------------------------------
# feat_person_role_progression
# ---------------------------------------------------------------------------

def load_feat_person_role_progression(
    conn: Any,
    person_id: str | None = None,
) -> dict[str, list[dict]]:
    """feat_person_role_progression から {person_id: [role_entries]} を返す."""
    if person_id:
        rows = conn.execute(
            "SELECT * FROM feat_person_role_progression WHERE person_id = ? "
            "ORDER BY first_year",
            (person_id,),
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT * FROM feat_person_role_progression ORDER BY person_id, first_year"
        ).fetchall()

    result: dict[str, list[dict]] = {}
    for r in rows:
        pid = r["person_id"]
        if pid not in result:
            result[pid] = []
        result[pid].append(dict(r))
    return result


# ---------------------------------------------------------------------------
# feat_causal_estimates
# ---------------------------------------------------------------------------

def load_feat_causal_estimates(conn: Any) -> dict[str, dict]:
    """feat_causal_estimates から {person_id: {...}} を全件返す."""
    rows = conn.execute("""
        SELECT person_id,
               peer_effect_boost, career_friction,
               era_fe, era_deflated_iv, opportunity_residual
        FROM feat_causal_estimates
    """).fetchall()
    return {r["person_id"]: dict(r) for r in rows}
