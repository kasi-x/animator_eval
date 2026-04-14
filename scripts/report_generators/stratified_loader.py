"""Stratified data provider for v2 report generation.

Provides StratifiedDataProvider: loads from feat_ tables with multi-axis
slicing (tier, gender, decade, career_track, community, studio_cluster).

Used by all report generators to produce tier/gender/era/cluster breakdowns
without duplicating SQL joins in each report.
"""

from __future__ import annotations

import sqlite3
from dataclasses import dataclass
from typing import Any

from .ci_utils import distribution_summary


# =========================================================================
# Stratification axis definitions
# =========================================================================

#: Valid stratification axes and their SQL expressions / join requirements
STRATIFICATION_AXES = {
    "tier": "fwc.scale_tier",
    "tier_label": "fwc.scale_label",
    "gender": "p.gender",
    "decade": "(fc.first_year / 10) * 10",
    "career_track": "fcm.career_track",
    "community": "fcm.community_id",
    "studio_cluster": "fcm.studio_cluster_id",
    "growth_trend": "fcm.growth_trend",
}

#: Human-readable labels for tier values
TIER_LABELS = {
    1: "Micro (tier 1)",
    2: "Small (tier 2)",
    3: "Standard (tier 3)",
    4: "Large (tier 4)",
    5: "Major (tier 5)",
}


@dataclass
class StratifiedResult:
    """Result of a stratified query.

    Attributes:
        groups: dict mapping group_key -> list of values
        axis: the stratification axis name
        metric: the metric that was queried
        total_n: total number of observations across all groups
    """

    groups: dict[Any, list[float]]
    axis: str
    metric: str
    total_n: int = 0

    def group_summaries(self, confidence: float = 0.95) -> dict[Any, dict]:
        """Compute distribution_summary for each group."""
        return {
            key: distribution_summary(vals, confidence=confidence, label=str(key))
            for key, vals in self.groups.items()
        }


@dataclass
class DirectorTierProfile:
    """Tier distribution of works directed by a person.

    Attributes:
        person_id: the person's ID
        tier_counts: {tier_int: count_of_works_directed}
        total_directed: total number of works directed
        primary_tier: the tier with the most directed works
        career_year_range: (first_career_year, last_career_year) when directing
    """

    person_id: str
    tier_counts: dict[int, int]
    total_directed: int
    primary_tier: int | None = None
    career_year_range: tuple[int | None, int | None] = (None, None)


class StratifiedDataProvider:
    """Multi-axis stratified data loader backed by feat_ tables.

    Usage:
        provider = StratifiedDataProvider(conn)
        result = provider.scores_stratified(by="tier", metric="iv_score")
        for key, summary in result.group_summaries().items():
            print(f"Tier {key}: median={summary['median']}, n={summary['n']}")
    """

    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    # -----------------------------------------------------------------
    # Core stratified query
    # -----------------------------------------------------------------

    def scores_stratified(
        self,
        by: str | list[str],
        metric: str = "iv_score",
        *,
        where: str = "",
        params: tuple = (),
    ) -> StratifiedResult | dict[str, StratifiedResult]:
        """Load a metric from feat_person_scores, stratified by one or more axes.

        Args:
            by: stratification axis name(s). Single string returns StratifiedResult;
                list returns dict[axis_name, StratifiedResult].
            metric: column name in feat_person_scores (e.g. 'iv_score', 'person_fe',
                'birank', 'patronage', 'awcc', 'dormancy', 'ndi').
            where: additional SQL WHERE clause (without 'WHERE' keyword).
            params: bind parameters for the where clause.

        Returns:
            StratifiedResult (single axis) or dict of them (multi-axis).
        """
        if isinstance(by, list):
            return {
                axis: self.scores_stratified(axis, metric, where=where, params=params)
                for axis in by
            }

        axis_expr = STRATIFICATION_AXES.get(by)
        if axis_expr is None:
            raise ValueError(
                f"Unknown stratification axis '{by}'. "
                f"Valid: {sorted(STRATIFICATION_AXES)}"
            )

        joins = self._joins_for_axis(by)
        where_clause = f"AND ({where})" if where else ""

        sql = f"""
            SELECT {axis_expr} AS group_key, fps.{metric} AS val
            FROM feat_person_scores fps
            JOIN persons p ON fps.person_id = p.id
            {joins}
            WHERE fps.{metric} IS NOT NULL
                  AND {axis_expr} IS NOT NULL
                  {where_clause}
        """
        rows = self.conn.execute(sql, params).fetchall()

        groups: dict[Any, list[float]] = {}
        for r in rows:
            key = r["group_key"]
            groups.setdefault(key, []).append(float(r["val"]))

        total_n = sum(len(v) for v in groups.values())
        return StratifiedResult(
            groups=groups, axis=by, metric=metric, total_n=total_n,
        )

    def metric_by_tier(
        self,
        metric: str = "iv_score",
        *,
        where: str = "",
        params: tuple = (),
    ) -> StratifiedResult:
        """Shortcut: stratify by work scale tier."""
        return self.scores_stratified("tier", metric, where=where, params=params)

    def metric_by_gender(
        self,
        metric: str = "iv_score",
        *,
        where: str = "",
        params: tuple = (),
    ) -> StratifiedResult:
        """Shortcut: stratify by gender."""
        return self.scores_stratified("gender", metric, where=where, params=params)

    def metric_by_decade(
        self,
        metric: str = "iv_score",
        *,
        where: str = "",
        params: tuple = (),
    ) -> StratifiedResult:
        """Shortcut: stratify by debut decade."""
        return self.scores_stratified("decade", metric, where=where, params=params)

    # -----------------------------------------------------------------
    # Director tier profile
    # -----------------------------------------------------------------

    def director_tier_profile(self, person_id: str) -> DirectorTierProfile:
        """Get tier distribution of works directed by a person.

        Uses feat_credit_contribution (role = direction) JOIN feat_work_context
        to count how many works at each tier this person directed.
        """
        sql = """
            SELECT
                fwc.scale_tier AS tier,
                COUNT(DISTINCT fcc.anime_id) AS cnt,
                MIN(fcc.career_year_at_credit) AS min_cy,
                MAX(fcc.career_year_at_credit) AS max_cy
            FROM feat_credit_contribution fcc
            JOIN feat_work_context fwc ON fcc.anime_id = fwc.anime_id
            WHERE fcc.person_id = ?
              AND fcc.role IN (
                  'director', 'chief_director', 'series_director',
                  'episode_director', 'assistant_director',
                  'unit_director', 'co_director'
              )
              AND fwc.scale_tier IS NOT NULL
            GROUP BY fwc.scale_tier
            ORDER BY fwc.scale_tier
        """
        rows = self.conn.execute(sql, (person_id,)).fetchall()

        tier_counts: dict[int, int] = {}
        total = 0
        min_cy, max_cy = None, None
        for r in rows:
            t = r["tier"]
            c = r["cnt"]
            tier_counts[t] = c
            total += c
            if r["min_cy"] is not None:
                min_cy = min(min_cy, r["min_cy"]) if min_cy is not None else r["min_cy"]
            if r["max_cy"] is not None:
                max_cy = max(max_cy, r["max_cy"]) if max_cy is not None else r["max_cy"]

        primary = max(tier_counts, key=tier_counts.get) if tier_counts else None

        return DirectorTierProfile(
            person_id=person_id,
            tier_counts=tier_counts,
            total_directed=total,
            primary_tier=primary,
            career_year_range=(min_cy, max_cy),
        )

    def director_tier_profiles_batch(
        self,
        person_ids: list[str] | None = None,
    ) -> dict[str, DirectorTierProfile]:
        """Batch load director tier profiles for multiple persons.

        If person_ids is None, loads for all persons with direction credits.
        """
        where_clause = ""
        bind_params: tuple = ()
        if person_ids is not None:
            placeholders = ",".join("?" for _ in person_ids)
            where_clause = f"AND fcc.person_id IN ({placeholders})"
            bind_params = tuple(person_ids)

        sql = f"""
            SELECT
                fcc.person_id,
                fwc.scale_tier AS tier,
                COUNT(DISTINCT fcc.anime_id) AS cnt,
                MIN(fcc.career_year_at_credit) AS min_cy,
                MAX(fcc.career_year_at_credit) AS max_cy
            FROM feat_credit_contribution fcc
            JOIN feat_work_context fwc ON fcc.anime_id = fwc.anime_id
            WHERE fcc.role IN (
                  'director', 'chief_director', 'series_director',
                  'episode_director', 'assistant_director',
                  'unit_director', 'co_director'
              )
              AND fwc.scale_tier IS NOT NULL
              {where_clause}
            GROUP BY fcc.person_id, fwc.scale_tier
        """
        rows = self.conn.execute(sql, bind_params).fetchall()

        # Accumulate per person
        data: dict[str, dict] = {}
        for r in rows:
            pid = r["person_id"]
            if pid not in data:
                data[pid] = {"tiers": {}, "total": 0, "min_cy": None, "max_cy": None}
            d = data[pid]
            d["tiers"][r["tier"]] = r["cnt"]
            d["total"] += r["cnt"]
            if r["min_cy"] is not None:
                d["min_cy"] = min(d["min_cy"], r["min_cy"]) if d["min_cy"] is not None else r["min_cy"]
            if r["max_cy"] is not None:
                d["max_cy"] = max(d["max_cy"], r["max_cy"]) if d["max_cy"] is not None else r["max_cy"]

        result: dict[str, DirectorTierProfile] = {}
        for pid, d in data.items():
            primary = max(d["tiers"], key=d["tiers"].get) if d["tiers"] else None
            result[pid] = DirectorTierProfile(
                person_id=pid,
                tier_counts=d["tiers"],
                total_directed=d["total"],
                primary_tier=primary,
                career_year_range=(d["min_cy"], d["max_cy"]),
            )
        return result

    # -----------------------------------------------------------------
    # Work-level stratified queries
    # -----------------------------------------------------------------

    def work_metric_by_tier(
        self,
        metric_expr: str = "fwc.production_scale",
        *,
        where: str = "",
        params: tuple = (),
    ) -> StratifiedResult:
        """Load a work-level metric from feat_work_context stratified by tier.

        Args:
            metric_expr: SQL expression for the metric (column of fwc).
            where: additional WHERE clause.
            params: bind parameters.
        """
        where_clause = f"AND ({where})" if where else ""
        sql = f"""
            SELECT fwc.scale_tier AS group_key, {metric_expr} AS val
            FROM feat_work_context fwc
            WHERE fwc.scale_tier IS NOT NULL
              AND ({metric_expr}) IS NOT NULL
              {where_clause}
        """
        rows = self.conn.execute(sql, params).fetchall()
        groups: dict[Any, list[float]] = {}
        for r in rows:
            groups.setdefault(r["group_key"], []).append(float(r["val"]))
        total_n = sum(len(v) for v in groups.values())
        return StratifiedResult(
            groups=groups, axis="tier", metric=metric_expr, total_n=total_n,
        )

    # -----------------------------------------------------------------
    # Credit-level stratified queries
    # -----------------------------------------------------------------

    def credits_by_tier_and_role(
        self,
        role_filter: str | list[str] | None = None,
    ) -> dict[int, dict[str, int]]:
        """Count credits per tier per role category.

        Returns: {tier: {role: count}}
        """
        where_parts = ["fwc.scale_tier IS NOT NULL"]
        bind: list[str] = []
        if role_filter:
            if isinstance(role_filter, str):
                role_filter = [role_filter]
            placeholders = ",".join("?" for _ in role_filter)
            where_parts.append(f"fcc.role IN ({placeholders})")
            bind.extend(role_filter)

        sql = f"""
            SELECT fwc.scale_tier AS tier, fcc.role, COUNT(*) AS cnt
            FROM feat_credit_contribution fcc
            JOIN feat_work_context fwc ON fcc.anime_id = fwc.anime_id
            WHERE {' AND '.join(where_parts)}
            GROUP BY fwc.scale_tier, fcc.role
        """
        rows = self.conn.execute(sql, tuple(bind)).fetchall()
        result: dict[int, dict[str, int]] = {}
        for r in rows:
            tier = r["tier"]
            if tier not in result:
                result[tier] = {}
            result[tier][r["role"]] = r["cnt"]
        return result

    # -----------------------------------------------------------------
    # Role progression with context
    # -----------------------------------------------------------------

    def role_progression_stratified(
        self,
        role_category: str = "direction",
        by: str = "tier",
    ) -> dict[str, Any]:
        """Analyze role progression (e.g. years to become director)
        stratified by tier, decade, or gender.

        Returns dict with:
            groups: {group_key: [career_year_first values]}
            summaries: {group_key: distribution_summary}
        """
        axis_expr = STRATIFICATION_AXES.get(by)
        if axis_expr is None:
            raise ValueError(f"Unknown axis '{by}'")

        joins = self._joins_for_axis(by)
        # For tier, we need a different join strategy: the tier of works
        # where they performed this role
        if by == "tier":
            # Use the tier of the person's directed works via credit_contribution
            sql = """
                SELECT
                    fwc.scale_tier AS group_key,
                    frp.career_year_first AS val
                FROM feat_person_role_progression frp
                JOIN feat_credit_contribution fcc
                    ON frp.person_id = fcc.person_id
                JOIN feat_work_context fwc
                    ON fcc.anime_id = fwc.anime_id
                WHERE frp.role_category = ?
                  AND frp.career_year_first IS NOT NULL
                  AND fwc.scale_tier IS NOT NULL
                  AND fcc.role IN (
                      'director', 'chief_director', 'series_director',
                      'episode_director', 'assistant_director',
                      'unit_director', 'co_director'
                  )
                GROUP BY frp.person_id, fwc.scale_tier
            """
            rows = self.conn.execute(sql, (role_category,)).fetchall()
        else:
            sql = f"""
                SELECT {axis_expr} AS group_key, frp.career_year_first AS val
                FROM feat_person_role_progression frp
                JOIN persons p ON frp.person_id = p.id
                {joins}
                WHERE frp.role_category = ?
                  AND frp.career_year_first IS NOT NULL
                  AND {axis_expr} IS NOT NULL
            """
            rows = self.conn.execute(sql, (role_category,)).fetchall()

        groups: dict[Any, list[float]] = {}
        for r in rows:
            groups.setdefault(r["group_key"], []).append(float(r["val"]))

        summaries = {
            key: distribution_summary(vals, label=str(key))
            for key, vals in groups.items()
        }
        return {"groups": groups, "summaries": summaries}

    # -----------------------------------------------------------------
    # Causal estimates stratified
    # -----------------------------------------------------------------

    def causal_estimates_stratified(
        self,
        metric: str = "peer_effect_boost",
        by: str = "tier",
    ) -> StratifiedResult:
        """Load causal estimates stratified by axis.

        metric: column from feat_causal_estimates
            (peer_effect_boost, career_friction, era_fe, era_deflated_iv,
             opportunity_residual)
        """
        axis_expr = STRATIFICATION_AXES.get(by)
        if axis_expr is None:
            raise ValueError(f"Unknown axis '{by}'")

        joins = self._joins_for_axis(by)

        sql = f"""
            SELECT {axis_expr} AS group_key, fce.{metric} AS val
            FROM feat_causal_estimates fce
            JOIN persons p ON fce.person_id = p.id
            {joins}
            WHERE fce.{metric} IS NOT NULL
              AND {axis_expr} IS NOT NULL
        """
        rows = self.conn.execute(sql).fetchall()

        groups: dict[Any, list[float]] = {}
        for r in rows:
            groups.setdefault(r["group_key"], []).append(float(r["val"]))

        total_n = sum(len(v) for v in groups.values())
        return StratifiedResult(
            groups=groups, axis=by, metric=metric, total_n=total_n,
        )

    # -----------------------------------------------------------------
    # Convenience: full population distribution with CI
    # -----------------------------------------------------------------

    def population_distribution(
        self,
        metric: str = "iv_score",
        *,
        where: str = "",
        params: tuple = (),
    ) -> dict[str, Any]:
        """Full population distribution summary with CI for a metric."""
        where_clause = f"AND ({where})" if where else ""
        sql = f"""
            SELECT fps.{metric} AS val
            FROM feat_person_scores fps
            WHERE fps.{metric} IS NOT NULL
              {where_clause}
        """
        rows = self.conn.execute(sql, params).fetchall()
        values = [float(r["val"]) for r in rows]
        return distribution_summary(values, label=f"population_{metric}")

    # -----------------------------------------------------------------
    # Internal: build JOIN clauses for each axis
    # -----------------------------------------------------------------

    def _joins_for_axis(self, axis: str) -> str:
        """Return SQL JOIN clauses needed for a given stratification axis.

        The tier axis requires special handling: we compute each person's
        modal (most frequent) work tier via a window function subquery.
        """
        if axis in ("tier", "tier_label"):
            # Person-level tier = mode of their work tiers
            return """
                LEFT JOIN (
                    SELECT person_id, scale_tier, scale_label
                    FROM (
                        SELECT
                            fcc_t.person_id,
                            fwc_t.scale_tier,
                            fwc_t.scale_label,
                            ROW_NUMBER() OVER (
                                PARTITION BY fcc_t.person_id
                                ORDER BY COUNT(*) DESC, fwc_t.scale_tier DESC
                            ) AS rn
                        FROM feat_credit_contribution fcc_t
                        JOIN feat_work_context fwc_t ON fcc_t.anime_id = fwc_t.anime_id
                        WHERE fwc_t.scale_tier IS NOT NULL
                        GROUP BY fcc_t.person_id, fwc_t.scale_tier
                    )
                    WHERE rn = 1
                ) fwc ON fwc.person_id = fps.person_id
            """

        if axis == "gender":
            return ""

        if axis == "decade":
            return "LEFT JOIN feat_career fc ON fps.person_id = fc.person_id"

        if axis in ("career_track", "community", "studio_cluster", "growth_trend"):
            return "LEFT JOIN feat_cluster_membership fcm ON fps.person_id = fcm.person_id"

        return ""
