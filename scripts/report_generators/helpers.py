#!/usr/bin/env python3
"""Helper functions for report generation.

Includes JSON I/O, feature extraction, visualization utilities, and data-driven clustering.
"""

import hashlib
import json
import random
import sqlite3
from pathlib import Path

import numpy as np
import plotly.graph_objects as go

# Global directories (must be configured by main script)
JSON_DIR = Path("result/json")

# Go Explorer server URL (start with: pixi run explorer, port 3000)
EXPLORER_URL = "http://localhost:3000"

def person_link(name: str, person_id: str) -> str:
    """Convert a person's name to a link to the Go Explorer detail page."""
    if not person_id:
        return name
    url = f"{EXPLORER_URL}/#person/{person_id}"
    return (
        f'<a href="{url}" target="_blank" '
        f'style="color:#a0d2db;text-decoration:none;" '
        f'onmouseover="this.style.textDecoration=\'underline\'" '
        f'onmouseout="this.style.textDecoration=\'none\'">'
        f"{name}</a>"
    )


def load_json(name: str) -> dict | list | None:
    """Safely load a JSON file."""
    path = JSON_DIR / name
    if not path.exists():
        print(f"  [SKIP] {name} not found")
        return None
    with open(path) as f:
        return json.load(f)


def get_footer_stats() -> str:
    """Dynamically generate data statistics for the footer."""
    summary = load_json("summary.json")
    if summary and "data" in summary:
        d = summary["data"]
        persons = d.get("persons", 0) or d.get("scored_persons", 0)
        credits = d.get("credits", 0)
        return f"{persons:,}人 / {credits:,}クレジット"
    # fallback: record count from scores.json
    scores = load_json("scores.json")
    if scores and isinstance(scores, list):
        return f"{len(scores):,}人"
    return "（統計情報なし）"


# ---------------------------------------------------------------------------
# Data accessors (table-name-based)
# Read from JSON files. Can be replaced with direct DB reads in the future.
# For axis aggregations by gender/role etc., use conn.execute(SQL) in the report.
# ---------------------------------------------------------------------------

def get_feat_person_scores() -> list[dict]:
    """Return scores.json (feat_person_scores compatible)."""
    return load_json("scores.json") or []


def get_agg_milestones() -> dict:
    """Return milestones.json (agg_milestones compatible)."""
    return load_json("milestones.json") or {}


def get_agg_director_circles() -> dict:
    """Return circles.json (agg_director_circles compatible)."""
    return load_json("circles.json") or {}


def get_feat_mentorships() -> list:
    """Return mentorships.json (feat_mentorships compatible)."""
    return load_json("mentorships.json") or []


def get_feat_career() -> dict:
    """Return growth.json (feat_career compatible)."""
    return load_json("growth.json") or {}


def get_feat_genre_affinity() -> dict:
    """Return genre_affinity.json (feat_genre_affinity compatible)."""
    return load_json("genre_affinity.json") or {}


def get_feat_network() -> dict:
    """Return bridges.json (feat_network compatible)."""
    return load_json("bridges.json") or {}


def get_feat_cluster_membership() -> dict:
    """feat_cluster_membership — not output to JSON, returns empty.
    Use conn.execute() inside the report instead.
    """
    return {}


def get_feat_birank_annual() -> dict:
    """Return temporal_pagerank.json (feat_birank_annual compatible)."""
    return load_json("temporal_pagerank.json") or {}


def compute_iv_percentiles() -> dict:
    """Pre-compute percentiles of IV scores."""
    scores = get_feat_person_scores()
    if not scores or not isinstance(scores, list):
        return {"p50": 0.0, "p75": 0.01, "p90": 0.1, "p95": 0.5, "p99": 2.0}
    ivs = [s["iv_score"] for s in scores if s.get("iv_score") is not None]
    if len(ivs) < 100:
        return {"p50": 0.0, "p75": 0.01, "p90": 0.1, "p95": 0.5, "p99": 2.0}
    arr = np.array(ivs)
    p = np.percentile(arr, [50, 75, 90, 95, 99])
    return {"p50": float(p[0]), "p75": float(p[1]), "p90": float(p[2]),
            "p95": float(p[3]), "p99": float(p[4])}


def fmt_num(n: int | float) -> str:
    """Format a number."""
    if isinstance(n, float):
        if n >= 1000:
            return f"{n:,.0f}"
        return f"{n:.2f}"
    return f"{n:,}"


def name_clusters_by_rank(
    centers,
    feat_specs: list[tuple[int, list[str]]],
) -> dict[int, str]:
    """Dynamically assign cluster names based on the relative rank of K-Means centroids.

    Instead of fixed thresholds (e.g., >= 70), labels are determined by the relative
    ordering of centroids, so results are scale-independent.

    Args:
        centers: Inverse-transformed centroid array (n_clusters × n_features)
        feat_specs: List of (feat_idx, [label_highest, label_mid, ..., label_lowest]).
                    For each feature, labels are assigned in descending centroid value order.

    Returns:
        Dict of {cluster_id: "C{n}: label1×label2×..."}
    """
    n_clusters = len(centers)
    feat_labels: dict[int, list[str]] = {c: [] for c in range(n_clusters)}

    for feat_idx, label_list in feat_specs:
        # sort descending
        ranked = sorted(range(n_clusters), key=lambda c: -float(centers[c, feat_idx]))
        n_labels = len(label_list)
        for rank, cid in enumerate(ranked):
            label_idx = min(rank * n_labels // n_clusters, n_labels - 1)
            feat_labels[cid].append(label_list[label_idx])

    return {c: f"C{c+1}: {'×'.join(feat_labels[c])}" for c in range(n_clusters)}


def name_clusters_distinctive(centers_orig, feature_names: list[str]) -> dict[int, str]:
    """Name each cluster by the z-score of its most distinctive dimensions (guaranteed unique).

    For each cluster, the top-3 features with the largest |z-score| are selected,
    and their direction (high/low) is expressed with Japanese labels.
    If the same name collides, a numeric suffix is appended to ensure uniqueness.
    """
    mean = centers_orig.mean(axis=0)
    std = centers_orig.std(axis=0) + 1e-10
    z = (centers_orig - mean) / std
    n_clusters = len(centers_orig)

    FEAT_POS = {
        "birank": "高BiRank", "patronage": "高Patronage", "person_fe": "高PersonFE",
        "iv_score": "高IV", "total_credits": "多作",
        "degree": "高次数", "betweenness": "高媒介", "eigenvector": "高固有",
        "active_years": "長キャリア", "highest_stage": "上位役職",
        "peak_credits": "高ピーク", "collaborators": "広人脈",
        "unique_anime": "多作品", "hub_score": "ハブ",
        "activity_ratio": "高活動", "recent_credits": "最近活発",
        "versatility_score": "多才", "categories": "多カテゴリ",
        "roles": "多役割", "confidence": "高確信",
    }
    FEAT_NEG = {
        "birank": "低BiRank", "patronage": "低Patronage", "person_fe": "低PersonFE",
        "iv_score": "低IV", "total_credits": "寡作",
        "degree": "低次数", "betweenness": "低媒介", "eigenvector": "低固有",
        "active_years": "短キャリア", "highest_stage": "下位役職",
        "peak_credits": "低ピーク", "collaborators": "狭人脈",
        "unique_anime": "少作品", "hub_score": "周辺",
        "activity_ratio": "低活動", "recent_credits": "最近不活発",
        "versatility_score": "専門特化", "categories": "少カテゴリ",
        "roles": "単一役割", "confidence": "低確信",
    }

    raw_names: list[str] = []
    for c in range(n_clusters):
        top_idx = np.argsort(-np.abs(z[c]))[:3]
        parts = []
        for fi in top_idx:
            fname = feature_names[int(fi)]
            lbl = (FEAT_POS.get(fname, f"高{fname}") if z[c, fi] > 0
                   else FEAT_NEG.get(fname, f"低{fname}"))
            parts.append(lbl)
        raw_names.append("・".join(parts))

    # deduplicate
    count: dict[str, int] = {}
    names: dict[int, str] = {}
    for c, nm in enumerate(raw_names):
        if nm in count:
            count[nm] += 1
            names[c] = f"C{c+1}: {nm}({count[nm]})"
        else:
            count[nm] = 1
            names[c] = f"C{c+1}: {nm}"
    return names


# ============================================================
# 共通統計・品質向上ヘルパー関数
# ============================================================


def add_distribution_stats(fig: go.Figure, values, axis: str = "x") -> go.Figure:
    """ヒストグラム/分布チャートに中央値・平均・P90ラインを追加.

    Args:
        fig: Plotly Figure
        values: 数値のシーケンス
        axis: 'x' or 'y' — 統計線を引く軸
    """
    arr = np.array([v for v in values if v is not None and np.isfinite(v)])
    if len(arr) == 0:
        return fig
    med = float(np.median(arr))
    avg = float(np.mean(arr))
    p90 = float(np.percentile(arr, 90))

    line_specs = [
        (med, "中央値", "#06D6A0", "dash"),
        (avg, "平均", "#EF476F", "dot"),
        (p90, "P90", "#9B59B6", "dashdot"),
    ]

    for val, label, color, dash_style in line_specs:
        if axis == "x":
            fig.add_vline(x=val, line_dash=dash_style, line_color=color, line_width=1.5,
                          annotation_text=f"{label}={val:.2f}", annotation_font_color=color,
                          annotation_font_size=10)
        else:
            fig.add_hline(y=val, line_dash=dash_style, line_color=color, line_width=1.5,
                          annotation_text=f"{label}={val:.2f}", annotation_font_color=color,
                          annotation_font_size=10)

    # タイトルに n= を付記
    current_title = fig.layout.title.text if fig.layout.title and fig.layout.title.text else ""
    if current_title and "n=" not in current_title:
        fig.update_layout(title_text=f"{current_title}  (n={len(arr):,})")

    return fig


def add_scatter_correlation(fig: go.Figure, x, y) -> go.Figure:
    """散布図にOLS回帰線＋Pearson r＋p値アノテーション追加."""
    from scipy import stats as sp_stats

    x_arr = np.array(x, dtype=float)
    y_arr = np.array(y, dtype=float)
    mask = np.isfinite(x_arr) & np.isfinite(y_arr)
    x_clean, y_clean = x_arr[mask], y_arr[mask]
    if len(x_clean) < 3:
        return fig

    r, p = sp_stats.pearsonr(x_clean, y_clean)
    slope, intercept = np.polyfit(x_clean, y_clean, 1)

    x_line = np.linspace(float(x_clean.min()), float(x_clean.max()), 100)
    y_line = slope * x_line + intercept

    fig.add_trace(go.Scatter(
        x=x_line.tolist(), y=y_line.tolist(), mode="lines",
        line=dict(color="#FFD166", dash="dash", width=2),
        name="OLS回帰線", showlegend=False,
    ))

    # Effect size label (Cohen's convention)
    abs_r = abs(r)
    effect = "大" if abs_r >= 0.5 else "中" if abs_r >= 0.3 else "小"
    n = len(x_clean)
    r2 = r ** 2

    if n > 1000:
        p_text = "大標本: p値は常に有意"
    else:
        p_text = "p<0.001" if p < 0.001 else f"p={p:.3f}"
    fig.add_annotation(
        x=0.02, y=0.98, xref="paper", yref="paper",
        text=f"r={r:.3f} (効果量:{effect}), R²={r2:.3f}, {p_text}, n={n:,}",
        showarrow=False, font=dict(size=11, color="#FFD166"),
        bgcolor="rgba(0,0,0,0.5)", bordercolor="#FFD166", borderwidth=1,
        borderpad=4,
    )

    return fig


def add_ci_band(fig: go.Figure, x, y_mean, y_lower, y_upper, color: str = "#667eea") -> go.Figure:
    """時系列チャートに信頼区間（半透明帯）を追加."""
    fig.add_trace(go.Scatter(
        x=list(x), y=list(y_upper), mode="lines",
        line=dict(width=0), showlegend=False, hoverinfo="skip",
    ))
    fig.add_trace(go.Scatter(
        x=list(x), y=list(y_lower), mode="lines",
        line=dict(width=0), fill="tonexty",
        fillcolor=f"rgba({int(color[1:3],16)},{int(color[3:5],16)},{int(color[5:7],16)},0.15)",
        showlegend=False, hoverinfo="skip",
    ))
    return fig


def adaptive_height(n_items: int, base: int = 400, per_item: int = 25, max_h: int = 900) -> int:
    """表示項目数に応じたチャート高さ自動計算."""
    return min(base + n_items * per_item, max_h)


def subsample_for_scatter(data: list[dict], max_n: int = 5000, seed: int = 42) -> list[dict]:
    """大規模データ用に層化サブサンプリング."""
    if len(data) <= max_n:
        return data
    rng = random.Random(seed)
    return rng.sample(data, max_n)


# ============================================================
# Advanced visualization helpers
# ============================================================

_PALETTE = [
    "#f093fb", "#667eea", "#06D6A0", "#EF476F", "#FFD166",
    "#a0d2db", "#fda085", "#9B59B6", "#2ECC71", "#E74C3C",
]


def density_scatter_2d(
    x: list[float], y: list[float], *,
    xlabel: str = "", ylabel: str = "", title: str = "",
    label_names: list[str] | None = None,
    label_top: int = 15,
    height: int = 550,
) -> go.Figure:
    """大規模データ用 2D密度等高線 + 上位ポイントラベル.

    N < 500 なら通常の scatter にフォールバック。
    """
    x_arr = np.asarray(x, dtype=float)
    y_arr = np.asarray(y, dtype=float)
    mask = np.isfinite(x_arr) & np.isfinite(y_arr)
    x_c, y_c = x_arr[mask], y_arr[mask]

    fig = go.Figure()

    if len(x_c) < 500:
        fig.add_trace(go.Scattergl(
            x=x_c.tolist(), y=y_c.tolist(), mode="markers",
            marker=dict(size=4, color="#667eea", opacity=0.6),
            showlegend=False,
        ))
    else:
        # 密度等高線
        fig.add_trace(go.Histogram2dContour(
            x=x_c.tolist(), y=y_c.tolist(),
            colorscale="Viridis", ncontours=20, showscale=True,
            contours=dict(coloring="heatmap"),
            line=dict(width=0.5, color="rgba(255,255,255,0.3)"),
            colorbar=dict(title="密度", len=0.6),
        ))
        # 上位ポイントをラベル付き scatter で重ねる
        if label_names and label_top > 0:
            # y 値上位 N を抽出
            top_idx = np.argsort(-y_c)[:label_top]
            fig.add_trace(go.Scatter(
                x=x_c[top_idx].tolist(), y=y_c[top_idx].tolist(),
                mode="markers+text",
                text=[label_names[int(i)] for i in top_idx] if label_names else None,
                textposition="top center",
                textfont=dict(size=9, color="#f093fb"),
                marker=dict(size=7, color="#f093fb", line=dict(width=1, color="white")),
                showlegend=False,
            ))

    fig.update_layout(
        title=title, xaxis_title=xlabel, yaxis_title=ylabel,
        height=height,
    )
    return fig


def ridge_plot(
    groups: dict[str, list[float]], *, title: str = "", xlabel: str = "",
    colors: list[str] | None = None, height: int = 500,
) -> go.Figure:
    """複数グループの分布を重ね KDE (ridge plot 風) で比較.

    Plotly の Violin (side="positive") を垂直にオフセットして並べる。
    """
    colors = colors or _PALETTE
    fig = go.Figure()

    group_names = list(groups.keys())
    for i, (name, vals) in enumerate(groups.items()):
        arr = np.array([v for v in vals if v is not None and np.isfinite(v)])
        if len(arr) < 2:
            continue
        fig.add_trace(go.Violin(
            x=arr.tolist(), y0=name, name=name,
            side="positive", meanline_visible=True,
            line_color=colors[i % len(colors)],
            fillcolor=colors[i % len(colors)],
            opacity=0.65, spanmode="soft",
            scalemode="width", width=0.8,
        ))

    fig.update_layout(
        title=title, xaxis_title=xlabel,
        showlegend=False, height=height,
        violingap=0.05, violinmode="overlay",
        yaxis=dict(categoryorder="array", categoryarray=list(reversed(group_names))),
    )
    return fig


def split_violin(
    values_a: list[float], values_b: list[float],
    categories_a: list[str], categories_b: list[str],
    *, label_a: str = "A", label_b: str = "B",
    title: str = "", ylabel: str = "",
    height: int = 500,
) -> go.Figure:
    """カテゴリ別の左右分割 violin plot.

    values_a/categories_a が左側、values_b/categories_b が右側。
    """
    fig = go.Figure()
    fig.add_trace(go.Violin(
        x=categories_a, y=values_a, name=label_a,
        side="negative", line_color="#667eea",
        fillcolor="rgba(102,126,234,0.3)",
        meanline_visible=True, scalemode="width",
    ))
    fig.add_trace(go.Violin(
        x=categories_b, y=values_b, name=label_b,
        side="positive", line_color="#EF476F",
        fillcolor="rgba(239,71,111,0.3)",
        meanline_visible=True, scalemode="width",
    ))
    fig.update_layout(
        title=title, yaxis_title=ylabel,
        violingap=0, violinmode="overlay",
        height=height, legend=dict(orientation="h", y=1.05),
    )
    return fig


def forest_plot(
    estimates: list[dict], *, title: str = "", xlabel: str = "効果量",
    height: int | None = None,
) -> go.Figure:
    """Forest plot (水平CI付きドットプロット).

    estimates: [{"name": str, "estimate": float, "ci_lower": float, "ci_upper": float}, ...]
    """
    names = [e["name"] for e in estimates]
    ests = [e["estimate"] for e in estimates]
    ci_lo = [e["estimate"] - e["ci_lower"] for e in estimates]
    ci_hi = [e["ci_upper"] - e["estimate"] for e in estimates]

    fig = go.Figure()
    fig.add_trace(go.Scatter(
        x=ests, y=names, mode="markers",
        marker=dict(size=10, color="#667eea"),
        error_x=dict(type="data", symmetric=False, array=ci_hi, arrayminus=ci_lo,
                     color="rgba(102,126,234,0.5)", thickness=2, width=6),
        showlegend=False,
    ))
    fig.add_vline(x=0, line_dash="dash", line_color="rgba(255,255,255,0.4)")
    fig.update_layout(
        title=title, xaxis_title=xlabel,
        height=height or max(300, len(estimates) * 40 + 100),
        yaxis=dict(categoryorder="array", categoryarray=list(reversed(names))),
    )
    return fig


def data_driven_badges(values: list[float]) -> tuple[float, float]:
    """P25/P75ベースの badge 閾値を返す. (low_thresh, high_thresh)."""
    arr = np.array([v for v in values if v is not None and np.isfinite(v)])
    if len(arr) == 0:
        return (0.0, 0.0)
    return (float(np.percentile(arr, 25)), float(np.percentile(arr, 75)))


def badge_class(value: float, low: float, high: float) -> str:
    """データ駆動閾値に基づく badge CSS class."""
    if value >= high:
        return "badge-high"
    if value >= low:
        return "badge-mid"
    return "badge-low"


def capped_categories(counter: dict[str, int], max_cats: int = 8) -> dict[str, int]:
    """上位 max_cats カテゴリを残し、残りを 'その他' にグルーピング."""
    if len(counter) <= max_cats:
        return dict(counter)
    sorted_items = sorted(counter.items(), key=lambda x: -x[1])
    result = dict(sorted_items[:max_cats])
    other_sum = sum(v for _, v in sorted_items[max_cats:])
    if other_sum > 0:
        result["その他"] = other_sum
    return result


# ============================================================
# Feature extraction for ML clustering
# ============================================================

FEATURE_NAMES = [
    "birank", "patronage", "person_fe", "iv_score", "total_credits",
    "degree", "betweenness", "eigenvector",
    "active_years", "highest_stage", "peak_credits",
    "collaborators", "unique_anime", "hub_score",
    "activity_ratio", "recent_credits",
    "versatility_score", "categories", "roles",
    "confidence",
]


def safe_nested(d: dict, *keys, default=0.0) -> float:
    """Safely extract nested dict value."""
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return float(default)
        cur = cur.get(k, default)
    return float(cur) if cur is not None else float(default)


def extract_features(scores: list[dict]):
    """Extract 20-dimensional feature vectors from scores.json.

    Returns (ids, names, features_array, primary_roles).
    """
    ids: list[str] = []
    names: list[str] = []
    roles: list[str] = []
    rows: list[list[float]] = []

    for p in scores:
        ids.append(p.get("person_id", ""))
        names.append(p.get("name", p.get("name_ja", "")))
        roles.append(p.get("primary_role", "unknown"))
        row = [
            float(p.get("birank", 0)),
            float(p.get("patronage", 0)),
            float(p.get("person_fe", 0)),
            float(p.get("iv_score", 0)),
            float(p.get("total_credits", 0)),
            safe_nested(p, "centrality", "degree"),
            safe_nested(p, "centrality", "betweenness"),
            safe_nested(p, "centrality", "eigenvector"),
            safe_nested(p, "career", "active_years"),
            safe_nested(p, "career", "highest_stage"),
            safe_nested(p, "career", "peak_credits"),
            safe_nested(p, "network", "collaborators"),
            safe_nested(p, "network", "unique_anime"),
            safe_nested(p, "network", "hub_score"),
            safe_nested(p, "growth", "activity_ratio"),
            safe_nested(p, "growth", "recent_credits"),
            safe_nested(p, "versatility", "score"),
            safe_nested(p, "versatility", "categories"),
            safe_nested(p, "versatility", "roles"),
            float(p.get("confidence", 0)),
        ]
        rows.append(row)

    return ids, names, np.array(rows, dtype=np.float64), roles


# ---------------------------------------------------------------------------
# meta_lineage registration
# ---------------------------------------------------------------------------

def insert_lineage(
    conn: sqlite3.Connection,
    *,
    table_name: str,
    audience: str,
    source_silver_tables: list[str],
    formula_version: str,
    description: str,
    ci_method: str | None = None,
    null_model: str | None = None,
    holdout_method: str | None = None,
    inputs_hash: str | None = None,
    notes: str | None = None,
    rng_seed: int | None = None,
) -> None:
    """Idempotently insert/replace a lineage row into meta_lineage.

    Gracefully skips if meta_lineage does not exist (fresh v2 schema uses
    ops_lineage instead; CI checks against whichever table the environment has).
    Also skips silently when the connection is a DuckDB connection (read-only
    gold layer) since lineage writes go through the pipeline, not reports.

    INSERT OR REPLACE ensures re-running a report overwrites rather than
    duplicates the row.
    """
    # DuckDB connections are not sqlite3.Connection — skip silently.
    # Lineage writes for reports executed against the read-only gold layer
    # are a no-op; lineage is registered by the pipeline phase instead.
    if not isinstance(conn, sqlite3.Connection):
        return

    # Check which lineage table is available
    available = {
        r[0]
        for r in conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name IN ('meta_lineage','ops_lineage')"
        ).fetchall()
    }
    if not available:
        return

    target_table = "meta_lineage" if "meta_lineage" in available else "ops_lineage"

    cols = {r[1] for r in conn.execute(f"PRAGMA table_info({target_table})").fetchall()}
    if not cols:
        return

    if inputs_hash is None:
        payload = json.dumps(
            {"table_name": table_name, "sources": sorted(source_silver_tables), "version": formula_version},
            ensure_ascii=False,
            sort_keys=True,
        )
        inputs_hash = hashlib.sha256(payload.encode()).hexdigest()[:16]

    values: dict = {
        "table_name": table_name,
        "audience": audience,
        "source_silver_tables": json.dumps(source_silver_tables, ensure_ascii=False),
        "source_bronze_forbidden": 1,
        "source_display_allowed": 0,
        "description": description,
        "formula_version": formula_version,
        "ci_method": ci_method,
        "null_model": null_model,
        "holdout_method": holdout_method,
        "notes": notes,
        "rng_seed": rng_seed,
        "inputs_hash": inputs_hash or "",
        "git_sha": "",
    }

    insert_cols = [c for c in values if c in cols]
    insert_cols_sql = ", ".join(insert_cols + ["computed_at"])
    placeholders = ", ".join(["?"] * len(insert_cols) + ["CURRENT_TIMESTAMP"])
    update_clause = ", ".join(
        f"{c} = excluded.{c}" for c in insert_cols if c != "table_name"
    )

    conn.execute(
        f"""INSERT INTO {target_table} ({insert_cols_sql})
            VALUES ({placeholders})
            ON CONFLICT(table_name) DO UPDATE SET
                {update_clause},
                computed_at = CURRENT_TIMESTAMP""",
        [values[c] for c in insert_cols],
    )
