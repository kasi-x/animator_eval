"""ネットワーク進化レポート用データプロバイダ."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from src.utils.json_io import load_json_file_or_return_default


@dataclass(frozen=True)
class NetworkEvolutionData:
    """network_evolution.json から抽出した構造化データ."""

    years: tuple[int, ...]
    active_persons: tuple[int, ...]
    new_persons: tuple[int, ...]
    cumulative_persons: tuple[int, ...]
    cumulative_edges: tuple[int, ...]
    density: tuple[float, ...]
    # 前年差分 (len = len(years) - 1)
    density_changes: tuple[float, ...]
    node_growth_pct: tuple[float, ...]
    edge_growth_pct: tuple[float, ...]
    # 四半期データ
    quarterly_labels: tuple[str, ...] = ()
    quarterly_active: tuple[int, ...] = ()
    quarterly_new: tuple[int, ...] = ()
    quarterly_credits: tuple[int, ...] = ()
    # 年別エッジ数（累積ではなく当年のみ）
    year_edges: tuple[int, ...] = ()
    new_edges: tuple[int, ...] = ()
    # トレンド統計
    total_person_growth: int = 0
    total_edge_growth: int = 0
    # 集計統計
    avg_new_persons_per_year: float = 0.0
    avg_new_edges_per_year: float = 0.0


def load_network_evolution_data(json_dir: Path) -> NetworkEvolutionData | None:
    """network_evolution.json を読み込み NetworkEvolutionData を返す."""
    raw = load_json_file_or_return_default(json_dir / "network_evolution.json", {})
    if not raw or not isinstance(raw, dict) or "years" not in raw:
        return None

    years_raw = raw["years"]
    snapshots = raw.get("snapshots", {})
    if not years_raw:
        return None

    sorted_years = sorted(years_raw)

    active = []
    new = []
    cumul_p = []
    cumul_e = []
    dens = []

    for y in sorted_years:
        snap = snapshots.get(str(y), {})
        active.append(snap.get("active_persons", 0))
        new.append(snap.get("new_persons", 0))
        cumul_p.append(snap.get("cumulative_persons", 0))
        cumul_e.append(snap.get("cumulative_edges", 0))
        dens.append(snap.get("density", 0.0))

    # Year-level edge counts (non-cumulative)
    year_edges = tuple(
        snapshots.get(str(y), {}).get("year_edges", 0) for y in sorted_years
    )
    new_edges = tuple(
        snapshots.get(str(y), {}).get("new_edges", 0) for y in sorted_years
    )

    # Quarterly snapshots
    quarterly_raw = raw.get("quarterly_snapshots", {})
    quarterly_labels_raw = raw.get("quarterly_labels", [])
    if quarterly_labels_raw and quarterly_raw:
        quarterly_labels = tuple(quarterly_labels_raw)
        quarterly_active = tuple(
            quarterly_raw.get(q, {}).get("active_persons", 0)
            for q in quarterly_labels_raw
        )
        quarterly_new = tuple(
            quarterly_raw.get(q, {}).get("new_persons", 0)
            for q in quarterly_labels_raw
        )
        quarterly_credits = tuple(
            quarterly_raw.get(q, {}).get("credit_count", 0)
            for q in quarterly_labels_raw
        )
    else:
        quarterly_labels = ()
        quarterly_active = ()
        quarterly_new = ()
        quarterly_credits = ()

    # Derived: density changes
    density_changes = [
        abs(dens[i] - dens[i - 1]) for i in range(1, len(dens))
    ]

    # Derived: growth rates (%)
    node_growth = []
    edge_growth = []
    for i in range(1, len(cumul_p)):
        ng = ((cumul_p[i] - cumul_p[i - 1]) / max(cumul_p[i - 1], 1)) * 100
        eg = ((cumul_e[i] - cumul_e[i - 1]) / max(cumul_e[i - 1], 1)) * 100
        node_growth.append(ng)
        edge_growth.append(eg)

    trends = raw.get("trends", {})

    return NetworkEvolutionData(
        years=tuple(sorted_years),
        active_persons=tuple(active),
        new_persons=tuple(new),
        cumulative_persons=tuple(cumul_p),
        cumulative_edges=tuple(cumul_e),
        density=tuple(dens),
        density_changes=tuple(density_changes),
        node_growth_pct=tuple(node_growth),
        edge_growth_pct=tuple(edge_growth),
        quarterly_labels=quarterly_labels,
        quarterly_active=quarterly_active,
        quarterly_new=quarterly_new,
        quarterly_credits=quarterly_credits,
        year_edges=year_edges,
        new_edges=new_edges,
        total_person_growth=int(trends.get("person_growth", 0)),
        total_edge_growth=int(trends.get("edge_growth", 0)),
        avg_new_persons_per_year=float(trends.get("avg_new_persons_per_year", 0.0)),
        avg_new_edges_per_year=float(trends.get("avg_new_edges_per_year", 0.0)),
    )
