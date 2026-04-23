"""Studio Clustering — K-Means clustering of studios by feature vector.

12-dimensional feature vector per studio:
[avg_fe, median_fe, gini, log(staff_count), log(anime_count), genre_entropy,
 tv_fraction, retention_3yr, net_talent_flow, studio_fe, avg_birank, eigenvector]

K=6 clusters with dynamic naming via centroid rank analysis.
"""

import math
from collections import defaultdict
from dataclasses import dataclass, field

import numpy as np
import structlog
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler

from src.analysis.production_analysis import StudioTalentDensity
from src.runtime.models import AnimeAnalysis as Anime, Credit

logger = structlog.get_logger()

# Feature names for the 12-dimensional vector
STUDIO_FEATURES = [
    "avg_fe",
    "median_fe",
    "gini",
    "log_staff_count",
    "log_anime_count",
    "genre_entropy",
    "tv_fraction",
    "retention_3yr",
    "net_talent_flow",
    "studio_fe",
    "avg_birank",
    "eigenvector",
]

# Cluster naming specs: (feature_index, [label_from_highest_to_lowest])
_CLUSTER_NAME_SPECS = [
    (0, ["elite", "strong", "mid-tier", "developing", "emerging", "struggling"]),
    (3, ["large", "large", "mid-size", "mid-size", "small", "boutique"]),
]


@dataclass
class StudioCluster:
    """Cluster assignment and metadata for a single studio.

    Attributes:
        studio: studio name
        cluster_id: assigned cluster
        cluster_name: human-readable cluster label
        feature_vector: raw 12-dimensional features
    """

    studio: str = ""
    cluster_id: int = 0
    cluster_name: str = ""
    feature_vector: list[float] = field(default_factory=list)


@dataclass
class StudioClusteringResult:
    """Studio clustering analysis result.

    Attributes:
        assignments: studio -> StudioCluster
        cluster_names: cluster_id -> name
        cluster_sizes: cluster_id -> count
        centroids: cluster_id -> centroid vector
    """

    assignments: dict[str, StudioCluster] = field(default_factory=dict)
    cluster_names: dict[int, str] = field(default_factory=dict)
    cluster_sizes: dict[int, int] = field(default_factory=dict)
    centroids: dict[int, list[float]] = field(default_factory=dict)


def _shannon_entropy(counts: list[int]) -> float:
    """Shannon entropy from counts."""
    total = sum(counts)
    if total == 0:
        return 0.0
    probs = [c / total for c in counts if c > 0]
    return -sum(p * math.log2(p) for p in probs)


def _name_clusters_by_rank(
    centers: np.ndarray,
    feat_specs: list[tuple[int, list[str]]],
) -> dict[int, str]:
    """Name clusters by ranking centroids on key features.

    Args:
        centers: (K, D) centroid matrix
        feat_specs: list of (feature_index, [label_highest, ..., label_lowest])

    Returns:
        cluster_id -> name string
    """
    k = centers.shape[0]
    names: dict[int, list[str]] = {i: [] for i in range(k)}

    for feat_idx, labels in feat_specs:
        # Rank clusters by this feature (descending)
        ranked = sorted(range(k), key=lambda c: centers[c, feat_idx], reverse=True)
        for rank, cid in enumerate(ranked):
            # Pick label based on rank (clamp to available labels)
            label_idx = min(rank, len(labels) - 1)
            names[cid].append(labels[label_idx])

    return {cid: " ".join(parts) for cid, parts in names.items()}


def compute_studio_clustering(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    talent_density: dict[str, StudioTalentDensity],
    studio_fe: dict[str, float] | None = None,
    birank_scores: dict[str, float] | None = None,
    eigenvector_centrality: dict[str, float] | None = None,
    talent_flow: dict[str, float] | None = None,
    retention_rates: dict[str, float] | None = None,
    n_clusters: int = 6,
) -> StudioClusteringResult:
    """Cluster studios using K-Means on 12-dimensional feature vector.

    Args:
        credits: all production credits
        anime_map: anime_id -> Anime
        talent_density: studio -> StudioTalentDensity (from production_analysis)
        studio_fe: studio -> studio fixed effect (from AKM)
        birank_scores: person_id -> BiRank (for studio average)
        eigenvector_centrality: studio -> eigenvector (from studio_network)
        talent_flow: studio -> net talent flow (from talent_pipeline)
        retention_rates: studio -> 3-year retention (from talent_pipeline)
        n_clusters: number of clusters (default 6)

    Returns:
        StudioClusteringResult
    """
    studio_fe = studio_fe or {}
    birank_scores = birank_scores or {}
    eigenvector_centrality = eigenvector_centrality or {}
    talent_flow = talent_flow or {}
    retention_rates = retention_rates or {}

    # Build studio-level aggregates
    studio_staff: dict[str, set[str]] = defaultdict(set)
    studio_anime: dict[str, set[str]] = defaultdict(set)
    studio_genre_counts: dict[str, dict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )
    studio_format_counts: dict[str, dict[str, int]] = defaultdict(
        lambda: defaultdict(int)
    )

    for c in credits:
        anime = anime_map.get(c.anime_id)
        if not anime or not anime.studios:
            continue
        for studio in anime.studios:
            studio_staff[studio].add(c.person_id)
            studio_anime[studio].add(c.anime_id)
            if anime.genres:
                for genre in anime.genres:
                    studio_genre_counts[studio][genre] += 1
            if anime.format:
                studio_format_counts[studio][anime.format] += 1

    # Compute person average BiRank per studio
    studio_avg_birank: dict[str, float] = {}
    for studio, pids in studio_staff.items():
        biranks = [birank_scores.get(pid, 0.0) for pid in pids]
        studio_avg_birank[studio] = float(np.mean(biranks)) if biranks else 0.0

    # Build feature vectors for studios with talent density
    studios = sorted(s for s in talent_density if len(studio_staff.get(s, set())) >= 5)

    if len(studios) < n_clusters:
        logger.info(
            "studio_clustering_skipped", studios=len(studios), min_needed=n_clusters
        )
        return StudioClusteringResult()

    features: list[list[float]] = []
    for studio in studios:
        td = talent_density[studio]
        staff_count = len(studio_staff.get(studio, set()))
        anime_count = len(studio_anime.get(studio, set()))
        genre_ent = _shannon_entropy(list(studio_genre_counts.get(studio, {}).values()))
        formats = studio_format_counts.get(studio, {})
        total_formats = sum(formats.values())
        tv_frac = formats.get("TV", 0) / total_formats if total_formats > 0 else 0.0

        vec = [
            td.mean_fe,
            td.median_fe,
            td.gini_coefficient,
            math.log1p(staff_count),
            math.log1p(anime_count),
            genre_ent,
            tv_frac,
            retention_rates.get(studio, 0.0),
            talent_flow.get(studio, 0.0),
            studio_fe.get(studio, 0.0),
            studio_avg_birank.get(studio, 0.0),
            eigenvector_centrality.get(studio, 0.0),
        ]
        features.append(vec)

    X = np.array(features, dtype=np.float64)

    # Replace NaN/inf with 0
    X = np.nan_to_num(X, nan=0.0, posinf=0.0, neginf=0.0)

    # Standardize
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # K-Means
    actual_k = min(n_clusters, len(studios))
    kmeans = KMeans(n_clusters=actual_k, random_state=42, n_init=10)
    labels = kmeans.fit_predict(X_scaled)

    # Name clusters using centroid ranks (in original feature space)
    centers_original = scaler.inverse_transform(kmeans.cluster_centers_)
    cluster_names = _name_clusters_by_rank(centers_original, _CLUSTER_NAME_SPECS)

    # Build result
    assignments: dict[str, StudioCluster] = {}
    cluster_sizes: dict[int, int] = defaultdict(int)
    for i, studio in enumerate(studios):
        cid = int(labels[i])
        cluster_sizes[cid] += 1
        assignments[studio] = StudioCluster(
            studio=studio,
            cluster_id=cid,
            cluster_name=cluster_names.get(cid, f"cluster_{cid}"),
            feature_vector=features[i],
        )

    centroids = {cid: centers_original[cid].tolist() for cid in range(actual_k)}

    logger.info(
        "studio_clustering_computed",
        studios=len(studios),
        clusters=actual_k,
        cluster_sizes=dict(cluster_sizes),
    )

    return StudioClusteringResult(
        assignments=assignments,
        cluster_names=cluster_names,
        cluster_sizes=dict(cluster_sizes),
        centroids=centroids,
    )
