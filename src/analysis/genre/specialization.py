"""Genre Specialization Analysis — ジャンル特化度分析.

クリエイターがどのジャンルに特化しているかを定量的に分析。
ジャンルごとのスコア、多様性指標、ニッチ度などを計算し、
「アクション専門」「日常系の名手」などの特性を数値化。
"""

from collections import defaultdict
from dataclasses import dataclass, field
import math

import structlog

from src.models import AnimeAnalysis as Anime, Credit

logger = structlog.get_logger()


@dataclass
class GenreProfile:
    """Genre profile.

    Attributes:
        person_id: person_id
        genre_distribution: ジャンル → 参加作品数
        primary_genre: 最も多いジャンル
        genre_diversity: ジャンル多様性（Shannon entropy）
        specialization_score: 特化度（0-100、高いほど特化）
        niche_genres: ニッチジャンル（平均より少ない作品のジャンル）
        genre_scores: ジャンルごとの平均スコア
    """

    person_id: str
    genre_distribution: dict[str, int] = field(default_factory=dict)
    primary_genre: str | None = None
    genre_diversity: float = 0.0
    specialization_score: float = 0.0
    niche_genres: list[str] = field(default_factory=list)
    genre_scores: dict[str, float] = field(default_factory=dict)


def normalize_genre(genre: str | None) -> str | None:
    """Normalise a genre name.

    Args:
        genre: ジャンル文字列

    Returns:
        正規化されたジャンル、Noneの場合はNone
    """
    if not genre:
        return None

    # lowercase and strip whitespace
    normalized = genre.lower().strip()

    # category mapping (extend as needed)
    genre_map = {
        "action": "action",
        "adventure": "adventure",
        "comedy": "comedy",
        "drama": "drama",
        "fantasy": "fantasy",
        "horror": "horror",
        "mystery": "mystery",
        "romance": "romance",
        "sci-fi": "sci-fi",
        "science fiction": "sci-fi",
        "slice of life": "slice_of_life",
        "sports": "sports",
        "supernatural": "supernatural",
        "thriller": "thriller",
        "mecha": "mecha",
        "music": "music",
        "school": "school",
        "military": "military",
        "psychological": "psychological",
        "seinen": "seinen",
        "shounen": "shounen",
        "shoujo": "shoujo",
        "josei": "josei",
    }

    return genre_map.get(normalized, normalized)


def compute_genre_profiles(
    credits: list[Credit],
    anime_map: dict[str, Anime],
    person_scores: dict[str, dict] | None = None,
) -> dict[str, GenreProfile]:
    """Compute genre profiles.

    Args:
        credits: 全クレジット
        anime_map: anime_id → Anime
        person_scores: person_id → スコア辞書

    Returns:
        person_id → GenreProfile
    """
    # person_id → genre → [credits] のマッピング
    person_genre_credits: dict[str, dict[str, list[Credit]]] = defaultdict(
        lambda: defaultdict(list)
    )

    # work count per genre (industry-wide)
    global_genre_counts: dict[str, int] = defaultdict(int)
    genre_anime_map: dict[str, list[str]] = defaultdict(list)

    for credit in credits:
        anime = anime_map.get(credit.anime_id)
        if not anime:
            continue

        # get anime genres (assumption: anime.genres is list[str])
        # adjust to match actual data structure
        genres = []
        if hasattr(anime, "genres") and anime.genres:
            genres = anime.genres
        elif hasattr(anime, "tags") and anime.tags:
            # use tags as genres
            genres = anime.tags

        if not genres:
            # use "unknown" when no genre info
            genres = ["unknown"]

        for genre_raw in genres:
            genre = normalize_genre(genre_raw)
            if genre:
                person_genre_credits[credit.person_id][genre].append(credit)
                if anime.id not in genre_anime_map[genre]:
                    genre_anime_map[genre].append(anime.id)
                    global_genre_counts[genre] += 1

    # compute average work count
    avg_anime_per_genre = (
        sum(global_genre_counts.values()) / len(global_genre_counts)
        if global_genre_counts
        else 0
    )

    # build profiles
    profiles: dict[str, GenreProfile] = {}

    for person_id, genre_credits_map in person_genre_credits.items():
        # genre distribution (work counts)
        genre_distribution = {
            genre: len({c.anime_id for c in credits_list})
            for genre, credits_list in genre_credits_map.items()
        }

        total_anime = sum(genre_distribution.values())
        if total_anime == 0:
            continue

        # primary genres
        primary_genre = max(genre_distribution.items(), key=lambda x: x[1])[0]

        # Shannon entropy（多様性）
        entropy = 0.0
        for count in genre_distribution.values():
            p = count / total_anime
            if p > 0:
                entropy -= p * math.log2(p)

        # maximum entropy (uniform distribution)
        max_entropy = (
            math.log2(len(genre_distribution)) if len(genre_distribution) > 1 else 1
        )

        # normalised diversity (0-1)
        genre_diversity = entropy / max_entropy if max_entropy > 0 else 0

        # specialisation score (inverse of diversity, 0-100)
        specialization_score = (1 - genre_diversity) * 100

        # niche genre detection (below industry average)
        niche_genres = [
            genre
            for genre, count in genre_distribution.items()
            if global_genre_counts[genre] < avg_anime_per_genre
        ]

        # average score per genre (assumption: score is constant)
        genre_scores = {}
        if person_scores and person_id in person_scores:
            base_score = person_scores[person_id].get("iv_score", 0)
            # ideally compute a distinct score per genre, but
            # simplified here: same score for all genres
            for genre in genre_distribution:
                genre_scores[genre] = base_score

        profiles[person_id] = GenreProfile(
            person_id=person_id,
            genre_distribution=genre_distribution,
            primary_genre=primary_genre,
            genre_diversity=round(genre_diversity, 3),
            specialization_score=round(specialization_score, 1),
            niche_genres=niche_genres,
            genre_scores=genre_scores,
        )

    logger.info("genre_profiles_computed", persons=len(profiles))
    return profiles


def find_genre_specialists(
    profiles: dict[str, GenreProfile],
    genre: str,
    min_works: int = 3,
    top_n: int = 10,
) -> list[tuple[str, int, float]]:
    """Find specialists in a specific genre.

    Args:
        profiles: ジャンルプロファイル
        genre: ターゲットジャンル
        min_works: 最小作品数
        top_n: 上位何人を返すか

    Returns:
        [(person_id, works_count, specialization_score), ...] のリスト
    """
    specialists = []

    for person_id, profile in profiles.items():
        works_in_genre = profile.genre_distribution.get(genre, 0)
        if works_in_genre >= min_works and profile.primary_genre == genre:
            specialists.append(
                (person_id, works_in_genre, profile.specialization_score)
            )

    # sort by work count × specialisation
    specialists.sort(key=lambda x: x[1] * x[2], reverse=True)

    logger.info(
        "genre_specialists_found",
        genre=genre,
        total=len(specialists),
        top_n=min(top_n, len(specialists)),
    )

    return specialists[:top_n]


def analyze_genre_trends(
    profiles: dict[str, GenreProfile],
) -> dict[str, dict]:
    """Genre-level trend analysis.

    Args:
        profiles: ジャンルプロファイル

    Returns:
        genre → 統計情報
    """
    genre_stats: dict[str, dict] = defaultdict(
        lambda: {
            "total_creators": 0,
            "specialists": 0,  # specialization_score > 70
            "avg_specialization": 0.0,
            "total_works": 0,
        }
    )

    for profile in profiles.values():
        for genre, count in profile.genre_distribution.items():
            stats = genre_stats[genre]
            stats["total_creators"] += 1
            stats["total_works"] += count

            if profile.primary_genre == genre and profile.specialization_score > 70:
                stats["specialists"] += 1

    # compute average specialisation
    for genre, stats in genre_stats.items():
        genre_profiles = [p for p in profiles.values() if genre in p.genre_distribution]
        if genre_profiles:
            avg_spec = sum(
                p.specialization_score
                for p in genre_profiles
                if p.primary_genre == genre
            ) / len([p for p in genre_profiles if p.primary_genre == genre])
            stats["avg_specialization"] = round(avg_spec, 1) if avg_spec else 0.0

    result = dict(genre_stats)
    logger.info("genre_trends_analyzed", genres=len(result))
    return result


def compute_genre_similarity(
    profile1: GenreProfile,
    profile2: GenreProfile,
) -> float:
    """Compute genre similarity between two creators (cosine similarity).

    Args:
        profile1: クリエイター1のプロファイル
        profile2: クリエイター2のプロファイル

    Returns:
        類似度（0-1、1に近いほど似ている）
    """
    # union of all genres
    all_genres = set(profile1.genre_distribution.keys()) | set(
        profile2.genre_distribution.keys()
    )

    if not all_genres:
        return 0.0

    # create vectors
    vec1 = [profile1.genre_distribution.get(g, 0) for g in all_genres]
    vec2 = [profile2.genre_distribution.get(g, 0) for g in all_genres]

    # cosine similarity
    dot_product = sum(v1 * v2 for v1, v2 in zip(vec1, vec2))
    mag1 = math.sqrt(sum(v * v for v in vec1))
    mag2 = math.sqrt(sum(v * v for v in vec2))

    if mag1 == 0 or mag2 == 0:
        return 0.0

    similarity = dot_product / (mag1 * mag2)
    return round(similarity, 3)


def find_similar_creators_by_genre(
    profiles: dict[str, GenreProfile],
    target_person_id: str,
    top_n: int = 5,
) -> list[tuple[str, float]]:
    """Find creators with similar genre preferences.

    Args:
        profiles: ジャンルプロファイル
        target_person_id: ターゲットのperson_id
        top_n: 上位何人を返すか

    Returns:
        [(person_id, similarity), ...] のリスト
    """
    if target_person_id not in profiles:
        logger.warning("target_person_not_found", person_id=target_person_id)
        return []

    target_profile = profiles[target_person_id]
    similarities = []

    for person_id, profile in profiles.items():
        if person_id == target_person_id:
            continue

        similarity = compute_genre_similarity(target_profile, profile)
        if similarity > 0:
            similarities.append((person_id, similarity))

    # sort by similarity
    similarities.sort(key=lambda x: x[1], reverse=True)

    logger.info(
        "similar_creators_found",
        target=target_person_id,
        matches=len(similarities),
        top_n=min(top_n, len(similarities)),
    )

    return similarities[:top_n]


def main():
    """Standalone entry point."""
    from src.database import (
        load_all_anime,
        load_all_credits,
        load_all_persons,
        load_all_scores,
        get_connection,
        init_db,
    )

    conn = get_connection()
    init_db(conn)

    persons = load_all_persons(conn)
    anime_list = load_all_anime(conn)
    credits = load_all_credits(conn)
    scores_list = load_all_scores(conn)

    # build lookup maps
    anime_map = {a.id: a for a in anime_list}
    person_names = {p.id: p.name_ja or p.name_en or p.id for p in persons}
    scores_map = {s.person_id: {"iv_score": s.iv_score} for s in scores_list}

    # compute genre profiles
    profiles = compute_genre_profiles(credits, anime_map, scores_map)

    # genre trends
    genre_trends = analyze_genre_trends(profiles)

    print("\nジャンルトレンド（上位10）:")
    for genre, stats in sorted(
        genre_trends.items(), key=lambda x: x[1]["total_works"], reverse=True
    )[:10]:
        print(f"\n{genre}:")
        print(f"  クリエイター数: {stats['total_creators']}")
        print(f"  スペシャリスト数: {stats['specialists']}")
        print(f"  総作品数: {stats['total_works']}")
        print(f"  平均特化度: {stats['avg_specialization']}")

    # specialisation distribution
    specializations = [p.specialization_score for p in profiles.values()]
    if specializations:
        avg_spec = sum(specializations) / len(specializations)
        high_spec = len([s for s in specializations if s > 70])
        low_spec = len([s for s in specializations if s < 30])

        print("\n特化度分布:")
        print(f"  平均: {avg_spec:.1f}")
        print(
            f"  高特化 (>70): {high_spec} ({100 * high_spec / len(specializations):.1f}%)"
        )
        print(
            f"  低特化 (<30): {low_spec} ({100 * low_spec / len(specializations):.1f}%)"
        )

    # genre specialist examples
    if genre_trends:
        top_genre = max(genre_trends.items(), key=lambda x: x[1]["total_works"])[0]
        specialists = find_genre_specialists(profiles, top_genre, min_works=2, top_n=5)

        print(f"\n{top_genre} スペシャリスト（上位5）:")
        for person_id, works, spec_score in specialists:
            name = person_names.get(person_id, person_id)
            print(f"  - {name}: {works}作品, 特化度{spec_score:.1f}")

    conn.close()


if __name__ == "__main__":
    main()
