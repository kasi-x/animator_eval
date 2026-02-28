"""Neo4j 互換 CSV エクスポート.

Neo4j の `neo4j-admin database import` で取り込める CSV を出力する。
ノードファイル: persons.csv, anime.csv
リレーションシップ: credits.csv, collaborations.csv
"""

import csv
from pathlib import Path

import structlog

from src.models import Anime, Credit, Person, ScoreResult

logger = structlog.get_logger()


def export_neo4j_csv(
    persons: list[Person],
    anime_list: list[Anime],
    credits: list[Credit],
    scores: list[ScoreResult] | None = None,
    output_dir: Path | None = None,
) -> Path:
    """Neo4j インポート用 CSV ファイルを出力する.

    Args:
        persons: 人物リスト
        anime_list: アニメリスト
        credits: クレジットリスト
        scores: スコアリスト（オプション）
        output_dir: 出力ディレクトリ

    Returns:
        出力ディレクトリのパス
    """
    if output_dir is None:
        from src.utils.config import JSON_DIR

        output_dir = JSON_DIR.parent / "neo4j"
    output_dir.mkdir(parents=True, exist_ok=True)

    score_map = {}
    if scores:
        score_map = {s.person_id: s for s in scores}

    # Persons node file
    persons_path = output_dir / "persons.csv"
    with open(persons_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "personId:ID(Person)",
                "name_ja",
                "name_en",
                "mal_id:int",
                "anilist_id:int",
                "person_fe:float",
                "birank:float",
                "patronage:float",
                "iv_score:float",
                ":LABEL",
            ]
        )
        for p in persons:
            s = score_map.get(p.id)
            writer.writerow(
                [
                    p.id,
                    p.name_ja,
                    p.name_en,
                    p.mal_id or "",
                    p.anilist_id or "",
                    round(s.person_fe, 2) if s else "",
                    round(s.birank, 2) if s else "",
                    round(s.patronage, 2) if s else "",
                    round(s.iv_score, 2) if s else "",
                    "Person",
                ]
            )

    # Anime node file
    anime_path = output_dir / "anime.csv"
    with open(anime_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "animeId:ID(Anime)",
                "title_ja",
                "title_en",
                "year:int",
                "season",
                "episodes:int",
                "mal_id:int",
                "anilist_id:int",
                "score:float",
                ":LABEL",
            ]
        )
        for a in anime_list:
            writer.writerow(
                [
                    a.id,
                    a.title_ja,
                    a.title_en,
                    a.year or "",
                    a.season or "",
                    a.episodes or "",
                    a.mal_id or "",
                    a.anilist_id or "",
                    a.score or "",
                    "Anime",
                ]
            )

    # Credits relationship file (Person → Anime)
    credits_path = output_dir / "credits.csv"
    with open(credits_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                ":START_ID(Person)",
                ":END_ID(Anime)",
                "role",
                "episode:int",
                "source",
                ":TYPE",
            ]
        )
        for c in credits:
            writer.writerow(
                [
                    c.person_id,
                    c.anime_id,
                    c.role.value,
                    c.episode if c.episode is not None else "",
                    c.source,
                    "CREDITED_IN",
                ]
            )

    # Collaboration edges (Person ↔ Person via shared anime)
    from collections import defaultdict

    anime_persons: dict[str, set[str]] = defaultdict(set)
    for c in credits:
        anime_persons[c.anime_id].add(c.person_id)

    collab_counts: dict[tuple[str, str], int] = defaultdict(int)
    for anime_id, pids in anime_persons.items():
        pids_list = sorted(pids)
        for i in range(len(pids_list)):
            for j in range(
                i + 1, min(i + 100, len(pids_list))
            ):  # Cap to avoid O(n²) explosion
                pair = (pids_list[i], pids_list[j])
                collab_counts[pair] += 1

    collabs_path = output_dir / "collaborations.csv"
    with open(collabs_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                ":START_ID(Person)",
                ":END_ID(Person)",
                "shared_works:int",
                ":TYPE",
            ]
        )
        for (pid1, pid2), count in sorted(collab_counts.items(), key=lambda x: -x[1]):
            if count >= 2:  # Only meaningful collaborations
                writer.writerow([pid1, pid2, count, "COLLABORATED_WITH"])

    logger.info(
        "neo4j_export_complete",
        output_dir=str(output_dir),
        persons=len(persons),
        anime=len(anime_list),
        credits=len(credits),
        collaborations=sum(1 for v in collab_counts.values() if v >= 2),
    )

    return output_dir
