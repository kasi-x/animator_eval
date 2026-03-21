"""合成データ生成 — APIなしでパイプラインをテストするためのダミーデータ.

実際のアニメ業界の構造を模倣した合成データを生成する:
- 少数の監督が多くの作品を手がける（べき乗則）
- アニメーターは複数の監督と仕事する
- 一部のアニメーターは特定の監督と強い結びつきを持つ
"""

import random

import structlog

from src.database import (
    get_connection,
    init_db,
    insert_credit,
    upsert_anime,
    upsert_person,
)
from src.models import Anime, Character, CharacterVoiceActor, Credit, Person, Role

logger = structlog.get_logger()

# 役職の分布（実際のアニメクレジットを模倣）
ROLE_DISTRIBUTION = [
    (Role.DIRECTOR, 1),
    (Role.EPISODE_DIRECTOR, 2),
    (Role.ANIMATION_DIRECTOR, 1),
    (Role.ANIMATION_DIRECTOR, 3),
    (Role.CHARACTER_DESIGNER, 1),
    (Role.KEY_ANIMATOR, 8),
    (Role.KEY_ANIMATOR, 5),
    (Role.IN_BETWEEN, 10),
    (Role.EPISODE_DIRECTOR, 2),
    (Role.BACKGROUND_ART, 1),
]


def generate_synthetic_data(
    n_directors: int = 10,
    n_animators: int = 100,
    n_anime: int = 50,
    seed: int = 42,
) -> tuple[list[Person], list[Anime], list[Credit]]:
    """合成データを生成する.

    Args:
        n_directors: 監督の数
        n_animators: アニメーターの数
        n_anime: アニメ作品の数
        seed: 乱数シード

    Returns:
        (persons, anime_list, credits)
    """
    rng = random.Random(seed)

    # 監督を生成
    directors = []
    for i in range(n_directors):
        directors.append(
            Person(
                id=f"syn:d{i}",
                name_ja=f"監督{i:03d}",
                name_en=f"Director {i:03d}",
            )
        )

    # アニメーターを生成
    animators = []
    for i in range(n_animators):
        animators.append(
            Person(
                id=f"syn:a{i}",
                name_ja=f"アニメーター{i:03d}",
                name_en=f"Animator {i:03d}",
            )
        )

    persons = directors + animators

    # スタジオを生成（5スタジオ）
    studio_names = [
        "Studio Alpha",
        "Studio Beta",
        "Studio Gamma",
        "Studio Delta",
        "Studio Epsilon",
    ]

    # アニメ作品を生成
    anime_list = []
    for i in range(n_anime):
        year = rng.randint(2000, 2025)
        score = round(rng.uniform(5.0, 9.5), 1)
        # Assign 1-2 studios per anime
        n_studios = rng.choices([1, 2], weights=[0.7, 0.3])[0]
        anime_studios = rng.sample(studio_names, min(n_studios, len(studio_names)))
        anime_list.append(
            Anime(
                id=f"syn:anime{i}",
                title_ja=f"合成アニメ{i:03d}",
                title_en=f"Synthetic Anime {i:03d}",
                year=year,
                season=rng.choice(["winter", "spring", "summer", "fall"]),
                episodes=rng.choice([12, 13, 24, 25, 26]),
                score=score,
                studios=anime_studios,
            )
        )

    # クレジットを生成
    credits = []

    # 監督の割り当て（べき乗則: 少数の監督が多くの作品を担当）
    director_weights = [1.0 / (i + 1) ** 0.8 for i in range(n_directors)]
    total_w = sum(director_weights)
    director_probs = [w / total_w for w in director_weights]

    for anime in anime_list:
        # 監督を選択（1-2人）
        n_dirs = rng.choices([1, 2], weights=[0.7, 0.3])[0]
        chosen_dirs = rng.choices(directors, weights=director_probs, k=n_dirs)
        chosen_dirs = list({d.id: d for d in chosen_dirs}.values())  # deduplicate

        for d in chosen_dirs:
            credits.append(
                Credit(
                    person_id=d.id,
                    anime_id=anime.id,
                    role=Role.DIRECTOR,
                    source="synthetic",
                )
            )

        # アニメーターを割り当て
        # 各役職ごとに人数を決定
        for role, base_count in ROLE_DISTRIBUTION:
            if role == Role.DIRECTOR:
                continue  # 監督は上で処理済み

            n_staff = max(1, rng.randint(base_count - 1, base_count + 2))

            # 一部のアニメーターは特定の監督と強い結びつき
            preferred_animators = []
            for d in chosen_dirs:
                # 各監督には「お気に入り」のアニメーターがいる
                dir_idx = int(d.id.split("d")[1])
                for j in range(min(5, n_animators)):
                    preferred_idx = (dir_idx * 7 + j * 3) % n_animators
                    preferred_animators.append(animators[preferred_idx])

            chosen_staff = []
            for _ in range(n_staff):
                if preferred_animators and rng.random() < 0.4:
                    # 40%の確率で「お気に入り」アニメーターを選択
                    chosen = rng.choice(preferred_animators)
                else:
                    chosen = rng.choice(animators)
                chosen_staff.append(chosen)

            # 重複を除去
            seen = set()
            for s in chosen_staff:
                if s.id not in seen:
                    seen.add(s.id)
                    credits.append(
                        Credit(
                            person_id=s.id,
                            anime_id=anime.id,
                            role=role,
                            source="synthetic",
                        )
                    )

    logger.info(
        "Generated synthetic data: %d persons, %d anime, %d credits",
        len(persons),
        len(anime_list),
        len(credits),
    )
    return persons, anime_list, credits


def generate_synthetic_va_data(
    anime_list: list[Anime],
    n_voice_actors: int = 30,
    n_characters: int = 60,
    n_sound_directors: int = 5,
    seed: int = 42,
) -> tuple[list[Person], list[Character], list[CharacterVoiceActor], list[Credit]]:
    """Generate synthetic voice actor, character, and sound director data.

    Args:
        anime_list: existing anime list to assign VAs to
        n_voice_actors: number of VAs to generate
        n_characters: number of characters to generate
        n_sound_directors: number of sound directors
        seed: random seed

    Returns:
        (va_persons, characters, va_credits, sd_credits)
    """
    rng = random.Random(seed + 100)  # Different seed offset from main data

    # Generate voice actors
    va_persons = []
    for i in range(n_voice_actors):
        gender = rng.choice(["Male", "Female"])
        va_persons.append(
            Person(
                id=f"syn:va{i}",
                name_ja=f"声優{i:03d}",
                name_en=f"VA {i:03d}",
                gender=gender,
            )
        )

    # Generate characters
    characters = []
    for i in range(n_characters):
        gender = rng.choice(["Male", "Female", None])
        characters.append(
            Character(
                id=f"syn:c{i}",
                name_ja=f"キャラ{i:03d}",
                name_en=f"Character {i:03d}",
                gender=gender,
            )
        )

    # Generate sound directors
    sd_persons = []
    for i in range(n_sound_directors):
        sd_persons.append(
            Person(
                id=f"syn:sd{i}",
                name_ja=f"音響監督{i:03d}",
                name_en=f"Sound Director {i:03d}",
            )
        )

    # Assign characters to anime (2-6 characters per anime)
    va_credits: list[CharacterVoiceActor] = []
    char_roles = ["MAIN", "SUPPORTING", "BACKGROUND"]
    char_role_weights = [0.2, 0.4, 0.4]

    for anime in anime_list:
        n_chars = rng.randint(2, min(6, n_characters))
        chosen_chars = rng.sample(characters, n_chars)

        for j, char in enumerate(chosen_chars):
            # First character is more likely MAIN
            if j == 0:
                role = rng.choices(char_roles, weights=[0.7, 0.2, 0.1])[0]
            else:
                role = rng.choices(char_roles, weights=char_role_weights)[0]

            # Assign VA (some VAs voice multiple characters, some characters recur)
            va = rng.choice(va_persons)
            va_credits.append(
                CharacterVoiceActor(
                    character_id=char.id,
                    person_id=va.id,
                    anime_id=anime.id,
                    character_role=role,
                    source="synthetic",
                )
            )

    # Generate sound director credits
    sd_credits: list[Credit] = []
    for anime in anime_list:
        sd = rng.choice(sd_persons)
        sd_credits.append(
            Credit(
                person_id=sd.id,
                anime_id=anime.id,
                role=Role.SOUND_DIRECTOR,
                source="synthetic",
            )
        )

    logger.info(
        "Generated synthetic VA data: %d VAs, %d chars, %d va_credits, %d SDs",
        len(va_persons),
        len(characters),
        len(va_credits),
        len(sd_persons),
    )
    return va_persons + sd_persons, characters, va_credits, sd_credits


def populate_db_with_synthetic(
    n_directors: int = 10,
    n_animators: int = 100,
    n_anime: int = 50,
    seed: int = 42,
) -> None:
    """合成データでDBを充填する."""
    persons, anime_list, credits = generate_synthetic_data(
        n_directors=n_directors,
        n_animators=n_animators,
        n_anime=n_anime,
        seed=seed,
    )

    conn = get_connection()
    init_db(conn)

    for p in persons:
        upsert_person(conn, p)
    for a in anime_list:
        upsert_anime(conn, a)
    for c in credits:
        insert_credit(conn, c)

    conn.commit()
    conn.close()

    logger.info("Synthetic data populated in DB")


def main() -> None:
    """エントリーポイント."""
    import argparse

    from src.log import setup_logging

    setup_logging()

    parser = argparse.ArgumentParser(description="合成データ生成")
    parser.add_argument("--directors", type=int, default=10)
    parser.add_argument("--animators", type=int, default=100)
    parser.add_argument("--anime", type=int, default=50)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    populate_db_with_synthetic(
        n_directors=args.directors,
        n_animators=args.animators,
        n_anime=args.anime,
        seed=args.seed,
    )


if __name__ == "__main__":
    main()
