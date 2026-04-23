"""Synthetic data generation — dummy data for testing the pipeline without a real API.

Generates synthetic data that mimics the structure of the actual anime industry:
- A small number of directors handle many works (power law)
- Animators work with multiple directors
- Some animators have strong ties to specific directors
"""

import random
from pathlib import Path

import duckdb
import structlog

from src.models import BronzeAnime, Character, CharacterVoiceActor, Credit, Person, Role

logger = structlog.get_logger()

# Role distribution (mimics real anime credits)
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
) -> tuple[list[Person], list[BronzeAnime], list[Credit]]:
    """Generate synthetic data.

    Args:
        n_directors: number of directors
        n_animators: number of animators
        n_anime: number of anime works
        seed: random seed

    Returns:
        (persons, anime_list, credits)
    """
    rng = random.Random(seed)

    # generate directors
    directors = []
    for i in range(n_directors):
        directors.append(
            Person(
                id=f"syn:d{i}",
                name_ja=f"監督{i:03d}",
                name_en=f"Director {i:03d}",
            )
        )

    # generate animators
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

    # generate studios (5 studios)
    studio_names = [
        "Studio Alpha",
        "Studio Beta",
        "Studio Gamma",
        "Studio Delta",
        "Studio Epsilon",
    ]

    # generate anime works
    anime_list = []
    for i in range(n_anime):
        year = rng.randint(2000, 2025)
        score = round(rng.uniform(5.0, 9.5), 1)
        # Assign 1-2 studios per anime
        n_studios = rng.choices([1, 2], weights=[0.7, 0.3])[0]
        anime_studios = rng.sample(studio_names, min(n_studios, len(studio_names)))
        anime_list.append(
            BronzeAnime(
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

    # generate credits
    credits = []

    # assign directors (power law: few directors handle many works)
    director_weights = [1.0 / (i + 1) ** 0.8 for i in range(n_directors)]
    total_w = sum(director_weights)
    director_probs = [w / total_w for w in director_weights]

    for anime in anime_list:
        # choose directors (1-2)
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

        # assign animators by role (decide count per role)
        for role, base_count in ROLE_DISTRIBUTION:
            if role == Role.DIRECTOR:
                continue  # directors already handled above

            n_staff = max(1, rng.randint(base_count - 1, base_count + 2))

            # some animators have strong ties to specific directors
            preferred_animators = []
            for d in chosen_dirs:
                # each director has favourite animators
                dir_idx = int(d.id.split("d")[1])
                for j in range(min(5, n_animators)):
                    preferred_idx = (dir_idx * 7 + j * 3) % n_animators
                    preferred_animators.append(animators[preferred_idx])

            chosen_staff = []
            for _ in range(n_staff):
                if preferred_animators and rng.random() < 0.4:
                    # 40% chance to pick a favourite animator
                    chosen = rng.choice(preferred_animators)
                else:
                    chosen = rng.choice(animators)
                chosen_staff.append(chosen)

            # deduplicate
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
    anime_list: list[BronzeAnime],
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


def populate_silver_duckdb(
    silver_path: Path | str | None = None,
    n_directors: int = 10,
    n_animators: int = 100,
    n_anime: int = 50,
    seed: int = 42,
) -> None:
    """Populate silver.duckdb with synthetic data.

    Args:
        silver_path: Path to silver.duckdb (uses DEFAULT_SILVER_PATH if None)
        n_directors: number of directors
        n_animators: number of animators
        n_anime: number of anime works
        seed: random seed
    """
    from src.analysis.silver_reader import DEFAULT_SILVER_PATH

    if silver_path is None:
        silver_path = DEFAULT_SILVER_PATH

    persons, anime_list, credits = generate_synthetic_data(
        n_directors=n_directors,
        n_animators=n_animators,
        n_anime=n_anime,
        seed=seed,
    )

    conn = duckdb.connect(str(silver_path))

    # Create tables
    conn.execute(
        """CREATE TABLE IF NOT EXISTS persons (
            id VARCHAR PRIMARY KEY,
            name_ja VARCHAR DEFAULT '',
            name_en VARCHAR DEFAULT '',
            name_ko VARCHAR DEFAULT '',
            name_zh VARCHAR DEFAULT '',
            aliases VARCHAR DEFAULT '[]',
            image_medium VARCHAR,
            date_of_birth DATE,
            site_url VARCHAR,
            gender VARCHAR
        )"""
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS anime (
            id VARCHAR PRIMARY KEY,
            title_ja VARCHAR DEFAULT '',
            title_en VARCHAR DEFAULT '',
            year INTEGER,
            season VARCHAR,
            quarter INTEGER,
            episodes INTEGER,
            format VARCHAR,
            status VARCHAR,
            start_date DATE,
            end_date DATE,
            duration INTEGER,
            source_mat VARCHAR,
            work_type VARCHAR,
            scale_class VARCHAR,
            studios VARCHAR DEFAULT '[]'
        )"""
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS credits (
            person_id VARCHAR,
            anime_id VARCHAR,
            role VARCHAR,
            credit_year INTEGER DEFAULT 0,
            credit_quarter INTEGER DEFAULT 0,
            episode VARCHAR,
            raw_role VARCHAR,
            evidence_source VARCHAR DEFAULT ''
        )"""
    )

    # Insert persons
    for p in persons:
        conn.execute(
            "INSERT OR REPLACE INTO persons (id, name_ja, name_en, gender) VALUES (?, ?, ?, ?)",
            [p.id, p.name_ja, p.name_en, p.gender],
        )

    # Insert anime
    for a in anime_list:
        studios_str = str(a.studios) if a.studios else "[]"
        conn.execute(
            "INSERT OR REPLACE INTO anime (id, title_ja, title_en, year, season, episodes, studios) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            [a.id, a.title_ja, a.title_en, a.year, a.season, a.episodes, studios_str],
        )

    # Insert credits
    for c in credits:
        conn.execute(
            "INSERT INTO credits (person_id, anime_id, role, evidence_source) VALUES (?, ?, ?, ?)",
            [c.person_id, c.anime_id, c.role.value, c.source or "synthetic"],
        )

    conn.commit()
    conn.close()

    logger.info("Synthetic data populated in silver.duckdb")


def populate_db_with_synthetic(
    n_directors: int = 10,
    n_animators: int = 100,
    n_anime: int = 50,
    seed: int = 42,
) -> None:
    """Deprecated: use populate_silver_duckdb instead."""
    logger.warning(
        "populate_db_with_synthetic is deprecated; use populate_silver_duckdb instead"
    )
    populate_silver_duckdb(
        silver_path=None,
        n_directors=n_directors,
        n_animators=n_animators,
        n_anime=n_anime,
        seed=seed,
    )


def main() -> None:
    """Entry point."""
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
