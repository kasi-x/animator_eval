"""AniList GraphQL API response parsers."""

from __future__ import annotations

import json as _json

import structlog

from src.runtime.models import (
    AnimeRelation,
    AnimeStudio,
    BronzeAnime,
    Character,
    CharacterVoiceActor,
    Credit,
    Person,
    Studio,
    parse_role,
)
from src.utils.episode_parser import parse_episodes
from src.utils.name_utils import parse_anilist_native_name

log = structlog.get_logger()


def parse_anilist_person(staff: dict) -> Person:
    """Parse individual person details from PERSON_DETAILS_QUERY response."""
    anilist_person_id = staff.get("id")
    if not anilist_person_id:
        raise ValueError("Person ID is required")

    person_id = f"anilist:p{anilist_person_id}"
    name = staff.get("name", {})

    aliases = []
    alternative_names = name.get("alternative", [])
    if alternative_names:
        aliases = list(set(a for a in alternative_names if a))

    image = staff.get("image", {})
    image_large = image.get("large")
    image_medium = image.get("medium")

    dob_obj = staff.get("dateOfBirth", {})
    date_of_birth = None
    if dob_obj and dob_obj.get("year"):
        year = dob_obj.get("year")
        month = dob_obj.get("month") or 1
        day = dob_obj.get("day") or 1
        date_of_birth = f"{year}-{month:02d}-{day:02d}"

    years_active_raw = staff.get("yearsActive", [])
    years_active = [y for y in years_active_raw if y] if years_active_raw else []

    primary_occupations_raw = staff.get("primaryOccupations") or []
    primary_occupations = [o for o in primary_occupations_raw if o]

    hometown_val = staff.get("homeTown")
    name_ja, name_ko, name_zh, names_alt, native, nationality = parse_anilist_native_name(name, hometown_val)

    return Person(
        id=person_id,
        name_ja=name_ja,
        name_ko=name_ko,
        name_zh=name_zh,
        names_alt=names_alt,
        name_native_raw=native,
        name_en=name.get("full") or "",
        aliases=aliases,
        nationality=nationality,
        anilist_id=anilist_person_id,
        image_large=image_large,
        image_medium=image_medium,
        date_of_birth=date_of_birth,
        age=staff.get("age"),
        gender=staff.get("gender"),
        primary_occupations=primary_occupations,
        years_active=years_active,
        hometown=hometown_val,
        blood_type=staff.get("bloodType"),
        description=staff.get("description"),
        favourites=staff.get("favourites"),
        site_url=staff.get("siteUrl"),
    )


def parse_anilist_anime(raw: dict) -> BronzeAnime:
    """Parse comprehensive anime data from AniList API response."""
    anilist_id = raw["id"]
    title = raw.get("title", {})
    season_map = {
        "WINTER": "winter",
        "SPRING": "spring",
        "SUMMER": "summer",
        "FALL": "fall",
    }
    avg = raw.get("averageScore")

    cover = raw.get("coverImage", {})
    cover_large = cover.get("large")
    cover_extra_large = cover.get("extraLarge")
    cover_medium = cover.get("medium")
    banner = raw.get("bannerImage")

    start_date_obj = raw.get("startDate", {})
    end_date_obj = raw.get("endDate", {})
    start_date = None
    end_date = None
    if start_date_obj and start_date_obj.get("year"):
        year = start_date_obj.get("year")
        month = start_date_obj.get("month") or 1
        day = start_date_obj.get("day") or 1
        start_date = f"{year}-{month:02d}-{day:02d}"
    if end_date_obj and end_date_obj.get("year"):
        year = end_date_obj.get("year")
        month = end_date_obj.get("month") or 1
        day = end_date_obj.get("day") or 1
        end_date = f"{year}-{month:02d}-{day:02d}"

    studios_obj = raw.get("studios", {})
    studios_edges = studios_obj.get("edges", [])
    if studios_edges:
        studios = [
            e.get("node", {}).get("name")
            for e in studios_edges
            if e.get("node", {}).get("name")
        ]
    else:
        studios_nodes = studios_obj.get("nodes", [])
        studios = [s.get("name") for s in studios_nodes if s.get("name")]

    tags_data = raw.get("tags", [])
    tags = [
        {"name": t.get("name"), "rank": t.get("rank")}
        for t in tags_data
        if t.get("name")
    ]
    tags = sorted(tags, key=lambda x: x.get("rank", 0), reverse=True)[:10]

    trailer_obj = raw.get("trailer") or {}
    trailer_url = None
    trailer_site = trailer_obj.get("site")
    trailer_id = trailer_obj.get("id")
    if trailer_id and trailer_site:
        if trailer_site == "youtube":
            trailer_url = f"https://www.youtube.com/watch?v={trailer_id}"
        elif trailer_site == "dailymotion":
            trailer_url = f"https://www.dailymotion.com/video/{trailer_id}"
        else:
            trailer_url = trailer_id

    relations_json = None
    relations_data = raw.get("relations", {}).get("edges", [])
    if relations_data:
        relations = []
        for edge in relations_data:
            node = edge.get("node", {})
            if node.get("id"):
                relations.append(
                    {
                        "id": node["id"],
                        "type": edge.get("relationType"),
                        "title": (node.get("title") or {}).get("romaji"),
                        "format": node.get("format"),
                    }
                )
        if relations:
            relations_json = _json.dumps(relations, ensure_ascii=False)

    external_links_json = None
    external_links_data = raw.get("externalLinks") or []
    if external_links_data:
        links = []
        for link in external_links_data:
            if link.get("url"):
                links.append(
                    {
                        "url": link["url"],
                        "site": link.get("site"),
                        "type": link.get("type"),
                    }
                )
        if links:
            external_links_json = _json.dumps(links, ensure_ascii=False)

    rankings_json = None
    rankings_data = raw.get("rankings") or []
    if rankings_data:
        rankings = []
        for r in rankings_data:
            rankings.append(
                {
                    "rank": r.get("rank"),
                    "type": r.get("type"),
                    "format": r.get("format"),
                    "year": r.get("year"),
                    "season": r.get("season"),
                    "allTime": r.get("allTime"),
                    "context": r.get("context"),
                }
            )
        if rankings:
            rankings_json = _json.dumps(rankings, ensure_ascii=False)

    airing_schedule_json = None
    airing_nodes = (raw.get("airingSchedule") or {}).get("nodes") or []
    if airing_nodes:
        schedule = [
            {"airingAt": n.get("airingAt"), "episode": n.get("episode")}
            for n in airing_nodes
            if n.get("airingAt") and n.get("episode")
        ]
        if schedule:
            airing_schedule_json = _json.dumps(schedule, ensure_ascii=False)

    native_title = title.get("native") or ""
    country = raw.get("countryOfOrigin") or "JP"
    if country == "JP" or not native_title:
        title_ja = native_title
        titles_alt_json = "{}"
    elif country == "KR":
        title_ja = ""
        titles_alt_json = _json.dumps({"ko": native_title}, ensure_ascii=False)
    elif country in ("CN", "TW", "HK"):
        title_ja = ""
        titles_alt_json = _json.dumps({"zh": native_title}, ensure_ascii=False)
    else:
        title_ja = ""
        titles_alt_json = _json.dumps({"native": native_title}, ensure_ascii=False)

    return BronzeAnime(
        id=f"anilist:{anilist_id}",
        title_ja=title_ja,
        titles_alt=titles_alt_json,
        title_en=title.get("english") or title.get("romaji") or "",
        year=raw.get("seasonYear"),
        season=season_map.get(raw.get("season", ""), None),
        episodes=raw.get("episodes"),
        mal_id=raw.get("idMal"),
        anilist_id=anilist_id,
        score=avg / 10.0 if avg else None,
        cover_large=cover_large,
        cover_extra_large=cover_extra_large,
        cover_medium=cover_medium,
        banner=banner,
        description=raw.get("description"),
        format=raw.get("format"),
        status=raw.get("status"),
        start_date=start_date,
        end_date=end_date,
        duration=raw.get("duration"),
        source=raw.get("source"),
        genres=raw.get("genres", []),
        tags=tags,
        popularity_rank=raw.get("popularity"),
        favourites=raw.get("favourites"),
        mean_score=raw.get("meanScore"),
        studios=studios,
        synonyms=raw.get("synonyms") or [],
        country_of_origin=raw.get("countryOfOrigin"),
        is_licensed=raw.get("isLicensed"),
        is_adult=raw.get("isAdult"),
        hashtag=raw.get("hashtag"),
        site_url=raw.get("siteUrl"),
        trailer_url=trailer_url,
        trailer_site=trailer_site,
        relations_json=relations_json,
        external_links_json=external_links_json,
        rankings_json=rankings_json,
        airing_schedule_json=airing_schedule_json,
    )


def parse_anilist_staff(
    staff_edges: list[dict], anime_id: str
) -> tuple[list[Person], list[Credit]]:
    """Parse comprehensive staff/person data from AniList API response."""
    persons = []
    credits = []
    for edge in staff_edges:
        node = edge.get("node", {})
        anilist_person_id = node.get("id")
        if not anilist_person_id:
            continue
        person_id = f"anilist:p{anilist_person_id}"
        name = node.get("name", {})

        aliases = []
        alternative_names = name.get("alternative", [])
        if alternative_names:
            aliases = list(set(a for a in alternative_names if a))

        image = node.get("image", {})
        image_large = image.get("large")
        image_medium = image.get("medium")

        dob_obj = node.get("dateOfBirth", {})
        date_of_birth = None
        if dob_obj and dob_obj.get("year"):
            year = dob_obj.get("year")
            month = dob_obj.get("month") or 1
            day = dob_obj.get("day") or 1
            date_of_birth = f"{year}-{month:02d}-{day:02d}"

        years_active_raw = node.get("yearsActive", [])
        years_active = [y for y in years_active_raw if y] if years_active_raw else []

        primary_occupations_raw = node.get("primaryOccupations") or []
        primary_occupations = [o for o in primary_occupations_raw if o]

        hometown_val = node.get("homeTown")
        name_ja, name_ko, name_zh, names_alt, native, nationality = parse_anilist_native_name(name, hometown_val)

        persons.append(
            Person(
                id=person_id,
                name_ja=name_ja,
                name_ko=name_ko,
                name_zh=name_zh,
                names_alt=names_alt,
                name_native_raw=native,
                name_en=name.get("full") or "",
                aliases=aliases,
                nationality=nationality,
                anilist_id=anilist_person_id,
                image_large=image_large,
                image_medium=image_medium,
                date_of_birth=date_of_birth,
                age=node.get("age"),
                gender=node.get("gender"),
                primary_occupations=primary_occupations,
                years_active=years_active,
                hometown=hometown_val,
                blood_type=node.get("bloodType"),
                description=node.get("description"),
                favourites=node.get("favourites"),
                site_url=node.get("siteUrl"),
            )
        )
        raw_role_str = edge.get("role", "")
        role = parse_role(raw_role_str)

        episodes = parse_episodes(raw_role_str)
        if episodes:
            for ep in sorted(episodes):
                credits.append(
                    Credit(
                        person_id=person_id,
                        anime_id=anime_id,
                        role=role,
                        raw_role=raw_role_str,
                        episode=ep,
                        source="anilist",
                    )
                )
        else:
            credits.append(
                Credit(
                    person_id=person_id,
                    anime_id=anime_id,
                    role=role,
                    raw_role=raw_role_str,
                    source="anilist",
                )
            )
    return persons, credits


def parse_anilist_voice_actors(
    character_edges: list[dict], anime_id: str
) -> tuple[list[Person], list[Credit]]:
    """Parse voice actor data from AniList character edges."""
    persons = []
    credits = []
    seen_vas: set = set()

    if not character_edges:
        return persons, credits

    for edge in character_edges:
        voice_actors = edge.get("voiceActors", [])
        if not voice_actors:
            continue
        for va in voice_actors:
            anilist_person_id = va.get("id")
            if not anilist_person_id or anilist_person_id in seen_vas:
                continue

            seen_vas.add(anilist_person_id)
            person_id = f"anilist:p{anilist_person_id}"
            name = va.get("name", {})

            aliases = []
            alternative_names = name.get("alternative", [])
            if alternative_names:
                aliases = list(set(a for a in alternative_names if a))

            image = va.get("image", {})
            image_large = image.get("large")
            image_medium = image.get("medium")

            dob_obj = va.get("dateOfBirth", {})
            date_of_birth = None
            if dob_obj and dob_obj.get("year"):
                year = dob_obj.get("year")
                month = dob_obj.get("month") or 1
                day = dob_obj.get("day") or 1
                date_of_birth = f"{year}-{month:02d}-{day:02d}"

            years_active_raw = va.get("yearsActive", [])
            years_active = (
                [y for y in years_active_raw if y] if years_active_raw else []
            )

            primary_occupations_raw = va.get("primaryOccupations") or []
            primary_occupations = [o for o in primary_occupations_raw if o]

            hometown_val = va.get("homeTown")
            name_ja, name_ko, name_zh, names_alt, native, nationality = parse_anilist_native_name(name, hometown_val)

            persons.append(
                Person(
                    id=person_id,
                    name_ja=name_ja,
                    name_ko=name_ko,
                    name_zh=name_zh,
                    names_alt=names_alt,
                    name_native_raw=native,
                    name_en=name.get("full") or "",
                    aliases=aliases,
                    nationality=nationality,
                    anilist_id=anilist_person_id,
                    image_large=image_large,
                    image_medium=image_medium,
                    date_of_birth=date_of_birth,
                    age=va.get("age"),
                    gender=va.get("gender"),
                    primary_occupations=primary_occupations,
                    years_active=years_active,
                    hometown=hometown_val,
                    blood_type=va.get("bloodType"),
                    description=va.get("description"),
                    favourites=va.get("favourites"),
                    site_url=va.get("siteUrl"),
                )
            )

            credits.append(
                Credit(
                    person_id=person_id,
                    anime_id=anime_id,
                    role=parse_role("voice actor"),
                    raw_role="Voice Actor",
                    source="anilist",
                )
            )

    return persons, credits


def parse_anilist_characters(
    character_edges: list[dict], anime_id: str
) -> tuple[list[Character], list[CharacterVoiceActor]]:
    """Parse character data and character-VA mappings from AniList character edges."""
    characters = []
    cva_list = []
    seen_chars: set = set()

    if not character_edges:
        return characters, cva_list

    for edge in character_edges:
        char_node = edge.get("node") or {}
        anilist_char_id = char_node.get("id")
        if not anilist_char_id:
            continue

        character_role = edge.get("role", "")

        if anilist_char_id not in seen_chars:
            seen_chars.add(anilist_char_id)
            char_id = f"anilist:c{anilist_char_id}"
            name = char_node.get("name", {})

            aliases = []
            alt_names = name.get("alternative", [])
            if alt_names:
                aliases = list(set(a for a in alt_names if a))

            image = char_node.get("image", {})

            dob_obj = char_node.get("dateOfBirth") or {}
            date_of_birth = None
            if dob_obj.get("year"):
                y = dob_obj["year"]
                m = dob_obj.get("month") or 1
                d = dob_obj.get("day") or 1
                date_of_birth = f"{y}-{m:02d}-{d:02d}"

            characters.append(
                Character(
                    id=char_id,
                    name_ja=name.get("native") or "",
                    name_en=name.get("full") or "",
                    aliases=aliases,
                    anilist_id=anilist_char_id,
                    image_large=image.get("large"),
                    image_medium=image.get("medium"),
                    description=char_node.get("description"),
                    gender=char_node.get("gender"),
                    date_of_birth=date_of_birth,
                    age=char_node.get("age"),
                    blood_type=char_node.get("bloodType"),
                    favourites=char_node.get("favourites"),
                    site_url=char_node.get("siteUrl"),
                )
            )

        char_id = f"anilist:c{anilist_char_id}"
        for va in edge.get("voiceActors") or []:
            va_id = va.get("id")
            if va_id:
                cva_list.append(
                    CharacterVoiceActor(
                        character_id=char_id,
                        person_id=f"anilist:p{va_id}",
                        anime_id=anime_id,
                        character_role=character_role,
                        source="anilist",
                    )
                )

    return characters, cva_list


def parse_anilist_studios(
    raw: dict, anime_id: str
) -> tuple[list[Studio], list[AnimeStudio]]:
    """Parse studio data from AniList Media response."""
    studios = []
    anime_studios = []
    seen: set = set()

    studios_obj = raw.get("studios", {})
    edges = studios_obj.get("edges", [])
    if not edges:
        return studios, anime_studios

    for edge in edges:
        node = edge.get("node") or {}
        anilist_studio_id = node.get("id")
        if not anilist_studio_id:
            continue

        studio_id = f"anilist:s{anilist_studio_id}"
        is_main = edge.get("isMain", False)

        if anilist_studio_id not in seen:
            seen.add(anilist_studio_id)
            studios.append(
                Studio(
                    id=studio_id,
                    name=node.get("name") or "",
                    anilist_id=anilist_studio_id,
                    is_animation_studio=node.get("isAnimationStudio"),
                    favourites=node.get("favourites"),
                    site_url=node.get("siteUrl"),
                )
            )

        anime_studios.append(
            AnimeStudio(
                anime_id=anime_id,
                studio_id=studio_id,
                is_main=is_main,
            )
        )

    return studios, anime_studios


def parse_anilist_relations(raw: dict, anime_id: str) -> list[AnimeRelation]:
    """Parse relation edges from AniList Media response.

    Extracts SEQUEL, PREQUEL, SIDE_STORY, PARENT, etc. links between anime.
    """
    relations = []
    relations_obj = raw.get("relations", {})
    edges = relations_obj.get("edges", [])
    if not edges:
        return relations

    for edge in edges:
        node = edge.get("node") or {}
        node_id = node.get("id")
        if not node_id:
            continue

        title_obj = node.get("title") or {}
        relations.append(
            AnimeRelation(
                anime_id=anime_id,
                related_anime_id=f"anilist:{node_id}",
                relation_type=edge.get("relationType", ""),
                related_title=title_obj.get("romaji", ""),
                related_format=node.get("format"),
            )
        )

    return relations
