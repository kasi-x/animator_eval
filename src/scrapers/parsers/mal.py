"""MyAnimeList / Jikan API response parsers."""

from src.runtime.models import BronzeAnime, Credit, Person, parse_role


def parse_anime_data(raw: dict) -> BronzeAnime:
    mal_id = raw.get("mal_id")
    titles = raw.get("titles", [])
    title_ja, title_en = "", ""
    synonyms: list[str] = []
    for t in titles:
        if t.get("type") == "Japanese":
            title_ja = t.get("title", "")
        elif t.get("type") == "Default":
            title_en = t.get("title", "")
        elif t.get("type") == "English" and not title_en:
            title_en = t.get("title", "")
        elif t.get("type") == "Synonym":
            synonyms.append(t.get("title", ""))
    if not title_en:
        title_en = raw.get("title", "")

    aired = raw.get("aired", {}) or {}
    prop = aired.get("prop", {}) or {}
    from_prop = prop.get("from", {}) or {}
    to_prop = prop.get("to", {}) or {}
    year = raw.get("year") or from_prop.get("year")

    def _build_date(p: dict) -> str | None:
        y, m, d = p.get("year"), p.get("month"), p.get("day")
        if not y:
            return None
        if m and d:
            return f"{y:04d}-{m:02d}-{d:02d}"
        if m:
            return f"{y:04d}-{m:02d}"
        return str(y)

    genres = [g["name"] for g in raw.get("genres", []) if g.get("name")]

    return BronzeAnime(
        id=f"mal:{mal_id}",
        title_ja=title_ja,
        title_en=title_en,
        year=year,
        season=raw.get("season"),
        episodes=raw.get("episodes"),
        mal_id=mal_id,
        score=raw.get("score"),
        format=raw.get("type"),
        status=raw.get("status"),
        start_date=_build_date(from_prop),
        end_date=_build_date(to_prop),
        genres=genres,
        synonyms=synonyms,
    )


def parse_staff_data(
    staff_list: list[dict], anime_id: str
) -> tuple[list[Person], list[Credit]]:
    persons, credits = [], []
    for entry in staff_list:
        person_data = entry.get("person", {})
        mal_person_id = person_data.get("mal_id")
        if not mal_person_id:
            continue
        person_id = f"mal:p{mal_person_id}"
        name = person_data.get("name", "")
        parts = name.split(", ", 1)
        name_en = f"{parts[1]} {parts[0]}" if len(parts) == 2 else name
        persons.append(Person(id=person_id, name_en=name_en, mal_id=mal_person_id))
        for pos in entry.get("positions", []):
            credits.append(
                Credit(
                    person_id=person_id,
                    anime_id=anime_id,
                    role=parse_role(pos),
                    source="mal",
                )
            )
    return persons, credits
