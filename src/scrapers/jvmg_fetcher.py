"""Japanese Visual Media Graph (JVMG) data retrieval via Wikidata SPARQL.

Wikidata publishes information about Japanese anime works and staff as RDF.
SPARQL endpoint: https://query.wikidata.org/sparql
"""

import asyncio
import json
from datetime import datetime, timezone
from pathlib import Path

import structlog
import typer

from src.models import BronzeAnime, Credit, Person, parse_role
from src.scrapers.cache_store import load_cached_json, save_cached_json
from src.scrapers.http_client import RetryingHttpClient
from src.scrapers.logging_utils import configure_file_logging

log = structlog.get_logger()

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
REQUEST_INTERVAL = 2.0  # Wikidata enforces strict rate limits

# Query to retrieve anime works and their staff
ANIME_STAFF_QUERY = """
SELECT ?anime ?animeLabel ?year ?person ?personLabel ?personLabelJa ?roleLabel
WHERE {{
  ?anime wdt:P31/wdt:P279* wd:Q63952888 .  # anime series
  ?anime wdt:P57|wdt:P1040|wdt:P3174|wdt:P58 ?person .
  OPTIONAL {{ ?anime wdt:P577 ?date . BIND(YEAR(?date) AS ?year) }}
  OPTIONAL {{
    ?anime p:P57|p:P1040|p:P3174|p:P58 ?stmt .
    ?stmt ps:P57|ps:P1040|ps:P3174|ps:P58 ?person .
    ?stmt pq:P3831 ?role .
  }}
  OPTIONAL {{ ?person rdfs:label ?personLabelJa . FILTER(LANG(?personLabelJa) = "ja") }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,ja" . }}
}}
LIMIT {limit}
OFFSET {offset}
"""

# Property mapping
WIKIDATA_ROLE_MAP = {
    "P57": "director",
    "P1040": "animation director",
    "P3174": "episode_director",
    "P58": "episode director",
}

app = typer.Typer()

CHECKPOINT_FILE = Path(__file__).parent.parent.parent / "data" / "jvmg_checkpoint.json"


def _load_checkpoint(path: Path) -> dict | None:
    """Load a checkpoint file."""
    if path.exists():
        with open(path) as f:
            return json.load(f)
    return None


def _save_checkpoint(path: Path, data: dict) -> None:
    """Save a checkpoint file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(data, f, indent=2)


def _delete_checkpoint(path: Path) -> None:
    """Delete a checkpoint file."""
    if path.exists():
        path.unlink()


class WikidataClient:
    """Async Wikidata SPARQL client (wraps RetryingHttpClient)."""

    def __init__(self, transport=None) -> None:
        self._http = RetryingHttpClient(
            source="wikidata",
            delay=REQUEST_INTERVAL,
            timeout=120.0,
            headers={
                "User-Agent": "AnimetorEval/0.1 (research project)",
                "Accept": "application/sparql-results+json",
            },
            transport=transport,
        )

    async def close(self) -> None:
        await self._http.aclose()

    async def query(self, sparql: str) -> list[dict]:
        cache_key = {"sparql": sparql}
        cached = load_cached_json("wikidata/sparql", cache_key)
        if cached is not None:
            return cached

        resp = await self._http.get(
            WIKIDATA_SPARQL,
            params={"query": sparql, "format": "json"},
        )
        resp.raise_for_status()
        data = resp.json()
        result = data.get("results", {}).get("bindings", [])
        save_cached_json("wikidata/sparql", cache_key, result)
        return result


def parse_wikidata_results(
    bindings: list[dict],
) -> tuple[list[BronzeAnime], list[Person], list[Credit]]:
    """Parse Wikidata SPARQL results."""
    anime_map: dict[str, BronzeAnime] = {}
    person_map: dict[str, Person] = {}
    credits: list[Credit] = []

    for row in bindings:
        anime_uri = row.get("anime", {}).get("value", "")
        anime_label = row.get("animeLabel", {}).get("value", "")
        year_str = row.get("year", {}).get("value", "")
        person_uri = row.get("person", {}).get("value", "")
        person_label = row.get("personLabel", {}).get("value", "")
        person_label_ja = row.get("personLabelJa", {}).get("value", "")
        role_label = row.get("roleLabel", {}).get("value", "")

        if not anime_uri or not person_uri:
            continue

        anime_qid = anime_uri.split("/")[-1]
        person_qid = person_uri.split("/")[-1]
        anime_id = f"wd:{anime_qid}"
        person_id = f"wd:p{person_qid}"

        if anime_id not in anime_map:
            year = None
            if year_str:
                try:
                    year = int(float(year_str))
                except ValueError:
                    pass
            anime_map[anime_id] = BronzeAnime(
                id=anime_id,
                title_en=anime_label,
                year=year,
            )

        if person_id not in person_map:
            person_map[person_id] = Person(
                id=person_id,
                name_ja=person_label_ja,
                name_en=person_label,
            )

        role = parse_role(role_label) if role_label else parse_role("other")
        credits.append(
            Credit(
                person_id=person_id,
                anime_id=anime_id,
                role=role,
                source="wikidata",
            )
        )

    return list(anime_map.values()), list(person_map.values()), credits


async def fetch_anime_staff(
    max_records: int = 5000,
) -> tuple[list[BronzeAnime], list[Person], list[Credit]]:
    """Fetch anime staff data from Wikidata."""
    client = WikidataClient()
    all_anime: list[BronzeAnime] = []
    all_persons: list[Person] = []
    all_credits: list[Credit] = []

    page_size = 500
    offset = 0

    try:
        while offset < max_records:
            log.info("fetching_wikidata", source="wikidata", offset=offset)
            query = ANIME_STAFF_QUERY.format(limit=page_size, offset=offset)
            bindings = await client.query(query)
            if not bindings:
                break

            anime, persons, credits = parse_wikidata_results(bindings)
            all_anime.extend(anime)
            all_persons.extend(persons)
            all_credits.extend(credits)

            if len(bindings) < page_size:
                break
            offset += page_size
    finally:
        await client.close()

    log.info(
        "wikidata_fetch_complete",
        source="wikidata",
        item_count=len(all_credits),
        anime=len(all_anime),
        persons=len(all_persons),
        credits=len(all_credits),
    )
    return all_anime, all_persons, all_credits


@app.command()
def main(
    max_records: int = typer.Option(5000, "--max-records", "-n", help="maximum number of records"),
    resume: bool = typer.Option(
        True, "--resume/--no-resume", help="resume from checkpoint"
    ),
    checkpoint_interval: int = typer.Option(
        2, "--checkpoint-interval", help="checkpoint save interval (pages)"
    ),
) -> None:
    """Collect anime staff data from Wikidata."""
    from src.log import setup_logging
    from src.scrapers.bronze_writer import BronzeWriter

    setup_logging()
    log_path = configure_file_logging("wikidata")
    log.info("wikidata_fetch_command_start", log_file=str(log_path))

    # Load checkpoint if resuming
    start_offset = 0
    if resume:
        checkpoint = _load_checkpoint(CHECKPOINT_FILE)
        if checkpoint:
            start_offset = checkpoint.get("last_offset", 0)
            log.info(
                "checkpoint_loaded",
                last_offset=start_offset,
                total_anime=checkpoint.get("total_anime", 0),
                total_persons=checkpoint.get("total_persons", 0),
                total_credits=checkpoint.get("total_credits", 0),
                timestamp=checkpoint.get("timestamp"),
            )

    async def _fetch_and_save() -> None:
        """Fetch Wikidata records incrementally with checkpoint support."""
        client = WikidataClient()
        total_anime = 0
        total_persons = 0
        total_credits = 0

        page_size = 500
        offset = start_offset
        pages_since_checkpoint = 0

        anime_bw = BronzeWriter("jvmg", table="anime")
        persons_bw = BronzeWriter("jvmg", table="persons")
        credits_bw = BronzeWriter("jvmg", table="credits")

        try:
            while offset < max_records:
                log.info("fetching_wikidata", source="wikidata", offset=offset)
                query = ANIME_STAFF_QUERY.format(limit=page_size, offset=offset)
                bindings = await client.query(query)
                if not bindings:
                    break

                anime_list, persons, credits = parse_wikidata_results(bindings)
                for anime in anime_list:
                    anime_bw.append(anime.model_dump(mode="json"))
                    total_anime += 1
                for person in persons:
                    persons_bw.append(person.model_dump(mode="json"))
                    total_persons += 1
                for credit in credits:
                    credits_bw.append(credit.model_dump(mode="json"))
                    total_credits += 1

                pages_since_checkpoint += 1
                offset += page_size

                # Flush checkpoint every N pages
                if pages_since_checkpoint >= checkpoint_interval:
                    anime_bw.flush()
                    persons_bw.flush()
                    credits_bw.flush()
                    _save_checkpoint(
                        CHECKPOINT_FILE,
                        {
                            "last_offset": offset,
                            "total_anime": total_anime,
                            "total_persons": total_persons,
                            "total_credits": total_credits,
                            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
                        },
                    )
                    log.info("checkpoint_saved", last_offset=offset)
                    pages_since_checkpoint = 0

                if len(bindings) < page_size:
                    break

        finally:
            await client.close()
            anime_bw.flush()
            persons_bw.flush()
            credits_bw.flush()

        # Delete checkpoint on successful completion
        _delete_checkpoint(CHECKPOINT_FILE)
        log.info(
            "bronze_parquet_written",
            source="jvmg",
            anime=total_anime,
            persons=total_persons,
            credits=total_credits,
        )

    asyncio.run(_fetch_and_save())


if __name__ == "__main__":
    app()
