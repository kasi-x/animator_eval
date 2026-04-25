"""Japanese Visual Media Graph (JVMG) data retrieval via Wikidata SPARQL.

Wikidata publishes information about Japanese anime works and staff as RDF.
SPARQL endpoint: https://query.wikidata.org/sparql
"""

import asyncio
from pathlib import Path

import structlog
import typer

from src.runtime.models import BronzeAnime, Credit, Person, parse_role
from src.scrapers.cache_store import load_cached_json, save_cached_json
from src.scrapers.checkpoint import resolve_checkpoint
from src.scrapers.cli_common import (
    CheckpointIntervalOpt,
    ProgressOpt,
    QuietOpt,
    ResumeOpt,
    resolve_progress_enabled,
)
from src.scrapers.http_base import RateLimitedHttpClient
from src.scrapers.http_client import RetryingHttpClient
from src.scrapers.logging_utils import configure_file_logging
from src.scrapers.progress import scrape_progress
from src.scrapers.wikidata_role_map import WIKIDATA_ROLE_MAP  # noqa: F401  re-exported

log = structlog.get_logger()

WIKIDATA_SPARQL = "https://query.wikidata.org/sparql"
REQUEST_INTERVAL = 2.0  # Wikidata enforces strict rate limits

# Query to retrieve anime works and their staff
# UNION-based query: each branch binds ?role from the property used so we always
# know the staff role even when P3831 qualifiers are absent.
# Role tokens below come from `WIKIDATA_ROLE_MAP` (imported above). Keep SPARQL
# BIND values in sync with that dict — tokens must be parse_role()-compatible
# (see src/models.py ROLE_MAP), otherwise credits fall back to Role.SPECIAL.
#
# P10800 ("animation director") omitted — property ID unverified on wikidata.org.
ANIME_STAFF_QUERY = """
SELECT ?anime ?animeLabel ?year ?person ?personLabel ?personLabelJa ?role
WHERE {{
  ?anime wdt:P31/wdt:P279* wd:Q63952888 .  # anime series
  OPTIONAL {{ ?anime wdt:P577 ?date . BIND(YEAR(?date) AS ?year) }}
  OPTIONAL {{ ?person rdfs:label ?personLabelJa . FILTER(LANG(?personLabelJa) = "ja") }}
  {{
    ?anime wdt:P57 ?person . BIND("director" AS ?role)
  }} UNION {{
    ?anime wdt:P58 ?person . BIND("screenplay" AS ?role)
  }} UNION {{
    ?anime wdt:P1040 ?person . BIND("film editor" AS ?role)
  }} UNION {{
    ?anime wdt:P3174 ?person . BIND("art director" AS ?role)
  }}
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "en,ja" . }}
}}
LIMIT {limit}
OFFSET {offset}
"""

app = typer.Typer()

CHECKPOINT_FILE = Path(__file__).parent.parent.parent / "data" / "jvmg_checkpoint.json"



class WikidataClient(RateLimitedHttpClient):
    """Async Wikidata SPARQL client (wraps RetryingHttpClient)."""

    def __init__(self, transport=None) -> None:
        super().__init__(delay=REQUEST_INTERVAL)
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
        role_label = row.get("role", {}).get("value", "")

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
    max_records: int = typer.Option(
        5000, "--max-records", "--limit", "-n",
        help="Maximum number of records. Alias: --limit",
    ),
    resume: ResumeOpt = True,
    checkpoint_interval: CheckpointIntervalOpt = 2,
    quiet: QuietOpt = False,
    progress: ProgressOpt = False,
) -> None:
    """Collect anime staff data from Wikidata."""
    from src.infra.logging import setup_logging
    from src.scrapers.bronze_writer import BronzeWriterGroup

    setup_logging()
    log_path = configure_file_logging("wikidata")
    log.info("wikidata_fetch_command_start", log_file=str(log_path))

    cp = resolve_checkpoint(CHECKPOINT_FILE, resume=resume)
    start_offset = cp.get("last_offset", 0)
    if resume and start_offset > 0:
        log.info(
            "checkpoint_loaded",
            last_offset=start_offset,
            total_anime=cp.get("total_anime", 0),
            total_persons=cp.get("total_persons", 0),
            total_credits=cp.get("total_credits", 0),
            timestamp=cp.get("last_run_at"),
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

        group = BronzeWriterGroup("jvmg", tables=["anime", "persons", "credits"])
        anime_bw = group["anime"]
        persons_bw = group["persons"]
        credits_bw = group["credits"]

        try:
            with scrape_progress(
                total=max_records,
                description="scraping wikidata",
                enabled=progress_override,
            ) as p:
                while offset < max_records:
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
                    p.advance(min(page_size, max(0, max_records - (offset - page_size))))

                    # Flush checkpoint every N pages
                    if pages_since_checkpoint >= checkpoint_interval:
                        group.flush_all()
                        cp["last_offset"] = offset
                        cp["total_anime"] = total_anime
                        cp["total_persons"] = total_persons
                        cp["total_credits"] = total_credits
                        cp.save()
                        p.log("checkpoint_saved", last_offset=offset)
                        pages_since_checkpoint = 0

                    if len(bindings) < page_size:
                        break

        finally:
            await client.close()
            group.flush_all()
            group.compact_all()

        # Delete checkpoint on successful completion
        cp.delete()
        log.info(
            "bronze_parquet_written",
            source="jvmg",
            anime=total_anime,
            persons=total_persons,
            credits=total_credits,
        )

    progress_override = resolve_progress_enabled(quiet, progress)
    asyncio.run(_fetch_and_save())


if __name__ == "__main__":
    app()
