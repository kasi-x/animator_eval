"""Media Arts Database (MADB) data collection.

Uses JSON-LD dumps published via GitHub Releases.
https://github.com/mediaarts-db/dataset

MADB contributor data is plain text (e.g. "[脚本]仲倉重郎 ／ [演出]須永 司"),
with no structured person URIs, so we use a text parser and deterministic ID generation.

This scraper only fetches and stores data. Matching with AniList is handled
separately in the pipeline's Entity Resolution phase.
"""

from __future__ import annotations

import asyncio
import hashlib
import io
import json
import re
import unicodedata
import zipfile
from pathlib import Path

import httpx
import structlog
import typer

from src.runtime.models import BronzeAnime, Credit, Person, parse_role
from src.scrapers.parsers.mediaarts import (  # noqa: F401
    parse_contributor_text,
    parse_jsonld_dump,
    make_madb_person_id,
    normalize_title,
    _extract_name_from_schema,
    _extract_year,
    _extract_studios,
)
from src.utils.config import SCRAPE_CHECKPOINT_INTERVAL

log = structlog.get_logger()

DATASET_REPO = "mediaarts-db/dataset"
GITHUB_API_BASE = "https://api.github.com"

# Anime collection files (series-level — contains key staff credits)
# Maps zip filename -> (collection_type_label, format_code)
# Primary: 207-210 are anime TV/Special/OVA/Movie
# Extended: 101-109, 201-205, 216, 301-305, 315-317, 401, 504-505 for wider coverage
ANIME_COLLECTION_FILES_PRIMARY: dict[str, tuple[str, str]] = {
    "metadata207_json.zip": ("AnimationTVRegularSeries", "TV"),
    "metadata208_json.zip": ("AnimationTVSpecialSeries", "SPECIAL"),
    "metadata209_json.zip": ("AnimationVideoPackageSeries", "OVA"),
    "metadata210_json.zip": ("AnimationMovieSeries", "MOVIE"),
}

ANIME_COLLECTION_FILES_EXTENDED: dict[str, tuple[str, str]] = {
    # Extended Japanese anime sets
    "metadata201_json.zip": ("AnimationTVSeries", "TV"),
    "metadata202_json.zip": ("AnimationSpecialSeries", "SPECIAL"),
    "metadata204_json.zip": ("AnimationVideoPackageSeries", "OVA"),
    "metadata205_json.zip": ("AnimationMovieSeries", "MOVIE"),
    "metadata216_json.zip": ("AnimationSeries", "OTHER"),
    # International anime sets
    "metadata301_json.zip": ("InternationalAnimationTVSeries", "TV"),
    "metadata305_json.zip": ("InternationalAnimationMovieSeries", "MOVIE"),
    "metadata306_json.zip": ("InternationalAnimationVideoPackageSeries", "OVA"),
    "metadata315_json.zip": ("AnimationVideoSeries", "VIDEO"),
    "metadata317_json.zip": ("AnimationSeries", "OTHER"),
    # Additional international sets
    "metadata401_json.zip": ("AnimationSeries", "OTHER"),
    "metadata504_json.zip": ("AnimationTVSeries", "TV"),
}

DEFAULT_DATA_DIR = Path("data/madb")

app = typer.Typer()


async def download_madb_dataset(
    data_dir: Path,
    version: str = "latest",
    extended: bool = False,
) -> dict[str, Path]:
    """Download MADB dataset from GitHub Releases.

    Args:
        data_dir: Directory to save extracted files
        version: Release version ("latest" or tag name)
        extended: If True, also download extended metadata sets (201-205, 301-317, etc.)
                  If False, only download primary sets (207-210)

    Returns: {anime_type: extracted_json_path}
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    version_file = data_dir / ".version"
    extended_marker = data_dir / ".extended"

    # Select which files to download
    collection_files = ANIME_COLLECTION_FILES_PRIMARY.copy()
    if extended:
        collection_files.update(ANIME_COLLECTION_FILES_EXTENDED)

    # Fetch release info
    async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
        if version == "latest":
            url = f"{GITHUB_API_BASE}/repos/{DATASET_REPO}/releases/latest"
        else:
            url = f"{GITHUB_API_BASE}/repos/{DATASET_REPO}/releases/tags/{version}"

        resp = await client.get(url)
        resp.raise_for_status()
        release = resp.json()
        tag = release["tag_name"]

        # Cache check — skip if same version and all files present
        cached_extended = extended_marker.exists()
        if (
            version_file.exists()
            and version_file.read_text().strip() == tag
            and (not extended or cached_extended)
        ):
            cached = {}
            all_cached = True
            for zip_name, (anime_type, _fmt) in collection_files.items():
                json_name = zip_name.replace("_json.zip", ".json")
                json_path = data_dir / json_name
                if json_path.exists():
                    cached[anime_type] = json_path
                else:
                    all_cached = False
                    break
            if all_cached:
                log.info(
                    "madb_dataset_cached",
                    version=tag,
                    files=len(cached),
                    extended=extended,
                )
                return cached

        # Build asset URL map
        assets = {
            a["name"]: a["browser_download_url"] for a in release.get("assets", [])
        }

        downloaded: dict[str, Path] = {}
        for zip_name, (anime_type, _fmt) in collection_files.items():
            download_url = assets.get(zip_name)
            if not download_url:
                log.warning("madb_asset_not_found", zip_name=zip_name, version=tag)
                continue

            log.info("madb_downloading", file=zip_name, version=tag)
            resp = await client.get(download_url)
            resp.raise_for_status()

            # Extract ZIP
            with zipfile.ZipFile(io.BytesIO(resp.content)) as zf:
                json_files = [n for n in zf.namelist() if n.endswith(".json")]
                if not json_files:
                    log.warning("madb_zip_empty", zip_name=zip_name)
                    continue

                # Extract the first JSON file
                json_name = zip_name.replace("_json.zip", ".json")
                json_path = data_dir / json_name
                with open(json_path, "wb") as out:
                    out.write(zf.read(json_files[0]))

                downloaded[anime_type] = json_path
                log.info(
                    "madb_extracted", file=json_name, size=json_path.stat().st_size
                )

        # Record version
        version_file.write_text(tag)
        if extended:
            extended_marker.write_text("true")
        log.info(
            "madb_download_complete",
            version=tag,
            files=len(downloaded),
            extended=extended,
        )
        return downloaded


async def scrape_madb(
    data_dir: Path | None = None,
    version: str = "latest",
    max_anime: int = 0,
    checkpoint_interval: int = SCRAPE_CHECKPOINT_INTERVAL,
    extended: bool = False,
) -> dict:
    """Fetch credit data from MADB dump.

    1. Download dump from GitHub Releases (with caching)
    2. Parse JSON-LD
    3. Write to BRONZE parquet (ETL handles dedup/normalization)

    Returns:
        Statistics dict
    """
    from src.scrapers.bronze_writer import BronzeWriter

    if data_dir is None:
        data_dir = DEFAULT_DATA_DIR

    stats = {
        "anime_fetched": 0,
        "anime_with_contributors": 0,
        "anime_metadata_only": 0,
        "credits_created": 0,
        "persons_created": 0,
    }
    person_cache: dict[str, Person] = {}

    # Phase A: Download dump
    dataset_files = await download_madb_dataset(
        data_dir, version=version, extended=extended
    )
    if not dataset_files:
        log.warning("madb_no_files_downloaded")
        return stats

    # Build format lookup
    all_collections = ANIME_COLLECTION_FILES_PRIMARY.copy()
    if extended:
        all_collections.update(ANIME_COLLECTION_FILES_EXTENDED)
    format_by_type = {label: fmt for _zip, (label, fmt) in all_collections.items()}

    anime_bw = BronzeWriter("mediaarts", table="anime")
    persons_bw = BronzeWriter("mediaarts", table="persons")
    credits_bw = BronzeWriter("mediaarts", table="credits")

    # Phase B: Parse JSON-LD per collection and write to parquet
    total_processed = 0
    for anime_type, json_path in dataset_files.items():
        format_code = format_by_type.get(anime_type, "")
        log.info(
            "madb_parsing", file=json_path.name, type=anime_type, format=format_code
        )
        records = parse_jsonld_dump(json_path, format_code=format_code)
        if max_anime > 0:
            records = records[:max_anime]
        log.info("madb_parsed", file=json_path.name, records=len(records))

        for i, record in enumerate(records):
            madb_id = record["id"]
            title = record["title"]
            year = record["year"]
            fmt = record["format"]
            contributors = record["contributors"]
            studios = record["studios"]

            anime_id = f"madb:{madb_id}"
            anime = BronzeAnime(
                id=anime_id,
                title_ja=title,
                year=year,
                format=fmt or None,
                madb_id=madb_id,
                studios=studios,
            )
            anime_bw.append(anime.model_dump(mode="json"))
            stats["anime_fetched"] += 1

            if not contributors:
                stats["anime_metadata_only"] += 1
            else:
                stats["anime_with_contributors"] += 1
                for role_ja, name_ja in contributors:
                    if len(name_ja) < 2:
                        continue

                    if name_ja not in person_cache:
                        person_id = make_madb_person_id(name_ja)
                        person = Person(
                            id=person_id,
                            name_ja=name_ja,
                            madb_id=person_id,
                        )
                        persons_bw.append(person.model_dump(mode="json"))
                        person_cache[name_ja] = person
                        stats["persons_created"] += 1
                    else:
                        person = person_cache[name_ja]

                    role = parse_role(role_ja)
                    credit = Credit(
                        person_id=person.id,
                        anime_id=anime_id,
                        role=role,
                        raw_role=role_ja,
                        source="mediaarts",
                    )
                    credits_bw.append(credit.model_dump(mode="json"))
                    stats["credits_created"] += 1

            total_processed += 1
            if total_processed % checkpoint_interval == 0:
                anime_bw.flush()
                persons_bw.flush()
                credits_bw.flush()
                log.info(
                    "madb_checkpoint",
                    collection=anime_type,
                    collection_progress=f"{i + 1}/{len(records)}",
                    total=total_processed,
                    credits=stats["credits_created"],
                    persons=stats["persons_created"],
                )

    # Final flush
    anime_bw.flush()
    persons_bw.flush()
    credits_bw.flush()

    log.info(
        "madb_scrape_complete",
        source="mediaarts",
        **stats,
    )
    return stats


@app.command()
def main(
    max_records: int = typer.Option(
        0,
        "--max-records",
        "-n",
        help="Maximum number of anime per collection (0 = unlimited)",
    ),
    checkpoint: int = typer.Option(
        50, "--checkpoint", "-c", help="Checkpoint interval"
    ),
    version: str = typer.Option("latest", "--version", "-v", help="Dataset version"),
    data_dir: Path = typer.Option(
        DEFAULT_DATA_DIR, "--data-dir", "-d", help="Download directory"
    ),
    extended: bool = typer.Option(
        False,
        "--extended",
        help="Download extended metadata sets (201-205, 301-317, etc.)",
    ),
) -> None:
    """Fetch credit data from Media Arts DB dump."""
    from src.infra.logging import setup_logging
    from src.scrapers.logging_utils import configure_file_logging

    setup_logging()
    log_path = configure_file_logging("mediaarts")
    log.info("mediaarts_command_start", log_file=str(log_path))

    stats = asyncio.run(
        scrape_madb(
            data_dir=data_dir,
            version=version,
            max_anime=max_records,
            checkpoint_interval=checkpoint,
            extended=extended,
        )
    )

    log.info("madb_scrape_complete", **stats)


if __name__ == "__main__":
    app()
