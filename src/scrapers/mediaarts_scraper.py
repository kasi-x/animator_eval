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

from src.models import Anime, Credit, Person, parse_role

log = structlog.get_logger()

DATASET_REPO = "mediaarts-db/dataset"
GITHUB_API_BASE = "https://api.github.com"

# Anime collection files (series-level — contains key staff credits)
ANIME_COLLECTION_FILES: dict[str, str] = {
    "metadata207_json.zip": "AnimationTVRegularSeries",
    "metadata208_json.zip": "AnimationTVSpecialSeries",
    "metadata209_json.zip": "AnimationVideoPackageSeries",
    "metadata210_json.zip": "AnimationMovieSeries",
}

DEFAULT_DATA_DIR = Path("data/madb")

app = typer.Typer()


def parse_contributor_text(text: str) -> list[tuple[str, str]]:
    """Parse MADB contributor text into (role_ja, name_ja) pairs.

    Input:  "[脚本]仲倉重郎 ／ [演出]須永 司 ／ [作画監督]数井浩子"
    Output: [("脚本", "仲倉重郎"), ("演出", "須永 司"), ("作画監督", "数井浩子")]

    Handles variations:
    - Fullwidth brackets: ［脚本］ -> [脚本]
    - No brackets: "仲倉重郎" -> ("other", "仲倉重郎")
    - Multiple roles: "[脚本・演出]名前" -> [("脚本", "名前"), ("演出", "名前")]
    - Fullwidth slash: ／ (JSON-LD dump format)
    """
    if not text or not text.strip():
        return []

    # NFKC normalization (fullwidth brackets -> halfwidth, etc.)
    text = unicodedata.normalize("NFKC", text)

    results: list[tuple[str, str]] = []

    # Split on " / " or " ／ " (slash with surrounding whitespace only)
    # Slashes inside brackets (e.g. [脚本/演出]) are not split
    segments = re.split(r"\s+[/／]\s+", text)

    for segment in segments:
        segment = segment.strip()
        if not segment:
            continue

        # [role]name pattern
        match = re.match(r"\[([^\]]+)\]\s*(.+)", segment)
        if match:
            role_text = match.group(1).strip()
            name = match.group(2).strip()
            if not name:
                continue
            # Multiple roles: "脚本・演出" -> ["脚本", "演出"]
            roles = re.split(r"[・/]", role_text)
            for role in roles:
                role = role.strip()
                if role:
                    results.append((role, name))
        else:
            # No brackets -> role="other"
            name = segment.strip()
            if name:
                results.append(("other", name))

    return results


def make_madb_person_id(name_ja: str) -> str:
    """Generate a deterministic ID from the SHA256 hash of a normalized name.

    Format: "madb:p_{hash12}"
    Same name -> always same ID (idempotent).
    """
    # NFKC normalization + whitespace removal
    normalized = unicodedata.normalize("NFKC", name_ja)
    normalized = re.sub(r"\s+", "", normalized)  # Remove all whitespace
    hash_hex = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:12]
    return f"madb:p_{hash_hex}"


def normalize_title(title: str) -> str:
    """Normalize a title for comparison."""
    if not title:
        return ""
    title = unicodedata.normalize("NFKC", title)
    # Normalize whitespace
    title = re.sub(r"\s+", " ", title).strip()
    return title


def _extract_name_from_schema(name_field: str | list | dict) -> str:
    """Extract a title string from the schema:name field.

    JSON-LD may contain:
    - String: "タイトル"
    - List: ["タイトル", {"@value": "カタカナ", "@language": "ja-hrkt"}]
    - Dict: {"@value": "タイトル", "@language": "ja"}
    """
    if isinstance(name_field, str):
        return name_field
    if isinstance(name_field, list):
        for item in name_field:
            if isinstance(item, str):
                return item
            if isinstance(item, dict) and "@value" in item:
                lang = item.get("@language", "")
                if lang not in ("ja-hrkt",):  # Exclude katakana readings
                    return item["@value"]
        # Fall back to katakana reading if nothing else available
        for item in name_field:
            if isinstance(item, str):
                return item
            if isinstance(item, dict) and "@value" in item:
                return item["@value"]
        return ""
    if isinstance(name_field, dict) and "@value" in name_field:
        return name_field["@value"]
    return str(name_field) if name_field else ""


def _extract_year(item: dict) -> int | None:
    """Extract year from datePublished or startDate."""
    for field in ("schema:datePublished", "schema:startDate"):
        val = item.get(field, "")
        if val:
            if isinstance(val, dict):
                val = val.get("@value", "")
            if isinstance(val, str) and len(val) >= 4:
                try:
                    return int(val[:4])
                except ValueError:
                    pass
    return None


def _extract_studios(item: dict) -> list[str]:
    """Extract studio names from schema:productionCompany."""
    raw = item.get("schema:productionCompany", "")
    if not raw:
        return []
    if isinstance(raw, dict):
        raw = raw.get("@value", "")
    if not isinstance(raw, str):
        return []

    studios = []
    text = unicodedata.normalize("NFKC", raw)
    # Format: "[アニメーション制作]マッドハウス ／ [制作]東映"
    segments = re.split(r"\s+[/／]\s+", text)
    for seg in segments:
        seg = seg.strip()
        # [role]name pattern -> extract name
        match = re.match(r"\[([^\]]*)\]\s*(.+)", seg)
        if match:
            name = match.group(2).strip()
            if name:
                studios.append(name)
        elif seg:
            studios.append(seg)
    return studios


def parse_jsonld_dump(json_path: Path) -> list[dict]:
    """Parse a JSON-LD file and extract anime information.

    Returns: [{
        "id": "C10001",
        "title": "ギャラクシー エンジェル",
        "year": 2001,
        "contributors": [(role, name), ...],
        "studios": ["マッドハウス"],
    }]
    """
    with open(json_path, encoding="utf-8") as f:
        data = json.load(f)

    graph = data.get("@graph", [])
    results = []

    for item in graph:
        identifier = item.get("schema:identifier", "")
        if not identifier:
            continue

        # Title
        name_field = item.get("schema:name", "")
        title = _extract_name_from_schema(name_field)
        if not title:
            continue

        # Year
        year = _extract_year(item)

        # Merge contributor + creator + originalWorkCreator
        contributors: list[tuple[str, str]] = []
        for field in ("schema:contributor", "schema:creator", "ma:originalWorkCreator"):
            raw = item.get(field, "")
            if not raw:
                continue
            if isinstance(raw, dict):
                raw = raw.get("@value", "")
            if isinstance(raw, str) and raw.strip():
                parsed = parse_contributor_text(raw)
                contributors.extend(parsed)

        # Studios
        studios = _extract_studios(item)

        results.append(
            {
                "id": identifier,
                "title": title,
                "year": year,
                "contributors": contributors,
                "studios": studios,
            }
        )

    return results


async def download_madb_dataset(
    data_dir: Path,
    version: str = "latest",
) -> dict[str, Path]:
    """Download MADB dataset from GitHub Releases.

    Returns: {anime_type: extracted_json_path}
    """
    data_dir.mkdir(parents=True, exist_ok=True)
    version_file = data_dir / ".version"

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
        if version_file.exists() and version_file.read_text().strip() == tag:
            cached = {}
            all_cached = True
            for zip_name, anime_type in ANIME_COLLECTION_FILES.items():
                json_name = zip_name.replace("_json.zip", ".json")
                json_path = data_dir / json_name
                if json_path.exists():
                    cached[anime_type] = json_path
                else:
                    all_cached = False
                    break
            if all_cached:
                log.info("madb_dataset_cached", version=tag, files=len(cached))
                return cached

        # Build asset URL map
        assets = {
            a["name"]: a["browser_download_url"] for a in release.get("assets", [])
        }

        downloaded: dict[str, Path] = {}
        for zip_name, anime_type in ANIME_COLLECTION_FILES.items():
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
        log.info("madb_download_complete", version=tag, files=len(downloaded))
        return downloaded


async def scrape_madb(
    conn,
    data_dir: Path | None = None,
    version: str = "latest",
    max_anime: int = 10000,
    checkpoint_interval: int = 50,
) -> dict:
    """Fetch credit data from MADB dump.

    1. Download dump from GitHub Releases (with caching)
    2. Parse JSON-LD
    3. Save to DB (using existing upsert_anime/upsert_person/insert_credit)

    Args:
        conn: SQLite connection
        data_dir: Download destination directory
        version: Dataset version ("latest" or tag name)
        max_anime: Maximum number of anime to process
        checkpoint_interval: DB commit interval

    Returns:
        Statistics dict
    """
    from src.database import (
        insert_credit,
        update_data_source,
        upsert_anime,
        upsert_person,
    )

    if data_dir is None:
        data_dir = DEFAULT_DATA_DIR

    stats = {
        "anime_fetched": 0,
        "anime_with_contributors": 0,
        "credits_created": 0,
        "persons_created": 0,
        "parse_failures": 0,
    }
    person_cache: dict[str, Person] = {}  # name -> Person (dedup)

    # Phase A: Download dump
    dataset_files = await download_madb_dataset(data_dir, version=version)
    if not dataset_files:
        log.warning("madb_no_files_downloaded")
        return stats

    # Phase B: Parse JSON-LD + save to DB
    all_records: list[dict] = []
    for anime_type, json_path in dataset_files.items():
        log.info("madb_parsing", file=json_path.name, type=anime_type)
        records = parse_jsonld_dump(json_path)
        all_records.extend(records)
        log.info("madb_parsed", file=json_path.name, records=len(records))

    if len(all_records) > max_anime:
        all_records = all_records[:max_anime]

    stats["anime_fetched"] = len(all_records)
    log.info("madb_total_records", total=len(all_records))

    # Phase C: Save to DB
    for i, record in enumerate(all_records):
        madb_id = record["id"]
        title = record["title"]
        year = record["year"]
        contributors = record["contributors"]
        studios = record["studios"]

        if not contributors:
            stats["parse_failures"] += 1
            continue

        stats["anime_with_contributors"] += 1
        anime_id = f"madb:{madb_id}"

        anime = Anime(
            id=anime_id,
            title_ja=title,
            year=year,
            madb_id=madb_id,
            studios=studios,
        )
        upsert_anime(conn, anime)

        # Register credits
        for role_ja, name_ja in contributors:
            # Skip names that are too short (legal risk)
            if len(name_ja) < 2:
                continue

            # Get or create Person
            if name_ja not in person_cache:
                person_id = make_madb_person_id(name_ja)
                person = Person(
                    id=person_id,
                    name_ja=name_ja,
                    madb_id=person_id,
                )
                upsert_person(conn, person)
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
            insert_credit(conn, credit)
            stats["credits_created"] += 1

        # Checkpoint
        if (i + 1) % checkpoint_interval == 0:
            conn.commit()
            log.info(
                "madb_checkpoint",
                progress=f"{i + 1}/{len(all_records)}",
                credits=stats["credits_created"],
                persons=stats["persons_created"],
            )

    # Final commit
    conn.commit()
    update_data_source(conn, "mediaarts", stats["credits_created"])
    conn.commit()

    log.info(
        "madb_scrape_complete",
        source="mediaarts",
        **stats,
    )
    return stats


@app.command()
def main(
    max_records: int = typer.Option(
        10000, "--max-records", "-n", help="Maximum number of anime"
    ),
    checkpoint: int = typer.Option(
        50, "--checkpoint", "-c", help="Checkpoint interval"
    ),
    version: str = typer.Option("latest", "--version", "-v", help="Dataset version"),
    data_dir: Path = typer.Option(
        DEFAULT_DATA_DIR, "--data-dir", "-d", help="Download directory"
    ),
) -> None:
    """Fetch credit data from Media Arts DB dump."""
    from src.database import db_connection, init_db
    from src.log import setup_logging

    setup_logging()

    with db_connection() as conn:
        init_db(conn)
        stats = asyncio.run(
            scrape_madb(
                conn,
                data_dir=data_dir,
                version=version,
                max_anime=max_records,
                checkpoint_interval=checkpoint,
            )
        )

    log.info("madb_scrape_saved", **stats)


if __name__ == "__main__":
    app()
