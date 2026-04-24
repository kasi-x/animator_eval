"""bangumi/Archive dump downloader and extractor.

Fetches the weekly JSONLINES dump from https://github.com/bangumi/Archive,
extracts it to `data/bangumi/dump/<dump_tag>/`, and writes a manifest.json.

The dump zip uses streaming format (no central directory), so we use a custom
streaming extractor that scans local file headers directly.

Supported files (glob ``*.jsonlines`` from extract_dir):
    subject.jsonlines, person.jsonlines, character.jsonlines,
    episode.jsonlines, subject-relations.jsonlines,
    subject-characters.jsonlines, subject-persons.jsonlines,
    person-characters.jsonlines, person-relations.jsonlines

Usage (library):
    meta = await fetch_latest_release_meta()
    zip_path = await download_zip(meta["url"], dest_zip, on_progress=cb)
    paths = extract_zip(zip_path, extract_dir)
    manifest = build_manifest(extract_dir, meta["tag"])
"""

from __future__ import annotations

import datetime as dt
import hashlib
import struct
import zlib
from collections.abc import Callable
from pathlib import Path
from typing import Any

import httpx
import structlog

log = structlog.get_logger()

_VERSION = "0.1.0"
_USER_AGENT = f"animetor_eval/{_VERSION} (https://github.com/kashi-x)"

_LATEST_JSON_URL = (
    "https://raw.githubusercontent.com/bangumi/Archive/master/aux/latest.json"
)
_GITHUB_API_URL = "https://api.github.com/repos/bangumi/Archive/releases/latest"

# Chunk sizes
_DOWNLOAD_CHUNK = 8192
_SHA256_CHUNK = 512 * 1024  # 512 KB

# ZIP local-file-header magic bytes
_SIG_LOCAL = b"PK\x03\x04"
_SIG_DD = b"PK\x07\x08"
_SIG_CD = b"PK\x01\x02"
_SIG_EOCD = b"PK\x05\x06"
_SIG_EOCD64 = b"PK\x06\x06"


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


async def fetch_latest_release_meta() -> dict[str, Any]:
    """Fetch metadata for the latest bangumi Archive dump.

    Prefers ``aux/latest.json`` (includes SHA-256 digest) over the GitHub
    release API.  Falls back to the release API if ``latest.json`` is
    unreachable.

    Returns:
        dict with keys:
            tag (str)   — dump name, e.g. ``dump-2026-04-21.210419Z``
            url (str)   — direct download URL for the zip
            size (int)  — file size in bytes
            sha256 (str | None) — hex digest if available from latest.json

    Raises:
        httpx.HTTPStatusError: on non-2xx after retries
    """
    async with httpx.AsyncClient(
        headers={"User-Agent": _USER_AGENT},
        follow_redirects=True,
        timeout=30.0,
    ) as client:
        log.info("bangumi_dump_fetch_meta", source="latest.json")
        try:
            resp = await client.get(_LATEST_JSON_URL)
            resp.raise_for_status()
            data = resp.json()
            tag = _tag_from_name(data["name"])
            log.info(
                "bangumi_dump_meta_ok",
                tag=tag,
                size=data.get("size"),
                has_sha256=bool(data.get("digest")),
            )
            sha256 = _parse_sha256_digest(data.get("digest", ""))
            return {
                "tag": tag,
                "url": data["browser_download_url"],
                "size": data.get("size"),
                "sha256": sha256,
            }
        except Exception as e:
            log.warning("bangumi_dump_latest_json_failed", error=str(e))

        log.info("bangumi_dump_fetch_meta", source="github_api")
        resp = await client.get(
            _GITHUB_API_URL, headers={"Accept": "application/vnd.github+json"}
        )
        resp.raise_for_status()
        release = resp.json()
        assets = release.get("assets", [])
        # Pick the largest zip (most complete dump)
        zip_assets = [
            a for a in assets if a.get("name", "").endswith(".zip")
        ]
        if not zip_assets:
            raise ValueError("No zip assets found in latest GitHub release")
        best = max(zip_assets, key=lambda a: a.get("size", 0))
        tag = _tag_from_name(best["name"])
        log.info("bangumi_dump_meta_ok", tag=tag, size=best.get("size"))
        return {
            "tag": tag,
            "url": best["browser_download_url"],
            "size": best.get("size"),
            "sha256": None,
        }


async def download_zip(
    url: str,
    dest_zip: Path,
    *,
    on_progress: Callable[[int, int | None], None] | None = None,
) -> Path:
    """Download a zip file with streaming, reporting progress.

    Args:
        url:          Direct download URL (follows redirects).
        dest_zip:     Destination path for the .zip file.
        on_progress:  Optional callback ``(bytes_downloaded, total_bytes|None)``.
                      Called each chunk.

    Returns:
        Path to the downloaded file (same as ``dest_zip``).

    Raises:
        httpx.HTTPStatusError: on non-2xx
    """
    dest_zip.parent.mkdir(parents=True, exist_ok=True)
    log.info("bangumi_dump_download_start", url=url, dest=str(dest_zip))

    downloaded = 0
    async with httpx.AsyncClient(
        headers={"User-Agent": _USER_AGENT},
        follow_redirects=True,
        timeout=httpx.Timeout(connect=30.0, read=300.0, write=30.0, pool=30.0),
    ) as client:
        async with client.stream("GET", url) as resp:
            resp.raise_for_status()
            total = _content_length(resp)
            with dest_zip.open("wb") as fp:
                async for chunk in resp.aiter_bytes(chunk_size=_DOWNLOAD_CHUNK):
                    fp.write(chunk)
                    downloaded += len(chunk)
                    if on_progress is not None:
                        on_progress(downloaded, total)

    log.info(
        "bangumi_dump_download_done",
        dest=str(dest_zip),
        bytes=downloaded,
    )
    return dest_zip


def extract_zip(zip_path: Path, extract_dir: Path) -> list[Path]:
    """Extract a streaming zip (no central directory) to ``extract_dir``.

    bangumi/Archive zips use streaming mode with data descriptors (flag bit 3
    set, sizes absent from local headers).  Standard ``zipfile.ZipFile``
    requires an end-of-central-directory record, which these files lack.
    This function scans local file headers directly.

    Args:
        zip_path:    Path to the .zip file.
        extract_dir: Directory to extract into (created if absent).

    Returns:
        List of extracted file paths.

    Raises:
        ValueError: if the file does not start with a PK local-file-header.
    """
    extract_dir.mkdir(parents=True, exist_ok=True)
    log.info("bangumi_dump_extract_start", zip=str(zip_path), dest=str(extract_dir))

    extracted: list[Path] = []

    with zip_path.open("rb") as fp:
        while True:
            sig = fp.read(4)
            if len(sig) < 4:
                break
            if sig in (_SIG_CD, _SIG_EOCD, _SIG_EOCD64):
                break  # reached central directory
            if sig != _SIG_LOCAL:
                log.warning(
                    "bangumi_dump_extract_unexpected_sig",
                    sig=sig.hex(),
                    pos=fp.tell() - 4,
                )
                break

            fname, method, flags, comp_size_hdr = _read_local_header(fp)
            has_dd = bool(flags & 0x08)

            out_path = extract_dir / fname
            out_path.parent.mkdir(parents=True, exist_ok=True)

            log.debug(
                "bangumi_dump_extract_entry",
                name=fname,
                method=method,
                has_dd=has_dd,
            )

            if method == 0:
                _extract_stored(fp, out_path, comp_size_hdr)
            elif method == 8:
                _extract_deflate(fp, out_path, comp_size_hdr, has_dd)
            else:
                log.warning(
                    "bangumi_dump_extract_unsupported_method",
                    name=fname,
                    method=method,
                )
                if comp_size_hdr > 0:
                    fp.seek(comp_size_hdr, 1)

            extracted.append(out_path)
            _skip_data_descriptor(fp, has_dd)

    log.info(
        "bangumi_dump_extract_done",
        dest=str(extract_dir),
        file_count=len(extracted),
    )
    return extracted


def build_manifest(
    extract_dir: Path,
    release_tag: str,
) -> dict[str, Any]:
    """Build a manifest dict describing extracted files.

    Computes SHA-256 (512 KB chunks) and line count for each ``*.jsonlines``
    file found in ``extract_dir``.

    Args:
        extract_dir:  Directory containing extracted ``*.jsonlines`` files.
        release_tag:  Tag string (e.g. ``dump-2026-04-21.210419Z``).

    Returns:
        dict with keys:
            release_tag (str)
            downloaded_at (str)  — ISO-8601 UTC timestamp
            files (list[dict])   — [{name, size, sha256, line_count}]
    """
    files: list[dict[str, Any]] = []
    for p in sorted(extract_dir.glob("*.jsonlines")):
        sha256 = _sha256_file(p)
        line_count = sum(1 for _ in p.open(encoding="utf-8", errors="replace"))
        files.append(
            {
                "name": p.name,
                "size": p.stat().st_size,
                "sha256": sha256,
                "line_count": line_count,
            }
        )
        log.debug(
            "bangumi_dump_manifest_entry",
            name=p.name,
            sha256=sha256[:16] + "…",
            lines=line_count,
        )

    manifest: dict[str, Any] = {
        "release_tag": release_tag,
        "downloaded_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "files": files,
    }
    log.info(
        "bangumi_dump_manifest_built",
        tag=release_tag,
        file_count=len(files),
    )
    return manifest


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _tag_from_name(asset_name: str) -> str:
    """Extract dump tag from asset filename.

    ``dump-2026-04-21.210419Z.zip`` → ``dump-2026-04-21.210419Z``
    """
    return asset_name.removesuffix(".zip").removesuffix(".7z")


def _parse_sha256_digest(digest: str) -> str | None:
    """Parse ``sha256:hexhex`` or return None."""
    if digest.startswith("sha256:"):
        return digest[7:]
    return None or (digest if len(digest) == 64 else None)


def _content_length(resp: httpx.Response) -> int | None:
    """Return Content-Length header as int, or None if absent/invalid."""
    raw = resp.headers.get("content-length")
    if raw:
        try:
            return int(raw)
        except ValueError:
            pass
    return None


def _sha256_file(path: Path) -> str:
    """Compute SHA-256 of a file using 512 KB chunks."""
    h = hashlib.sha256()
    with path.open("rb") as fp:
        while True:
            chunk = fp.read(_SHA256_CHUNK)
            if not chunk:
                break
            h.update(chunk)
    return h.hexdigest()


def _read_local_header(fp) -> tuple[str, int, int, int]:
    """Read a local file header (28 bytes) after the PK\x03\x04 signature.

    Returns:
        (filename, compression_method, flags, comp_size_from_header)
    """
    buf = fp.read(26)
    (
        _version,
        flags,
        method,
        _mod_time,
        _mod_date,
        _crc32,
        comp_size,
        _uncomp_size,
        fname_len,
        extra_len,
    ) = struct.unpack("<HHHHHIIIHH", buf)

    fname = fp.read(fname_len).decode("utf-8", errors="replace")
    fp.read(extra_len)  # skip extra field
    return fname, method, flags, comp_size


def _extract_stored(fp, out_path: Path, comp_size: int) -> None:
    """Extract a stored (method=0) entry."""
    with out_path.open("wb") as out:
        remaining = comp_size
        while remaining > 0:
            chunk = fp.read(min(_DOWNLOAD_CHUNK, remaining))
            if not chunk:
                break
            out.write(chunk)
            remaining -= len(chunk)


def _extract_deflate(
    fp, out_path: Path, comp_size_hdr: int, has_dd: bool
) -> None:
    """Extract a deflate-compressed (method=8) entry.

    When ``has_dd`` is True the local header reports comp_size=0 and we must
    scan for the next PK signature to delimit the compressed stream.
    """
    dobj = zlib.decompressobj(-15)

    with out_path.open("wb") as out:
        if not has_dd and comp_size_hdr > 0:
            # Sizes known: simple bounded read
            remaining = comp_size_hdr
            while remaining > 0:
                chunk = fp.read(min(65536, remaining))
                if not chunk:
                    break
                out.write(dobj.decompress(chunk))
                remaining -= len(chunk)
        else:
            # Streaming: accumulate until next PK marker
            buf = bytearray()
            while True:
                chunk = fp.read(65536)
                if not chunk:
                    break
                buf.extend(chunk)
                pk_pos = _find_pk_sig(buf)
                if pk_pos is not None:
                    out.write(dobj.decompress(bytes(buf[:pk_pos])))
                    # Seek back so the next iteration reads the PK sig
                    excess = len(buf) - pk_pos
                    fp.seek(-excess, 1)
                    buf = bytearray()
                    break
                else:
                    # Keep last 3 bytes (PK sig might straddle chunks)
                    out.write(dobj.decompress(bytes(buf[:-3])))
                    buf = buf[-3:]

        try:
            out.write(dobj.flush())
        except zlib.error:
            pass


def _find_pk_sig(buf: bytearray) -> int | None:
    """Return position of the first PK signature in buf, or None."""
    for i in range(len(buf) - 3):
        b4 = buf[i : i + 4]
        if b4 in (_SIG_DD, _SIG_LOCAL, _SIG_CD, _SIG_EOCD, _SIG_EOCD64):
            return i
    return None


def _skip_data_descriptor(fp, has_dd: bool) -> None:
    """Consume the optional data descriptor that follows compressed data.

    The data descriptor contains CRC-32, compressed size, and uncompressed
    size (12 bytes), optionally preceded by the ``PK\x07\x08`` signature
    (4 bytes).
    """
    if not has_dd:
        return
    sig = fp.read(4)
    if sig == _SIG_DD:
        fp.read(12)
    elif sig in (_SIG_LOCAL, _SIG_CD, _SIG_EOCD, _SIG_EOCD64):
        fp.seek(-4, 1)  # not a data descriptor — put back
    else:
        # Plain data descriptor without signature (older spec)
        fp.read(8)  # comp_size + uncomp_size remain
