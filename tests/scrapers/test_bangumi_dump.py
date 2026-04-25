"""Unit tests for bangumi_dump.py.

Tests:
- SHA-256 helper produces known values
- manifest builder counts lines and includes sha256
- extract_zip works on a standard zip (zipfile-created)
- extract_zip works on a hand-crafted streaming zip (no EOCD) fixture
"""

from __future__ import annotations

import hashlib
import json
import zipfile
from pathlib import Path

from src.scrapers.bangumi_dump import (
    _sha256_file,
    build_manifest,
    extract_zip,
)

FIXTURES = Path(__file__).parent.parent / "fixtures" / "scrapers" / "bangumi"


# ---------------------------------------------------------------------------
# SHA-256 helper
# ---------------------------------------------------------------------------


def test_sha256_file_known_value(tmp_path: Path):
    """_sha256_file must produce the correct SHA-256 hex digest."""
    content = b"hello bangumi"
    expected = hashlib.sha256(content).hexdigest()

    p = tmp_path / "test.bin"
    p.write_bytes(content)
    assert _sha256_file(p) == expected


def test_sha256_file_empty(tmp_path: Path):
    """Empty file has a well-known SHA-256."""
    expected = hashlib.sha256(b"").hexdigest()
    p = tmp_path / "empty.bin"
    p.write_bytes(b"")
    assert _sha256_file(p) == expected


# ---------------------------------------------------------------------------
# build_manifest
# ---------------------------------------------------------------------------


def test_build_manifest_counts_lines(tmp_path: Path):
    """build_manifest counts lines in each .jsonlines file."""
    f = tmp_path / "subject.jsonlines"
    lines = [json.dumps({"id": i}) for i in range(7)]
    f.write_text("\n".join(lines) + "\n", encoding="utf-8")

    manifest = build_manifest(tmp_path, "dump-2026-04-21.210419Z")

    assert manifest["release_tag"] == "dump-2026-04-21.210419Z"
    assert len(manifest["files"]) == 1
    entry = manifest["files"][0]
    assert entry["name"] == "subject.jsonlines"
    assert entry["line_count"] == 7
    assert len(entry["sha256"]) == 64  # hex SHA-256


def test_build_manifest_multiple_files(tmp_path: Path):
    """build_manifest includes all .jsonlines files found."""
    for name in ("subject.jsonlines", "person.jsonlines", "character.jsonlines"):
        (tmp_path / name).write_text('{"id":1}\n', encoding="utf-8")

    manifest = build_manifest(tmp_path, "dump-2026-04-21.210419Z")
    assert len(manifest["files"]) == 3
    names = {e["name"] for e in manifest["files"]}
    assert names == {"subject.jsonlines", "person.jsonlines", "character.jsonlines"}


def test_build_manifest_sha256_matches_file_content(tmp_path: Path):
    """SHA-256 in manifest matches independently computed hash."""
    content = b"line1\nline2\nline3\n"
    f = tmp_path / "episode.jsonlines"
    f.write_bytes(content)

    manifest = build_manifest(tmp_path, "dump-x")
    entry = manifest["files"][0]
    assert entry["sha256"] == hashlib.sha256(content).hexdigest()


def test_build_manifest_includes_downloaded_at(tmp_path: Path):
    """Manifest includes a non-empty downloaded_at ISO timestamp."""
    (tmp_path / "a.jsonlines").write_text("{}\n", encoding="utf-8")
    manifest = build_manifest(tmp_path, "dump-x")
    assert "downloaded_at" in manifest
    assert manifest["downloaded_at"]  # non-empty string


# ---------------------------------------------------------------------------
# extract_zip — standard (zipfile-created) zip
# ---------------------------------------------------------------------------


def _make_standard_zip(tmp_path: Path, filename: str, content: bytes) -> Path:
    """Build a normal (EOCD-containing) zip and return its path."""
    zip_path = tmp_path / "standard.zip"
    with zipfile.ZipFile(zip_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(filename, content)
    return zip_path


def test_extract_zip_standard_zip(tmp_path: Path):
    """extract_zip should handle a standard zipfile-created zip."""
    content = b"subject data line 1\nsubject data line 2\n"
    zip_path = _make_standard_zip(tmp_path, "subject.jsonlines", content)
    extract_dir = tmp_path / "out"

    extracted = extract_zip(zip_path, extract_dir)

    assert len(extracted) >= 1
    out_file = extract_dir / "subject.jsonlines"
    assert out_file.exists()
    assert out_file.read_bytes() == content


# ---------------------------------------------------------------------------
# extract_zip — hand-crafted streaming zip (no EOCD) fixture
# ---------------------------------------------------------------------------


def test_extract_zip_streaming_fixture(tmp_path: Path):
    """extract_zip recovers file from the streaming zip fixture (no central directory)."""
    fixture = FIXTURES / "streaming.zip"
    assert fixture.exists(), f"Fixture not found: {fixture}"

    extract_dir = tmp_path / "streaming_out"
    extracted = extract_zip(fixture, extract_dir)

    assert len(extracted) >= 1
    # The fixture contains 'test_entry.jsonlines' with known content
    out_file = extract_dir / "test_entry.jsonlines"
    assert out_file.exists(), f"Expected file not extracted: {out_file}"
    content = out_file.read_bytes()
    assert content == b"hello bangumi streaming zip\n"


def test_extract_zip_invalid_file_raises(tmp_path: Path):
    """extract_zip on a non-zip file should not crash fatally — it may warn and return []."""
    bad_zip = tmp_path / "bad.zip"
    bad_zip.write_bytes(b"this is not a zip file at all")

    # Should not raise; returns empty list or partial list
    extracted = extract_zip(bad_zip, tmp_path / "out")
    assert isinstance(extracted, list)
