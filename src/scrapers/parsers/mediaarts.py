"""Media Arts Database (MADB) JSON-LD parsers.

Extended parser coverage (§10.2):
  - parse_broadcasters        → broadcaster list from schema:publisher
  - parse_broadcast_schedule  → time-slot text from ma:periodDisplayed
  - parse_production_committee → committee members (製作/著作/製作委員会 labels)
  - parse_production_companies → animation studios (アニメーション制作 etc.), is_main flag
  - parse_video_releases      → video package release info (ma:mediaFormat etc.)
  - parse_original_work_link  → original work name/creator + madb series @id
"""

from __future__ import annotations

import hashlib
import json
import re
import unicodedata
from dataclasses import dataclass
from pathlib import Path


def parse_contributor_text(text: str) -> list[tuple[str, str]]:
    """Parse MADB contributor text into (role_ja, name_ja) pairs.

    Input:  "[脚本]仲倉重郎 ／ [演出]須永 司 ／ [作画監督]数井浩子"
    Output: [("脚本", "仲倉重郎"), ("演出", "須永 司"), ("作画監督", "数井浩子")]

    Handles variations:
    - Fullwidth brackets: ［脚本］ -> [脚本]
    - No brackets: "仲倉重郎" -> ("other", "仲倉重郎")
    - Multiple roles: "[脚本・演出]名前" -> [("脚本", "名前"), ("演出", "名前")]
    - Fullwidth slash: ／ (JSON-LD dump format)
    - Trailing role suffix: "日映科学映画製作所[製作]" -> ("製作", "日映科学映画製作所")
      (madb の一部レコードで観測される、規約外だが防御的に対応)
    - Wrap-only brackets: "[こだま兼嗣]" -> ("other", "こだま兼嗣")
      (role 部分が name で名前が空の異常 — 中身を name として救済)
    """
    if not text or not text.strip():
        return []

    text = unicodedata.normalize("NFKC", text)

    results: list[tuple[str, str]] = []
    segments = re.split(r"\s+[/／]\s+", text)

    for segment in segments:
        segment = segment.strip()
        if not segment:
            continue

        # 1. 先頭 [role]name (公式形式) — name 空の "[name]" 単独も拾う
        match = re.match(r"\[([^\]]+)\]\s*(.*)$", segment)
        if match:
            role_text = match.group(1).strip()
            name = match.group(2).strip()
            if not name:
                # "[こだま兼嗣]" 単独 → role 部分を name として救済
                if role_text:
                    results.append(("other", role_text))
                continue
            roles = re.split(r"[・/]", role_text)
            for role in roles:
                role = role.strip()
                if role:
                    results.append((role, name))
            continue

        # 2. 末尾 name[role] (madb 異常データ対策)
        m_suffix = re.match(r"^(.+?)\s*[\[【]([^\]】]*)[\]】]\s*$", segment)
        if m_suffix:
            name = m_suffix.group(1).strip()
            role_text = m_suffix.group(2).strip()
            if name:
                if role_text:
                    roles = re.split(r"[・/]", role_text)
                    for role in roles:
                        role = role.strip()
                        if role:
                            results.append((role, name))
                else:
                    results.append(("other", name))
            continue

        # 3. plain
        results.append(("other", segment))

    return results


def make_madb_person_id(name_ja: str) -> str:
    """Generate a deterministic ID from the SHA256 hash of a normalized name.

    Format: "madb:p_{hash12}"
    Same name -> always same ID (idempotent).
    """
    normalized = unicodedata.normalize("NFKC", name_ja)
    normalized = re.sub(r"\s+", "", normalized)
    hash_hex = hashlib.sha256(normalized.encode("utf-8")).hexdigest()[:12]
    return f"madb:p_{hash_hex}"


def normalize_title(title: str) -> str:
    """Normalize a title for comparison."""
    if not title:
        return ""
    title = unicodedata.normalize("NFKC", title)
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
                if lang not in ("ja-hrkt",):
                    return item["@value"]
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
    for date_field in ("schema:datePublished", "schema:startDate"):
        val = item.get(date_field, "")
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
    segments = re.split(r"\s+[/／]\s+", text)
    for seg in segments:
        seg = seg.strip()
        match = re.match(r"\[([^\]]*)\]\s*(.+)", seg)
        if match:
            name = match.group(2).strip()
            if name:
                studios.append(name)
        elif seg:
            studios.append(seg)
    return studios


# ---------------------------------------------------------------------------
# §10.2 new dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Broadcaster:
    """A single broadcaster entry from schema:publisher."""

    madb_id: str
    name: str
    is_network_station: bool  # True when value contains ほか/・/、 (multi-station)


@dataclass(frozen=True, slots=True)
class BroadcastSchedule:
    """Broadcast time-slot text from ma:periodDisplayed."""

    madb_id: str
    raw_text: str  # immutable; structured extraction is intentionally deferred


@dataclass(frozen=True, slots=True)
class CommitteeMember:
    """One member of a 製作委員会 / joint production body."""

    madb_id: str
    company_name: str
    role_label: str  # e.g. "製作", "著作", "制作・著作", "製作委員会"


@dataclass(frozen=True, slots=True)
class ProductionCompany:
    """Anime production studio with main/support flag."""

    madb_id: str
    company_name: str
    role_label: str   # e.g. "アニメーション制作", "制作プロダクション"
    is_main: bool     # True = primary animation studio, False = support/협력


@dataclass(frozen=True, slots=True)
class VideoRelease:
    """Video package release record (DVD/BD/VHS).

    Populated from AnimationVideoPackage items (metadata202 etc.).
    """

    madb_id: str              # package item identifier (e.g. M1000000)
    series_madb_id: str       # parent series via schema:isPartOf
    media_format: str         # e.g. "DVD", "Blu-ray", "VHS"
    date_published: str       # raw datePublished string
    publisher: str            # distributor/label
    product_id: str           # schema:productID (catalogue number)
    gtin: str                 # schema:gtin (JAN/EAN barcode)
    runtime_min: int | None   # schema:duration (minutes)
    volume_number: str        # schema:volumeNumber
    release_title: str        # ma:releasedTitle (if different from series title)


@dataclass(frozen=True, slots=True)
class OriginalWorkLink:
    """Link to the original work (manga / LN / game / etc.)."""

    madb_id: str           # anime series identifier
    work_name: str         # ma:originalWorkName (cleaned; brackets stripped)
    creator_text: str      # ma:originalWorkCreator (raw; may have role labels)
    series_link_id: str    # from schema:isPartOf @id (madb series URI fragment)


# ---------------------------------------------------------------------------
# Labels used to classify production company entries
# ---------------------------------------------------------------------------

# Labels that identify the core animation studio (is_main=True)
_MAIN_PRODUCTION_LABELS: frozenset[str] = frozenset(
    [
        "アニメーション制作",
        "アニメーション 制作",
        "アニメーション製作",
        "アニメーション制作プロダクション",
        "アニメ制作",
        "アニメ製作",
        "制作プロダクション",
        "animation production",
        "animation_production",
        "animation by",
        "animated by",
    ]
)

# Labels that identify production committee / rights-holder entries
_COMMITTEE_LABELS: frozenset[str] = frozenset(
    [
        "製作",
        "製作委員会",
        "製作著作",
        "制作・著作",
        "著作",
        "製作・著作",
        "製作 著作",
        "制作著作",
        "produced by",
        "production",
    ]
)


def _normalize_label(label: str) -> str:
    """Lowercase + NFKC normalize a role label for comparison."""
    return unicodedata.normalize("NFKC", label).strip().lower()


def _split_production_company_segments(raw: str) -> list[tuple[str, str]]:
    """Split productionCompany text into (role_label, company_name) pairs.

    対応形式:
    - 先頭 "[役割]会社名"      (公式形式)
    - 末尾 "会社名[役割]"      (madb 異常データ、`日映科学映画製作所[製作]` 等)
    - 包み "[会社名]"          (role 空 → company として救済)
    - bare "会社名"
    Returns empty list for missing/non-string input.
    """
    if not raw or not isinstance(raw, str):
        return []
    text = unicodedata.normalize("NFKC", raw)
    segments = re.split(r"\s+[/／]\s+", text)
    pairs: list[tuple[str, str]] = []
    for seg in segments:
        seg = seg.strip()
        if not seg:
            continue
        # 1. 先頭 [role]name
        m = re.match(r"\[([^\]]*)\]\s*(.+)", seg)
        if m:
            role = m.group(1).strip()
            name = m.group(2).strip()
            if name:
                pairs.append((role, name))
            elif role:
                # "[会社名]" 単独 → role 部分を name として救済
                pairs.append(("", role))
            continue
        # 2. 末尾 name[role]
        m_suffix = re.match(r"^(.+?)\s*[\[【]([^\]】]*)[\]】]\s*$", seg)
        if m_suffix:
            name = m_suffix.group(1).strip()
            role = m_suffix.group(2).strip()
            if name:
                pairs.append((role, name))
            continue
        # 3. plain
        pairs.append(("", seg))
    return pairs


# ---------------------------------------------------------------------------
# §10.2 parser functions
# ---------------------------------------------------------------------------


def parse_broadcasters(item: dict) -> list[Broadcaster]:
    """Extract broadcaster list from schema:publisher.

    schema:publisher is a free-text field holding broadcaster codes or names,
    e.g. "TX", "CX", "ABC、TOKYO MX、BS11、AT-Xほか".

    Args:
        item: Single JSON-LD graph item dict.

    Returns:
        List of Broadcaster dataclasses (empty list if field absent).
    """
    madb_id = item.get("schema:identifier", "")
    raw = item.get("schema:publisher", "")
    if not raw:
        return []
    if isinstance(raw, dict):
        raw = raw.get("@value", "") or raw.get("@id", "")
    if not isinstance(raw, str) or not raw.strip():
        return []

    text = unicodedata.normalize("NFKC", raw).strip()
    is_multi = bool(re.search(r"[、・,]|ほか", text))

    # Split on common separator patterns into individual broadcaster tokens
    tokens = re.split(r"[、・,]+|\s+ほか", text)
    broadcasters: list[Broadcaster] = []
    for tok in tokens:
        tok = tok.strip()
        if tok and tok != "ほか":
            broadcasters.append(
                Broadcaster(
                    madb_id=madb_id,
                    name=tok,
                    is_network_station=is_multi,
                )
            )

    # Fallback: raw value was not split-worthy, return as single entry
    if not broadcasters and text:
        broadcasters.append(
            Broadcaster(madb_id=madb_id, name=text, is_network_station=False)
        )

    return broadcasters


def parse_broadcast_schedule(item: dict) -> BroadcastSchedule | None:
    """Extract broadcast schedule text from ma:periodDisplayed.

    The field is free-form prose (e.g. "「プチプチ・アニメ」（木曜8:30～8:35/NHK教育）枠内で放送。").
    Structured parsing of this field is deferred; raw text is preserved in BRONZE.

    Args:
        item: Single JSON-LD graph item dict.

    Returns:
        BroadcastSchedule if field present, else None.
    """
    madb_id = item.get("schema:identifier", "")
    raw = item.get("ma:periodDisplayed", "")
    if not raw or not isinstance(raw, str) or not raw.strip():
        return None
    text = unicodedata.normalize("NFKC", raw).strip()
    return BroadcastSchedule(madb_id=madb_id, raw_text=text)


def parse_production_committee(item: dict) -> list[CommitteeMember]:
    """Extract 製作委員会 members from schema:productionCompany.

    Committee members are identified by their role label falling into
    the committee label set (製作, 著作, 製作委員会, etc.).

    Args:
        item: Single JSON-LD graph item dict.

    Returns:
        List of CommitteeMember (empty list if none found).
    """
    madb_id = item.get("schema:identifier", "")
    raw = item.get("schema:productionCompany", "")
    pairs = _split_production_company_segments(raw)
    members: list[CommitteeMember] = []
    for role_label, name in pairs:
        if _normalize_label(role_label) in _COMMITTEE_LABELS:
            members.append(
                CommitteeMember(
                    madb_id=madb_id,
                    company_name=name,
                    role_label=role_label,
                )
            )
    return members


def parse_production_companies(item: dict) -> list[ProductionCompany]:
    """Extract production companies (主 + 協力) from schema:productionCompany.

    Companies with a main animation studio label (アニメーション制作 etc.) get
    is_main=True; all others get is_main=False.

    Note: Entries without a role bracket (bare company name) are included with
    an empty role_label and is_main=False.

    Args:
        item: Single JSON-LD graph item dict.

    Returns:
        List of ProductionCompany (empty list if field absent).
    """
    madb_id = item.get("schema:identifier", "")
    raw = item.get("schema:productionCompany", "")
    pairs = _split_production_company_segments(raw)
    companies: list[ProductionCompany] = []
    for role_label, name in pairs:
        norm = _normalize_label(role_label)
        is_main = norm in _MAIN_PRODUCTION_LABELS
        companies.append(
            ProductionCompany(
                madb_id=madb_id,
                company_name=name,
                role_label=role_label,
                is_main=is_main,
            )
        )
    return companies


def parse_video_releases(item: dict) -> list[VideoRelease]:
    """Extract video release info from a VideoPackage item.

    Applicable to AnimationVideoPackage type items (metadata202, etc.).
    Series-level items (AnimationVideoPackageSeries) are typically parents;
    package items carry the release-level metadata.

    Args:
        item: Single JSON-LD graph item dict.

    Returns:
        List with one VideoRelease, or empty list if insufficient data.
    """
    madb_id = item.get("schema:identifier", "")
    if not madb_id:
        return []

    # Require at least a media format or date to constitute a meaningful record
    media_format_raw = item.get("ma:mediaFormat", "") or ""
    date_published = item.get("schema:datePublished", "") or ""
    if isinstance(date_published, dict):
        date_published = date_published.get("@value", "")

    if not media_format_raw and not date_published:
        return []

    # Normalise media format label
    media_format = unicodedata.normalize("NFKC", str(media_format_raw)).strip()

    # Derive parent series ID from schema:isPartOf
    series_link = item.get("schema:isPartOf", {})
    if isinstance(series_link, dict):
        series_uri = series_link.get("@id", "")
        # Extract the C-number from URI: ".../id/C12345"
        m = re.search(r"/id/([^/]+)$", series_uri)
        series_madb_id = m.group(1) if m else ""
    else:
        series_madb_id = ""

    publisher_raw = (
        item.get("schema:publisher", "")
        or item.get("ma:publisher", "")
        or ""
    )
    if isinstance(publisher_raw, dict):
        publisher_raw = publisher_raw.get("@value", "")
    publisher = unicodedata.normalize("NFKC", str(publisher_raw)).strip()

    product_id_raw = item.get("schema:productID", "") or ""
    product_id = unicodedata.normalize("NFKC", str(product_id_raw)).strip()

    gtin_raw = item.get("schema:gtin", "") or ""
    gtin = str(gtin_raw).strip()

    duration_raw = item.get("schema:duration", None)
    runtime_min: int | None = None
    if duration_raw is not None:
        try:
            runtime_min = int(duration_raw)
        except (ValueError, TypeError):
            pass

    volume_raw = item.get("schema:volumeNumber", "") or ""
    volume_number = unicodedata.normalize("NFKC", str(volume_raw)).strip()

    released_title_raw = item.get("ma:releasedTitle", "") or ""
    release_title = unicodedata.normalize("NFKC", str(released_title_raw)).strip()

    return [
        VideoRelease(
            madb_id=madb_id,
            series_madb_id=series_madb_id,
            media_format=media_format,
            date_published=str(date_published),
            publisher=publisher,
            product_id=product_id,
            gtin=gtin,
            runtime_min=runtime_min,
            volume_number=volume_number,
            release_title=release_title,
        )
    ]


def _clean_bracket_text(text: str) -> str:
    """Strip leading bracket labels like ［原作］「タイトル」 -> タイトル.

    Also strips surrounding 「」 quotes.
    """
    text = unicodedata.normalize("NFKC", text).strip()
    # Remove bracket label at start: [原作] or ［原作］
    text = re.sub(r"^\[[^\]]*\]\s*", "", text)
    # Remove Japanese corner brackets
    text = text.strip("「」『』")
    return text.strip()


def parse_original_work_link(item: dict) -> OriginalWorkLink | None:
    """Extract original work information (manga/LN/game source link).

    Uses:
      - ma:originalWorkName  → cleaned work title
      - ma:originalWorkCreator → raw creator text (may contain role labels)
      - schema:isPartOf → series-level MADB URI (series collection parent)

    Args:
        item: Single JSON-LD graph item dict.

    Returns:
        OriginalWorkLink if at least one of originalWorkName / originalWorkCreator
        is present, else None.
    """
    madb_id = item.get("schema:identifier", "")
    if not madb_id:
        return None

    work_name_raw = item.get("ma:originalWorkName", "") or ""
    creator_text_raw = item.get("ma:originalWorkCreator", "") or ""

    if not work_name_raw and not creator_text_raw:
        return None

    work_name = _clean_bracket_text(str(work_name_raw))
    creator_text = unicodedata.normalize("NFKC", str(creator_text_raw)).strip()

    # Extract series link ID from schema:isPartOf
    series_link = item.get("schema:isPartOf", {})
    if isinstance(series_link, dict):
        series_uri = series_link.get("@id", "")
        m = re.search(r"/id/([^/]+)$", series_uri)
        series_link_id = m.group(1) if m else ""
    else:
        series_link_id = ""

    return OriginalWorkLink(
        madb_id=madb_id,
        work_name=work_name,
        creator_text=creator_text,
        series_link_id=series_link_id,
    )


def parse_jsonld_dump(json_path: Path, format_code: str = "") -> list[dict]:
    """Parse a JSON-LD file and extract anime information.

    Returns: [{
        "id": "C10001",
        "title": "ギャラクシー エンジェル",
        "year": 2001,
        "format": "TV",
        "contributors": [(role, name), ...],
        "studios": ["マッドハウス"],
        # §10.2 additions:
        "broadcasters": [Broadcaster, ...],
        "broadcast_schedule": BroadcastSchedule | None,
        "production_committee": [CommitteeMember, ...],
        "production_companies": [ProductionCompany, ...],
        "video_releases": [VideoRelease, ...],
        "original_work_link": OriginalWorkLink | None,
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

        name_field = item.get("schema:name", "")
        title = _extract_name_from_schema(name_field)
        if not title:
            continue

        year = _extract_year(item)

        contributors: list[tuple[str, str]] = []
        for fld in ("schema:contributor", "schema:creator", "ma:originalWorkCreator"):
            raw = item.get(fld, "")
            if not raw:
                continue
            if isinstance(raw, dict):
                raw = raw.get("@value", "")
            if isinstance(raw, str) and raw.strip():
                parsed = parse_contributor_text(raw)
                contributors.extend(parsed)

        studios = _extract_studios(item)

        results.append(
            {
                "id": identifier,
                "title": title,
                "year": year,
                "format": format_code,
                "contributors": contributors,
                "studios": studios,
                # §10.2 additions
                "broadcasters": parse_broadcasters(item),
                "broadcast_schedule": parse_broadcast_schedule(item),
                "production_committee": parse_production_committee(item),
                "production_companies": parse_production_companies(item),
                "video_releases": parse_video_releases(item),
                "original_work_link": parse_original_work_link(item),
            }
        )

    return results
