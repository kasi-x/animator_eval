"""keyframe_vs_others_diff.csv 深掘り分析.

各 entity × field の差分を heuristics で分類:

person.name_en:
  - case_only       大小文字違いのみ
  - punct_only      句読点/記号のみ違い
  - whitespace      whitespace 違い
  - word_reorder    トークン同一・順序違い (姓名順入替)
  - subset          片方がもう片方の部分集合 (middle name 有無等)
  - levenshtein_le2 編集距離 ≤2 (typo/transliteration 微差)
  - encoding        macron / accent / katakana long mark 等の文字差
  - distinct        上記いずれにも該当しない (実体違い疑い含)

credit_role.role:
  - same_after_normalize  case/whitespace 正規化で一致
  - hierarchy_pair        既知の (parent, child) 役職階層
  - distinct_category     全く別カテゴリ

anime 系日付:
  - year_only             year 単位は一致、月日違い
  - month_only            月単位は一致、日違い
  - same_after_format     ISO/JP format 違い
  - distinct              実日付違い

出力: 標準出力 + result/keyframe_diff_taxonomy.{csv,md}
"""

from __future__ import annotations

import csv
import re
import unicodedata
from collections import Counter
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent.parent
INPUT_CSV = REPO_ROOT / "result" / "keyframe_vs_others_diff.csv"
OUT_CSV = REPO_ROOT / "result" / "keyframe_diff_taxonomy.csv"
OUT_MD = REPO_ROOT / "result" / "keyframe_diff_taxonomy.md"


# ───────────────── name classifiers ─────────────────


def _lev_at_most(a: str, b: str, k: int) -> bool:
    """O(|a|*k) 距離判定: <= k かどうかだけ."""
    if abs(len(a) - len(b)) > k:
        return False
    if a == b:
        return True
    n, m = len(a), len(b)
    prev = list(range(m + 1))
    for i in range(1, n + 1):
        cur = [i] + [0] * m
        lo, hi = max(1, i - k), min(m, i + k)
        for j in range(1, m + 1):
            if lo <= j <= hi:
                cost = 0 if a[i - 1] == b[j - 1] else 1
                cur[j] = min(prev[j] + 1, cur[j - 1] + 1, prev[j - 1] + cost)
            else:
                cur[j] = k + 1
        if min(cur[lo : hi + 1]) > k:
            return False
        prev = cur
    return prev[m] <= k


_PUNCT_RE = re.compile(r"[\s　\-\._'\"’“”‘’,!\?:;\(\)\[\]\\/]+")


def _strip_punct(s: str) -> str:
    return _PUNCT_RE.sub("", s)


def _norm_case(s: str) -> str:
    return s.casefold()


def _norm_unicode(s: str) -> str:
    """NFKD 分解 → diacritic 除去 → 正規化."""
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c))


def _tokens(s: str) -> list[str]:
    return [t for t in _PUNCT_RE.split(s) if t]


def classify_name(a: str, b: str) -> str:
    if not a or not b:
        return "missing_side"
    if a == b:
        return "identical"  # should not appear (we filter equals upstream)
    if a.strip() == b.strip():
        return "whitespace"
    if _norm_case(a) == _norm_case(b):
        return "case_only"
    if _strip_punct(a) == _strip_punct(b):
        return "punct_only"
    if _norm_case(_strip_punct(a)) == _norm_case(_strip_punct(b)):
        return "case_punct"
    ta, tb = sorted(_tokens(_norm_case(a))), sorted(_tokens(_norm_case(b)))
    if ta == tb and ta:
        return "word_reorder"
    set_a, set_b = set(ta), set(tb)
    if set_a and set_b and (set_a < set_b or set_b < set_a):
        return "subset"
    if _norm_unicode(_norm_case(a)) == _norm_unicode(_norm_case(b)):
        return "encoding_diacritic"
    if _lev_at_most(a.casefold(), b.casefold(), 2):
        return "levenshtein_le2"
    return "distinct"


# ───────────────── role classifier ─────────────────

# Known hierarchy pairs (parent → children). Source: src/utils/role_groups.py
# rough scaffold; expand as needed.
ROLE_HIERARCHY: dict[str, set[str]] = {
    "animation_director": {
        "key_animator",
        "in_between_animator",
        "second_key_animator",
        "chief_animation_director",
        "general_animation_director",
    },
    "director": {
        "episode_director",
        "assistant_director",
        "series_director",
    },
    "writer": {"screenplay", "series_composition"},
    "art": {"art_director", "background_artist", "art_setting"},
    "sound": {"sound_director", "sound_effect", "audio_director"},
}


def classify_role(a: str, b: str) -> str:
    if not a or not b:
        return "missing_side"
    if a == b:
        return "identical"
    na, nb = a.casefold().strip(), b.casefold().strip()
    if na == nb:
        return "whitespace_case"
    for parent, children in ROLE_HIERARCHY.items():
        ps, cs = {parent}, children
        if (na in ps and nb in cs) or (nb in ps and na in cs):
            return f"hierarchy_pair:{parent}"
        if na in cs and nb in cs and na != nb:
            return f"sibling:{parent}"
    return "distinct_category"


# ───────────────── date classifier ─────────────────

_DATE_ISO_RE = re.compile(r"^(\d{4})-(\d{1,2})-(\d{1,2})")
_DATE_SLASH_RE = re.compile(r"^(\d{4})/(\d{1,2})/(\d{1,2})")
_DATE_YEAR_RE = re.compile(r"^(\d{4})$")
_DATE_JSON_RE = re.compile(
    r'\{"year":\s*(\d+|null),'
    r'\s*"month":\s*(\d+|null),'
    r'\s*"day":\s*(\d+|null)\}'
)


def _norm_date(s: str) -> tuple[str, str, str]:
    """Return (year, month, day) as zero-padded strings, '' if missing."""
    if not s:
        return "", "", ""
    s = s.strip()
    for pat in (_DATE_ISO_RE, _DATE_SLASH_RE):
        m = pat.match(s)
        if m:
            return (m.group(1), m.group(2).zfill(2), m.group(3).zfill(2))
    m = _DATE_YEAR_RE.match(s)
    if m:
        return (m.group(1), "", "")
    m = _DATE_JSON_RE.search(s)
    if m:
        y, mo, d = m.group(1), m.group(2), m.group(3)
        return (
            y if y != "null" else "",
            mo.zfill(2) if mo != "null" else "",
            d.zfill(2) if d != "null" else "",
        )
    return "", "", ""


def classify_date(a: str, b: str) -> str:
    if not a or not b:
        return "missing_side"
    ya, ma, da = _norm_date(a)
    yb, mb, db = _norm_date(b)
    if not ya and not yb:
        return "unparseable"
    if (ya, ma, da) == (yb, mb, db):
        return "same_after_format"
    # JSON-vs-ISO 検出 (kf 側は JSON 形式, other は ISO)
    if (
        _DATE_JSON_RE.search(a or "") and _DATE_ISO_RE.match((b or "").strip())
    ) or (
        _DATE_JSON_RE.search(b or "") and _DATE_ISO_RE.match((a or "").strip())
    ):
        if (ya, ma, da) == (yb, mb, db):
            return "json_vs_iso_match"
    if ya == yb and ma == mb and da != db:
        return "day_only"
    if ya == yb and ma != mb:
        return "month_diff"
    if ya != yb:
        return "year_diff"
    return "distinct"


# ───────────────── enum classifier (season / source_mat / status / format) ─────────────────


def classify_enum(a: str, b: str) -> str:
    if not a or not b:
        return "missing_side"
    if a == b:
        return "identical"
    if a.strip().casefold() == b.strip().casefold():
        return "case_only"
    return "distinct_category"


# ───────────────── source_mat: keyframe scraper bug detector ─────────────────

KNOWN_SOURCE_NAMES = {
    "anilist", "mal", "ann", "seesaa", "madb", "bgm", "tmdb", "keyframe", "kf",
    "sakuga", "allcinema",
}


def classify_source_mat(a: str, b: str) -> str:
    if not a or not b:
        return "missing_side"
    if a == b:
        return "identical"
    if a.strip().casefold() == b.strip().casefold():
        return "case_only"
    # kf 側が source 名を入れている = parser バグ
    if a.strip().casefold() in KNOWN_SOURCE_NAMES:
        return "kf_bug_source_name_in_field"
    if b.strip().casefold() in KNOWN_SOURCE_NAMES:
        return "other_bug_source_name_in_field"
    return "distinct_category"


# ───────────────── driver ─────────────────


def main() -> None:
    if not INPUT_CSV.exists():
        raise SystemExit(f"missing input: {INPUT_CSV}")

    rows = []
    with INPUT_CSV.open(encoding="utf-8") as fp:
        rows = list(csv.DictReader(fp))
    print(f"loaded rows: {len(rows)}")

    # bucket: (entity, field) -> Counter(class)
    buckets: dict[tuple[str, str], Counter] = {}
    classified_rows: list[dict[str, str]] = []

    for r in rows:
        e, f = r["entity_type"], r["field"]
        a, b = r["kf_value"], r["other_value"]
        if e == "person" and f in ("name_en", "name_ja", "name_ko", "name_zh"):
            cls = classify_name(a, b)
        elif e == "anime" and f in ("title_en", "title_ja"):
            cls = classify_name(a, b)
        elif e == "studio" and f == "name":
            cls = classify_name(a, b)
        elif e == "credit_role" and f == "role":
            cls = classify_role(a, b)
        elif e == "anime" and f in ("start_date", "end_date"):
            cls = classify_date(a, b)
        elif e == "anime" and f == "source_mat":
            cls = classify_source_mat(a, b)
        elif e == "anime" and f in (
            "season",
            "format",
            "status",
            "country_of_origin",
            "duration",
            "episodes",
        ):
            cls = classify_enum(a, b)
        elif e == "person" and f in ("gender", "nationality", "birth_date", "death_date"):
            cls = classify_enum(a, b)
        elif e == "studio" and f in ("country_of_origin", "is_animation_studio"):
            cls = classify_enum(a, b)
        else:
            cls = "uncategorized"
        buckets.setdefault((e, f), Counter())[cls] += 1
        new_r = dict(r)
        new_r["diff_class"] = cls
        classified_rows.append(new_r)

    # write CSV
    with OUT_CSV.open("w", newline="", encoding="utf-8") as fp:
        w = csv.DictWriter(fp, fieldnames=list(classified_rows[0].keys()))
        w.writeheader()
        w.writerows(classified_rows)
    print(f"output: {OUT_CSV}")

    # write markdown report
    lines = ["# keyframe vs others diff — taxonomy", ""]
    lines.append(f"Total rows: **{len(rows)}**")
    lines.append("")
    for (e, f), c in sorted(buckets.items(), key=lambda x: -sum(x[1].values())):
        total = sum(c.values())
        lines.append(f"## {e}.{f} — {total} rows")
        lines.append("")
        lines.append("| class | count | pct |")
        lines.append("|---|---:|---:|")
        for cls, n in c.most_common():
            pct = 100 * n / total
            lines.append(f"| {cls} | {n} | {pct:.1f}% |")
        lines.append("")
    OUT_MD.write_text("\n".join(lines), encoding="utf-8")
    print(f"output: {OUT_MD}")

    # console summary
    print("\nTop (entity, field) by row count:")
    for (e, f), c in sorted(buckets.items(), key=lambda x: -sum(x[1].values()))[:8]:
        total = sum(c.values())
        top = c.most_common(3)
        print(f"  {e}.{f:<14} {total:>6}  top={top}")


if __name__ == "__main__":
    main()
