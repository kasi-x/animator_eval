# Task: ANN parser 拡張 (XML / HTML 全フィールド抽出)

**ID**: `10_ann_scraper_extend/02_parser_extend`
**Priority**: 🟠
**Estimated changes**: 約 +350 / -30 lines, 1 file (`src/scrapers/parsers/ann.py`) + test
**Requires senior judgment**: yes (XML 構造の微妙なケース、多言語 title の正規化)
**Blocks**: `10_ann_scraper_extend/03_scraper_integration`
**Blocked by**: `10_ann_scraper_extend/01_schema_design`

---

## Goal

`parse_anime_xml` と `parse_person_html` を拡張、新規 parser 関数 6 個を追加、Card 01 で定義した 8 種 dataclass を全て埋める。**追加 HTTP fetch なし** (既存 XML/HTML から全抽出)。

---

## Hard constraints

- **H1**: rating は `display_*` に限定、scoring path への誤流入を防ぐため docstring で explicit に明記。
- **raw 原則**: 文字列正規化は `_normalize_format` / `_parse_vintage` / `_parse_dob_html` の既存 3 関数のみ。追加の文字列変換関数を安易に増やさない (role 正規化は parse_role に委ねる既存方針を維持)。
- **multi-lang alt title**: 全言語を `titles_alt` JSON dict `{"ja": [...], "zh": [...], ...}` に格納。既存 `title_ja` 単独フィールドは互換維持。

---

## Pre-conditions

- [ ] Card 01 完了 (dataclass 定義済)
- [ ] 参照サンプル取得: `curl 'https://cdn.animenewsnetwork.com/encyclopedia/api.xml?anime=1' > /tmp/ann_sample.xml`
- [ ] サンプル person HTML: `curl 'https://www.animenewsnetwork.com/encyclopedia/people.php?id=1' > /tmp/ann_person.html`

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/scrapers/parsers/ann.py` | parser 関数群拡張、新規 parser 6 個追加 |
| `tests/scrapers/test_ann_parser.py` (新規 or 既存) | fixture XML/HTML + 全 parser のユニットテスト |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/scrapers/ann_scraper.py` | 03 で統合 |
| `src/utils/name_utils.py` | 既存 `assign_native_name_fields` そのまま使用 |

---

## Steps

### Step 1: `parse_anime_xml` 拡張

追加抽出項目:

```python
# 既存 for info in anime_el.findall("info"): ブロック内に追加
elif itype == "Themes":
    rec.themes = [t.strip() for t in text.split(";") if t.strip()]
elif itype == "Plot Summary":
    rec.plot_summary = text
elif itype == "Running time":
    rec.running_time_raw = text
elif itype == "Objectionable content":
    rec.objectionable_content = text
elif itype == "Opening Theme":
    # text = "『Title』by Artist" 形式。Title/Artist 分離 (既存パターン他 source に無ければここで小関数)
    rec.opening_themes.append(_parse_theme(text))
elif itype == "Ending Theme":
    rec.ending_themes.append(_parse_theme(text))
elif itype == "Insert song":
    rec.insert_songs.append(_parse_theme(text))
elif itype == "Official website":
    rec.official_websites.append({"lang": info.get("lang") or "", "url": text})
elif itype == "Alternative title":
    lang = info.get("lang") or ""
    rec.alt_titles_by_lang.setdefault(lang, []).append(text)
elif itype == "Picture":
    src = info.get("src")
    if src and rec.image_url is None:
        rec.image_url = src
elif itype == "Vintage":
    rec.vintage_raw = text
    rec.year, rec.start_date, rec.end_date = _parse_vintage(text)
```

`rec` の dataclass を dump 前に JSON serialize:
- `opening_themes_json = json.dumps(rec.opening_themes, ensure_ascii=False)`
- `official_websites_json = json.dumps(rec.official_websites, ensure_ascii=False)`
- `titles_alt = json.dumps(rec.alt_titles_by_lang, ensure_ascii=False)` ← 既存列を拡張

内部保持 field を別に持ち、最後に JSON 化するデザイン (dataclass は parquet 保存時の shape に揃える)。

### Step 2: `<ratings>` / `<staff gid>` / `<cast>` / `<credit>` / `<company>` / `<episode>` / `<release>` / `<news>` / `<related-prev>` / `<related-next>` 抽出

新規 parser (`parse_anime_xml` 内で生成 or separate helper):

```python
def _extract_ratings(anime_el) -> tuple[int|None, float|None, float|None]:
    r = anime_el.find("ratings")
    if r is None:
        return None, None, None
    votes = int(r.get("nb_votes")) if r.get("nb_votes") else None
    w = float(r.get("weighted_score")) if r.get("weighted_score") else None
    b = float(r.get("bayesian_score")) if r.get("bayesian_score") else None
    return votes, w, b


def _extract_cast(anime_el, ann_id) -> list[AnnCastEntry]:
    out = []
    for cast_el in anime_el.findall("cast"):
        role = cast_el.get("lang") or ""  # "JA" / "EN" or cast role
        for person_el in cast_el.findall("person"):
            pid = int(person_el.get("id")) if person_el.get("id") else None
            char_el = cast_el.find("role")  # or similar — 実 XML 構造で確認
            ...
    return out
```

ANN XML の `<cast>` 具体構造は取得サンプルで確認必須 — ドキュメント化少ないため実応答駆動。

同パターンで credit / company / episode / release / news / related-prev / related-next parser 実装。

### Step 3: `parse_anime_xml` 戻り値変更

現行: `list[AnnAnimeRecord]` → 新: `AnimeXmlParseResult` (NamedTuple or dataclass):

```python
@dataclass
class AnimeXmlParseResult:
    anime: list[AnnAnimeRecord]
    cast: list[AnnCastEntry]
    company: list[AnnCompanyEntry]
    episodes: list[AnnEpisodeEntry]
    releases: list[AnnReleaseEntry]
    news: list[AnnNewsEntry]
    related: list[AnnRelatedEntry]
```

`AnnAnimeRecord.staff` は従来通り (anime 本体に含めて 03 で展開書き出し)。

### Step 4: `parse_person_html` 拡張

`_HTML_LABEL_MAP` に以下追加:

```python
"gender": "gender",
"nickname": "nickname",
"nicknames": "nickname",
"family name": "family_name_ja",
"given name": "given_name_ja",
"height": "height_raw",
"picture": "image_url",     # <img id="pic" src=...>
```

`infotype-*` div を full scan。`description` は 2000 char truncate 撤廃 → `description_raw` (既存 `description` は deprecated field 化、同値で両方埋める)。

`<div id="credit_list">` or 類似 section から credit history 抽出:

```python
credit_items = []
for li in soup.select("#credit_list li") or soup.select("div.credit-item"):
    anime_link = li.find("a", href=re.compile(r"anime\.php\?id=(\d+)"))
    task_el = li.find("span", class_="task") or li  # 実構造で確認
    if anime_link:
        m = re.search(r"id=(\d+)", anime_link["href"])
        aid = int(m.group(1)) if m else None
        credit_items.append({
            "ann_anime_id": aid,
            "anime_title": anime_link.get_text(strip=True),
            "task": task_el.get_text(" ", strip=True),
        })
credits_json = json.dumps(credit_items, ensure_ascii=False)
```

実セレクタは取得サンプル HTML で確認必須。

### Step 5: staff gid / task_raw

```python
# parse_anime_xml の staff loop 内
gid_str = staff_el.get("gid")
gid = int(gid_str) if gid_str and gid_str.isdigit() else None
rec.staff.append(AnnStaffEntry(
    ann_person_id=pid, name_en=name, task=task, task_raw=task, gid=gid,
))
```

`task_raw = task` で二重持ち (将来 `task` を parsed 版にしても raw が残る)。

### Step 6: テスト

`tests/scrapers/test_ann_parser.py`:

- fixture: `tests/fixtures/ann_anime_sample.xml` (実 ANN XML の縮小版)
- fixture: `tests/fixtures/ann_person_sample.html`
- test: `parse_anime_xml` 戻り値で anime / cast / company / episodes / related の 5 種 list が期待値通り
- test: `parse_person_html` で gender / nickname / credits_json 抽出
- test: `<ratings>` 無い anime で `display_rating_*` が None
- test: 多言語 alt title が `titles_alt` JSON に dict 形式で保存

### Step 7: docstring

`parse_anime_xml` / `parse_person_html` の docstring に「raw 保存原則」「rating は display のみ」「新戻り値 shape」を明記。

---

## Verification

```bash
# 1. import
pixi run python -c "from src.scrapers.parsers.ann import parse_anime_xml, parse_person_html; print('OK')"

# 2. 実 XML で parse 通ること (fixture 1 件)
pixi run python -c "
import xml.etree.ElementTree as ET
from src.scrapers.parsers.ann import parse_anime_xml
root = ET.parse('tests/fixtures/ann_anime_sample.xml').getroot()
r = parse_anime_xml(root)
print(f'anime: {len(r.anime)}, cast: {len(r.cast)}, company: {len(r.company)}, episodes: {len(r.episodes)}')
"

# 3. test
pixi run test-scoped tests/scrapers/test_ann_parser.py

# 4. lint
pixi run lint

# 5. Hard Rule invariant
rg -n 'anime\.score|rating' src/scrapers/parsers/ann.py | grep -v 'display_rating\|#.*display\|docstring' | grep -v '^$' || echo "HR OK"
```

---

## Stop-if conditions

- [ ] 実 ANN XML の構造が想定と乖離 (例: `<cast>` が `<role>` でなく別名) → user 確認 (1 anime で実 XML を見せる)
- [ ] `<div id="credit_list">` が 2026 現在の ANN HTML に存在しない → credit_json は `null` 許容、description_raw のみ取れれば OK
- [ ] test が 20% 超変更 → 設計やり直し

---

## Rollback

```bash
git checkout src/scrapers/parsers/ann.py
rm -f tests/scrapers/test_ann_parser.py tests/fixtures/ann_anime_sample.xml tests/fixtures/ann_person_sample.html
```

---

## Completion signal

- [ ] `parse_anime_xml` / `parse_person_html` 新 shape 返却
- [ ] 全 dataclass が parser 経由で埋まる
- [ ] `test_ann_parser.py` 全 pass
- [ ] `DONE: 10_ann_scraper_extend/02_parser_extend` 記録
