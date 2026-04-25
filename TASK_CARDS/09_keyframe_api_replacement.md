# Task: keyframe scraper を非公式 API 経路へ完全置換

**ID**: `09_keyframe_api_replacement`
**Priority**: 🟠 (前 task 08 を deprecate、こちらが正系)
**Estimated changes**: 約 +800 lines, scraper 完全書き換え + 新 dataclass 6+
**Requires senior judgment**: yes (API 経路、レート設計、ストレージ戦略)
**Blocks**: keyframe 全件 scrape の本実施
**Blocked by**: なし

**前 task 08 (`08_keyframe_parser_expansion`) を deprecate**: worktree branch `worktree-agent-aafbf69f` (commit `7f08278`) は捨てる。本タスクは main から fresh で再実装。

---

## 背景と目的

### 旧方式 (task 08) の限界

- HTML preloadData 経由のみ
- 取得粒度は anime page 単位
- person 単位の集約 (alias / studios / cross-anime credits) は事後 SILVER 統合で構築する必要あり
- API の存在を未確認だったため、scraping cost が無駄に大きい

### 2026-04-25 の API 探索結果

JS bundle 解析 + 実 API 試行で **9 endpoint 発見**:

| endpoint | status | 用途 |
|----------|--------|------|
| `/api/data/roles.php` | ✅ 200, 318KB, **list[1924]** | role master 全件 (id/name_ja/name_en/category/episode_category/description) |
| `/api/data/studios.php` | ⚠️ 200, 6 件のみ | featured studio (master ではない) |
| `/api/person/show.php?id=<id>&type=person` | ✅ 200, ~200KB | **person 詳細 + 全 credit ツリー** (preloadData より詳細) |
| `/api/person/get_by_id.php?id=<id>&studio=<0\|1>` | ✅ 200, 60B | name lookup 軽量 |
| `/api/search/?q=<q>&type=staff\|all` | ✅ 200, 50 件/page | 横断検索、`offset=` で pagination |
| `/api/stafflists/preview.php` | ✅ 200, 14KB | top 用 recent/airing 6 件 + total/contributors |
| `/api/data/translate.v4.php` | (未試行) | auto translation |
| `/api/account/auth.php` | (認証必須) | login |
| `/api/stafflists/preview.php` | preview | 上記と重複 |

### show.php credits 構造 (preloadData よりも詳細)

```json
credits[i] = {
  uuid, slug, episodes, status,
  stafflist_name, stafflist_name_ja, stafflist_studios, stafflist_kv, stafflist_is_adult,
  seasonYear,
  names: [{
    ja, en,
    categories: [{
      category: "Key Animation",
      roles: [{
        role_ja: "原画", role_en: "Key Animation",
        credits: [{
          studio: null|str,        // 各話レベルで studio 指定
          episode: "#01",
          is_nc: 0|1,              // ★ Not Credited フラグ (preloadData に無い)
          comment: null|str,       // ★ 各 credit の注釈
          is_primary_alias: bool,  // ★ 別名表記フラグ
        }]
      }]
    }]
  }]
}
```

### sitemap.xml (5,518 loc)

- `/staff/<slug-or-uuid>` のみ (anime ページ)、計 5,512 entries
- person/studio 独立 sitemap **無し**
- → person 全件取得は anime page 経由で id 集約 → show.php 個別取得 (これが新方式の鍵)

### 狙い

API + HTML hybrid 構成で BRONZE keyframe を構築:

- A. **roles master** API 一発取得
- B. **anime list + credit ツリー** sitemap → HTML preloadData (raw HTML 保存)
- C. **person 詳細** API show.php?type=person 個別取得 (raw JSON 保存、gzip)
- D. **recent/airing 差分検出** preview.php を cron 用に保存

---

## Hard constraints

- H1 (CLAUDE.md): anime.score / popularity を scoring に入れない (BRONZE 保持のみ可)
- H3 entity resolution 不変
- 旧 worktree branch `worktree-agent-aafbf69f` は **マージしない**、削除可
- raw HTML / raw JSON は gzip 圧縮で保存 (storage 10GB+ 想定)
- HTTP delay は **5.0s 以上**、API も同様に保護的に
- **dynamic API は static asset と独立して 429 が起こりうる** (実観測: 3.0s で連続 429) → delay 5.0s + max_retries=5 + Retry-After 尊重
- Phase 2 (HTML) と Phase 3 (API) は **直列実行** (並列叩きしない、サーバー保護)
- person 重複は単一取得 (delay × 重複数 を避ける)
- ANN との rate limit 干渉なし (別ホスト) だが、本人マシンの帯域同時消費を考慮

---

## Pre-conditions

- [ ] `git status` clean (旧 worktree とは別世界、main で作業)
- [ ] `data/keyframe/checkpoint.json.bak-20260425` 存在 (旧データ退避済)
- [ ] `result/bronze/source=keyframe.bak-*` 存在 (旧 BRONZE 退避済) — 無ければ Step 0 で退避
- [ ] keyframe-staff-list.com sitemap / API 到達可能

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/scrapers/parsers/keyframe.py` | 既存 `parse_credits_from_data` を維持しつつ、`parse_anime_meta`, `parse_anime_studios`, `parse_settings_categories` 追加。 task 08 の関数群を **そのまま採用** |
| `src/scrapers/keyframe_scraper.py` | **完全書き換え**。Phase 0/1/2/3/4 のオーケストレータ、API client (`KeyframeApiClient`) を新規実装、raw HTML / raw JSON gzip 保存、5+ BronzeWriter |
| `src/scrapers/keyframe_api.py` | **新規**。API endpoint client (httpx async wrapper、retry、Retry-After 尊重) |
| `src/scrapers/parsers/keyframe_api.py` | **新規**。show.php / roles.php / preview.php / search.php の JSON parser |
| `src/runtime/models.py` | **新規 dataclass 6+**: BronzeKeyframeRolesMaster, BronzeKeyframeAnime, BronzeKeyframePersonProfile, BronzeKeyframePersonStudios, BronzeKeyframePersonCredits, BronzeKeyframePreview ほか task 08 の 5 個 |
| `src/db/schema.py` | 同上の table 宣言 |
| `tests/scrapers/test_keyframe_api.py` | 新規。API client mock test |
| `tests/scrapers/test_keyframe_api_parser.py` | 新規。JSON parser unit test |
| `tests/scrapers/test_keyframe_parser.py` | 既存。task 08 の test を main へ移植 |
| `tests/scrapers/test_keyframe_scraper.py` | 新規。Phase 0-4 oracle test (mock) |
| `tests/fixtures/scrapers/keyframe/` | sample_one-piece.html (合成) + roles_sample.json + show_sample.json + preview_sample.json |

---

## Files to NOT touch

| File | 理由 |
|------|------|
| `data/keyframe/checkpoint.json.bak-20260425` | safety net |
| `result/bronze/source=keyframe.bak-*/` | safety net |
| 他 source の scraper / parser | scope 外 |
| `src/runtime/models.py` の共通 BronzeAnime / Credit | keyframe 専用 dataclass を新設、共通は触らない |

---

## API endpoint 仕様まとめ (実測値)

| endpoint | 用途 | レスポンス | 注意 |
|----------|------|-----------|------|
| GET `/sitemap.xml` | anime slug 列挙 | XML、5512 entries | 1 リクで全件 |
| GET `/staff/<slug>` | anime preloadData 含む HTML | HTML, ~200KB/件 | preloadData は `<script>preloadData = {...};` パターン |
| GET `/api/data/roles.php` | role master | JSON list[1924] | 1 リクで全件、毎週程度の頻度で再取得 |
| GET `/api/person/show.php?id=<numeric>&type=person` | person 詳細 + 全 credit | JSON ~200KB | id = preloadData の staff[].id (内部 numeric)、type=person 必須 |
| GET `/api/person/get_by_id.php?id=<numeric>&studio=<0\|1>` | name 軽量 lookup | JSON `{ja, en}` 60B | 不要 (show.php の subset) |
| GET `/api/search/?q=<q>&type=staff\|all&offset=<N>` | search | JSON `{staff: list, stafflists: list}` 50/page | type=all は staff + stafflists を返す |
| GET `/api/stafflists/preview.php` | top 用 recent/airing | JSON 14KB | 6 件 limit、cron 用 |

---

## BRONZE 設計 (新 11 テーブル)

```
result/bronze/source=keyframe/
├── table=roles_master/         # roles.php 全 1924 件 master
├── table=anime/                # /staff/<slug> preloadData (anime メタ)
├── table=anime_studios/        # preloadData anilist.studios.edges
├── table=settings_categories/  # preloadData settings.categories
├── table=credits/              # preloadData の credit ツリー (anime page level)
├── table=studios_master/       # preloadData isStudio=true 集約 (id ベース dedup)
├── table=person_profile/       # show.php staff (id, ja/en, aliases, avatar, bio)
├── table=person_jobs/          # show.php jobs (career role 集約)
├── table=person_studios/       # show.php studios (所属 studio + 別表記 dict)
├── table=person_credits/       # show.php credits ツリー (cross-anime、is_nc/comment/is_primary_alias 含む)
└── table=preview/              # preview.php cron snapshot (差分検出)
```

raw 保存:
```
data/keyframe/
├── raw/<slug>.html.gz          # anime HTML (5512 件 × ~50KB gzip = ~280MB)
├── person_raw/<id>.json.gz     # person JSON (~50K 件 × ~50KB gzip = ~2.5GB)
├── api/roles.json              # 全 master snapshot
├── api/preview.json            # cron snapshot
└── checkpoint.json             # 統合 checkpoint
```

統合 checkpoint:
```python
{
  "anime_phase": {"completed_slugs": [...], "all_slugs": [...]},
  "person_phase": {"completed_ids": [...], "all_ids": [...]},
  "roles_master_fetched_at": "2026-04-25T...",
  "preview_fetched_at": "2026-04-25T...",
  "stats": {...}
}
```

---

## Steps

### Step 0: 旧 BRONZE / checkpoint 退避 (no-op の場合あり)

```bash
# 既に退避済なら skip
[ -d result/bronze/source=keyframe ] && mv result/bronze/source=keyframe result/bronze/source=keyframe.bak-$(date +%Y%m%d-%H%M)
[ -f data/keyframe/checkpoint.json ] && mv data/keyframe/checkpoint.json data/keyframe/checkpoint.json.dirty-$(date +%Y%m%d-%H%M)
```

### Step 1: fixture 作成

```bash
# 合成 fixture (実 fetch 不可時の代替) を 4 種作成:
# tests/fixtures/scrapers/keyframe/sample_one-piece.html  (preloadData 含む)
# tests/fixtures/scrapers/keyframe/sample_roles.json      (3-5 件抜粋)
# tests/fixtures/scrapers/keyframe/sample_show.json       (1 person、credits 3 件)
# tests/fixtures/scrapers/keyframe/sample_preview.json    (recent/airing 各 1 件)

# サンプル取得は本タスクで作成済の /tmp/B.json (show), /tmp/G.json (preview), /tmp/kf_roles.json (roles), /tmp/kf_sample.html (one-piece) を活用してよい。
# main に取り込む際は sensitive 情報なし、size を抑えて (3-5 件まで)
```

### Step 2: API client 実装

`src/scrapers/keyframe_api.py`:

```python
from __future__ import annotations
import asyncio, gzip, json
from pathlib import Path
import httpx
import structlog

log = structlog.get_logger()

BASE = "https://keyframe-staff-list.com"
HEADERS = {"User-Agent": "Mozilla/5.0 ... (KeyFrame study, contact: <maintainer email>)"}
DEFAULT_DELAY = 5.0
MAX_RETRIES = 5


class KeyframeApiClient:
    def __init__(self, delay: float = DEFAULT_DELAY) -> None:
        self.delay = delay
        self._client = httpx.AsyncClient(
            timeout=60.0, follow_redirects=True, headers=HEADERS
        )
        self._last_request_at = 0.0

    async def close(self) -> None:
        await self._client.aclose()

    async def _get_with_retry(self, url: str) -> dict | list | None:
        # delay 制御
        import time
        now = time.monotonic()
        wait = self.delay - (now - self._last_request_at)
        if wait > 0:
            await asyncio.sleep(wait)
        self._last_request_at = time.monotonic()

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                resp = await self._client.get(url)
            except httpx.RequestError as e:
                wait_s = max(self.delay * 2, 10 * attempt)
                log.warning("keyframe_api_request_error", url=url, attempt=attempt, wait_s=wait_s, err=str(e)[:120])
                await asyncio.sleep(wait_s)
                continue

            if resp.status_code == 200:
                return resp.json()
            if resp.status_code == 404:
                log.debug("keyframe_api_not_found", url=url)
                return None
            if resp.status_code == 429:
                retry_after = int(resp.headers.get("Retry-After", 30))
                wait_s = max(retry_after, 30, 10 * attempt)
                log.warning("keyframe_api_rate_limited", url=url, attempt=attempt, wait_s=wait_s)
                await asyncio.sleep(wait_s)
                continue
            if resp.status_code in (500, 502, 503, 504):
                wait_s = max(self.delay * 2, 15 * attempt)
                log.warning("keyframe_api_server_error", status=resp.status_code, attempt=attempt, wait_s=wait_s)
                await asyncio.sleep(wait_s)
                continue
            log.warning("keyframe_api_unhandled_status", status=resp.status_code, url=url)
            return None
        log.warning("keyframe_api_max_retries", url=url)
        return None

    async def get_roles_master(self) -> list[dict] | None:
        return await self._get_with_retry(f"{BASE}/api/data/roles.php")

    async def get_person_show(self, person_id: int, type_: str = "person") -> dict | None:
        return await self._get_with_retry(f"{BASE}/api/person/show.php?id={person_id}&type={type_}")

    async def get_preview(self) -> dict | None:
        return await self._get_with_retry(f"{BASE}/api/stafflists/preview.php")

    async def search_staff(self, query: str, offset: int = 0) -> dict | None:
        from urllib.parse import quote
        return await self._get_with_retry(
            f"{BASE}/api/search/?q={quote(query)}&type=staff&offset={offset}"
        )
```

### Step 3: API parser 実装

`src/scrapers/parsers/keyframe_api.py`:

```python
def parse_roles_master(data: list[dict]) -> list[dict]:
    """list[{id, name_en, name_ja, category, episode_category, description}]."""
    return [
        {
            "role_id": int(r["id"]),
            "name_en": r.get("name_en"),
            "name_ja": r.get("name_ja"),
            "category": r.get("category"),
            "episode_category": r.get("episode_category"),
            "description": r.get("description"),
        }
        for r in data
    ]


def parse_person_show(data: dict) -> dict:
    """{
        profile: {id, isStudio, ja, en, aliases (json), avatar, bio},
        jobs: list[str],
        studios: list[{name, alt_names: list[str]}],
        credits: list[parsed_credit_dict]
    }"""
    staff = data.get("staff") or {}
    studios_raw = data.get("studios") or {}
    credits_raw = data.get("credits") or []

    studios = [
        {"studio_name": k, "alt_names": v if isinstance(v, list) else []}
        for k, v in studios_raw.items()
    ]

    credits = []
    for c in credits_raw:
        for name_obj in c.get("names", []):
            for cat in name_obj.get("categories", []):
                for role in cat.get("roles", []):
                    for credit in role.get("credits", []):
                        credits.append({
                            "anime_uuid": c.get("uuid"),
                            "anime_slug": c.get("slug"),
                            "anime_episodes": c.get("episodes"),
                            "anime_status": c.get("status"),
                            "anime_name_en": c.get("stafflist_name"),
                            "anime_name_ja": c.get("stafflist_name_ja"),
                            "anime_studios_str": c.get("stafflist_studios"),
                            "anime_kv": c.get("stafflist_kv"),
                            "anime_is_adult": c.get("stafflist_is_adult"),
                            "anime_season_year": c.get("seasonYear"),
                            "name_used_ja": name_obj.get("ja"),
                            "name_used_en": name_obj.get("en"),
                            "category": cat.get("category"),
                            "role_ja": role.get("role_ja"),
                            "role_en": role.get("role_en"),
                            "episode": credit.get("episode"),
                            "studio_at_credit": credit.get("studio"),
                            "is_nc": bool(credit.get("is_nc")),
                            "comment": credit.get("comment"),
                            "is_primary_alias": bool(credit.get("is_primary_alias")),
                        })

    return {
        "profile": {
            "id": int(staff["id"]),
            "isStudio": bool(staff.get("isStudio")),
            "ja": staff.get("ja"),
            "en": staff.get("en"),
            "aliases_json": staff.get("aliases") or [],
            "avatar": staff.get("avatar"),
            "bio": staff.get("bio"),
        },
        "jobs": data.get("jobs") or [],
        "studios": studios,
        "credits": credits,
    }


def parse_preview(data: dict) -> dict:
    """recent/airing/data list を normalize"""
    out = {
        "total": int(data.get("total") or 0),
        "total_contributors": int(data.get("totalContributors") or 0),
        "total_updated": int(data.get("totalUpdated") or 0),
        "recent": data.get("recent") or [],
        "airing": data.get("airing") or [],
        "data": data.get("data") or [],
    }
    return out
```

### Step 4: dataclass 追加 (`src/runtime/models.py`)

```python
class BronzeKeyframeRolesMaster(BaseModel):
    role_id: int
    name_en: str | None
    name_ja: str | None
    category: str | None
    episode_category: str | None
    description: str | None


class BronzeKeyframePersonProfile(BaseModel):
    person_id: int
    is_studio: bool
    name_ja: str | None
    name_en: str | None
    aliases_json: list[dict] = Field(default_factory=list)
    avatar: str | None
    bio: str | None


class BronzeKeyframePersonJob(BaseModel):
    person_id: int
    job: str


class BronzeKeyframePersonStudio(BaseModel):
    person_id: int
    studio_name: str
    alt_names: list[str] = Field(default_factory=list)


class BronzeKeyframePersonCredit(BaseModel):
    person_id: int
    anime_uuid: str
    anime_slug: str | None
    anime_episodes: int | None
    anime_status: str | None
    anime_name_en: str | None
    anime_name_ja: str | None
    anime_studios_str: str | None
    anime_kv: str | None
    anime_is_adult: bool | None
    anime_season_year: int | None
    name_used_ja: str | None
    name_used_en: str | None
    category: str | None
    role_ja: str | None
    role_en: str | None
    episode: str | None
    studio_at_credit: str | None
    is_nc: bool
    comment: str | None
    is_primary_alias: bool


class BronzeKeyframePreview(BaseModel):
    fetched_at: int  # unix seconds
    section: str  # "recent" | "airing" | "data"
    anilist_id: int | None
    uuid: str
    slug: str | None
    title: str | None
    title_native: str | None
    status: str | None
    last_modified: int | None
    season: str | None
    season_year: int | None
    studios_str: list[str] = Field(default_factory=list)
    contributors_json: list[dict] = Field(default_factory=list)
```

(task 08 の 5 個 + 上記 6 個 = 11 dataclass)

### Step 5: scraper オーケストレータ (`src/scrapers/keyframe_scraper.py` 完全書き換え)

```python
import asyncio, gzip, json
from pathlib import Path
import typer
import structlog

from src.scrapers.keyframe_api import KeyframeApiClient
from src.scrapers.parsers import keyframe as html_parser
from src.scrapers.parsers import keyframe_api as api_parser
from src.scrapers.bronze_writer import BronzeWriter

log = structlog.get_logger()
app = typer.Typer()


@app.command("scrape-all")
def cmd_scrape_all(
    delay: float = typer.Option(5.0),
    skip_persons: bool = typer.Option(False, help="Phase 3 を skip"),
    max_anime: int = typer.Option(0, help="0=全件"),
    fresh: bool = typer.Option(False),
) -> None:
    """Phase 0-4 を順次実行。"""
    asyncio.run(_run(delay=delay, skip_persons=skip_persons, max_anime=max_anime, fresh=fresh))


async def _run(*, delay: float, skip_persons: bool, max_anime: int, fresh: bool) -> None:
    data_dir = Path("data/keyframe")
    raw_html_dir = data_dir / "raw"
    raw_json_dir = data_dir / "person_raw"
    api_dir = data_dir / "api"
    for d in (raw_html_dir, raw_json_dir, api_dir):
        d.mkdir(parents=True, exist_ok=True)

    cp_path = data_dir / "checkpoint.json"
    cp = {} if fresh or not cp_path.exists() else json.loads(cp_path.read_text())
    cp.setdefault("anime_phase", {"completed_slugs": [], "all_slugs": []})
    cp.setdefault("person_phase", {"completed_ids": [], "all_ids": []})

    client = KeyframeApiClient(delay=delay)
    try:
        # Phase 0: roles master
        roles = await client.get_roles_master()
        if roles:
            (api_dir / "roles.json").write_text(json.dumps(roles, ensure_ascii=False))
            bw = BronzeWriter("keyframe", table="roles_master")
            for r in api_parser.parse_roles_master(roles):
                bw.append(r)
            bw.flush()
            cp["roles_master_fetched_at"] = ...
            log.info("phase0_roles_done", count=len(roles))

        # Phase 1: sitemap
        async with httpx.AsyncClient(...) as html_client:
            slugs = await fetch_sitemap(html_client)
        cp["anime_phase"]["all_slugs"] = slugs

        remaining = [s for s in slugs if s not in set(cp["anime_phase"]["completed_slugs"])]
        if max_anime > 0:
            remaining = remaining[:max_anime]

        # Phase 2: anime HTML scrape (旧 task 08 と同じロジックを移植)
        person_ids: set[int] = set()
        for slug in remaining:
            await asyncio.sleep(delay)
            data = await fetch_anime_page(client, slug)
            if data is None:
                cp["anime_phase"]["completed_slugs"].append(slug)
                continue
            # raw HTML 保存 (gzip)
            ...
            # parse + 5 BronzeWriter
            anime_meta = html_parser.parse_anime_meta(data, slug)
            # ... (task 08 の Step 4 と同じ)
            cp["anime_phase"]["completed_slugs"].append(slug)

            # person id 集約
            for menu in data.get("menus", []):
                for sec in menu.get("credits", []):
                    for r in sec.get("roles", []):
                        for s in r.get("staff", []):
                            if s.get("id") is not None and not s.get("isStudio"):
                                person_ids.add(int(s["id"]))

            # checkpoint flush
            ...

        cp["person_phase"]["all_ids"] = sorted(person_ids)

        # Phase 3: person API
        if not skip_persons:
            done = set(cp["person_phase"]["completed_ids"])
            remaining_ids = [i for i in cp["person_phase"]["all_ids"] if i not in done]
            for pid in remaining_ids:
                data = await client.get_person_show(pid)
                if data is None:
                    cp["person_phase"]["completed_ids"].append(pid)
                    continue
                # raw JSON 保存 (gzip)
                with gzip.open(raw_json_dir / f"{pid}.json.gz", "wt", encoding="utf-8") as fh:
                    json.dump(data, fh, ensure_ascii=False)
                parsed = api_parser.parse_person_show(data)
                # 5 BronzeWriter (profile, jobs, studios, credits)
                ...
                cp["person_phase"]["completed_ids"].append(pid)
                # checkpoint flush

        # Phase 4: preview
        prev = await client.get_preview()
        if prev:
            (api_dir / "preview.json").write_text(json.dumps(prev, ensure_ascii=False))
            for section in ("recent", "airing", "data"):
                for entry in prev.get(section, []):
                    bw_prev.append({"section": section, ...})
            bw_prev.flush()
    finally:
        cp_path.write_text(json.dumps(cp, ensure_ascii=False, indent=2))
        await client.close()


# 既存 keyframe_scraper.py の sitemap fetch / fetch_anime_page を移植
```

### Step 6: tests

`tests/scrapers/test_keyframe_api.py` (mock):
- `KeyframeApiClient.get_roles_master`: 200 / 429 / 500 / network error
- delay 制御 (連続 call 間隔測定)
- `Retry-After` 尊重

`tests/scrapers/test_keyframe_api_parser.py`:
- `parse_roles_master`: list[1924] 入力 → 全 6 列出力
- `parse_person_show`: credits ツリー展開 (1 person 269 credit を 269+ 行 に flat 化)
- `parse_preview`: section=recent/airing/data の各 normalize

`tests/scrapers/test_keyframe_scraper.py`:
- mock client + tmp BRONZE で Phase 0/1/2/3/4 が全 11 テーブルに行を入れる事を確認
- checkpoint resume が動く事
- fresh=True で checkpoint 無視

### Step 7: smoke test (3 anime, 5 person)

```bash
# 必要なら API 単発 fetch を 1-3 回試行 (delay 5s)
pixi run python -c "
import asyncio
from src.scrapers.keyframe_api import KeyframeApiClient
async def f():
    c = KeyframeApiClient(delay=5.0)
    roles = await c.get_roles_master()
    print('roles:', len(roles) if roles else None)
    p = await c.get_person_show(133359)
    print('person credits:', len(p.get('credits', [])) if p else None)
    prev = await c.get_preview()
    print('preview total:', prev.get('total') if prev else None)
    await c.close()
asyncio.run(f())
"

# scraper smoke
pixi run python -m src.scrapers.keyframe_scraper scrape-all --delay 5.0 --max-anime 3

# 確認
python3 -c "
import duckdb, glob
con = duckdb.connect(':memory:')
for t in ['roles_master', 'anime', 'anime_studios', 'settings_categories', 'credits', 'studios_master',
         'person_profile', 'person_jobs', 'person_studios', 'person_credits', 'preview']:
    f = glob.glob(f'result/bronze/source=keyframe/table={t}/**/*.parquet', recursive=True)
    n = con.execute(f'SELECT count(*) FROM read_parquet({f!r})').fetchone()[0] if f else 0
    print(f'  {t}: {n}')
"
```

期待: 全 11 テーブルに > 0 行。roles_master ~1924、anime 3、preview 18 (6×3 section)。

### Step 8: lint / test

```bash
pixi run lint
pixi run test-scoped tests/scrapers/test_keyframe_api.py tests/scrapers/test_keyframe_api_parser.py tests/scrapers/test_keyframe_scraper.py tests/scrapers/test_keyframe_parser.py
```

### Step 9: 全件 scrape (user 判断、本タスクでは実行しない)

```bash
# 想定時間: 5512 anime × 5s + 50K person × 5s = ~3.5 日
# user が認可してから実行
pixi run python -m src.scrapers.keyframe_scraper scrape-all --delay 5.0 --fresh 2>&1 | tee logs/scrapers/keyframe_$(date +%Y%m%d).log
```

→ **本タスクの担当範囲は Step 0-8 までに留める**。Step 9 は user 判断で別実行。

---

## Verification

```bash
# 1. lint
pixi run lint

# 2. test (新規 + 移植)
pixi run test-scoped tests/scrapers/test_keyframe_api.py tests/scrapers/test_keyframe_api_parser.py tests/scrapers/test_keyframe_scraper.py tests/scrapers/test_keyframe_parser.py

# 3. smoke (3 anime, person 数件)
pixi run python -m src.scrapers.keyframe_scraper scrape-all --delay 5.0 --max-anime 3
ls data/keyframe/raw/*.html.gz | wc -l       # >= 1 (404 等で 3 未満も可)
ls data/keyframe/person_raw/*.json.gz | wc -l  # >= 1
python3 -c "...11テーブル件数..."             # 全 > 0

# 4. checkpoint 整合
python3 -c "
import json
cp = json.load(open('data/keyframe/checkpoint.json'))
assert cp['anime_phase']['completed_slugs'], 'anime checkpoint'
assert cp['roles_master_fetched_at'], 'roles fetched'
print('OK')
"
```

---

## Stop-if conditions

- [ ] keyframe-staff-list.com sitemap 404/500 → サイト構造変更、user に報告
- [ ] preloadData 構造が one-piece fixture と乖離
- [ ] API endpoint 404/410 → API 仕様変更、user に報告
- [ ] delay 5.0s でも継続 429 → delay 10s+ / 別日 / cron 化
- [ ] ディスク容量不足 (推定 raw HTML 280MB + raw JSON 2.5GB = 2.8GB)。allow 5GB 以上
- [ ] Pydantic v2 制約引っかかり (Optional + default Field の併用ミス等) → models.py 修正
- [ ] BronzeWriter のスキーマ違反 → bronze_writer.py の type hint 確認

---

## Rollback

```bash
# コード破棄
git checkout src/scrapers/parsers/keyframe.py src/scrapers/keyframe_scraper.py src/runtime/models.py src/db/schema.py
rm -f src/scrapers/keyframe_api.py src/scrapers/parsers/keyframe_api.py

# tests 破棄
rm -rf tests/scrapers/test_keyframe_api.py tests/scrapers/test_keyframe_api_parser.py tests/scrapers/test_keyframe_scraper.py tests/scrapers/test_keyframe_parser.py tests/fixtures/scrapers/keyframe/

# BRONZE 戻し
rm -rf result/bronze/source=keyframe
mv result/bronze/source=keyframe.bak-* result/bronze/source=keyframe

# raw 削除
rm -rf data/keyframe/raw data/keyframe/person_raw data/keyframe/api

# checkpoint 戻し
mv data/keyframe/checkpoint.json.bak-20260425 data/keyframe/checkpoint.json
```

---

## Completion signal

- [ ] Step 0-8 が PR 上で lint / test pass
- [ ] Step 7 smoke で 11 テーブル全てに行 > 0
- [ ] roles_master の row 数が 1900-2000 程度 (1924 ± サイト更新差)
- [ ] commit (例: `scraper: keyframe API 経路へ完全置換 — 11 テーブル / 4 phase / raw gzip 保存 (09_keyframe_api_replacement Step 0-8)`)
- [ ] 旧 worktree branch `worktree-agent-aafbf69f` の取り扱い (削除推奨) を user に確認
- [ ] 残る判断 (Step 9 全件 scrape の認可、3-4 日 dedicated 実行可否) は user に投げる
- [ ] 作業ログに `DONE: 09_keyframe_api_replacement Step 0-8 (Step 9 は user 判断待ち)` 記録

---

## 参考: 実観測値 (2026-04-25)

| 検証項目 | 結果 |
|---------|------|
| sitemap.xml 取得 | ✅ 200, 5518 entries |
| /staff/<slug> HTML 連続取得 | ⚠️ 3.0s で 8 連続 429 (Cloudflare) |
| static asset 取得 | ✅ 200 (cf-cache-status: EXPIRED) |
| /api/data/roles.php | ✅ 200, 318KB, list[1924] |
| /api/person/show.php?id=133359&type=person | ✅ 200, ~200KB, credits[269] |
| /api/person/show.php (type 無し or al) | ❌ 400 Invalid ID format |
| /api/search/?q=miyazaki&type=staff | ✅ 200, 50 件 |
| /api/search/?q=*&offset=50 | ✅ 200, 別 50 件 |
| /api/stafflists/preview.php | ✅ 200, 14KB, 6×3 section |
| /api/data/studios.php | ⚠️ 200, 6 件のみ (master ではない) |
| /api/person/get_by_id.php | ✅ 200, 60B name only |

API は HTML より rate limit 緩やかな印象 (試行 9 回で 429 0)。ただし大量 (50K+ 連続) は不明、慎重に delay 5.0s 維持推奨。
