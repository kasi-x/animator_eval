# Task: HTTP client の rate-limit + retry を共通基底クラスに抽出

**ID**: `11_scraper_unification/03_http_client_base`
**Priority**: 🔴
**Estimated changes**: 約 +200 / -150 lines, 5 files (新規 1 + scraper 4)
**Requires senior judgment**: yes (挙動繊細、rate-limit / 429 / Retry-After 解析、graceful degradation)
**Blocks**: (なし)
**Blocked by**: `01_anilist_cli_unify`, `02_seesaawiki_progress` (rebase コンフリクト回避)

---

## Goal

`BangumiClient` / `AnnClient` / `AllcinemaClient` / `JikanClient` (mal) / `WikidataClient` (jvmg) が個別実装している以下のロジックを、共通基底クラス `RateLimitedHttpClient` に集約する:

- `asyncio.Lock` ベースの per-request throttle (`_throttle()`)
- 5xx / 429 への retry + exponential backoff
- `Retry-After` ヘッダ解析 (秒数 / HTTP date)
- async context manager (`__aenter__` / `__aexit__`) で httpx.AsyncClient を保持

各 scraper はこの基底を継承し、固有の URL builder / parse logic だけ書く。

---

## Hard constraints

(`_hard_constraints.md` を事前に読むこと)

- **rate limit を緩めない**: 各 scraper の現在の `min_interval` / `delay` 値を **そのまま受け継ぐ** こと (例: bangumi 1.0 req/sec、ANN 1.5 sec、allcinema 2.0 sec)
- **retry policy を変えない**: bangumi の 429 / Retry-After 解析、ANN の 5xx exponential backoff など、現挙動を移植
- **既存テストが pass**: scraper 個別のテストは触らない、基底クラス追加のみ
- **graceful degradation**: 既存 `ScraperError` 例外を維持、message 文言も極力同じ

---

## Pre-conditions

- [ ] `01_anilist_cli_unify` 完了
- [ ] `02_seesaawiki_progress` 完了
- [ ] `git status` clean
- [ ] **既存 5 client の挙動を読んで理解した** ことを確認:
  - `src/scrapers/bangumi_scraper.py:91-275` (BangumiClient)
  - `src/scrapers/ann_scraper.py:93-194` (AnnClient)
  - `src/scrapers/allcinema_scraper.py:159-230` (AllcinemaClient)
  - `src/scrapers/mal_scraper.py:49-100` (JikanClient — `RetryingHttpClient` baseで簡素)
  - `src/scrapers/jvmg_fetcher.py` (WikidataClient — SPARQL 専用、別構造かも)

---

## Files to modify / create

| File | 変更内容 |
|------|---------|
| `src/scrapers/http_base.py` | **新規**: `RateLimitedHttpClient` 抽象クラス |
| `src/scrapers/bangumi_scraper.py` | `BangumiClient` を継承で再実装 |
| `src/scrapers/ann_scraper.py` | `AnnClient` を継承で再実装 |
| `src/scrapers/allcinema_scraper.py` | `AllcinemaClient` を継承で再実装 |
| `src/scrapers/mal_scraper.py` | `JikanClient` を継承で再実装 (現在 `RetryingHttpClient` 経由) |
| `src/scrapers/jvmg_fetcher.py` | `WikidataClient` を継承で再実装 |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/scrapers/http_client.py` (`RetryingHttpClient`) | 既存の lower-level wrapper、別レイヤなので残す |
| `src/scrapers/retry.py` | retry decorator 既存 |
| `tests/scrapers/` | 既存テストは pass し続けることが invariant |

---

## Steps

### Step 1: 5 client の現挙動を表にまとめる

```bash
grep -n "_min_interval\|min_interval\|delay\|MAX_ATTEMPTS\|_RETRY_AFTER\|backoff" \
  src/scrapers/bangumi_scraper.py \
  src/scrapers/ann_scraper.py \
  src/scrapers/allcinema_scraper.py \
  src/scrapers/mal_scraper.py \
  src/scrapers/jvmg_fetcher.py
```

各 client の以下を比較:

| client | min_interval | max_attempts | retry on | backoff | Retry-After 解析 |
|---|---|---|---|---|---|
| bangumi | 1.0 sec | _MAX_ATTEMPTS | 5xx, 429 | exp + cap | 秒 / HTTP date |
| ANN | 1.5 sec | ? | 5xx | exp | ? |
| allcinema | 2.0 sec | ? | 5xx | exp | ? |
| mal (Jikan) | ? | ? | (RetryingHttpClient) | ? | ? |
| jvmg (Wikidata) | ? | ? | ? | ? | ? |

→ 表が揃ったら設計判断:
- 共通化できるパラメータ → `__init__` 引数
- 各 scraper 固有 → 継承先の override / class 変数

### Step 2: `src/scrapers/http_base.py` 新規作成

```python
"""Base class for rate-limited + retrying async HTTP scrapers.

Subclass to get _throttle() + _get_with_retry() for free; override
parse_response() / build_url() / etc.
"""
from __future__ import annotations

import asyncio
import time
from abc import ABC
from typing import Any

import httpx
import structlog

from src.scrapers.exceptions import ScraperError

logger = structlog.get_logger()


class RateLimitedHttpClient(ABC):
    """Async HTTP client with single-lock rate limiter and exp backoff retry.

    Subclasses should:
      - Set DEFAULT_USER_AGENT, DEFAULT_RATE_PER_SEC, DEFAULT_MAX_ATTEMPTS
      - Override _classify_error() if their API has unusual status codes
      - Use self._get_with_retry(url) for all fetches
    """

    DEFAULT_USER_AGENT: str = "animetor-eval/0.1 (research bot)"
    DEFAULT_RATE_PER_SEC: float = 1.0
    DEFAULT_MAX_ATTEMPTS: int = 5
    DEFAULT_RETRY_AFTER_CAP: float = 60.0
    DEFAULT_TIMEOUT: float = 30.0

    def __init__(
        self,
        *,
        user_agent: str | None = None,
        rate_per_sec: float | None = None,
        max_attempts: int | None = None,
        timeout: float | None = None,
        headers: dict[str, str] | None = None,
    ) -> None:
        self._user_agent = user_agent or self.DEFAULT_USER_AGENT
        self._min_interval = 1.0 / (rate_per_sec or self.DEFAULT_RATE_PER_SEC)
        self._max_attempts = max_attempts or self.DEFAULT_MAX_ATTEMPTS
        self._timeout = timeout or self.DEFAULT_TIMEOUT
        self._extra_headers = headers or {}
        self._client: httpx.AsyncClient | None = None
        self._rate_lock = asyncio.Lock()
        self._last_request_at: float = 0.0

    async def __aenter__(self) -> "RateLimitedHttpClient":
        headers = {"User-Agent": self._user_agent, **self._extra_headers}
        self._client = httpx.AsyncClient(timeout=self._timeout, headers=headers)
        return self

    async def __aexit__(self, *_: object) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    async def close(self) -> None:
        await self.__aexit__()

    async def _throttle(self) -> None:
        async with self._rate_lock:
            now = time.monotonic()
            gap = self._min_interval - (now - self._last_request_at)
            if gap > 0:
                await asyncio.sleep(gap)
            self._last_request_at = time.monotonic()

    async def _get_with_retry(
        self, url: str, *, context: str = "", expect_404_as_none: bool = True
    ) -> Any:
        """GET url with throttle + retry. Returns parsed JSON or None on 404."""
        assert self._client is not None, f"{type(self).__name__} must be used as async ctx mgr"

        for attempt in range(1, self._max_attempts + 1):
            await self._throttle()
            try:
                resp = await self._client.get(url)
            except httpx.TransportError as exc:
                if attempt >= self._max_attempts:
                    raise ScraperError(f"transport error: {context}: {exc}") from exc
                await asyncio.sleep(self._compute_backoff(attempt, retry_after=None))
                continue

            if resp.status_code == 404 and expect_404_as_none:
                return None
            if resp.status_code == 429:
                wait = self._parse_retry_after(resp) or self._compute_backoff(attempt, retry_after=None)
                await asyncio.sleep(min(wait, self.DEFAULT_RETRY_AFTER_CAP))
                continue
            if 500 <= resp.status_code < 600:
                if attempt >= self._max_attempts:
                    raise ScraperError(f"server error {resp.status_code}: {context}")
                await asyncio.sleep(self._compute_backoff(attempt, retry_after=None))
                continue
            resp.raise_for_status()
            return resp.json()

        raise ScraperError(f"max attempts exhausted: {context}")

    @staticmethod
    def _parse_retry_after(resp: httpx.Response) -> float | None:
        """Parse Retry-After header. Subclasses override for custom parsing."""
        ra = resp.headers.get("Retry-After")
        if not ra:
            return None
        try:
            return float(ra)
        except ValueError:
            return None

    @staticmethod
    def _compute_backoff(attempt: int, retry_after: float | None) -> float:
        if retry_after is not None:
            return min(retry_after, RateLimitedHttpClient.DEFAULT_RETRY_AFTER_CAP)
        return min(2 ** attempt, RateLimitedHttpClient.DEFAULT_RETRY_AFTER_CAP)
```

→ 上記は **テンプレ**。Step 1 で集めた表に基づき、bangumi の `_compute_retry_after()` 等の固有挙動が必要なら基底に取り込むか、subclass の override にする。

### Step 3: BangumiClient を継承形に書換

```python
# Before
class BangumiClient:
    def __init__(self, user_agent=..., rate_limit_per_sec=1.0, timeout=30.0): ...
    async def __aenter__(self): ...
    async def __aexit__(self, *_): ...
    async def _throttle(self): ...
    async def _get_with_retry(self, url, context=""): ...
    async def fetch_subject_persons(self, subject_id): ...
    # ...

# After
class BangumiClient(RateLimitedHttpClient):
    DEFAULT_USER_AGENT = "animetor-eval/0.1 (bangumi)"
    DEFAULT_RATE_PER_SEC = 1.0
    DEFAULT_MAX_ATTEMPTS = 5

    async def fetch_subject_persons(self, subject_id):
        url = subject_persons_url(subject_id)
        raw = await self._get_with_retry(url, context=f"subject={subject_id}/persons")
        # ... 既存の post-processing ...
```

`_throttle()` / `_get_with_retry()` / `__aenter__` / `__aexit__` は継承で得るので削除。

### Step 4: AnnClient / AllcinemaClient / JikanClient / WikidataClient も同様

各 scraper の固有値:
- AnnClient: `DEFAULT_RATE_PER_SEC = 1/1.5 = 0.667` (1.5 sec interval)
- AllcinemaClient: `DEFAULT_RATE_PER_SEC = 0.5` (2.0 sec)
- JikanClient: 既存 `RetryingHttpClient` 経由 → 継承構造を整理
- WikidataClient: SPARQL 専用、POST request → 基底の `_get_with_retry()` だけでは不足、`_post_with_retry()` を基底に追加するか、subclass で override

### Step 5: 各 scraper module 末尾に backward-compat エイリアスを残す (必要なら)

```python
# 旧コードが `from src.scrapers.bangumi_scraper import BangumiClient` してる → そのまま動く
# `from src.scrapers.ann_scraper import AnnClient` も同様
```

シグネチャを変えないので alias 不要なはず。

---

## Verification

```bash
# 1. import OK
pixi run python -c "
from src.scrapers.http_base import RateLimitedHttpClient
from src.scrapers.bangumi_scraper import BangumiClient
from src.scrapers.ann_scraper import AnnClient
from src.scrapers.allcinema_scraper import AllcinemaClient
from src.scrapers.mal_scraper import JikanClient
print('OK')
"

# 2. 継承関係確認
pixi run python -c "
from src.scrapers.http_base import RateLimitedHttpClient
from src.scrapers.bangumi_scraper import BangumiClient
assert issubclass(BangumiClient, RateLimitedHttpClient)
print('BangumiClient is subclass: OK')
"

# 3. 既存テスト
pixi run test-scoped tests/scrapers/

# 4. Lint
pixi run lint

# 5. Smoke test (実 API へ 1 req のみ)
pixi run python -c "
import asyncio
from src.scrapers.bangumi_scraper import BangumiClient
async def main():
    async with BangumiClient() as c:
        r = await c.fetch_person(1)  # 既知の person_id
        print('fetched:', r.get('name') if r else 'None')
asyncio.run(main())
"
```

---

## Stop-if conditions

- import エラー
- 既存 scraper テスト (`tests/scrapers/`) が 1 件でも fail
- Smoke test で 1 req に 5 sec 以上 (rate-limit が間違って厳しすぎる)
- 既存 `ScraperError` の message format が変わって callers が壊れる
- `git diff --stat` が +500/-300 を超える (想定 2 倍超)

---

## Rollback

```bash
git checkout src/scrapers/bangumi_scraper.py src/scrapers/ann_scraper.py \
  src/scrapers/allcinema_scraper.py src/scrapers/mal_scraper.py \
  src/scrapers/jvmg_fetcher.py
rm src/scrapers/http_base.py
pixi run python -c "from src.scrapers import ann_scraper; print('OK')"
pixi run test-scoped tests/scrapers/
```

---

## Completion signal

- [ ] `RateLimitedHttpClient` が `src/scrapers/http_base.py` に存在
- [ ] 5 client が継承
- [ ] 既存 scraper テスト全 pass
- [ ] Smoke test で 1 req 成功
- [ ] git log message: `scraper: extract RateLimitedHttpClient base for HTTP rate-limit + retry (11_scraper_unification/03)`
