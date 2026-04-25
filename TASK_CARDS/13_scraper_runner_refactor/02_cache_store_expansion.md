# Task: `cache_store` を全 fetcher に拡張 + `Fetcher` ヘルパ抽出

**ID**: `13_scraper_runner_refactor/02_cache_store_expansion`
**Priority**: 🟠
**Estimated changes**: 約 +180 / -50 lines, 4 files (新規 `fetchers.py` + scraper 3 改修)
**Requires senior judgment**: no
**Blocks**: `04_runner_abstraction`
**Blocked by**: `01_http_client_dedupe`

---

## Goal

現状 `src/scrapers/cache_store.py` は **MAL のみ** が利用 (`src/scrapers/mal_scraper.py` の `JikanClient.get()`)。同じパターン (ID → URL → GET → JSON/HTML) を持つ allcinema / ann / keyframe にも cache を効かせる。

そのために `src/scrapers/fetchers.py` を新規作成し、URL builder + cache + HTTP GET をパッケージ化する `HtmlFetcher` / `XmlBatchFetcher` / `JsonFetcher` を提供する。

---

## Hard constraints

- **既存 fetch 挙動を変えない**: cache miss 時は従来通り HTTP GET、結果を従来通り返す
- **cache 無効化フラグ尊重**: `SCRAPER_CACHE_DISABLE=1` / `PYTEST_CURRENT_TEST` (test 中は cache 自動無効) を維持
- **cache key の安定性**: 同じ URL + params で同じ key になる必要 (key 計算が変わると既存 cache が無効化される)
- **rate-limit を緩めない**: cache miss 時は必ず `RateLimitedHttpClient._gate()` 経由

---

## Pre-conditions

- [ ] `01_http_client_dedupe` 完了
- [ ] `git status` clean
- [ ] `pixi run test-scoped tests/scrapers/` baseline pass
- [ ] `cache_store.py` の API 確認: `load_cached_json(namespace, key_payload)` / `save_cached_json(namespace, key_payload, payload)`

---

## Files to create / modify

| File | 変更内容 |
|------|---------|
| `src/scrapers/fetchers.py` | **新規**: `HtmlFetcher` / `XmlBatchFetcher` / `JsonFetcher` クラス + `cached()` decorator |
| `src/scrapers/cache_store.py` | `load_cached_text()` / `save_cached_text()` を追加 (HTML / XML 用) |
| `src/scrapers/allcinema_scraper.py` | `scrape_cinema()` / `scrape_person()` を `HtmlFetcher` 経由に切替 (この CARD では実装せず、`04` で Runner 移行と同時に行う) |
| `src/scrapers/ann_scraper.py` | 同上 |
| `src/scrapers/keyframe_scraper.py` | 同上 |

`04_runner_abstraction` で実際の scraper 移行を行うため、本 CARD では **`fetchers.py` + `cache_store.py` 拡張のみ** が範囲。

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/scrapers/parsers/*.py` | 純関数 parser、I/O 関係なし |
| `src/scrapers/mal_scraper.py` | 既に `cache_store` 使用、pattern が既に適用されている。Runner 移行時も触らない |
| `src/scrapers/anilist_scraper.py` | GraphQL pagination が特殊、Runner / Fetcher 移行対象外 |

---

## Steps

### Step 1: `cache_store.py` に text 版を追加

```python
# 追加
def load_cached_text(namespace: str, key_payload: dict[str, Any]) -> str | None:
    """Load text content (HTML/XML) from cache. Returns None if missing/disabled."""
    path = _cache_file(namespace, key_payload)
    if path is None or not path.exists():
        return None
    try:
        return path.read_text(encoding="utf-8")
    except OSError:
        return None


def save_cached_text(namespace: str, key_payload: dict[str, Any], text: str) -> None:
    """Save text content (HTML/XML) to cache. No-op if disabled."""
    path = _cache_file(namespace, key_payload)
    if path is None:
        return
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text, encoding="utf-8")
    except OSError:
        return
```

ファイル末尾は **`.txt`** 拡張子か検討 — 既存 `.json` と digest 衝突しないよう `_cache_file` の拡張子分岐を追加するか、namespace で分離 (`mal/rest` vs `allcinema/cinema`) で十分か判断。**namespace 分離で十分** と判断。

### Step 2: `src/scrapers/fetchers.py` 新規作成

```python
"""Fetcher abstractions: ID → raw payload, with cache integration.

Each fetcher wraps a RateLimitedHttpClient and a URL builder, and
transparently caches successful responses via cache_store.

Subclasses:
    HtmlFetcher       — single-ID HTML pages (allcinema cinema/person, ann person, keyframe anime)
    XmlBatchFetcher   — multi-ID XML batch endpoints (ann anime: ?anime=1/2/3...)
    JsonFetcher       — single-ID JSON endpoints (mal-style, but mal already uses cache_store directly)

All fetchers honor SCRAPER_CACHE_DISABLE=1 and PYTEST_CURRENT_TEST.
"""
from __future__ import annotations

from typing import Awaitable, Callable, Generic, TypeVar

import httpx
import structlog

from src.scrapers.cache_store import (
    load_cached_json,
    load_cached_text,
    save_cached_json,
    save_cached_text,
)

log = structlog.get_logger()

ID = TypeVar("ID")


class HtmlFetcher(Generic[ID]):
    """Fetch HTML for a single ID with cache.

    url_builder: ID → str  (full URL)
    namespace:   cache namespace e.g. 'allcinema/cinema'
    accept_404:  if True, return None on 404 (not raise)
    """

    def __init__(
        self,
        client,                                  # RateLimitedHttpClient or wrapper with .get()
        url_builder: Callable[[ID], str],
        *,
        namespace: str,
        accept_404: bool = True,
    ) -> None:
        self._client = client
        self._url_builder = url_builder
        self._namespace = namespace
        self._accept_404 = accept_404

    async def __call__(self, id_: ID) -> str | None:
        url = self._url_builder(id_)
        cache_key = {"url": url}
        cached = load_cached_text(self._namespace, cache_key)
        if cached is not None:
            return cached
        resp = await self._client.get(url)
        if self._accept_404 and resp.status_code == 404:
            return None
        resp.raise_for_status() if hasattr(resp, "raise_for_status") else None
        text = resp.text
        save_cached_text(self._namespace, cache_key, text)
        return text


class XmlBatchFetcher(Generic[ID]):
    """Fetch XML for a batch of IDs (e.g. ANN ?anime=1/2/3).

    Returns the raw XML string for the entire batch.
    Caches per batch — same batch IDs (sorted, joined) → same cache key.
    """

    def __init__(
        self,
        client,
        url_builder: Callable[[list[ID]], str],
        *,
        namespace: str,
    ) -> None:
        self._client = client
        self._url_builder = url_builder
        self._namespace = namespace

    async def __call__(self, ids: list[ID]) -> str | None:
        url = self._url_builder(ids)
        cache_key = {"url": url, "ids": [str(i) for i in sorted(ids, key=str)]}
        cached = load_cached_text(self._namespace, cache_key)
        if cached is not None:
            return cached
        resp = await self._client.get(url)
        text = resp.text
        save_cached_text(self._namespace, cache_key, text)
        return text


class JsonFetcher(Generic[ID]):
    """Fetch JSON for a single ID with cache.

    For mal-style usage. mal_scraper already does this inline; this class
    is for new scrapers or future migrations.
    """

    def __init__(
        self,
        client,
        url_builder: Callable[[ID], tuple[str, dict | None]],
        *,
        namespace: str,
    ) -> None:
        self._client = client
        self._url_builder = url_builder
        self._namespace = namespace

    async def __call__(self, id_: ID) -> dict | None:
        url, params = self._url_builder(id_)
        cache_key = {"url": url, "params": params or {}}
        cached = load_cached_json(self._namespace, cache_key)
        if cached is not None:
            return cached
        resp = await self._client.get(url, params=params) if params else await self._client.get(url)
        if resp.status_code == 404:
            return None
        resp.raise_for_status()
        data = resp.json()
        save_cached_json(self._namespace, cache_key, data)
        return data
```

### Step 3: 動作確認 (単体)

```bash
pixi run python -c "
import asyncio
from src.scrapers.fetchers import HtmlFetcher
# Mock client
class MockClient:
    async def get(self, url):
        class R:
            status_code = 200
            text = '<html>test</html>'
            def raise_for_status(self): pass
        return R()

async def main():
    f = HtmlFetcher(MockClient(), lambda i: f'http://x/{i}', namespace='test/x')
    r = await f(1)
    assert r == '<html>test</html>'
    # 2 回目は cache から
    r2 = await f(1)
    assert r2 == '<html>test</html>'
    print('OK')

import os
os.environ['SCRAPER_CACHE_DISABLE'] = '1'  # cache 無効モードでも動作
asyncio.run(main())
"
```

### Step 4: テスト追加

`tests/scrapers/test_fetchers.py` を新規作成 (40-60 行):
- `HtmlFetcher` cache miss → HTTP, cache hit → no HTTP
- 404 で None
- `XmlBatchFetcher` で複数 ID
- `SCRAPER_CACHE_DISABLE=1` で cache 無効化

---

## Verification

```bash
# 1. import OK
pixi run python -c "
from src.scrapers.fetchers import HtmlFetcher, XmlBatchFetcher, JsonFetcher
from src.scrapers.cache_store import load_cached_text, save_cached_text
print('OK')
"

# 2. 新規テスト pass
pixi run test-scoped tests/scrapers/test_fetchers.py

# 3. cache_store 既存テスト (もしあれば)
pixi run test-scoped -k cache_store

# 4. 既存 mal scraper テスト (cache_store API 変更で壊れてないか)
pixi run test-scoped tests/scrapers/test_mal*.py 2>/dev/null || echo 'no mal tests'

# 5. Lint
pixi run lint
```

---

## Stop-if conditions

- 新規テスト fail
- 既存 cache_store / mal テスト fail
- `git diff --stat` が +300/-100 を超える

---

## Rollback

```bash
git checkout src/scrapers/cache_store.py
rm -f src/scrapers/fetchers.py tests/scrapers/test_fetchers.py
pixi run test-scoped tests/scrapers/
```

---

## Completion signal

- [ ] `src/scrapers/fetchers.py` 存在、3 クラス定義
- [ ] `src/scrapers/cache_store.py` に `load_cached_text` / `save_cached_text` 追加
- [ ] `tests/scrapers/test_fetchers.py` pass
- [ ] git log message: `feat(scraper): add Fetcher abstractions + cache text helpers (13_scraper_runner_refactor/02)`
