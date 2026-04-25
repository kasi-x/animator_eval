# Task: Jikan 二重 rate limit 厳密化 (3 req/s + 60 req/min)

**ID**: `12_mal_scraper_jikan/04_rate_limit_strict`
**Priority**: 🟠
**Estimated changes**: 約 +200 / -30 lines, 主に `src/scrapers/http_base.py` 拡張 + mal_scraper 統合
**Requires senior judgment**: yes (semaphore 2 段構成、429/503 Retry-After 解釈、jitter 適用)
**Blocks**: `12_mal_scraper_jikan/05_rescrape`
**Blocked by**: `12_mal_scraper_jikan/03_scraper_phases`

---

## Goal

Jikan v4 公式制限 = **3 req/s + 60 req/min** を **両方** 厳密に守る。現実装は秒間 (`REQUEST_INTERVAL=0.4` ≒ 2.5 req/s) のみで、60/min は守れていない (60 req in 24s で枯渇 → 429)。

---

## Hard constraints

- **両 limit 同時遵守**: 秒 3 / 分 60 のうち厳しい方が支配。ピーク時は 60/min が支配的。
- **Retry-After 尊重**: 429 / 503 レスポンスに `Retry-After` header あれば必ず従う (bangumi 04 と同じ方針)。
- **既存 cache hit 時 = limit 消費なし**: `cache_store` で hit した場合は HTTP 飛ばないため semaphore も触らない (cache_key 計算 → cache 確認の順、semaphore 取得は HTTP 直前)。
- **graceful close**: scraper 中断時、開いている `RetryingHttpClient` を必ず close (既存 `try/finally` 維持)。

---

## Pre-conditions

- [ ] Card 03 完了 (mal_scraper 3 Phase 動作確認)
- [ ] 現 `RateLimitedHttpClient` (`src/scrapers/http_base.py`) 実装把握
- [ ] `RetryingHttpClient` (`src/scrapers/http_client.py`) の retry 既存ロジック把握 (重複しないこと確認)

---

## Files to modify

| File | 変更 |
|------|------|
| `src/scrapers/http_base.py` | `RateLimitedHttpClient` を二重 window semaphore へ拡張 (新クラス `DualWindowRateLimiter` 追加 + 既存 backward compat) |
| `src/scrapers/mal_scraper.py` | `JikanClient.__init__` で `DualWindowRateLimiter(per_second=3, per_minute=60)` 適用 |
| `src/scrapers/http_client.py` | 429/503 Retry-After header 解釈 (既存 retry に組み込み、未対応なら追加) |
| `tests/scrapers/test_http_rate_limit.py` | 新規、二重 window 動作の unit test |

## Files to NOT touch

| File | 理由 |
|------|------|
| その他 scraper (`anilist_scraper.py` 等) | mal 以外の rate limit 設定は変更しない (将来 opt-in) |
| `cache_store.py` | cache hit 経路は限定変更不要 |

---

## 実装

### `DualWindowRateLimiter`

```python
import asyncio
import time
from collections import deque


class DualWindowRateLimiter:
    """秒/分 二重 sliding window rate limiter。

    Jikan v4: per_second=3, per_minute=60。
    """

    def __init__(self, per_second: int, per_minute: int) -> None:
        self.per_second = per_second
        self.per_minute = per_minute
        self._sec_window: deque[float] = deque()  # acquire 時刻 (mono)
        self._min_window: deque[float] = deque()
        self._lock = asyncio.Lock()

    async def acquire(self) -> None:
        async with self._lock:
            while True:
                now = time.monotonic()
                # prune 古い acquire 履歴
                while self._sec_window and now - self._sec_window[0] >= 1.0:
                    self._sec_window.popleft()
                while self._min_window and now - self._min_window[0] >= 60.0:
                    self._min_window.popleft()
                # capacity ok?
                wait_sec = 0.0
                if len(self._sec_window) >= self.per_second:
                    wait_sec = max(wait_sec, 1.0 - (now - self._sec_window[0]))
                if len(self._min_window) >= self.per_minute:
                    wait_sec = max(wait_sec, 60.0 - (now - self._min_window[0]))
                if wait_sec <= 0:
                    self._sec_window.append(now)
                    self._min_window.append(now)
                    return
                await asyncio.sleep(wait_sec + 0.01)  # epsilon
```

### `RateLimitedHttpClient` 統合

```python
class RateLimitedHttpClient:
    def __init__(self, *, delay: float = 0.0,
                 limiter: DualWindowRateLimiter | None = None) -> None:
        self._delay = delay  # 旧式 (互換)
        self._limiter = limiter
        self._last = 0.0
        self._lock = asyncio.Lock()

    async def _gate(self) -> None:
        if self._limiter is not None:
            await self._limiter.acquire()
            return
        # 旧 fixed-delay 経路
        async with self._lock:
            elapsed = time.monotonic() - self._last
            if elapsed < self._delay:
                await asyncio.sleep(self._delay - elapsed)
            self._last = time.monotonic()
```

### `JikanClient` 適用 (mal_scraper.py)

```python
JIKAN_LIMITER = DualWindowRateLimiter(per_second=3, per_minute=60)


class JikanClient(RateLimitedHttpClient):
    def __init__(self, transport=None) -> None:
        super().__init__(limiter=JIKAN_LIMITER)
        self._http = RetryingHttpClient(
            source="mal",
            base_url=BASE_URL,
            delay=0.0,  # limiter が制御するので 0
            timeout=30.0,
            headers={"Accept": "application/json"},
            transport=transport,
        )

    async def get(self, endpoint, params=None) -> dict:
        cache_key = {"endpoint": endpoint, "params": params or {}}
        cached = load_cached_json("mal/rest", cache_key)
        if cached is not None:
            return cached  # ← limiter 触らず

        await self._gate()  # ← HTTP 直前で acquire
        resp = await self._http.get(endpoint, params=params)
        resp.raise_for_status()
        data = resp.json()
        save_cached_json("mal/rest", cache_key, data)
        return data
```

### Retry-After 解釈

`src/scrapers/http_client.py` の `RetryingHttpClient` (httpx 上 wrapper) の retry policy 内で:

```python
import httpx

async def _retry_with_backoff(self, method, url, **kwargs):
    for attempt in range(self.max_retries):
        try:
            resp = await self._client.request(method, url, **kwargs)
            if resp.status_code in (429, 503):
                retry_after = resp.headers.get("Retry-After")
                if retry_after is not None:
                    try:
                        wait = float(retry_after)
                    except ValueError:
                        # HTTP-date 形式 (稀)
                        wait = 60.0
                    log.warning("retry_after_honored", source=self.source,
                                status=resp.status_code, wait_s=wait)
                    await asyncio.sleep(wait + 0.5)  # epsilon
                    continue
                # Retry-After なしは exponential backoff (既存)
                await asyncio.sleep(2 ** attempt)
                continue
            return resp
        except httpx.TimeoutException:
            await asyncio.sleep(2 ** attempt)
    raise RuntimeError(f"max retries exceeded: {url}")
```

既存実装に Retry-After 解釈ロジックがあれば差分追加のみ。

---

## Steps

### Step 1: `DualWindowRateLimiter` 単体実装 + test

```python
# tests/scrapers/test_http_rate_limit.py
import asyncio
import time
import pytest
from src.scrapers.http_base import DualWindowRateLimiter


@pytest.mark.asyncio
async def test_per_second_limit():
    """秒間 3 req 厳守 (4 個目は 1s 待機)"""
    lim = DualWindowRateLimiter(per_second=3, per_minute=60)
    t0 = time.monotonic()
    for _ in range(4):
        await lim.acquire()
    elapsed = time.monotonic() - t0
    assert 0.95 < elapsed < 1.5, f"expected ~1s, got {elapsed}"


@pytest.mark.asyncio
async def test_per_minute_limit():
    """分間 60 req 厳守 (61 個目は 60s 待機 — テストでは per_minute=5 で短縮)"""
    lim = DualWindowRateLimiter(per_second=100, per_minute=5)  # per_second 緩和、per_minute だけ検査
    t0 = time.monotonic()
    for _ in range(6):
        await lim.acquire()
    # 検査用に 5 -> 60s wait は test では難しい → per_minute=5 で 6 個目が ~60s 待つはずだが
    # test は dry-run、wait なしで終わる前提で 60s sleep を想定 mock するか skip。
    # ここでは monotonic patch で実装確認のみ。
    elapsed = time.monotonic() - t0
    # 短縮テスト用に per_minute_window を 5s に override する monkeypatch を別 test で実施


@pytest.mark.asyncio
async def test_cache_hit_no_acquire():
    """既存 cache hit 経路で limiter が触られないこと (mock cache_store で確認)"""
    # mal_scraper.JikanClient で実施
```

### Step 2: `RateLimitedHttpClient` に limiter 注入対応

backward compat: 既存 `delay=` 経路は維持、新 `limiter=` を優先。

### Step 3: `JikanClient` 適用

`JIKAN_LIMITER` を module-level singleton (multi process scrape はしないので OK)。

### Step 4: Retry-After 統合

既存 `RetryingHttpClient` を読み、未対応なら追加。

### Step 5: integration smoke

```bash
# 1 分間で 60 req 強制送信 → 61 req 目で ~60s 待機ログ確認
pixi run python -c "
import asyncio
from src.scrapers.mal_scraper import JikanClient
async def smoke():
    c = JikanClient()
    try:
        for i in range(70):
            await c.get(f'/anime/{i+1}')
            print(i+1, 'ok')
    finally:
        await c.close()
asyncio.run(smoke())
" 2>&1 | tail -20
# 期待: 60 件目以降 'rate_limit_wait' ログ + 約 60s 停止
```

---

## Verification

```bash
# 1. unit test
pixi run test-scoped tests/scrapers/test_http_rate_limit.py -v

# 2. mal smoke (上記)
pixi run python -m src.scrapers.mal_scraper --phase A --resume false &
PID=$!
sleep 90 && kill $PID
grep -E "rate_limit|429|retry_after" logs/scrapers/mal_*.jsonl | tail -10

# 3. lint
pixi run lint
```

---

## Stop-if conditions

- [ ] `asyncio.Lock` で接続が serialized され想定 throughput 出ない (3 req/s に達しない) → lock-free 化検討
- [ ] Jikan が 60/min より厳しい hidden limit 持つ (公式 docs と乖離) → 観測値で per_minute 下方修正
- [ ] Retry-After header 不在で 429 連発 → exponential backoff 上限延長

---

## Rollback

```bash
git checkout src/scrapers/http_base.py src/scrapers/http_client.py src/scrapers/mal_scraper.py
rm -f tests/scrapers/test_http_rate_limit.py
```

---

## Completion signal

- [ ] `DualWindowRateLimiter` unit test 3+ ケース pass
- [ ] mal smoke で 60 req/min 超過時に明示的 wait log 出る
- [ ] 既存 anilist / ann scraper の動作 regression なし (旧 delay= 経路維持確認)
- [ ] `lint` pass
- [ ] `DONE: 12_mal_scraper_jikan/04_rate_limit_strict` 記録
