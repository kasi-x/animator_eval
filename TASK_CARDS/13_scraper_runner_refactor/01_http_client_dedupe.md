# Task: `RetryingHttpClient` 二重定義の統合

**ID**: `13_scraper_runner_refactor/01_http_client_dedupe`
**Priority**: 🔴
**Estimated changes**: 約 +60 / -180 lines, 3 files (`http_client.py` 拡張, `retrying_http_client.py` 削除, `anilist_scraper.py` import 切替)
**Requires senior judgment**: yes (anilist 挙動繊細、X-RateLimit ヘッダ + コールバック)
**Blocks**: `02_cache_store_expansion`, `04_runner_abstraction`
**Blocked by**: なし

---

## Goal

`src/scrapers/retrying_http_client.py` (anilist 専用、174 行) と `src/scrapers/http_client.py` の `RetryingHttpClient` (汎用、208 行) が **同名クラスで別実装**。anilist 用の機能を汎用版にマージし、`retrying_http_client.py` を削除する。

---

## Hard constraints

(`_hard_constraints.md` を事前に読むこと)

- **anilist の rate-limit 挙動を変えない**: X-RateLimit-Remaining/Reset/Limit ヘッダの context dict 格納、`on_rate_limit` callback、429 後の wait 動作を維持
- **既存テスト pass**: `tests/scrapers/test_anilist*.py` (もしあれば) が pass
- **REQUEST_INTERVAL = 0.1 (burst mode)** を維持
- **probe-after-wait 機能は判断**: rate-limit 解除後の確認 GET (`/SiteStatistics` query) は anilist 固有。**統合後は削除候補**だが既存テストが落ちないか確認後に判断

---

## Pre-conditions

- [ ] `git status` clean
- [ ] `pixi run test-scoped tests/scrapers/` baseline pass (件数記録)
- [ ] 両 `RetryingHttpClient` の差分を読んで理解した:
  - `src/scrapers/http_client.py` (汎用): GET/POST/request、Retry-After 解析、シンプル
  - `src/scrapers/retrying_http_client.py` (anilist 専用): POST のみ、X-RateLimit ヘッダ抽出、callback、probe

---

## Files to modify / delete

| File | 変更内容 |
|------|---------|
| `src/scrapers/http_client.py` | `RetryingHttpClient.request()` に `rate_limit_context: dict \| None`、`on_rate_limit: Callable \| None` パラメータ追加。X-RateLimit-* ヘッダを context に格納するロジック追加 |
| `src/scrapers/retrying_http_client.py` | **削除** |
| `src/scrapers/anilist_scraper.py` | `from src.scrapers.retrying_http_client import RetryingHttpClient` → `from src.scrapers.http_client import RetryingHttpClient`。`AnilistRetryingHttpClient` のような薄いラッパが必要なら作る |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/scrapers/http_base.py` (`RateLimitedHttpClient`) | 別レイヤ。既存 5 client が継承中 |
| `src/scrapers/parsers/anilist.py` | 純関数 parser、HTTP 関係なし |
| 他 scraper (`ann_scraper.py` 等) | `http_client.py` の `RetryingHttpClient` を使用中、API 変更 (新規パラメータ追加) のみで動作 |

---

## Steps

### Step 1: 差分を表に起こす

```bash
diff -u src/scrapers/http_client.py src/scrapers/retrying_http_client.py | less
```

| 機能 | http_client.py | retrying_http_client.py |
|---|---|---|
| 構築引数 | `source / delay / timeout / headers / base_url / max_attempts / initial_backoff / retryable_status / transport` | `timeout` のみ |
| GET | あり | なし |
| POST | あり | あり |
| 内部 throttle | `_throttle()` (delay) | なし (caller が rate を管理) |
| 429 wait | `Retry-After` ヘッダ + backoff | `X-RateLimit-Reset` 優先 + `Retry-After` fallback |
| X-RateLimit-* ヘッダ抽出 | なし | `rate_limit_context` dict に格納 |
| 進行中 callback | なし | `on_rate_limit(secs)` 0.5 sec ステップ |
| Probe-after-wait | なし | `/SiteStatistics` query で再確認 |
| Retry on 5xx | あり (status set 設定可能) | なし (`(400/401/404) を返す + 他 retry`) |

### Step 2: `http_client.py` の `request()` を拡張

```python
# Before (Step 抜粋)
async def request(
    self,
    method: str,
    url: str,
    *,
    params: dict | None = None,
    json: object | None = None,
    headers: dict | None = None,
    max_attempts: int | None = None,
) -> httpx.Response:
    ...

# After
async def request(
    self,
    method: str,
    url: str,
    *,
    params: dict | None = None,
    json: object | None = None,
    headers: dict | None = None,
    max_attempts: int | None = None,
    rate_limit_context: dict | None = None,
    on_rate_limit: Callable[[int | None], None] | None = None,
) -> httpx.Response:
    ...
    # 各 response 受信後:
    if rate_limit_context is not None and "X-RateLimit-Remaining" in resp.headers:
        rate_limit_context["remaining"] = int(resp.headers["X-RateLimit-Remaining"])
        rate_limit_context["reset_at"] = int(resp.headers.get("X-RateLimit-Reset", 0))
        rate_limit_context["limit"] = int(resp.headers.get("X-RateLimit-Limit", 0))
    # 429 wait 中に callback 呼び出し:
    if resp.status_code == 429 and on_rate_limit is not None:
        # 0.5 sec ステップで callback(remaining_secs) を呼ぶ
        ...
```

`Retry-After` の優先度は `_parse_retry_after` を拡張: `X-RateLimit-Reset` (epoch) があればこちらを優先、なければ `Retry-After` 秒数。

### Step 3: `anilist_scraper.py` の import 切替

```python
# Before
from src.scrapers.retrying_http_client import RetryingHttpClient

# After
from src.scrapers.http_client import RetryingHttpClient
```

`anilist_scraper.py` 内の `RetryingHttpClient(timeout=60.0)` 呼び出しは `RetryingHttpClient(source="anilist", base_url=ANILIST_URL, delay=REQUEST_INTERVAL, timeout=60.0)` に変更。`post(url, json=..., on_rate_limit=cb, rate_limit_context=ctx)` の呼び出し箇所はそのまま動くようパラメータ名を保持。

### Step 4: probe-after-wait 機能の扱い

**判断基準**:
- 既存 `pixi run test-scoped tests/scrapers/test_anilist*.py` が probe を期待しているか確認
- 期待しているなら `_anilist_probe_after_wait` を `RetryingHttpClient` に optional で追加 (`probe_url` + `probe_payload` 引数)
- 期待していないなら **削除** (汎用 retry-loop で十分)

判断後、本カードに結論を記録。

### Step 5: `retrying_http_client.py` 削除

```bash
git rm src/scrapers/retrying_http_client.py
```

### Step 6: 他箇所の import 検索

```bash
grep -rn "from src.scrapers.retrying_http_client" src/ tests/
grep -rn "import retrying_http_client" src/ tests/
```

ヒットあれば `http_client` に切替。

---

## Verification

```bash
# 1. import エラーなし
pixi run python -c "
from src.scrapers.http_client import RetryingHttpClient
from src.scrapers.anilist_scraper import app
print('OK')
"

# 2. 旧モジュール存在しない
test ! -f src/scrapers/retrying_http_client.py && echo 'deleted: OK'

# 3. anilist 関連テスト
pixi run test-scoped tests/scrapers/test_anilist*.py 2>/dev/null || \
  pixi run test-scoped -k anilist

# 4. http_client の他 caller (ann/mal/allcinema) も壊れてないか
pixi run test-scoped tests/scrapers/test_ann*.py tests/scrapers/test_allcinema*.py tests/scrapers/test_mal*.py 2>/dev/null || \
  pixi run test-scoped tests/scrapers/

# 5. Smoke test (anilist 1 query — 既知 anime ID)
pixi run python -c "
import asyncio
from src.scrapers.anilist_scraper import AnilistClient  # クラス名要確認
async def main():
    cl = AnilistClient()
    try:
        # 既知の小さいクエリで動作確認
        print('client constructed OK')
    finally:
        await cl.close()
asyncio.run(main())
"

# 6. Lint
pixi run lint
```

---

## Stop-if conditions

- import エラー
- anilist 関連テスト 1 件でも fail
- `git diff --stat` が +200/-300 を超える
- probe-after-wait 削除で挙動が変わると判定 (テスト fail)

---

## Rollback

```bash
git checkout src/scrapers/http_client.py src/scrapers/anilist_scraper.py
git restore --source=HEAD --staged --worktree -- src/scrapers/retrying_http_client.py
pixi run test-scoped tests/scrapers/
```

---

## Completion signal

- [ ] `src/scrapers/retrying_http_client.py` が存在しない
- [ ] `src/scrapers/http_client.py` の `RetryingHttpClient` が `rate_limit_context` + `on_rate_limit` 対応
- [ ] anilist 関連テスト pass
- [ ] 他 scraper テスト pass
- [ ] git log message: `refactor(scraper): unify RetryingHttpClient duplicates (13_scraper_runner_refactor/01)`
