# Task: Playwright 基盤導入と共通 async HTTP wrapper 作成

**ID**: `09_sakuga_atwiki/01_playwright_infra`
**Priority**: 🔴
**Estimated changes**: 約 +250 / -0 lines, 4 files
**Requires senior judgment**: no
**Blocks**: `09_sakuga_atwiki/02_page_discovery`
**Blocked by**: なし

---

## Goal

Cloudflare Managed Challenge を通過可能な Playwright ベース async HTTP wrapper を `src/scrapers/http_playwright.py` に実装し、将来他 scraper (atwiki 系全般 / CF 保護サイト) で再利用可能にする。

---

## Hard constraints

(`_hard_constraints.md` を事前に読むこと)

- H1 anime.score を scoring に使わない (今 card では無関係)
- H5 既存テスト 2450+ 件は green 維持
- H6 pre-commit `--no-verify` 禁止
- **追加**: UA 文字列に `Claudebot`, `GPTBot`, `ChatGPT-User`, `Bytespider` を **含めない** (atwiki root robots.txt で明示 block されている AI bot 識別子の使用回避)

---

## Pre-conditions

- [ ] `git status` が clean
- [ ] `pixi run test` が baseline pass
- [ ] **atwiki 利用規約** (https://atwiki.jp/) の「自動アクセス」「クローラ」関連条項を人間が確認済み

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `pyproject.toml` or `pixi.toml` | `playwright` + `playwright-stealth` 依存追加 |
| `src/scrapers/http_playwright.py` | **新規**: `PlaywrightFetcher` async context manager + `fetch(url) -> str` API |
| `tests/scrapers/test_http_playwright.py` | **新規**: mock 環境でのユニットテスト (実 HTTP はかけない) |
| `docs/SCRAPING.md` | **既存であれば追記、無ければ新規**: Playwright 運用手順 (chromium install, headless/headful 切替, 失敗時デバッグ) |

---

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/scrapers/http_client.py`, `retrying_http_client.py` | 既存 httpx 路線は維持。Playwright は並列路線 |
| 既存 scraper (`anilist_scraper.py` 等) | 今 card では移行しない |

---

## Steps

### Step 1: 依存追加

```bash
pixi add playwright
pixi add playwright-stealth
# Chromium バイナリ取得 (~300MB)
pixi run playwright install chromium
```

`pixi run playwright install --help` で確認し、pixi task として `install-playwright` を登録推奨。

### Step 2: `src/scrapers/http_playwright.py` 実装

要件:

- `PlaywrightFetcher` = async context manager
- `__aenter__`: chromium launch (headless=True 既定、`HEADFUL=1` env で切替)、persistent context で cookie 永続化 (`data/playwright_profile/`)
- `__aexit__`: browser close
- `async def fetch(url: str, *, wait_selector: str | None = None, timeout_ms: int = 30000) -> str`:
  - `page.goto(url, wait_until="networkidle")`
  - CF challenge 検知: `page.title()` に `"Just a moment"` / `"Attention Required"` 含まれたら最大 30 秒 `wait_for_load_state` で通過待機
  - 通過後 `page.content()` を返す (decoded HTML)
- UA: Chrome 系の通常 UA (`Mozilla/5.0 (X11; Linux x86_64) ... Chrome/125.0.0.0`)。**Claudebot / GPTBot 類は含めない**
- `stealth_async(page)` 適用 (navigator.webdriver 等の指紋を隠す)
- レート制限: `fetch()` 内では sleep しない。**caller 側で** `await asyncio.sleep(delay)` を制御

擬似シグネチャ:

```python
class PlaywrightFetcher:
    def __init__(self, *, headless: bool = True, profile_dir: Path | None = None): ...
    async def __aenter__(self) -> "PlaywrightFetcher": ...
    async def __aexit__(self, *exc_info) -> None: ...
    async def fetch(self, url: str, *, wait_selector: str | None = None, timeout_ms: int = 30000) -> str: ...
```

### Step 3: ユニットテスト

`tests/scrapers/test_http_playwright.py`:

- `PlaywrightFetcher.__init__` のデフォルト値テスト
- UA 文字列に禁止トークン (`Claudebot`, `GPTBot`) が含まれないことのテスト
- 実 HTTP は走らせない (`pytest.mark.skipif(os.getenv("RUN_E2E") != "1", ...)` で E2E 層と分離)

### Step 4: ドキュメント

`docs/SCRAPING.md` (新規 or 追記):

- Playwright 導入済 scraper の一覧
- chromium 再 install 手順
- headful デバッグ (`HEADFUL=1 pixi run python -m src.scrapers.sakuga_atwiki_scraper ...`)
- CF 通過失敗時のトラブルシュート

---

## Verification

```bash
# 1. Unit test (新規 + 既存)
pixi run test-scoped tests/scrapers/test_http_playwright.py
pixi run test   # 既存 2450+ 件 green 維持

# 2. Lint
pixi run lint

# 3. 依存確認
pixi run python -c "from playwright.async_api import async_playwright; print('ok')"
pixi run playwright install --dry-run chromium

# 4. 禁止 UA トークン検証
rg -i "Claudebot|GPTBot|ChatGPT-User|Bytespider" src/scrapers/http_playwright.py
# → 0 件

# 5. 手動 smoke (任意、atwiki 実アクセス)
HEADFUL=1 pixi run python -c "
import asyncio
from src.scrapers.http_playwright import PlaywrightFetcher
async def main():
    async with PlaywrightFetcher(headless=False) as f:
        html = await f.fetch('https://www18.atwiki.jp/sakuga/pages/1.html')
        print(len(html), '作画' in html)
asyncio.run(main())
"
# → 10000+ chars, True
```

---

## Stop-if conditions

- [ ] `pixi run test` が失敗
- [ ] `pixi run lint` が失敗
- [ ] chromium install が失敗 (ディスク容量 / network 問題)
- [ ] smoke test で CF を通過できない (3 試行 × 2 分) → Cloudflare 対策強化された可能性、別戦略を検討

---

## Rollback

```bash
git checkout pyproject.toml pixi.lock 2>/dev/null || true
rm -f src/scrapers/http_playwright.py
rm -f tests/scrapers/test_http_playwright.py
rm -f docs/SCRAPING.md   # 新規作成した場合のみ
pixi install
pixi run test
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] `git diff --stat` が ±250 lines 以内
- [ ] 作業ログに `DONE: 09_sakuga_atwiki/01_playwright_infra` と記録
