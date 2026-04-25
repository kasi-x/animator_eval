# Task: 作画@wiki 全ページ ID 発見とページ分類

**ID**: `09_sakuga_atwiki/02_page_discovery`
**Priority**: 🔴
**Estimated changes**: 約 +350 / -0 lines, 4 files
**Requires senior judgment**: no
**Blocks**: `09_sakuga_atwiki/03_person_parser`
**Blocked by**: `09_sakuga_atwiki/01_playwright_infra`

---

## Goal

作画@wiki (`www18.atwiki.jp/sakuga/`) 内の全ページ ID を列挙し、各ページを {人物 / 作品 / 索引 / メタ / 不明} の 5 分類でラベル付けして `data/sakuga/discovered_pages.json` に保存する。

---

## Hard constraints

- `_hard_constraints.md` 参照
- **robots.txt 遵守**: `www18.atwiki.jp/robots.txt` で disallow されているパス (`/*/search`, `/*/backup`, `/*/edit*` 等) には **絶対に** リクエストを送らない
- レート制限: `delay >= 3.0s` (`src/utils/config.py` の `SCRAPE_DELAY_SECONDS` を流用)
- robots.txt は毎回 fetch し直さず、`01_playwright_infra` の fetcher を使って 1 回取得 → 遵守対象パスを list に抽出 → 全クロール URL でこの list 照合

---

## Pre-conditions

- [ ] `09_sakuga_atwiki/01_playwright_infra` 完了
- [ ] `git status` が clean
- [ ] `pixi run test` が pass

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/scrapers/sakuga_atwiki_scraper.py` | **新規**: entrypoint CLI (`discover` subcommand) |
| `src/scrapers/parsers/sakuga_atwiki.py` | **新規**: ページ分類ヘルパー (`classify_page_kind(title, html) -> Literal["person", "work", "index", "meta", "unknown"]`) |
| `src/scrapers/parsers/sakuga_atwiki_robots.py` | **新規**: robots.txt の許可判定 |
| `tests/scrapers/test_sakuga_atwiki_discovery.py` | **新規**: 分類器のユニットテスト (fixture HTML 使用) |

---

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/scrapers/http_playwright.py` | Phase 1 で完成済、Phase 2 では consumer |
| 既存 scraper 全般 | 本 card では他 scraper に触らない |

---

## Steps

### Step 1: robots.txt parser

`src/scrapers/parsers/sakuga_atwiki_robots.py`:

- `async def fetch_robots(fetcher) -> list[str]` — disallow path 一覧取得 (glob パターンとして保持)
- `def is_allowed(url_path: str, disallow_patterns: list[str]) -> bool` — glob マッチで判定
- 起動時に 1 回取得 → メモリ保持。以降の全 crawl でこれを照合

### Step 2: ページ分類器

`src/scrapers/parsers/sakuga_atwiki.py` 内 `classify_page_kind()`:

判定ロジック (優先度順):

1. **メタ**: `title == "メニュー - 作画@wiki - atwiki..."` または `pages/1.html` (トップ) → `meta`
2. **索引**: `title` に「一覧」「索引」「〜行」含む、かつ h2 が 50音分類パターン (「あ行」「か行」...) → `index`
3. **人物**: wikibody 内に `<h3>フィルモグラフィ</h3>` または「参加作品」「代表作」見出し有、かつ h2 に作品分類 (「劇場」「TV」等) が **無い** → `person`
4. **作品**: h2 に「スタッフ」「キャスト」「話数」含む → `work`
5. それ以外: `unknown`

`title` パターン例 (偵察結果より):
- 人物: `"下谷智之 - 作画@wiki - atwiki（アットウィキ）"` (個人名 + サイト名接尾)
- 索引: `"作画アニメ - 作画@wiki - atwiki..."` (一覧系タイトル)
- メタ: `"メニュー - 作画@wiki - atwiki..."`

### Step 3: 発見クローラ

`src/scrapers/sakuga_atwiki_scraper.py` に `discover` CLI 追加:

- **Seed**:
  - メニュー (`pages/2.html`)
  - 作画アニメ一覧 (`pages/100.html`)
  - トップ (`pages/1.html`)
- **BFS**:
  - 各ページから `href="/sakuga/pages/\d+\.html"` 正規表現で内部 ID 抽出
  - 訪問済 set で重複除外
  - robots.txt `is_allowed()` で事前フィルタ (但し `/pages/\d+\.html` は全面 allow のため実質的には異常パス検出用)
  - 1 req = `delay=3.0s` sleep + fetch
- **分類**: `classify_page_kind(title, html)` で 5 分類
- **永続化**: `data/sakuga/discovered_pages.json` に `[{id, url, title, page_kind, discovered_at, last_hash}]` として dump
- **再開**: 既存 JSON があれば読込 → 差分のみ fetch (訪問済 skip)
- **上限保護**: `--max-pages` オプション (既定: 3000) で暴走防止

### Step 4: HTML cache

- 取得した HTML は `data/sakuga/cache/<id>.html.gz` として gzip 保存
- `hash_utils.hash_anime_data` ではなく **HTML 生本体の SHA256** を `last_hash` に記録
- Phase 3 parser はこの cache から再読込 (HTTP 再発行なし)

### Step 5: テスト

`tests/scrapers/test_sakuga_atwiki_discovery.py`:

- `classify_page_kind` について 5 分類 × 2 fixture ずつ = 10 ケース (fixture HTML は `tests/fixtures/sakuga/*.html` に最小サンプルを保存)
- `is_allowed` について robots.txt パターン表 3 件
- discovery の BFS ロジックは実 HTTP 経由せず `PlaywrightFetcher` を monkeypatch したスタブで検証

---

## Verification

```bash
# 1. Unit
pixi run test-scoped tests/scrapers/test_sakuga_atwiki_discovery.py
pixi run test   # 全体 green 維持

# 2. Lint
pixi run lint

# 3. 分類カバレッジ (fixture 経由)
pixi run python -c "
from src.scrapers.parsers.sakuga_atwiki import classify_page_kind
# fixture 全件 kind が期待どおりに返るかアサート
"

# 4. smoke discovery (実 HTTP、10 ページ上限)
pixi run python -m src.scrapers.sakuga_atwiki_scraper discover --max-pages 10 --delay 3.0
# → data/sakuga/discovered_pages.json に 10 件以下記録、json構造が正しいこと

# 5. robots.txt 遵守確認
rg -E '(search|backup|edit|preview)' data/sakuga/discovered_pages.json
# → 0 件 (disallow パスを誤って踏んでいない)
```

---

## Stop-if conditions

- [ ] smoke discovery で CF 通過率 < 80% → Phase 1 に戻って stealth 調整
- [ ] 10 ページの smoke に 10 分以上かかる (= 1 req > 60s) → delay 値またはロジックに問題
- [ ] 分類 fixture で 3 件以上 `unknown` 返却 → 分類ロジックが甘すぎ、ルール見直し

---

## Rollback

```bash
git checkout src/scrapers/
rm -rf data/sakuga/
pixi run test
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] smoke で `page_kind` の `person` / `index` / `meta` がそれぞれ 1 件以上出現
- [ ] `git diff --stat` が ±350 lines 以内
- [ ] 作業ログに `DONE: 09_sakuga_atwiki/02_page_discovery` と記録
