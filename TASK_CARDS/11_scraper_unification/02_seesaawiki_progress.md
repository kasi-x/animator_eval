# Task: seesaawiki 内部 loop に scrape_progress 統合

**ID**: `11_scraper_unification/02_seesaawiki_progress`
**Priority**: 🟡
**Estimated changes**: 約 +30 / -30 lines, 1 file (`src/scrapers/seesaawiki_scraper.py`)
**Requires senior judgment**: no (構造的書換だがパターン明確)
**Blocks**: (なし)
**Blocked by**: (なし)

---

## Goal

`scrape_seesaawiki()` (line ~620) の内部 main loop に `scrape_progress` を統合し、CLI で `--quiet/--progress` を効かせる。`reparse()` 関数の loop も同様に統合する。

CLI 側の `progress_override` 引数受付は完了済 (`scrape_seesaawiki(progress_override=...)` 引数あり、内部未使用)。

---

## Hard constraints

- 既存の structlog log line を維持 (`log.info("seesaa_checkpoint", ...)` 等は `p.log(...)` で置換)
- `list_only` / `fetch_only` 分岐の挙動を変えない
- regex parser / LLM fallback の挙動を変えない
- 出力 parquet の内容を変えない

---

## Pre-conditions

- [ ] `git status` clean
- [ ] `pixi run python -c "from src.scrapers import seesaawiki_scraper; print('OK')"` 通る
- [ ] `src/scrapers/progress.py` 存在

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/scrapers/seesaawiki_scraper.py` | `scrape_seesaawiki()` 内 main loop / `reparse_seesaawiki()` 内 loop |

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/scrapers/parsers/seesaawiki.py` | parser ロジック不変 |
| `src/scrapers/seesaawiki_scraper.py` の CLI 部 | 完了済 |

---

## Steps

### Step 1: 構造把握

```bash
grep -n "for idx, page_info in enumerate\|stats\[\"pages_processed\"\]" src/scrapers/seesaawiki_scraper.py
```

主要 loop:
- line ~702: `fetch_only` 分岐内の for loop (HTML 取得のみ)
- line ~890: regular path の for loop (parse + write)
- `reparse()` 関数内の for loop (HTML 再パース)

各 loop の頭部で `scrape_progress` を開き、`p.advance()` を呼ぶ。`log.info("seesaa_checkpoint", ...)` を `p.log(...)` に置換。

### Step 2: `scrape_seesaawiki` の `progress_override` 受付確認

```bash
grep -n "progress_override" src/scrapers/seesaawiki_scraper.py
```

→ signature に `progress_override: bool | None = None` がすでにある (前 session で追加済)。内部で未使用 = ここを使う。

### Step 3: import 追加

ファイル先頭に既存 import が以下のようにある:

```python
from src.scrapers.cli_common import (
    CheckpointIntervalOpt,
    DataDirOpt,
    DelayOpt,
    ProgressOpt,
    QuietOpt,
    resolve_progress_enabled,
)
from src.scrapers.progress import scrape_progress  # ← 既にあるはず
```

ない場合は追加。

### Step 4: regular loop (line ~890 付近) に `scrape_progress` 適用

下記パターンで囲む:

```python
# Before
for idx, page_info in enumerate(all_pages):
    ...
    stats["pages_processed"] += 1
    if stats["pages_processed"] % checkpoint_interval == 0:
        group.flush_all()
        save_checkpoint(...)
        log.info("seesaa_checkpoint", progress=f"{idx + 1}/{len(all_pages)}", **stats)

# After
with scrape_progress(
    total=len(all_pages),
    description="seesaawiki scrape",
    enabled=progress_override,
) as p:
    for idx, page_info in enumerate(all_pages):
        ...
        stats["pages_processed"] += 1
        p.advance()
        if stats["pages_processed"] % checkpoint_interval == 0:
            group.flush_all()
            save_checkpoint(...)
            p.log("seesaa_checkpoint", progress=f"{idx + 1}/{len(all_pages)}", **stats)
```

ループ全体を `with` ブロック内に下げ、インデント 4 space 増。

### Step 5: `fetch_only` 分岐の loop (line ~702 付近) も同様

`scrape_progress(description="seesaawiki fetch-only", ...)` で別インスタンス。

### Step 6: `reparse()` 関数 (line ~1055 付近) も同様

`scrape_progress(description="seesaawiki reparse", ...)`。`reparse()` の signature にもまだ `progress_override` がない場合は追加し、CLI `def reparse(...)` から `resolve_progress_enabled(quiet, progress)` を渡す。

```bash
grep -n "def reparse" src/scrapers/seesaawiki_scraper.py
```

CLI signature に未追加なら以下を追加:

```python
@app.command()
def reparse(
    data_dir: DataDirOpt = DEFAULT_DATA_DIR,
    # ... 既存引数 ...
    quiet: QuietOpt = False,
    progress: ProgressOpt = False,
) -> None:
    ...
    asyncio.run(
        reparse_seesaawiki(
            ...,
            progress_override=resolve_progress_enabled(quiet, progress),
        )
    )
```

`reparse_seesaawiki()` 関数 signature にも `progress_override` 追加。

---

## Verification

```bash
# 1. import OK
pixi run python -c "from src.scrapers import seesaawiki_scraper"

# 2. CLI help (3 commands: scrape, reparse, validate-samples)
pixi run python -m src.scrapers.seesaawiki_scraper scrape --help | grep -E 'quiet|progress'
pixi run python -m src.scrapers.seesaawiki_scraper reparse --help | grep -E 'quiet|progress'

# 3. Lint
pixi run lint src/scrapers/seesaawiki_scraper.py

# 4. Dry run (実 fetch なし) — reparse は raw/ ディレクトリ前提
ls data/seesaawiki/raw/ 2>/dev/null | head -5  # 存在確認
pixi run python -m src.scrapers.seesaawiki_scraper reparse --quiet --max-pages 1 || true
# エラーなければ OK (raw/ なくても CLI parse は動く)
```

---

## Stop-if conditions

- import エラー
- インデント崩れによる SyntaxError (Step 4-6 でループ全体を 1 段下げる際の事故)
- `git diff --stat` が +100/-100 を超える
- `list_only` / `fetch_only` 分岐の挙動が変わる証拠 (例: 出力 stats dict の key が違う)

---

## Rollback

```bash
git checkout src/scrapers/seesaawiki_scraper.py
pixi run python -c "from src.scrapers import seesaawiki_scraper"
```

---

## Completion signal

- [ ] 3 つの CLI command (`scrape` / `reparse` / `validate-samples`) で `--quiet` / `--progress` 動作
- [ ] 内部 main loop が `scrape_progress` で囲まれている
- [ ] `log.info("seesaa_checkpoint", ...)` → `p.log(...)` 置換完了
- [ ] git log message: `scraper(seesaawiki): integrate scrape_progress into main loops (11_scraper_unification/02)`
