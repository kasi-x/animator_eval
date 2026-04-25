# Task: allcinema アニメ絞込み scraping 実装

**ID**: `07_json_to_parquet/07_allcinema_anime_only`
**Priority**: 🟠 (効率化、時間 59h → ~1h)
**Estimated changes**: 約 +200 lines, 1-2 files
**Requires senior judgment**: yes (外部サイト API 探索、UI 変更追随)
**Blocks**: allcinema 本番 scrape
**Blocked by**: なし

---

## 背景と目的

### 現状
- `data/allcinema/checkpoint_cinema.json` の `cinema_ids` は sitemap 由来で全 141,757 件 (映画/ドラマ/アニメ全混在)
- 既存 `allcinema cinema` コマンドは全 ID を走査 → `_parse_cinema_html` 内で `animeFlag != "アニメ"` の場合 None を返し skip
- 100 件試行の結果: **anime_found=1 / total=100 = HIT 率 1%**
- delay=2s で全走査すると **約 59 時間**、99% は非アニメへの無駄リクエスト

### 狙い
allcinema 側の検索/ジャンル絞込み機能を活用し、アニメの cinema_id リストを事前構築する。
- 推定アニメ数: 1,000 〜 5,000 件 (見積)
- 新 scrape 時間: 5,000 × 2s = **約 3 時間** (現 59h → 劇的短縮)

---

## Hard constraints

- H3 entity resolution 不変
- 既存 `cinema_ids` (141,757件) を**削除しない** — バックアップ/退避
- 既存 `completed` (21,582件) を**削除しない** — バックアップ
- パーサ (`src/scrapers/parsers/allcinema.py`) は**不変** (anime 確定済 ID に対しても同じパースで OK)
- HTTP delay 下限 1.0 秒 (allcinema サーバー負荷配慮)

---

## Pre-conditions

- [ ] `git status` clean
- [ ] `data/allcinema/checkpoint_cinema.json.bak` 存在 (作成済)
- [ ] `pixi run test-scoped tests/ -k "allcinema"` pass

---

## 調査が必要な事項 (Step 0 で実施)

既に分かっていること:
- `https://www.allcinema.net/prog/search_c.php?genre=animation` = 固定ピックアップ 10 件 (ページング効かず)
- `https://www.allcinema.net/search/` = JS 駆動フォーム (GET params から直接検索不可)
- `href="https://www.allcinema.net/user/search/"` がフォーム action

未知:
- ジャンル別全件一覧 URL (年指定 / alphabetical / 50音順 など)
- ページング仕様 (`pg=`, `page=`, `pageNum=`, path-based `/p/N/` 等)
- 可能な発見経路:
  - `/search/year/YYYY/genre/animation/` 等の path-based URL 試行
  - ブラウザ DevTools で `/search/` 検索送信時の XHR 確認 (curl で再現)
  - `robots.txt` / sitemap の詳細構造 (`sitemap_c{1,2,3}.xml.gz` 以外)
  - allcinema のランキング (`/prog/search_c.php` 関連エンドポイント列挙)
  - 既存 sitemap XML に genre 情報が埋め込まれているか (`<image:image>`, `<news:news>`)

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/scrapers/allcinema_scraper.py` | 新 phase 追加: `sitemap_anime` コマンド (アニメ ID 一覧構築)、または既存 `sitemap` を拡張 |
| `data/allcinema/checkpoint_cinema.json` | `cinema_ids` をアニメ ID のみに差し替え (要バックアップ保持) |
| `TODO.md` | 完了後、新 scrape エントリを閉じる |

### 代替案: 独立 script

`scripts/build_allcinema_anime_ids.py` を新規作成して発見ロジックを分離してもよい。`cinema_ids` への差し替えは別 commit で。

---

## Files to NOT touch

| File | 理由 |
|------|------|
| `src/scrapers/parsers/allcinema.py` | パーサは anime 確定 ID でも非アニメ ID でも同じ動作、不変 |
| `src/scrapers/bronze_writer.py` | 既存経路そのまま使う |
| `data/allcinema/checkpoint_cinema.json.bak` | 安全網、touch 禁止 |

---

## Steps

### Step 0: API / URL 探索 (最重要)

以下の手順で allcinema のアニメ絞込み取得方法を確定する。**Step 0 結果により Step 1 以降の実装が変わる。**

#### 0-A: sitemap 詳細確認
```bash
pixi run python -c "
import asyncio
from src.scrapers.allcinema_scraper import AllcinemaClient, SITEMAP_CINEMA_PATTERN
async def f():
    c = AllcinemaClient(delay=0.3)
    r = await c.get(SITEMAP_CINEMA_PATTERN.format(n=1))
    # gzipped XML、先頭 2000 文字
    import gzip, io
    text = gzip.decompress(r.content).decode('utf-8')
    print(text[:2000])
asyncio.run(f())
"
```

sitemap に genre/category 情報含まれていれば即解決。

#### 0-B: path-based URL パターン試行

```bash
pixi run python -c "
import asyncio, re
from src.scrapers.allcinema_scraper import AllcinemaClient, SITE_BASE
async def probe():
    c = AllcinemaClient(delay=0.5)
    urls = [
        f'{SITE_BASE}/search/year/2020/genre/animation/',
        f'{SITE_BASE}/prog/search_c.php?genre=animation&year1=2020&year2=2020',
        f'{SITE_BASE}/prog/search_c.php?genre=animation&sort=year&order=desc&pg=1',
        f'{SITE_BASE}/prog/search_c.php?genre_c=animation',
        f'{SITE_BASE}/ranking/search_c.php?genre=animation&pg=1',
        # 50音順アニメ一覧
        f'{SITE_BASE}/syllabary/cinema/a/',
        f'{SITE_BASE}/alphabet/cinema/a/?genre=animation',
    ]
    for u in urls:
        r = await c.get(u)
        ids = list(dict.fromkeys(re.findall(r'/cinema/(\d+)', r.text)))
        print(f'{r.status_code} len={len(r.text)} ids={len(ids)} | {u[-80:]}')
asyncio.run(probe())
"
```

#### 0-C: /search/ の XHR 仕様を推測

`/search/` ページの HTML 内 `<script>` で fetch/XHR 先を確認:

```bash
pixi run python -c "
import asyncio, re
from src.scrapers.allcinema_scraper import AllcinemaClient, SITE_BASE
async def f():
    c = AllcinemaClient(delay=0.3)
    r = await c.get(f'{SITE_BASE}/search/')
    # fetch / ajax / api エンドポイント推定
    for m in re.finditer(r'(?:url|endpoint|api)[^,;{}]*[\"\\']([^\"\\']+)[\"\\']', r.text, re.IGNORECASE):
        print(m.group(0)[:120])
    print('---')
    # inline JS 検索処理付近
    js_snippets = re.findall(r'\.ajax\(\{[^}]+\}', r.text)[:5]
    for s in js_snippets:
        print(s[:200])
asyncio.run(f())
"
```

#### 0-D: 確定 → Step 1 実装

Step 0 で見つけた URL / API を Step 1 で実装。**3 つの代表シナリオ**:

- シナリオ α: `sitemap_cN.xml.gz` に genre 情報あり → 既存 sitemap 読み込みにフィルタ追加
- シナリオ β: `prog/search_c.php?genre=animation&sort=year&pg=N` が効く (ページング動作) → ページ走査
- シナリオ γ: 上記いずれも不可 → Fallback: 全 141,757 ID 走査、delay=1.0 に短縮 (約 40 時間)

---

### Step 1: 実装 (Step 0 結果に応じて分岐)

#### シナリオ α (sitemap filter) の場合
`fetch_sitemap_ids` に genre フィルタ追加。

#### シナリオ β (search paginate) の場合
新関数 `async def fetch_anime_ids_from_search(client) -> list[int]`:
```python
async def fetch_anime_ids_from_search(client: AllcinemaClient) -> list[int]:
    all_ids: list[int] = []
    pg = 1
    while True:
        r = await client.get(f"{SITE_BASE}/prog/search_c.php?genre=animation&...&pg={pg}")
        ids = _extract_cinema_ids(r.text)
        if not ids or set(ids) <= set(all_ids):
            break  # no new IDs → reached end
        all_ids.extend(ids)
        pg += 1
    return list(dict.fromkeys(all_ids))
```

#### シナリオ γ の場合
delay=1.0 強制、`cmd_run` に警告 log 追加。

#### 新 CLI コマンド追加

```python
@app.command("sitemap-anime")
def cmd_sitemap_anime(
    data_dir: Path = typer.Option(DEFAULT_DATA_DIR),
) -> None:
    """Phase 1-alt: アニメ作品のみの cinema_ids を構築し checkpoint に上書き。"""
    asyncio.run(_run_sitemap_anime(data_dir=data_dir))
```

`_run_sitemap_anime()` は:
1. 現 checkpoint をロード
2. アニメ ID を発見 (Step 0 で確定した方法)
3. `cinema_ids` を新リストに差し替え、`completed` は空にリセット (既存バックアップは touch しない)
4. ログに before/after 件数を出す

---

### Step 2: 動作確認 (小規模)

```bash
# 1. バックアップ確認
ls -la data/allcinema/*.bak

# 2. 新コマンドで anime ID リスト構築
pixi run python -m src.scrapers.allcinema_scraper sitemap-anime

# 3. 件数確認
pixi run python -c "
import json
d = json.load(open('data/allcinema/checkpoint_cinema.json'))
print('new cinema_ids:', len(d['cinema_ids']))
print('completed:', len(d.get('completed', [])))
print('sample:', d['cinema_ids'][:5])
"

# 4. 5 件試行で anime_found > 0 を確認
pixi run python -m src.scrapers.allcinema_scraper cinema --limit 5 --delay 2.0

# 5. parquet 出力確認
find result/bronze/source=allcinema -name "*.parquet" -newer data/allcinema/checkpoint_cinema.json.bak
```

**期待**: 5 件試行で `anime_found>=4` (HIT 率 80%+)。シナリオ γ なら HIT 率改善なし。

---

### Step 3: ユニットテスト追加

`tests/scrapers/test_allcinema_sitemap_anime.py` 新規 (3-5 ケース):
- モック HTTP レスポンスでページング解析
- 空結果処理
- 重複 ID dedup

---

### Step 4: lint / test

```bash
pixi run lint
pixi run test-scoped tests/scrapers/test_allcinema_sitemap_anime.py tests/ -k allcinema
```

---

## Verification

```bash
# 1. lint
pixi run lint

# 2. test
pixi run test-scoped tests/ -k allcinema

# 3. smoke
pixi run python -m src.scrapers.allcinema_scraper cinema --limit 5 --delay 2.0 2>&1 | grep anime_found

# 4. 期待値 (シナリオ α/β の場合): anime_found=5 (100% HIT)
# 5. シナリオ γ の場合: anime_found≤1 想定通り、ただし delay=1.0 で運用
```

---

## Stop-if conditions

- [ ] Step 0 で 3 シナリオいずれにも該当せず、アクセス手段不明 → user に報告、手動で DevTools 確認依頼
- [ ] allcinema から IP ブロック/429 多発 → delay 上げて再試行、それでもダメなら中断
- [ ] anime ID リストが 100 件未満 (発見漏れ) → 発見ロジック見直し
- [ ] Step 2 smoke で anime_found=0 → パーサ側問題の可能性、user 確認

---

## Rollback

```bash
# checkpoint 復元
cp data/allcinema/checkpoint_cinema.json.bak data/allcinema/checkpoint_cinema.json

# コード変更破棄
git checkout src/scrapers/allcinema_scraper.py

# 新規ファイル削除
rm -f tests/scrapers/test_allcinema_sitemap_anime.py
rm -f scripts/build_allcinema_anime_ids.py
```

---

## Completion signal

- [ ] Step 0 で採用シナリオ (α/β/γ) 確定、理由を commit message に記録
- [ ] `cinema_ids` がアニメ確定リストに差し替え済 (シナリオ α/β の場合)
- [ ] smoke test で HIT 率改善確認 (シナリオ α/β の場合)
- [ ] lint / test pass
- [ ] commit (message 例: `scraper: allcinema アニメ絞込み sitemap-anime コマンド追加 — シナリオ β 採用 (07_json_to_parquet/07)`)
- [ ] 作業ログに `DONE: 07_json_to_parquet/07_allcinema_anime_only (シナリオ X)` 記録
- [ ] 残る判断 (全件 scrape の開始許可、delay 値、persons phase 並行可否) は user に投げる
