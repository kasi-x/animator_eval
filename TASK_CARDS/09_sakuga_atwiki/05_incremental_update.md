# Task: 差分更新 (月次) の仕組み

**ID**: `09_sakuga_atwiki/05_incremental_update`
**Priority**: 🟠
**Estimated changes**: 約 +200 / -20 lines, 3 files
**Requires senior judgment**: no
**Blocks**: なし
**Blocked by**: `09_sakuga_atwiki/04_bronze_export`

---

## Goal

`discovered_pages.json` の `last_hash` と fresh fetch の SHA256 を比較し、変更ありページのみ再 parse → BRONZE に **新しい `date=YYYYMMDD` パーティション** として追記する増分更新を実装する。

---

## Hard constraints

- `_hard_constraints.md` 参照
- BRONZE は immutable: 既存 parquet を上書きしない (`date=` パーティションで追記のみ)
- H4 evidence_source = `"sakuga_atwiki"` 維持
- **robots.txt** 再確認: 差分 run のたびに最新 robots.txt を fetch し、disallow 追加があればクロール対象から即除外

---

## Pre-conditions

- [ ] `09_sakuga_atwiki/04_bronze_export` 完了
- [ ] 初回 full crawl 済 (discovered_pages.json と BRONZE 初版が存在)

---

## Files to modify

| File | 変更内容 |
|------|---------|
| `src/scrapers/sakuga_atwiki_scraper.py` | 追加: `incremental` CLI subcommand |
| `src/scrapers/hash_utils.py` | 既存関数を流用。必要なら SHA256 ベース `page_content_hash` を追加 |
| `tests/scrapers/test_sakuga_atwiki_incremental.py` | **新規**: 差分判定ロジックテスト |

---

## Files to NOT touch

| File | 理由 |
|------|------|
| 既存 parquet (`date=<初回日>`) | immutable |
| `src/db/schema.py` | Phase 4 で確定 |

---

## Steps

### Step 1: 差分判定ロジック

`incremental` subcommand:

1. `discovered_pages.json` をロード (各エントリ: `{page_id, last_hash, ...}`)
2. 各 `page_kind == "person"` ページについて:
   - fresh fetch (Playwright, `delay=3.0s`)
   - `new_hash = sha256(html)`
   - `new_hash == last_hash` → skip、log のみ
   - 差異あり → parse → changed list に追加
   - `last_hash` 更新
3. 新規ページ発見の追加 BFS (前回未発見の `/pages/N.html` リンクが出現した場合のみ)

### Step 2: Parquet 追記

- changed + new pages のみを `result/bronze/source=sakuga_atwiki/table=*/date=YYYYMMDD/` に書出
- **既存パーティションは触らない**
- Writer は Phase 4 のものを再利用

### Step 3: CLI

```bash
pixi run python -m src.scrapers.sakuga_atwiki_scraper incremental \
    --cache-dir data/sakuga/ \
    --output result/bronze/ \
    --date $(date +%Y%m%d)
```

出力サマリ:
- fetched: N
- unchanged: M
- changed: K
- new_pages: L
- errors: E

### Step 4: テスト

`tests/scrapers/test_sakuga_atwiki_incremental.py`:

- monkeypatched `PlaywrightFetcher` でハッシュ一致時 skip、不一致時 parse→writer 呼び出しをアサート
- 新規ページ発見時に `discovered_pages.json` に追加されることをアサート
- 既存 parquet 非破壊確認 (初版 parquet の mtime 不変)

### Step 5: cron スケジュール文書化

`docs/SCRAPING.md` に追記:

- 推奨 cron: 月次 (第 1 日曜日 AM 3:00 等)
- 失敗時のリトライ戦略
- CF 通過失敗 (= 全ページ skip) が発生した場合のアラート基準

この card では cron 登録の自動化は含めない (運用判断)。

---

## Verification

```bash
# 1. Test
pixi run test-scoped tests/scrapers/test_sakuga_atwiki_incremental.py
pixi run test

# 2. Lint
pixi run lint

# 3. smoke (実 HTTP 10 ページ)
pixi run python -m src.scrapers.sakuga_atwiki_scraper incremental \
    --cache-dir data/sakuga/ --output /tmp/bronze_inc --date 20260424 --max-pages 10

# 4. 既存パーティション非破壊確認
stat result/bronze/source=sakuga_atwiki/table=credits/date=<初回日>/*.parquet
# mtime が変わっていないこと

# 5. サマリ妥当性
# → unchanged + changed + new_pages = fetched が成立
```

---

## Stop-if conditions

- [ ] 既存パーティションのファイル mtime が変化 → 致命的、即 rollback
- [ ] smoke で CF 通過率 < 80%
- [ ] 差分が `fetched` の 100% (= 全ページが変わった判定) → hash ロジックに bug の可能性

---

## Rollback

```bash
git checkout src/scrapers/
rm -f tests/scrapers/test_sakuga_atwiki_incremental.py
rm -rf /tmp/bronze_inc
# 誤って差分 parquet を本番ディレクトリに書いた場合のみ該当 date パーティションを削除 (要確認)
```

---

## Completion signal

- [ ] Verification 全 pass
- [ ] 既存パーティション mtime 不変
- [ ] smoke で unchanged/changed/new_pages カウントが整合
- [ ] `git diff --stat` が想定範囲内
- [ ] 作業ログに `DONE: 09_sakuga_atwiki/05_incremental_update` と記録
