# 09_sakuga_atwiki

作画@wiki (https://www18.atwiki.jp/sakuga/) を新規 BRONZE ソースとして統合。**アニメーター視点** (人物 1 ページ = 1 エントリ) の credit データを取得する。

## 方針

**Playwright headless 方式**。atwiki は Cloudflare Managed Challenge 保護下 → 生 httpx 不可、JS 実行環境必須。

- 候補として Wayback Machine 経由も検討したが、CDX API で 2025 以降 ~196 ページしか捕捉されておらず (推定全体 2000+ ページの 10% 未満)、網羅性に致命的な穴が有るため却下 (2026-04-24)。
- Playwright で実サイトを直接クロール → `data/sakuga/cache/` に HTML 永続化 → 既存 `cache_store.py` + `hash_utils.py` パターン踏襲。
- 初回 full crawl は単発 (推定 1 req ~3s × 2500 page ≒ 2h)、以降は月次差分。

## 既存 scraper との位置付け

| Scraper | 視点 | 取得元 | ステータス |
|---|---|---|---|
| `keyframe_scraper.py` | 作品視点 (話数別 staff) | keyframe-staff-list.com | 既存 |
| `seesaawiki_scraper.py` | 作品視点 (radioi_34 系) | seesaawiki.jp | 既存 |
| **`sakuga_atwiki_scraper.py`** | **人物視点 (アニメーター1人1ページ)** | www18.atwiki.jp/sakuga/ | **本 card 群で新設** |

既存 2 つとは視点軸が直交 → entity resolution で合流させることで credit 再現率の向上を狙う。

## BRONZE 出力

```
result/bronze/source=sakuga_atwiki/table={persons,credits,pages}/date=YYYYMMDD/*.parquet
```

- `src_sakuga_atwiki_pages`: 発見済み全ページ (メタ/索引/人物/作品を含む、分類列 `page_kind` 付き)
- `src_sakuga_atwiki_persons`: 人物ページ抽出結果 (名前・活動開始年・別名・所属スタジオ推定 等)
- `src_sakuga_atwiki_credits`: 人物 → 参加作品 → 役職 の raw 3 つ組 (role label は raw のまま保持)

## Phase 構成

| Card | Phase | 優先 | 内容 |
|---|---|---|---|
| `01_playwright_infra.md` | 1 | 🔴 | `pixi add playwright` + `playwright-stealth` + `src/scrapers/http_playwright.py` (再利用可能 async wrapper) |
| `02_page_discovery.md` | 2 | 🔴 | メニュー (page 2) + 索引 (page 100 等) からリンク収集 → 全ページ ID 列挙 |
| `03_person_parser.md` | 3 | 🔴 | `src/scrapers/parsers/sakuga_atwiki.py` — 人物ページ分類 + 構造抽出 (regex 主 + LLM fallback) |
| `04_bronze_export.md` | 4 | 🔴 | parquet 3 テーブル書出し + schema 登録 |
| `05_incremental_update.md` | 5 | 🟠 | hash ベース差分更新 (月次 cron、任意) |

## Hard constraints

- **H1**: 主観指標 (「作画評価」ランクのようなサイト独自評価) は抽出しない。credit (人物 × 作品 × 役職) のみ
- **H3**: entity resolution 不変 — 本 card 群では BRONZE 書出まで。SILVER 統合は別タスクで起票
- **H4**: `evidence_source = "sakuga_atwiki"` を credits 層へ確実に伝播
- **H6**: `--no-verify` 禁止
- **robots.txt**: `www18.atwiki.jp/robots.txt` 時点 (2026-04-24) `/pages/` は allow。`/search`, `/backup`, `/edit*` 等は disallow → クロール対象から明示除外
- **atwiki root の robots.txt**: `atwiki.jp/robots.txt` が Claudebot / GPTBot / Bytespider を `/wiki/` で明示ブロック。今回の対象パスは `/sakuga/pages/` で非該当だが、**UA に Claudebot / GPTBot 等の名前を含めない** こと
- **レート制限**: `delay >= 3.0s` 固定。`SCRAPE_DELAY_SECONDS` 設定を流用
- **ToS**: 実行前に atwiki 利用規約の自動アクセス条項を人間が確認すること (Pre-conditions に必須項目として記載)

## 法的注意

- 抽出データは公開 wiki のクレジット情報のみ。主観評価 (「○○は神作画」等の地の文) は取得しない / 保存しない
- 本文テキストのうち credit 抽出に不要な部分は parquet に保存しない (雑談・個人攻撃的記述の保全は defamation リスク)
- 人物ページの「本名」「生年月日」等個人情報は wiki 上に公開されていても **保存対象外**。プロジェクトで扱うのは credit network のみ

## 生データ保全方針

- role label (「原画」「作画監督」「3話 OP作画」等) は **raw 文字列のまま** parquet column へ
- 話数情報 (`3話` / `第3話` / `#3` 等) は raw 保持 + 正規化列 `episode_num` 併存
- role_groups.py への正規化 mapping は SILVER 移行タスクで別途起票 (この card 群では触らない)
