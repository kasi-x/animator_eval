# TODO.md — 未完了作業の一元管理

作成日: 2026-04-22 / 最終更新: 2026-05-05 (14/12 + 14/13 完了 + §10 SILVER 再統合確認 → DONE.md 反映 + TODO 大幅 trim)

未完了項目のみ。完了済みは `DONE.md`、設計原則は `CLAUDE.md`。

---

## 優先度マトリクス

| 優先度 | カテゴリ | 内容 |
|--------|---------|------|
| 🟠 High | MAL/Jikan scraper | Card 05 全件 scrape (~9.4 日完走) → `TASK_CARDS/12_mal_scraper_jikan/05_rescrape.md` |
| 🟡 Medium | Report methodology | Temporal foresight Section 3.3 holdout validation 実装 (Option A、feat_career_annual / feat_person_scores データ投入後) |
| 🟡 Medium | gender enrichment | SILVER `persons.gender` null 80.9% (2026-05-05、TMDb 修正後) → O1 70% 閾値達成には MAL Card 05 + AniList orphan backfill 必要 (§15) |
| 🟡 Medium | AniList orphan backfill | credits 由来 90K orphan persons に Staff query batch fetch (gender + hometown 追加) — 新規 Card 候補 |
| 🟢 Future | データ修正 | WIKIDATA_ROLE_MAP 修正後の JVMG credits 再マップ (Wikidata rate limit 解放待ち) |
| 🟢 Strategy | 拡張目的レポート | 8 種拡張目的レポート → `TASK_CARDS/15_extension_reports/` (順: O3 → O1 → O2 → O4 → O6 → O8 → O7 → O5、05/06/07/08 は部分着手済) |
| 🟢 Future | 新ソース | LiveChart / Wikidata 受賞 → `TASK_CARDS/16_new_sources/` (TMDb は 14/09 で完了済) |
| 🟢 Future | bangumi cron | 日次差分 API cron → `TASK_CARDS/17_bangumi_diff_cron.md` (dump 安定運用後) |

---

## SECTION 12: 他ソース拡張

### 12.1 AniList GraphQL query 拡張 — 残務

- [ ] staff `homeTown` v56 backfill 起動 (`scripts/maintenance/backfill_anilist_hometown.py`、SQLite 前提なので Conformed 対応に書き換え要)
- [ ] orphan persons backfill (新規): credits 由来 anilist:p 90K に Staff GraphQL batch (gender / hometown / birthday / image) fetch、BRONZE 追記 → re-integrate

完了済:
- characters / character_voice_actors SILVER 統合 (148K + 633K row)
- anime 主要列 (`source` / `season` / `seasonYear` / `relations` / `studios` / `tags`)
- staff (`yearsActive` / `primaryOccupations` / `dateOfBirth` / `gender` / `hometown` ← BRONZE 5,894/2,239 充足分は統合済)
- anime 拡張列 5 列 (external_links_json / airing_schedule_json / trailer_url / trailer_site / display_rankings_json)

### 12.3 MAL / Jikan — Card 05 のみ未実行

`TASK_CARDS/12_mal_scraper_jikan/05_rescrape.md`

`pixi run python -m src.scrapers.mal_scraper` で開始。~9.4 日完走。完了後:
- mal persons: 40,551 (現状 = staff_credits + va_credits union 上限) → 大幅増の見込み
- gender enrichment (§15) の MAL 経路がアンロック

### 12.4 新ソース → `TASK_CARDS/16_new_sources/`

- `01_livechart` 🟡 LiveChart.me (放送スケジュール精密化)
- Wikidata 受賞データ (rate limit 解放待ち、未カード化)

完了: `02_tmdb` (14/09 で Conformed 統合済、anime 79K + persons 293K)

---

## SECTION 13: bangumi.tv — 残務

- 🟡 13.6 日次差分 API cron — `TASK_CARDS/17_bangumi_diff_cron.md` (待機中、dump 安定運用後)

BRONZE / Conformed 統合は完了 (anime 3,715 / persons 21,125 / credits 232K)。3,715 は BRONZE subjects type=2 の真の上限と確認済 (2026-05-05)。

---

## SECTION 14: 拡張目的レポート群 → `TASK_CARDS/15_extension_reports/`

PROJECT.md §6.14 で宣言した 8 種の拡張目的レポートを 8 カード + 横断タスクに分割。

### カード一覧 (実施推奨順)

| ID | 拡張目的 | 内容 | 依存 | Priority | 状態 |
|----|--------|------|------|---------|------|
| `15/01_o3_ip_dependency` | O3 | IP 人的依存リスク (counterfactual + null model) | 既存 SILVER のみ | 🟠 | 未着手 |
| `15/02_o1_gender_ceiling` | O1 | ジェンダー天井効果 (Cox + ego-net + DID) | gender カバレッジ (§15 完了後) | 🟠 | ブロック中 |
| `15/03_o2_mid_management` | O2 | 中堅枯渇 (KM + スタジオ別 blockage) | 既存 SILVER のみ | 🟠 | 未着手 |
| `15/04_o4_foreign_talent` | O4 | 海外人材 (国籍別 FE / 役職進行) | gender / nationality カバレッジ | 🟡 | 待機 |
| `15/05_o6_cross_border` | O6 | 国際共同制作 (PageRank / community) | `04_o4` 完了 | 🟡 | 部分実装 |
| `15/06_o8_soft_power` | O8 | ソフトパワー指標 | 海外配信メタ (Card 16 / 別経路) | 🟢 | Tier2 拡張済 |
| `15/07_o7_historical` | O7 | 失われたクレジット復元 | schema 変更 (`confidence_tier`) | 🟢 | 実装済 (10dd2f3) |
| `15/08_o5_education` | O5 | 教育機関キャリア追跡 | 出身校データ取得経路 (現状不在) | 🟢 | Stop-if 報告済 |
| `15/x_cross_cutting` | — | brief マッピング / 新 audience / lint_vocab / method_note template | 個別 O カードと並行可 | 🟡 | 未着手 |

### 共通実装規約

- `scripts/report_generators/reports/base.py` 継承
- lint_vocab 通過 (`ability/skill/talent/competence/capability` + 日本語「能力」「実力」「優秀」「劣る」)
- method gate 表示 (CI / null model / holdout)
- `anime.score` SELECT 禁止 (lint で検証)
- disclaimer (JA + EN) を `build_disclaimer()` で挿入
- テスト: `tests/reports/test_<name>.py` で smoke + lint_vocab + method gate

---

## SECTION 15: gender enrichment scraper

`15_extension_reports/02_o1_gender_ceiling` の Pre-condition: SILVER `persons.gender` null 率 **80.9%** (2026-05-05、TMDb 修正後)、閾値 70% に未達。

### Conformed gender 充足現状 (2026-05-05)

| Source | persons | gender | 充足率 | 状態 |
|--------|--------:|-------:|-------:|------|
| anilist | 97,596 | 5,894 | 6.0% | ✅ 統合済 (BRONZE 7,528 中 5,894 = AniList Staff query 結果上限。orphan 90K は credits 由来 id-only) |
| bangumi | 21,125 | 12,646 | 59.9% | ✅ 統合済 |
| tmdb | 293,115 | 109,040 | 37.2% | ✅ 統合済 (2026-05-05 loader 拡張) |
| ann | 36,350 | 0 | 0.0% | ❌ ANN HTML person ページに gender label 無し = データソース側制約、scrape 不可 |
| keyframe | 35,395 | 0 | 0.0% | ❌ keyframe API/HTML に gender field 無し = データソース側制約、scrape 不可 |
| mal | 40,551 | 0 | 0.0% | 🟡 BRONZE persons table 不在 (Card 05 待ち) → scrape 後 統合可 |
| seesaawiki | 137,014 | 0 | 0.0% | ❌ seesaawiki が staff gender を出さない設計 |

**全体**: persons 732,460 / gender 140,226 / **null 率 80.9%**

### 改善経路

- **MAL Card 05 完了** → gender 大幅追加見込み (Jikan `/v4/people/{id}` で gender 取得可)
- **AniList orphan backfill** (新規 Card 起票): credits 由来の anilist:p 90K に Staff query batch fetch → 数千 gender 追加見込み
- **gender-guesser** (名前推定、補助のみ、信頼度別管理要)
- **ANN bio NLP** (description から pronoun 抽出、低精度)

依存: 完了後に `15_extension_reports/02_o1_gender_ceiling` 再起動 (閾値 70% 確認)。

---

## 実施順序

```
即時着手可:
  TASK_CARDS/15  拡張目的レポート (O3 / O2 から、SILVER のみで完結する 2 カード)

中期:
  §12.3 Card 05      MAL/Jikan 全件 scrape (~9.4 日、放置可)
  §12.1              AniList anime 拡張列 SILVER mapping + homeTown backfill
  §15                gender enrichment scraper 設計レビュー → 実装
  §14 O1             gender カバレッジ確保後に再起動

長期:
  TASK_CARDS/16  LiveChart / Wikidata 受賞
  TASK_CARDS/17  bangumi 日次差分 cron
  §3 JVMG 再マップ (Wikidata quota 解放待ち)
```

---

## 禁止事項 (再提案しない)

- **OpenTelemetry / 分散トレーシング**: 単一プロセス分析に過剰
- **Hydra / Pydantic Settings**: method gate で固定宣言
- **Polars**: DuckDB 移行後は冗長
- **GPU (cuGraph / cuDF)**: Rust 比較データ不在、投資正当化困難

詳細: `~/.claude/projects/-home-user-dev-animetor-eval/memory/feedback_framework_rejections.md`
