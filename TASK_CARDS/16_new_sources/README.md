# 16 新ソース統合 (LiveChart / TMDb)

**Objective**: TODO §12.4 の新ソースを BRONZE → SILVER 統合経路まで設計し、既存 9 source 体制を補完する。

**前提**:
1. 既存 9 source (anilist / ann / bangumi / mal / mediaarts / seesaawiki / keyframe / sakuga_atwiki / jvmg) では補完しきれない情報を回収
2. ToS / API 利用規約遵守必須
3. BRONZE parquet 設計、SILVER 統合は別カード化 (`14_silver_extend` パターン踏襲)

---

## カード構成

| ID | source | 内容 | 優先度 |
|----|--------|------|--------|
| [01_livechart](01_livechart.md) | LiveChart.me | 放送スケジュール精密化 (放送局・時間帯・先行配信) | 🟡 |
| [02_tmdb](02_tmdb.md) | TMDb | 劇場アニメ国際配信メタデータ (海外公開日・配給) | 🟢 |

将来候補 (本カード群外):
- Wikidata 受賞データ (rate limit 解放待ち、`TODO.md §3` JVMG と同じ条件)
- Netflix / Crunchyroll 公式メタ (海外配信、`15_extension_reports/06_o8` 依存)

---

## 共通実装規約

- BRONZE: `result/bronze/source=<name>/table=*/date=YYYY-MM-DD/*.parquet`
- BronzeWriterGroup + ScrapeRunner パターン (`TASK_CARDS/13_scraper_runner_refactor` 完了済 abstraction 利用)
- Rate limit 厳守 (`http_client.py` の `DualWindowRateLimiter`)
- ToS / robots.txt 確認必須 (`docs/scraper_ethics.md` 記録)
- SILVER 統合は本カード群では行わない (`14_silver_extend` 同様、別 PR)

---

## Hard Rule リマインダ

- **H1**: BRONZE では score 系列保持可、SILVER 統合時に display_* prefix で隔離
- **H4**: `credits.evidence_source` 維持、新 source は固有 string で挿入 (`'livechart'` / `'tmdb'`)
- **H5**: 既存テスト破壊禁止
- **H8**: 行番号信頼禁止

---

## 完了判定

- 各 Card の Verification 全 pass
- BRONZE parquet row count > 0
- `pixi run lint` clean
- 既存テスト green

## 関連

- `TODO.md §12.4`: 旧記述。本カード群完了時に「→ TASK_CARDS/16」へ書き換え
- `TASK_CARDS/13_scraper_runner_refactor`: scraper abstraction (BronzeWriterGroup / ScrapeRunner)
- `TASK_CARDS/14_silver_extend`: SILVER 統合パターン参考
