# 36_anilist_orphan_backfill

AniList credits 由来の orphan persons (id-only、メタ情報なし) に対し Staff GraphQL batch fetch でメタ情報 (gender / hometown / birthday / image / yearsActive / primaryOccupations) を追加し BRONZE 追記 → re-integrate。

## 背景

`conformed.persons` の anilist 集合は二層構造:

| 層 | row 数 | gender | hometown | 来源 |
|----|-------:|-------:|---------:|------|
| **Staff query 取得済** | ~7,528 | 5,894 (78.3%) | 充足 | scrapers/anilist Staff query (本人ページ scrape) |
| **credits orphan** | ~90,000 | 0 | 0 | credits の staffEdges / voiceActors edge から id-only 流入 |

合計 anilist persons 97,596 のうち orphan ~90K が gender / hometown 不在。
§15 gender enrichment の閾値達成 (null 率 80.9% → 70%) には orphan ~10-20% 充足が必要。

## カード

| ID | 内容 | Priority | Status |
|----|------|----------|--------|
| [01_orphan_backfill.md](01_orphan_backfill.md) | Staff GraphQL batch fetch (50 ids / request) → BRONZE 追記 → re-integrate | 🟡 Medium | 起票済 |

## 関連

- `TODO.md §12.1` (起票根拠)
- `TASK_CARDS/15_extension_reports/02_o1_gender_ceiling` (Pre-condition、§15 gender 70% 達成必須)
- `TASK_CARDS/35_data_quality_backfill/01_nationality_backfill` (hometown→nationality 連鎖、本カード完了で gain 倍増見込み)
