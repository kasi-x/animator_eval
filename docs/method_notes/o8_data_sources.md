# O8 Soft Power — Tier2 Data Source Survey

**Status**: Stop-if triggered (2026-05-02)
**Reason**: Neither international awards data nor overseas sales ratio data exists
in BRONZE, SILVER/Conformed, or any active scraper.
**Next step required**: Dedicated scraper task cards + user approval before Tier2 can proceed.

---

## Investigation Summary (2026-05-02)

### 1. International Awards (Annecy / Anima Mundi / Asia Pacific Screen Awards)

**Confirmed absent** across all layers:

- `src/db/schema.py` (BRONZE DDL): no `awards_international` table, no `src_awards_*` table.
  Full grep for `award`, `annecy`, `anima_mundi`, `asia_pacific_screen` — zero matches.
- SILVER Conformed (`src/etl/conformed_loaders/`): no awards loader.
  Loaders present: anilist, ann, bangumi, keyframe, madb, mal, sakuga_atwiki, seesaawiki.
  None touches award/festival data.
- ETL cross-source copy (`src/etl/cross_source_copy/`): no awards propagation.
- GOLD / feat_* / agg_* tables: no awards columns.

Row coverage: **0 %** (hard Stop-if threshold: < 1 row).

### 2. Overseas Sales Ratio

**Confirmed absent** across all layers:

- No `overseas_sales`, `foreign_sales`, `export_ratio`, or `international_revenue`
  column in any BRONZE/SILVER/GOLD table.
- Source note from task card: overseas sales ratios come from aggregate industry
  statistics (日本動画協会 / VIPO / JETRO) — these are **not per-anime structured data**
  and cannot be scraped from public web without explicit data licensing.
- Row coverage: **0 %**.

### 3. `external_links_json` (already in use by Tier1)

AniList `external_links_json` is present in SILVER `anime` table
(`src/etl/conformed_loaders/anilist.py` line 144, 308).
This column drives Tier1 platform detection and is confirmed populated.
It does NOT contain awards or sales data.

---

## Source Candidate Comparison

### International Awards

| Source | Coverage | Acquisition | Ethics / ToS | Notes |
|--------|----------|-------------|--------------|-------|
| **Annecy International Animation Festival** (1960–) | High for feature/short anime winners | Medium — structured results pages, annual | Public results; no explicit scraping prohibition found (verify robots.txt before build) | Best primary source; Cristal / Special Jury / LGBTQ+ Award hierarchy well-defined |
| **Asia Pacific Screen Awards (APSA)** (2007–) | Medium — anime subset of broader APAC film | Low–Medium — results on official site | Public; ToS check required | Structured JSON-LD on awards pages (2018+); earlier years require HTML parsing |
| **Anima Mundi (Brazil)** (1993–) | Low for Japanese anime specifically | High — site in Portuguese; partial English | Public; robots.txt check required | Narrow coverage of Japanese titles; secondary priority |
| **Anime Award (Crunchyroll)** (2017–) | High for mainstream recent anime | Low — structured JSON available | Public; Crunchyroll ToS review required | Not an international film festival; audience-vote component conflicts with H1 if used as weight. **Use for distribution metadata only, not as weight.** |
| **IMDb Awards pages** | Medium — aggregates multiple festivals | Medium — HTML scraping; rate limits | ToS prohibits automated scraping. **Do not use.** | Blocked by ToS |
| **Wikipedia anime awards lists** | Low (coverage gaps) | Low | CC BY-SA; attribution required | Useful as cross-check only; not primary |

**Recommended primary sources**: Annecy + APSA.
**Scraper task scope**: new `src/scrapers/awards_scraper.py` with per-festival modules.
BRONZE table: `src_awards_international`.

### Overseas Sales Ratio

| Source | Data type | Acquisition | Usability |
|--------|-----------|-------------|-----------|
| **日本動画協会 (AJA) Anime Industry Report** (annual, 2013–) | Aggregate market figures — domestic + overseas revenue totals | PDF download (free) | Macro-level only; **not per-anime**. Cannot be joined to anime table. |
| **VIPO (映像産業振興機構) reports** | Market research; overseas licensing volumes | PDF + negotiated data access | Similar to AJA — aggregate. Individual title data requires direct industry partnership. |
| **JETRO content industry reports** | Export value by category | PDF / published reports | Aggregate. Not per-anime. |
| **Netflix / Crunchyroll licensed title lists** | Per-title licensing presence in each country | No public API. Netflix requires commercial data agreement. Crunchyroll: partial via AniList external_links_json (already used in Tier1). | Per-anime data requires commercial licensing agreement. |
| **JustWatch API** (commercial) | Per-anime, per-country streaming availability | Commercial API ($); requires subscription | Most actionable for per-anime country coverage — but paid and ToS-gated. |
| **AniList external_links_json** (already in Tier1) | Platform link presence — proxy for international availability | Already collected | Not country-specific; coarse proxy. Tier1 already uses this. |

**Conclusion**: Overseas sales ratio at per-anime granularity is **not obtainable**
from any freely accessible structured source without commercial licensing or
direct industry partnership. Aggregate figures (AJA / VIPO / JETRO) are
macro-level and cannot be joined to the anime table.

---

## Recommended Scraper Task Cards

### Card A: `src/scrapers/awards_scraper.py` (New)

**Priority**: Medium (unblocks Tier2 international awards component)

Scope:
- Module `awards_scraper.py` with sub-modules per festival:
  - `annecy.py`: parse Annecy official results (JSON + HTML; 1960–present)
  - `apsa.py`: parse Asia Pacific Screen Awards results (2007–present)
  - (optional) `anima_mundi.py`: Anima Mundi (1993–present; Portuguese)
- Output BRONZE table: `src_awards_international`
  ```sql
  CREATE TABLE src_awards_international (
      id           INTEGER PRIMARY KEY AUTOINCREMENT,
      festival     TEXT NOT NULL,          -- 'annecy' | 'apsa' | 'anima_mundi'
      year         INTEGER NOT NULL,
      category     TEXT NOT NULL,          -- e.g. 'Cristal' | 'Special Jury'
      award_tier   INTEGER NOT NULL,       -- 1=Grand Prix, 2=Special, 3=Nomination
      title_en     TEXT NOT NULL DEFAULT '',
      title_ja     TEXT NOT NULL DEFAULT '',
      country      TEXT NOT NULL DEFAULT '',
      director     TEXT NOT NULL DEFAULT '',
      anilist_id   INTEGER,               -- FK to src_anilist_anime after entity resolution
      scraped_at   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
      UNIQUE(festival, year, category, title_en)
  );
  ```
- ETL Silver loader: `src/etl/conformed_loaders/awards_international.py`
  (entity resolution: title match → `anime_international_awards` SILVER table)
- SILVER table: `anime_international_awards`
  ```sql
  CREATE TABLE anime_international_awards (
      anime_id   TEXT NOT NULL,
      festival   TEXT NOT NULL,
      year       INTEGER NOT NULL,
      category   TEXT NOT NULL,
      award_tier INTEGER NOT NULL,        -- fixed weight: 1=1.0, 2=0.6, 3=0.3
      PRIMARY KEY (anime_id, festival, year, category)
  );
  ```
- Coverage gate: require >= 50 rows (Annecy has 60+ years × ~5 anime categories)
  before enabling Tier2 awards section.
- ToS gate: robots.txt + ToS check for Annecy (annecy.org) and APSA (asiapacificscreenawards.com)
  before scraping.

**Blocks**: O8 Tier2 awards section in `o8_soft_power.py`.

### Card B: Overseas Sales Ratio — Partnership Track (Low feasibility, long lead)

Not a scraper task — requires industry data agreement.

Options:
1. **AJA partnership**: request per-anime export licensing data (requires formal MOU).
2. **JustWatch API subscription**: per-anime country availability as proxy for
   international reach (commercial cost; ToS allows API use with subscription).
3. **Crunchyroll + Netflix title lists** (via manual collection or data agreement):
   per-anime, per-region licensing presence.

**Recommendation**: Use JustWatch API (if budget approved) as best per-anime
proxy for international distribution scope (country count). This is structurally
superior to sales ratio for O8 purposes (H1 compliance: structural fact, not revenue).

---

## O8 Tier2 Conditional Implementation Plan

When Card A (awards scraper) is complete and `anime_international_awards` has
>= 50 rows, the following additions can be made to `o8_soft_power.py`:

### Award Weight Formula (method-gated, fixed weights)

```
award_weight[anime] = sum over awards of award_tier_weight[award_tier]

award_tier_weight = {1: 1.0, 2: 0.6, 3: 0.3}   # fixed, pre-declared
```

No anime.score or viewer-popularity input. Weights are structural (award tier hierarchy),
fixed prior to data, and declared in method note (this document).

### Tier2 soft_power_index Extension

```
soft_power_index_tier2[platform] =
    anime_count[platform]
    × mean_theta_proxy(persons involved in platform anime)
    × (1 + sum(award_weight[anime]) for anime in platform_anime)
```

Platform weight remains 1.0 (fixed). Award weight additive multiplier.
Bootstrap CI on full index (n=1000).

### New Sections to add to `o8_soft_power.py`

1. `_build_awards_section()`: bar chart of anime by festival × award tier
2. `_build_tier2_spi_section()`: extended soft_power_index_tier2 per platform
3. Update `generate()` to check `anime_international_awards` coverage
   and conditionally render Tier2 sections (skip with method note if < 50 rows)

---

## Verdict

| Component | Status | Blocker |
|-----------|--------|---------|
| 国際賞受賞 (Annecy / APSA / Anima Mundi) | **Stop-if** | `src_awards_international` BRONZE table + Silver loader not built |
| 海外売上比率 | **Stop-if** (structural) | Aggregate-only sources; per-anime data requires commercial licensing or industry MOU |
| `anime_international_awards` SILVER table | **Stop-if** | Depends on Card A completion |
| Tier2 `o8_soft_power.py` extension | **Blocked** | Both components above |

O8 Tier1 (配信プラットフォームリンク分析) is complete and functional.
O8 Tier2 remains **blocked** on scraper Card A completion.

---

## References

- Annecy International Animation Festival: https://www.annecy.org/
- Asia Pacific Screen Awards: https://asiapacificscreenawards.com/
- Anima Mundi: https://animamundi.com.br/
- 一般社団法人日本動画協会 (AJA) Anime Industry Report: https://aja.gr.jp/english/japan-anime-data
- VIPO 映像産業振興機構: https://www.vipo.or.jp/en/
- JETRO: https://www.jetro.go.jp/en/reports/
- JustWatch API: https://www.justwatch.com/us/JustWatch-Streaming-API (commercial)
