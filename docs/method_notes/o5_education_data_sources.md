# O5 Education Outcome Tracking — Data Source Survey

**Status**: Stop-if triggered (2026-05-02)
**Reason**: No education data exists in BRONZE, SILVER, or any active scraper.
**Next step required**: Scraper task + user approval before this card can proceed.

---

## Current State

Checked as of 2026-05-02:

- `persons` table (SILVER): columns are `name_ja`, `name_en`, `name_ko`, `name_zh`,
  `nationality`, `hometown`, `date_of_birth`, `blood_type`, `years_active`, etc.
  **No `alma_mater`, `school`, `education_history`, or any education column.**
- BRONZE tables (`src_anilist_persons`, `src_ann_persons`, `src_tmdb_persons`):
  same absence confirmed via `grep -rn` across all `src/db/schema.py` and all
  `src/scrapers/*.py`.
- No ETL loader (`src/etl/silver_loaders/`) touches education data.

Row coverage for education: **0 %** (hard Stop-if threshold: < 5%).

---

## Source Candidate Comparison

| Candidate | Coverage estimate | Acquisition difficulty | Ethics / ToS |
|-----------|------------------|----------------------|--------------|
| **Anime school official alumni pages** (代アニ, AMG, OAS, etc.) | Low–Medium (school-specific) | Medium — requires per-school scraper; page structure varies | Public pages only. ToS check per domain required before scraping. |
| **University anime department pages** (京都精華, 東京工芸, 武蔵野美術, etc.) | Very low — few publish alumni names | High — most universities do not publish structured alumni lists | ToS check required; GDPR/個人情報保護法 risk for EU/JP nationals. |
| **Industry magazine NLP** (アニメージュ, アニメディア, NewType interview articles) | Low–Medium (interview-driven, selective) | High — semi-structured text, NLP extraction error-prone | Public publication content. Copyright: citation/excerpt may suffice; full scrape risky. |
| **LinkedIn / X (Twitter) profiles** | Medium (self-reported, incomplete) | Low for manual; **API scraping violates ToS on both platforms** | LinkedIn ToS explicitly prohibits automated scraping. X API v2 does not expose education fields. |
| **Direct survey / creator consent forms** | Highest quality | Very high — voluntary participation, long lead time | Requires explicit informed consent (個人情報保護法 §17). |
| **Partnership with schools (data agreement)** | Highest coverage per school | Very high — legal MOU required | Privacy-safe if school controls anonymisation before delivery. |

---

## Recommended Sequencing (if approved)

1. **Negotiate data agreements** with 2–3 major anime vocational schools
   (代々木アニメーション学院, アミューズメントメディア総合学院, バンタンデザイン研究所)
   as primary path — yields structured, consented data.

2. **Alumni page scraper** (secondary): build `src/scrapers/education_scraper.py`
   targeting public alumni pages only, with explicit ToS review gate before each domain.
   Recommended fields: `person_name_ja`, `school_name_ja`, `graduation_year`, `source_url`.

3. **Schema addition** (after source confirmed):
   - BRONZE: `src_education` (raw scraped rows)
   - SILVER: `education_history (person_id FK, school_id FK, enroll_year, graduate_year, source)`
   - SILVER: `schools (id PK, name_ja, name_en, school_type {vocational|university|other})`

4. **ETL loader**: `src/etl/silver_loaders/education_history.py`
   following the same medallion pattern as other silver loaders.

5. **Coverage gate**: require >= 5% row coverage in `persons` before enabling
   O5 report generation (hard gate in `o5_education.py`).

---

## Scraper Task Card Recommendation

This investigation recommends creating a new task card:

```
TASK_CARDS/scrapers/education_scraper.md
```

Scope:
- Per-school alumni page ToS review checklist
- Structured scraper with rate limiting and robots.txt respect
- Schema DDL for `src_education` BRONZE table
- Silver loader and coverage check
- User/stakeholder approval gate (school partnership or signed ToS confirmation)

O5 card (`15_extension_reports/08_o5_education`) should remain **blocked** on this
scraper task completion + >= 5% coverage confirmation.

---

## References

- Dale & Krueger (2002, 2014): "Estimating the Payoff to Attending a More Selective
  College: An Application of Selection on Observables and Unobservables" — propensity
  score method baseline for school-effect estimation.
- Card & Krueger (1992): "Does School Quality Matter?" — school quality structural
  identification reference.
- 個人情報の保護に関する法律 (個人情報保護法) 第17条: purpose limitation for
  personal data collection in Japan.
