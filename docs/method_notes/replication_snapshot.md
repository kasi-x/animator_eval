# Replication Snapshot — Standard Operating Procedure

## Purpose

This document defines when and how to create a reproducibility snapshot for
submission to a high-venue conference or journal.  Default
`snapshot_policy = not_taken`; this SOP documents the exception process per
§5.4 of the project stance.

A snapshot bundles:

| Artefact | Contents |
|----------|---------|
| `resolved.duckdb` | Entity-resolved canonical data (Resolved layer) |
| `gold.duckdb` | Mart layer: `person_scores`, `feat_*`, `meta_score_frozen` |
| `pixi.lock` | Exact dependency pinning |
| `method_notes/` | All methodological notes at time of submission |
| `README.txt` | Zenodo metadata stub + EN/JA disclaimers |
| `MANIFEST.json` | SHA-256 checksums + snapshot metadata |

## When to Create a Snapshot

Create a snapshot **before** paper submission when all of the following hold:

1. The paper reports numeric results derived from `person_scores` or any
   `feat_*` table in the Mart layer.
2. The target venue requires or strongly encourages data/code deposit (e.g.
   Zenodo DOI, OSF, institutional repository).
3. The pipeline has completed without validation errors on the full dataset.

Do **not** create a snapshot for:

- Internal reports, briefs, or dashboards (use `meta_lineage` audit trail).
- Exploratory analyses that do not appear in the published paper.
- Pipeline runs that ended with `validation_error=True`.

## Trigger Conditions for a New Snapshot

A new snapshot is required (new `snapshot_id`) when any of the following
change after a prior snapshot:

| Changed component | Required action |
|------------------|----------------|
| λ weights in IV formula | New snapshot — spec_hash will differ |
| `resolved.duckdb` data (new scrape round) | New snapshot |
| `pixi.lock` (dependency update) | New snapshot if pipeline output changes |
| Paper anchor (new submission) | New snapshot — different `paper_anchor` |

## Running the Snapshot Script

```bash
pixi run python scripts/publication/snapshot.py \
    --venue JASSS \
    --paper-anchor career_network_2026 \
    --output-dir /path/to/deposit_staging
```

### Options

| Flag | Default | Description |
|------|---------|-------------|
| `--venue` | `JASSS` | Target venue name (free text) |
| `--paper-anchor` | `animetor_2026` | Short paper identifier, used in `snapshot_id` |
| `--output-dir` | `/tmp/animetor_snapshots` | Parent directory for output |
| `--resolved-db` | `result/resolved.duckdb` | Override resolved DB path |
| `--gold-db` | `result/animetor.duckdb` | Override gold DB path |
| `--dry-run` | `False` | Build tarball without writing to `meta_score_frozen` |

### Output

The script creates:

```
<output-dir>/
  <paper_anchor>_<yyyymmdd>/       # staging directory
    resolved.duckdb
    gold.duckdb
    pixi.lock
    method_notes/
      *.md
    README.txt
    MANIFEST.json
  <paper_anchor>_<yyyymmdd>.tar.gz  # Zenodo upload-ready tarball
```

### Database record

Unless `--dry-run` is set, the script writes one row to
`mart.meta_score_frozen` in `gold.duckdb`:

```sql
SELECT snapshot_id, venue, spec_hash, score_hash, frozen_at
FROM mart.meta_score_frozen
WHERE paper_anchor = 'career_network_2026';
```

## Frozen Score Semantics

`meta_score_frozen` captures a verbatim copy of `person_scores` at snapshot
time (serialised as JSON in `score_rows_json`).  This means:

- Subsequent λ recalibrations that alter `person_scores` do **not** affect
  the frozen record.
- Reviewers can verify the paper's numbers by reading `score_rows_json`
  directly from the deposited `gold.duckdb`.
- `spec_hash` identifies the exact formula version; it changes whenever λ
  weights or the pipeline version string changes.

## Zenodo Deposit Checklist

1. Run the snapshot script (no `--dry-run`).
2. Verify `MANIFEST.json` contains expected `file_hashes` for all three DBs.
3. Upload `<paper_anchor>_<yyyymmdd>.tar.gz` to Zenodo.
4. Set the Zenodo record type to **Dataset**.
5. Add the DOI to the paper's Data Availability Statement.
6. Record the Zenodo DOI in `DONE.md` under the relevant task card.

## Disclaimer Requirements

All deposited artefacts must include the following disclaimers verbatim.

**English:**
> All scores represent structural network position and co-credit density
> derived from public credit records.  No claim about individual capability,
> competence, or performance is made or implied.

**Japanese:**
> 全スコアは公開クレジットデータに基づくネットワーク上の位置と共クレジット密度を
> 示す構造的指標です。個人の能力・資質・業務遂行水準についての主張または示唆は
> 一切含まれていません。

These are auto-inserted by `snapshot.py` into `README.txt`.

## Forbidden Vocabulary

The following terms must not appear in any deposited artefact (enforced by
`scripts/report_generators/lint_vocab.py`):

`ability`, `skill`, `talent`, `competence`, `capability`,
`優秀`, `劣る`, `実力`, `能力`

See `docs/REPORT_PHILOSOPHY.md` for the full vocabulary policy.
