"""Cross-source value diff — marimo reactive notebook.

Interactive dashboard for exploring cross-source attribute differences
across anime / persons / studios entities, with optional LLM enrichment
columns (llm_patterns / llm_best_guess / llm_confidence / llm_rationale).

Usage:
    pixi run marimo run notebooks/cross_source_diff.py
    pixi run marimo edit notebooks/cross_source_diff.py

CSVs are read from result/audit/cross_source_diff/{anime,persons,studios}.csv.
Run the ETL first if CSVs are absent:
    python -m src.etl.audit.cross_source_diff

LLM-enriched CSVs ({entity}_llm_classified.csv) are loaded if present.
Generate them with:
    python -m src.etl.audit.cross_source_diff_llm --sample 100

Disclaimer (JA): 本ノートは公開クレジットデータの表記差異を可視化するものであり、
個人の評価や能力判断には一切使用しない。

Disclaimer (EN): This notebook visualises structural discrepancies in public
credit records. The figures reflect data-quality patterns, not any subjective
assessment of individuals or organisations.
"""

import marimo

__generated_with = "0.23.4"
app = marimo.App(width="wide", app_title="Cross-source Value Diff Audit")


@app.cell
def _imports():
    from pathlib import Path

    import marimo as mo
    import pandas as pd

    return mo, pd, Path


@app.cell
def _load_csvs(mo, pd, Path):
    """Load the three audit CSVs produced by export_audit()."""
    _audit_dir = Path("result/audit/cross_source_diff")

    _missing = [
        str(_audit_dir / f"{e}.csv")
        for e in ("anime", "persons", "studios")
        if not (_audit_dir / f"{e}.csv").exists()
    ]

    if _missing:
        mo.stop(
            True,
            mo.callout(
                mo.md(
                    f"**CSVs not found**: {', '.join(_missing)}\n\n"
                    "Run the ETL first:\n"
                    "```\npixi run python -m src.etl.audit.cross_source_diff\n```"
                ),
                kind="warn",
            ),
        )

    df_anime = pd.read_csv(_audit_dir / "anime.csv", dtype=str).fillna("")
    df_persons = pd.read_csv(_audit_dir / "persons.csv", dtype=str).fillna("")
    df_studios = pd.read_csv(_audit_dir / "studios.csv", dtype=str).fillna("")

    _counts = {
        "anime": len(df_anime),
        "persons": len(df_persons),
        "studios": len(df_studios),
    }

    return df_anime, df_persons, df_studios, _counts


@app.cell
def _load_llm_csvs(pd, Path):
    """Load LLM-enriched CSVs if available (optional — graceful fallback)."""
    _audit_dir = Path("result/audit/cross_source_diff")

    def _load_llm(entity: str):
        p = _audit_dir / f"{entity}_llm_classified.csv"
        if p.exists():
            return pd.read_csv(p, dtype=str).fillna("")
        return None

    df_anime_llm = _load_llm("anime")
    df_persons_llm = _load_llm("persons")
    df_studios_llm = _load_llm("studios")

    _llm_counts = {
        "anime": len(df_anime_llm) if df_anime_llm is not None else 0,
        "persons": len(df_persons_llm) if df_persons_llm is not None else 0,
        "studios": len(df_studios_llm) if df_studios_llm is not None else 0,
    }

    return df_anime_llm, df_persons_llm, df_studios_llm, _llm_counts


@app.cell
def _summary(mo, _counts, _llm_counts):
    """Summary callout showing total diff counts per entity."""
    _total = sum(_counts.values())
    _llm_total = sum(_llm_counts.values())
    _llm_note = (
        f" · **LLM enriched**: {_llm_total:,} rows"
        if _llm_total > 0
        else " · *LLM CSVs not found (run `--m src.etl.audit.cross_source_diff_llm`)*"
    )
    _banner = mo.callout(
        mo.md(
            f"**Loaded**: {_counts['anime']:,} anime diffs · "
            f"{_counts['persons']:,} person diffs · "
            f"{_counts['studios']:,} studio diffs · "
            f"**{_total:,} total**"
            + _llm_note
        ),
        kind="info",
    )
    _banner


@app.cell
def _controls(mo):
    """Entity and classification filter controls."""
    entity_select = mo.ui.dropdown(
        options=["anime", "persons", "studios"],
        value="anime",
        label="Entity",
    )
    classification_select = mo.ui.dropdown(
        options=[
            "all",
            "null_in_one",
            "identical_after_normalize",
            "digit_count_mismatch",
            "off_by_year",
            "single_char_diff",
            "multi_char_diff",
            "completely_different",
        ],
        value="all",
        label="Classification",
    )
    sample_n = mo.ui.slider(
        start=10,
        stop=500,
        step=10,
        value=50,
        label="Sample rows",
    )
    _row = mo.hstack([entity_select, classification_select, sample_n], gap=2)
    _row
    return entity_select, classification_select, sample_n


@app.cell
def _llm_controls(mo):
    """LLM-specific filter controls (active when LLM CSVs are loaded)."""
    llm_mode_toggle = mo.ui.checkbox(
        label="Show LLM-enriched view (requires *_llm_classified.csv)",
        value=False,
    )
    llm_best_guess_filter = mo.ui.dropdown(
        options=["all", "value_a", "value_b", "neither"],
        value="all",
        label="LLM best_guess",
    )
    llm_confidence_slider = mo.ui.slider(
        start=0.0,
        stop=1.0,
        step=0.05,
        value=0.0,
        label="Min LLM confidence",
    )
    _row = mo.hstack(
        [llm_mode_toggle, llm_best_guess_filter, llm_confidence_slider], gap=2
    )
    _row
    return llm_mode_toggle, llm_best_guess_filter, llm_confidence_slider


@app.cell
def _active_df(
    entity_select,
    classification_select,
    llm_mode_toggle,
    llm_best_guess_filter,
    llm_confidence_slider,
    df_anime,
    df_persons,
    df_studios,
    df_anime_llm,
    df_persons_llm,
    df_studios_llm,
    mo,
):
    """Select and filter the active dataframe based on controls."""
    _entity_map_base = {
        "anime": df_anime,
        "persons": df_persons,
        "studios": df_studios,
    }
    _entity_map_llm = {
        "anime": df_anime_llm,
        "persons": df_persons_llm,
        "studios": df_studios_llm,
    }

    _use_llm = llm_mode_toggle.value
    _llm_df = _entity_map_llm[entity_select.value]

    if _use_llm and _llm_df is not None:
        _df_raw = _llm_df.copy()
    elif _use_llm and _llm_df is None:
        mo.stop(
            True,
            mo.callout(
                mo.md(
                    f"**LLM CSV not found** for `{entity_select.value}`.\n\n"
                    "Generate it first:\n"
                    "```\n"
                    f"python -m src.etl.audit.cross_source_diff_llm "
                    f"--entity {entity_select.value} --sample 100\n"
                    "```"
                ),
                kind="warn",
            ),
        )
    else:
        _df_raw = _entity_map_base[entity_select.value].copy()

    # Apply rule classification filter
    if classification_select.value != "all":
        _df_raw = _df_raw[_df_raw["classification"] == classification_select.value].copy()

    # Apply LLM filters (only meaningful when LLM columns are present)
    if _use_llm and "llm_best_guess" in _df_raw.columns:
        if llm_best_guess_filter.value != "all":
            _df_raw = _df_raw[
                _df_raw["llm_best_guess"] == llm_best_guess_filter.value
            ].copy()
        min_conf = llm_confidence_slider.value
        if min_conf > 0.0:
            _df_raw = _df_raw[
                _df_raw["llm_confidence"].astype(float) >= min_conf
            ].copy()

    df_active = _df_raw
    llm_mode = _use_llm and "llm_best_guess" in _df_raw.columns
    return df_active, llm_mode


@app.cell
def _stats_section(mo, df_active):
    """Classification breakdown table."""
    if df_active.empty:
        _out = mo.callout(mo.md("No diffs match the current filters."), kind="warn")
    else:
        _breakdown = (
            df_active.groupby("classification")
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )
        _breakdown["pct"] = (
            (_breakdown["count"] / _breakdown["count"].sum() * 100)
            .round(1)
            .astype(str)
            + "%"
        )
        _out = mo.vstack([
            mo.md("## Classification Breakdown"),
            mo.ui.table(_breakdown.reset_index(drop=True)),
        ])
    _out


@app.cell
def _attribute_breakdown(mo, df_active):
    """Per-attribute diff count table."""
    if df_active.empty:
        _attr_out = mo.md("")
    else:
        _attr_counts = (
            df_active.groupby(["attribute", "classification"])
            .size()
            .reset_index(name="count")
            .sort_values(["attribute", "count"], ascending=[True, False])
        )
        _attr_out = mo.vstack([
            mo.md("## Diffs by Attribute"),
            mo.ui.table(_attr_counts.reset_index(drop=True)),
        ])
    _attr_out


@app.cell
def _source_matrix(mo, df_active):
    """Source pair frequency table."""
    if df_active.empty:
        _pair_out = mo.md("")
    else:
        _df2 = df_active.copy()
        _df2["source_pair"] = _df2["source_a"] + " ↔ " + _df2["source_b"]
        _pair_counts = (
            _df2.groupby("source_pair")
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )
        _pair_out = mo.vstack([
            mo.md("## Source Pair Frequency"),
            mo.ui.table(_pair_counts.reset_index(drop=True)),
        ])
    _pair_out


@app.cell
def _llm_pattern_breakdown(mo, df_active, llm_mode):
    """LLM pattern frequency table — shown only in LLM mode."""
    import json

    if not llm_mode or df_active.empty:
        _llm_pat_out = mo.md("")
    else:
        # Explode llm_patterns JSON lists into individual pattern counts
        _pattern_counts: dict[str, int] = {}
        for _val in df_active["llm_patterns"]:
            try:
                _pats = json.loads(_val) if _val else []
            except Exception:
                _pats = []
            for _p in _pats:
                _pattern_counts[_p] = _pattern_counts.get(_p, 0) + 1

        if _pattern_counts:
            import pandas as _pd

            _pat_df = (
                _pd.DataFrame(
                    list(_pattern_counts.items()), columns=["llm_pattern", "count"]
                )
                .sort_values("count", ascending=False)
                .reset_index(drop=True)
            )
            _llm_pat_out = mo.vstack([
                mo.md("## LLM Pattern Breakdown"),
                mo.ui.table(_pat_df),
            ])
        else:
            _llm_pat_out = mo.md("")
    _llm_pat_out


@app.cell
def _llm_best_guess_stats(mo, df_active, llm_mode):
    """LLM best_guess distribution — shown only in LLM mode."""
    if not llm_mode or df_active.empty:
        _bg_out = mo.md("")
    else:
        _bg = (
            df_active.groupby("llm_best_guess")
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )
        _bg["pct"] = (
            (_bg["count"] / _bg["count"].sum() * 100).round(1).astype(str) + "%"
        )
        _bg_out = mo.vstack([
            mo.md("## LLM Best-Guess Distribution"),
            mo.ui.table(_bg.reset_index(drop=True)),
        ])
    _bg_out


@app.cell
def _sample_table(mo, df_active, sample_n, llm_mode):
    """Sample diff rows table — shows LLM columns when in LLM mode."""
    if df_active.empty:
        _sample_out = mo.md("")
    else:
        _n = min(sample_n.value, len(df_active))
        _sample = df_active.sample(n=_n, random_state=42).reset_index(drop=True)

        # Determine columns to display
        _base_cols = [
            "canonical_id", "attribute",
            "source_a", "value_a",
            "source_b", "value_b",
            "classification",
        ]
        _llm_cols = [
            "llm_patterns", "llm_best_guess", "llm_best_value",
            "llm_confidence", "llm_rationale",
        ]
        if llm_mode:
            _display_cols = _base_cols + [c for c in _llm_cols if c in _sample.columns]
        else:
            _display_cols = _base_cols

        _sample_out = mo.vstack([
            mo.md(f"## Sample Rows ({_n} of {len(df_active):,} matching diffs)"),
            mo.ui.table(_sample[_display_cols]),
        ])
    _sample_out


@app.cell
def _disclaimer(mo):
    """JA/EN disclaimer."""
    _disc = mo.callout(
        mo.md(
            "**免責 (JA)**: 本ノートは公開クレジットデータの表記差異を可視化するものであり、"
            "個人の評価や能力判断には一切使用しない。数値はデータ品質パターンの指標。"
            "LLM 判定結果は補助情報であり、自動マージや能力評価には使用しない。\n\n"
            "**Disclaimer (EN)**: This notebook visualises structural discrepancies in "
            "public credit records. The figures reflect data-quality patterns, not any "
            "subjective assessment of individuals or organisations. "
            "LLM judgments are advisory only — not used for automatic merges."
        ),
        kind="neutral",
    )
    _disc


# ---------------------------------------------------------------------------
# Consensus tab (24/03) — N-source majority / outlier aggregation
# ---------------------------------------------------------------------------


@app.cell
def _load_consensus_csvs(mo, pd, Path):
    """Load the three consensus CSVs produced by export_consensus() (24/03).

    These files live alongside the pairwise diff CSVs with a *_consensus.csv suffix.
    Graceful fallback: callout + stop when files are absent.
    """
    _audit_dir = Path("result/audit/cross_source_diff")

    _missing_consensus = [
        str(_audit_dir / f"{e}_consensus.csv")
        for e in ("anime", "persons", "studios")
        if not (_audit_dir / f"{e}_consensus.csv").exists()
    ]

    if _missing_consensus:
        mo.stop(
            False,  # don't stop the whole notebook — pairwise section still works
            mo.callout(
                mo.md(
                    f"**Consensus CSVs not found**: {', '.join(_missing_consensus)}\n\n"
                    "Run the ETL first:\n"
                    "```\npixi run python -m src.etl.audit.cross_source_consensus\n```"
                ),
                kind="warn",
            ),
        )

    df_anime_con = pd.read_csv(_audit_dir / "anime_consensus.csv", dtype=str).fillna("")
    df_persons_con = pd.read_csv(_audit_dir / "persons_consensus.csv", dtype=str).fillna("")
    df_studios_con = pd.read_csv(_audit_dir / "studios_consensus.csv", dtype=str).fillna("")

    _con_counts = {
        "anime": len(df_anime_con),
        "persons": len(df_persons_con),
        "studios": len(df_studios_con),
    }

    return df_anime_con, df_persons_con, df_studios_con, _con_counts


@app.cell
def _consensus_summary(mo, _con_counts):
    """Summary callout for consensus rows."""
    _total = sum(_con_counts.values())
    mo.callout(
        mo.md(
            "## Cross-source Consensus (24/03)\n\n"
            f"**Loaded**: {_con_counts['anime']:,} anime · "
            f"{_con_counts['persons']:,} persons · "
            f"{_con_counts['studios']:,} studios · "
            f"**{_total:,} total** consensus records"
        ),
        kind="info",
    )


@app.cell
def _consensus_controls(mo):
    """Entity and flag filter controls for the consensus tab."""
    con_entity_select = mo.ui.dropdown(
        options=["anime", "persons", "studios"],
        value="anime",
        label="Entity (Consensus)",
    )
    con_flag_select = mo.ui.dropdown(
        options=["all", "unanimous", "majority", "unique_outlier", "plurality", "tie"],
        value="all",
        label="consensus_flag",
    )
    con_sample_n = mo.ui.slider(
        start=10,
        stop=500,
        step=10,
        value=50,
        label="Sample rows",
    )
    _row = mo.hstack([con_entity_select, con_flag_select, con_sample_n], gap=2)
    _row
    return con_entity_select, con_flag_select, con_sample_n


@app.cell
def _active_consensus_df(
    con_entity_select,
    con_flag_select,
    df_anime_con,
    df_persons_con,
    df_studios_con,
    mo,
):
    """Select and filter the active consensus dataframe."""
    _con_map = {
        "anime": df_anime_con,
        "persons": df_persons_con,
        "studios": df_studios_con,
    }
    _df_con = _con_map[con_entity_select.value].copy()

    if con_flag_select.value != "all":
        _df_con = _df_con[_df_con["consensus_flag"] == con_flag_select.value].copy()

    if _df_con.empty:
        mo.stop(
            False,
            mo.callout(mo.md("No consensus rows match the current filters."), kind="warn"),
        )

    df_consensus_active = _df_con
    return (df_consensus_active,)


@app.cell
def _consensus_flag_breakdown(mo, df_consensus_active):
    """consensus_flag distribution table."""
    if df_consensus_active.empty:
        _out = mo.md("")
    else:
        _breakdown = (
            df_consensus_active.groupby("consensus_flag")
            .size()
            .reset_index(name="count")
            .sort_values("count", ascending=False)
        )
        _breakdown["pct"] = (
            (_breakdown["count"] / _breakdown["count"].sum() * 100)
            .round(1)
            .astype(str)
            + "%"
        )
        _out = mo.vstack([
            mo.md("### consensus_flag Breakdown"),
            mo.ui.table(_breakdown.reset_index(drop=True)),
        ])
    _out


@app.cell
def _consensus_attribute_breakdown(mo, df_consensus_active):
    """Per-attribute consensus_flag counts."""
    if df_consensus_active.empty:
        _out = mo.md("")
    else:
        _attr = (
            df_consensus_active.groupby(["attribute", "consensus_flag"])
            .size()
            .reset_index(name="count")
            .sort_values(["attribute", "count"], ascending=[True, False])
        )
        _out = mo.vstack([
            mo.md("### Consensus by Attribute"),
            mo.ui.table(_attr.reset_index(drop=True)),
        ])
    _out


@app.cell
def _normalized_flag_delta(mo, df_consensus_active):
    """Rows where normalized_consensus_flag differs from consensus_flag.

    These are cases where column-level normalization (kyu→shin, alias maps,
    punct-clean, etc.) changes the classification — typically unanimous after
    normalization even when raw values differed.
    """
    if df_consensus_active.empty or "normalized_consensus_flag" not in df_consensus_active.columns:
        _out = mo.md("")
    else:
        _delta = df_consensus_active[
            df_consensus_active["consensus_flag"]
            != df_consensus_active["normalized_consensus_flag"]
        ].copy()
        _count = len(_delta)
        if _count == 0:
            _out = mo.callout(
                mo.md("No rows where normalization changes the consensus_flag."), kind="success"
            )
        else:
            _out = mo.vstack([
                mo.md(
                    f"### Normalization Changes consensus_flag ({_count:,} rows)\n\n"
                    "These rows are unanimous (or higher consensus) after column-level "
                    "normalization — superficial differences only."
                ),
                mo.ui.table(
                    _delta[
                        [
                            "canonical_id", "attribute",
                            "consensus_flag", "normalized_consensus_flag",
                            "normalized_majority_value", "values_json",
                        ]
                    ].head(200).reset_index(drop=True)
                ),
            ])
    _out


@app.cell
def _unique_outlier_table(mo, df_consensus_active, con_sample_n):
    """Unique-outlier rows — LLM 判定対象 (24/02 統合)."""
    if df_consensus_active.empty:
        _out = mo.md("")
    else:
        _outliers = df_consensus_active[
            df_consensus_active["consensus_flag"] == "unique_outlier"
        ].copy()
        _n_outliers = len(_outliers)
        if _n_outliers == 0:
            _out = mo.md("*No unique_outlier rows in current filter.*")
        else:
            _sample_n = min(con_sample_n.value, _n_outliers)
            _sample = _outliers.sample(n=_sample_n, random_state=42).reset_index(drop=True)
            _display_cols = [
                "canonical_id", "attribute",
                "majority_value", "majority_count", "majority_share",
                "outlier_sources", "outlier_values",
                "normalized_consensus_flag",
            ]
            _out = mo.vstack([
                mo.md(
                    f"### Unique Outlier Rows ({_n_outliers:,} total, showing {_sample_n})\n\n"
                    "These rows are candidates for LLM judgment (24/02 pipeline). "
                    "One source deviates from the majority — likely a typo or encoding variant."
                ),
                mo.ui.table(_sample[_display_cols]),
            ])
    _out


@app.cell
def _consensus_sample_table(mo, df_consensus_active, con_sample_n):
    """Full sample of active consensus rows."""
    if df_consensus_active.empty:
        _out = mo.md("")
    else:
        _n = min(con_sample_n.value, len(df_consensus_active))
        _sample = df_consensus_active.sample(n=_n, random_state=42).reset_index(drop=True)
        _display_cols = [
            "canonical_id", "attribute", "n_sources", "n_distinct_values",
            "majority_value", "majority_count", "majority_share",
            "consensus_flag", "outlier_sources", "outlier_values",
            "normalized_consensus_flag", "normalized_majority_value",
        ]
        _available = [c for c in _display_cols if c in _sample.columns]
        _out = mo.vstack([
            mo.md(f"### Consensus Sample Rows ({_n} of {len(df_consensus_active):,})"),
            mo.ui.table(_sample[_available]),
        ])
    _out


# ---------------------------------------------------------------------------
# format 3-layer taxonomy (24/05) — fine vs broad display
# ---------------------------------------------------------------------------


@app.cell
def _format_taxonomy_section(mo, df_anime_con):
    """Format taxonomy: fine_format vs broad_format (8 categories) comparison.

    Shows the distribution of broad_format categories across all anime consensus
    records where attribute == "format", and highlights rows where
    format_taxonomy_diff is True (candidates for LLM judgment via 24/02).

    Broad categories: tv / movie / ova_special / ona / short / music / cm / other.
    """
    _FORMAT_COLS = [
        "broad_format_consensus_flag",
        "broad_format_majority_value",
        "format_taxonomy_diff",
    ]
    _has_broad = all(c in df_anime_con.columns for c in _FORMAT_COLS)

    if not _has_broad:
        _out = mo.callout(
            mo.md(
                "**broad_format columns not found** in anime_consensus.csv.\n\n"
                "Re-generate consensus CSVs after upgrading to 24/05:\n"
                "```\npixi run python -m src.etl.audit.cross_source_consensus\n```"
            ),
            kind="warn",
        )
    else:
        _df_fmt = df_anime_con[df_anime_con["attribute"] == "format"].copy()

        if _df_fmt.empty:
            _out = mo.md("*No format rows in anime consensus.*")
        else:
            # broad_format category distribution
            _broad_dist = (
                _df_fmt.groupby("broad_format_majority_value")
                .size()
                .reset_index(name="count")
                .sort_values("count", ascending=False)
            )
            _broad_dist["pct"] = (
                (_broad_dist["count"] / _broad_dist["count"].sum() * 100)
                .round(1)
                .astype(str)
                + "%"
            )

            # broad_format consensus flag distribution
            _broad_flag_dist = (
                _df_fmt.groupby("broad_format_consensus_flag")
                .size()
                .reset_index(name="count")
                .sort_values("count", ascending=False)
            )
            _broad_flag_dist["pct"] = (
                (_broad_flag_dist["count"] / _broad_flag_dist["count"].sum() * 100)
                .round(1)
                .astype(str)
                + "%"
            )

            # format_taxonomy_diff rows (LLM judgment candidates)
            _diff_rows = _df_fmt[_df_fmt["format_taxonomy_diff"].astype(str) == "True"].copy()
            _n_diff = len(_diff_rows)

            _diff_section = (
                mo.callout(
                    mo.md(f"**{_n_diff} rows** flagged `format_taxonomy_diff=True` "
                          "— candidate for LLM judgment (24/02 pipeline)."),
                    kind="warn",
                )
                if _n_diff > 0
                else mo.callout(
                    mo.md("No `format_taxonomy_diff` rows — broad_format consensus is clean."),
                    kind="success",
                )
            )

            _display_diff_cols = [
                "canonical_id", "majority_value", "consensus_flag",
                "broad_format_majority_value", "broad_format_consensus_flag",
                "outlier_sources", "outlier_values",
            ]
            _available_diff_cols = [c for c in _display_diff_cols if c in _diff_rows.columns]

            _out = mo.vstack([
                mo.md(
                    "## Format Taxonomy: fine vs broad (24/05)\n\n"
                    f"Showing {len(_df_fmt):,} `format` attribute records.\n\n"
                    "**fine_format** = source-level label (e.g. OVA, Special, TV_SHORT).  \n"
                    "**broad_format** = 8-category taxonomy (ova_special / tv / short / …)."
                ),
                mo.md("### broad_format Category Distribution"),
                mo.ui.table(_broad_dist.reset_index(drop=True)),
                mo.md("### broad_format consensus_flag Distribution"),
                mo.ui.table(_broad_flag_dist.reset_index(drop=True)),
                _diff_section,
                *(
                    [
                        mo.md(f"### format_taxonomy_diff Rows ({_n_diff})"),
                        mo.ui.table(_diff_rows[_available_diff_cols].head(100).reset_index(drop=True)),
                    ]
                    if _n_diff > 0
                    else []
                ),
            ])
    _out


if __name__ == "__main__":
    app.run()
