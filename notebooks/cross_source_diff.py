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


if __name__ == "__main__":
    app.run()
