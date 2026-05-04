"""Cross-source value diff — marimo reactive notebook.

Interactive dashboard for exploring cross-source attribute differences
across anime / persons / studios entities.

Usage:
    pixi run marimo run notebooks/cross_source_diff.py
    pixi run marimo edit notebooks/cross_source_diff.py

CSVs are read from result/audit/cross_source_diff/{anime,persons,studios}.csv.
Run the ETL first if CSVs are absent:
    python -m src.etl.audit.cross_source_diff

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
def _summary(mo, _counts):
    """Summary callout showing total diff counts per entity."""
    _total = sum(_counts.values())
    _banner = mo.callout(
        mo.md(
            f"**Loaded**: {_counts['anime']:,} anime diffs · "
            f"{_counts['persons']:,} person diffs · "
            f"{_counts['studios']:,} studio diffs · "
            f"**{_total:,} total**"
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
def _active_df(
    entity_select,
    classification_select,
    df_anime,
    df_persons,
    df_studios,
):
    """Select and filter the active dataframe based on controls."""
    _entity_map = {
        "anime": df_anime,
        "persons": df_persons,
        "studios": df_studios,
    }
    _df_raw = _entity_map[entity_select.value].copy()

    if classification_select.value != "all":
        _df_filtered = _df_raw[_df_raw["classification"] == classification_select.value].copy()
    else:
        _df_filtered = _df_raw

    df_active = _df_filtered
    return (df_active,)


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
def _sample_table(mo, df_active, sample_n):
    """Sample diff rows table."""
    if df_active.empty:
        _sample_out = mo.md("")
    else:
        _n = min(sample_n.value, len(df_active))
        _sample = df_active.sample(n=_n, random_state=42).reset_index(drop=True)
        _sample_out = mo.vstack([
            mo.md(f"## Sample Rows ({_n} of {len(df_active):,} matching diffs)"),
            mo.ui.table(_sample),
        ])
    _sample_out


@app.cell
def _disclaimer(mo):
    """JA/EN disclaimer."""
    _disc = mo.callout(
        mo.md(
            "**免責 (JA)**: 本ノートは公開クレジットデータの表記差異を可視化するものであり、"
            "個人の評価や能力判断には一切使用しない。数値はデータ品質パターンの指標。\n\n"
            "**Disclaimer (EN)**: This notebook visualises structural discrepancies in "
            "public credit records. The figures reflect data-quality patterns, not any "
            "subjective assessment of individuals or organisations."
        ),
        kind="neutral",
    )
    _disc


if __name__ == "__main__":
    app.run()
