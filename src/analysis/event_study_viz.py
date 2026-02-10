"""Event study visualization (イベントスタディの可視化).

This module provides visualization functions for event study results,
enabling visual inspection of parallel trends and dynamic treatment effects.
"""

from pathlib import Path
from typing import Any

import matplotlib.pyplot as plt
import structlog

logger = structlog.get_logger()


def plot_event_study(
    event_study_results: dict[int, Any],
    output_path: Path | str,
    title: str = "Event Study: Dynamic Treatment Effects",
    ylabel: str = "Effect on Skill Score (β_k)",
    figsize: tuple[float, float] = (10, 6),
) -> None:
    """Plot event study coefficients with confidence intervals.

    Creates a publication-quality plot showing:
    - Pre-treatment coefficients (should be ≈ 0)
    - Treatment year coefficient (immediate effect)
    - Post-treatment coefficients (cumulative effects)
    - 95% confidence intervals (shaded region)
    - Zero reference line

    Args:
        event_study_results: Dict from estimate_event_study()
        output_path: Path to save the plot
        title: Plot title
        ylabel: Y-axis label
        figsize: Figure size (width, height) in inches

    Example:
        >>> from src.analysis.structural_estimation import estimate_event_study
        >>> event_results = estimate_event_study(panel_data)
        >>> plot_event_study(event_results, "event_study.png")
    """
    if not event_study_results:
        logger.warning("event_study_plot_skipped", reason="no_results")
        return

    # Extract data
    relative_times = sorted(event_study_results.keys())
    betas = [event_study_results[k].beta for k in relative_times]
    ci_lowers = [event_study_results[k].ci_lower for k in relative_times]
    ci_uppers = [event_study_results[k].ci_upper for k in relative_times]

    # Create figure
    fig, ax = plt.subplots(figsize=figsize)

    # Plot coefficients with confidence intervals
    ax.plot(relative_times, betas, 'o-', color='#2E86AB', linewidth=2,
            markersize=8, label='Estimated Effect', zorder=3)

    # Shade confidence interval
    ax.fill_between(relative_times, ci_lowers, ci_uppers,
                     alpha=0.3, color='#2E86AB', label='95% CI')

    # Zero reference line
    ax.axhline(y=0, color='black', linestyle='--', linewidth=1,
               alpha=0.5, label='No Effect', zorder=1)

    # Vertical line at treatment year (k=0)
    ax.axvline(x=0, color='red', linestyle=':', linewidth=2,
               alpha=0.7, label='Treatment Year', zorder=2)

    # Shade pre-treatment region
    pre_treatment_times = [k for k in relative_times if k < 0]
    if pre_treatment_times:
        ax.axvspan(min(pre_treatment_times) - 0.5, -0.5,
                   alpha=0.1, color='gray', label='Pre-Treatment')

    # Labels and title
    ax.set_xlabel('Relative Time (Years from Major Studio Entry)', fontsize=12)
    ax.set_ylabel(ylabel, fontsize=12)
    ax.set_title(title, fontsize=14, fontweight='bold')

    # Grid
    ax.grid(True, alpha=0.3, linestyle='-', linewidth=0.5)

    # Legend
    ax.legend(loc='best', framealpha=0.95, fontsize=10)

    # Tighten layout
    plt.tight_layout()

    # Save
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close(fig)

    logger.info("event_study_plot_saved", path=str(output_path))


def plot_event_study_with_annotation(
    event_study_results: dict[int, Any],
    parallel_trends_test: dict[str, Any],
    output_path: Path | str,
    title: str = "Event Study with Parallel Trends Test",
) -> None:
    """Plot event study with parallel trends test annotation.

    Enhanced version that includes:
    - Event study plot
    - Parallel trends test result as text annotation
    - Color-coded pre-treatment region (green=passed, red=failed)

    Args:
        event_study_results: Dict from estimate_event_study()
        parallel_trends_test: Dict from test_parallel_trends()
        output_path: Path to save the plot
        title: Plot title
    """
    if not event_study_results:
        logger.warning("event_study_annotated_plot_skipped", reason="no_results")
        return

    # Extract data
    relative_times = sorted(event_study_results.keys())
    betas = [event_study_results[k].beta for k in relative_times]
    ci_lowers = [event_study_results[k].ci_lower for k in relative_times]
    ci_uppers = [event_study_results[k].ci_upper for k in relative_times]

    # Create figure
    fig, ax = plt.subplots(figsize=(12, 7))

    # Plot coefficients
    ax.plot(relative_times, betas, 'o-', color='#2E86AB', linewidth=2.5,
            markersize=9, label='Estimated Effect', zorder=3)

    # Confidence interval
    ax.fill_between(relative_times, ci_lowers, ci_uppers,
                     alpha=0.25, color='#2E86AB', label='95% CI')

    # Zero line
    ax.axhline(y=0, color='black', linestyle='--', linewidth=1.5,
               alpha=0.6, zorder=1)

    # Treatment year line
    ax.axvline(x=0, color='red', linestyle=':', linewidth=2.5,
               alpha=0.8, zorder=2)

    # Pre-treatment shading (color-coded by test result)
    pre_treatment_times = [k for k in relative_times if k < 0]
    if pre_treatment_times and parallel_trends_test:
        test_result = parallel_trends_test.get("result", "inconclusive")

        if test_result == "passed":
            shade_color = '#06D6A0'  # Green
            shade_label = 'Pre-Treatment (✓ Parallel Trends)'
        elif test_result == "failed":
            shade_color = '#EF476F'  # Red
            shade_label = 'Pre-Treatment (✗ Trends Violated)'
        else:
            shade_color = '#FFD166'  # Yellow
            shade_label = 'Pre-Treatment (? Questionable)'

        ax.axvspan(min(pre_treatment_times) - 0.5, -0.5,
                   alpha=0.15, color=shade_color, label=shade_label)

    # Annotate parallel trends test result
    if parallel_trends_test:
        test_detail = parallel_trends_test.get("detail", "No test performed")
        evidence = parallel_trends_test.get("evidence", {})

        annotation_text = "Parallel Trends Test:\n"
        annotation_text += f"Result: {parallel_trends_test.get('result', 'N/A').upper()}\n"

        if evidence:
            avg_beta = evidence.get("avg_abs_beta", 0)
            max_beta = evidence.get("max_abs_beta", 0)
            annotation_text += f"Avg |β_pre|: {avg_beta:.2f}\n"
            annotation_text += f"Max |β_pre|: {max_beta:.2f}"

        # Place annotation in upper right
        ax.text(0.98, 0.98, annotation_text,
                transform=ax.transAxes,
                fontsize=10,
                verticalalignment='top',
                horizontalalignment='right',
                bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))

    # Labels
    ax.set_xlabel('Relative Time (Years from Major Studio Entry)', fontsize=12, fontweight='bold')
    ax.set_ylabel('Effect on Skill Score (β_k)', fontsize=12, fontweight='bold')
    ax.set_title(title, fontsize=14, fontweight='bold', pad=15)

    # Grid
    ax.grid(True, alpha=0.3, linestyle='-', linewidth=0.5)

    # Legend
    ax.legend(loc='upper left', framealpha=0.95, fontsize=10)

    # Tighten layout
    plt.tight_layout()

    # Save
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close(fig)

    logger.info("event_study_annotated_plot_saved", path=str(output_path))


def plot_event_study_decomposition(
    event_study_results: dict[int, Any],
    output_path: Path | str,
) -> None:
    """Plot event study with decomposition into pre/treatment/post periods.

    Creates a three-panel figure showing:
    - Panel 1: Pre-treatment trends (parallel trends check)
    - Panel 2: Treatment year effect (immediate impact)
    - Panel 3: Post-treatment dynamics (cumulative effects)

    Args:
        event_study_results: Dict from estimate_event_study()
        output_path: Path to save the plot
    """
    if not event_study_results:
        logger.warning("event_study_decomposition_skipped", reason="no_results")
        return

    # Separate by period
    pre_results = {k: v for k, v in event_study_results.items() if k < 0}
    treatment_result = event_study_results.get(0)
    post_results = {k: v for k, v in event_study_results.items() if k > 0}

    # Create figure with 3 panels
    fig, axes = plt.subplots(1, 3, figsize=(15, 5))

    # Panel 1: Pre-treatment
    if pre_results:
        ax = axes[0]
        times = sorted(pre_results.keys())
        betas = [pre_results[k].beta for k in times]
        cis_lower = [pre_results[k].ci_lower for k in times]
        cis_upper = [pre_results[k].ci_upper for k in times]

        ax.plot(times, betas, 'o-', color='#06D6A0', linewidth=2, markersize=8)
        ax.fill_between(times, cis_lower, cis_upper, alpha=0.3, color='#06D6A0')
        ax.axhline(y=0, color='black', linestyle='--', linewidth=1, alpha=0.5)
        ax.set_title('Pre-Treatment Period\n(Parallel Trends Check)', fontsize=11, fontweight='bold')
        ax.set_xlabel('Years Before Entry', fontsize=10)
        ax.set_ylabel('Effect (β_k)', fontsize=10)
        ax.grid(True, alpha=0.3)

    # Panel 2: Treatment year
    if treatment_result:
        ax = axes[1]
        beta = treatment_result.beta
        ci_lower = treatment_result.ci_lower
        ci_upper = treatment_result.ci_upper

        ax.bar([0], [beta], color='#EF476F', alpha=0.7, label='Immediate Effect')
        ax.errorbar([0], [beta], yerr=[[beta - ci_lower], [ci_upper - beta]],
                    fmt='none', ecolor='black', capsize=10, capthick=2)
        ax.axhline(y=0, color='black', linestyle='--', linewidth=1, alpha=0.5)
        ax.set_title('Treatment Year\n(Immediate Impact)', fontsize=11, fontweight='bold')
        ax.set_xlabel('Entry Year', fontsize=10)
        ax.set_ylabel('Effect (β_0)', fontsize=10)
        ax.set_xticks([0])
        ax.set_xticklabels(['k=0'])
        ax.grid(True, alpha=0.3, axis='y')

    # Panel 3: Post-treatment
    if post_results:
        ax = axes[2]
        times = sorted(post_results.keys())
        betas = [post_results[k].beta for k in times]
        cis_lower = [post_results[k].ci_lower for k in times]
        cis_upper = [post_results[k].ci_upper for k in times]

        ax.plot(times, betas, 'o-', color='#2E86AB', linewidth=2, markersize=8)
        ax.fill_between(times, cis_lower, cis_upper, alpha=0.3, color='#2E86AB')
        ax.axhline(y=0, color='black', linestyle='--', linewidth=1, alpha=0.5)
        ax.set_title('Post-Treatment Period\n(Cumulative Effects)', fontsize=11, fontweight='bold')
        ax.set_xlabel('Years After Entry', fontsize=10)
        ax.set_ylabel('Effect (β_k)', fontsize=10)
        ax.grid(True, alpha=0.3)

    # Overall title
    fig.suptitle('Event Study Decomposition: Pre/During/Post Treatment',
                 fontsize=14, fontweight='bold', y=1.02)

    # Tighten layout
    plt.tight_layout()

    # Save
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    fig.savefig(output_path, dpi=150, bbox_inches='tight')
    plt.close(fig)

    logger.info("event_study_decomposition_saved", path=str(output_path))
