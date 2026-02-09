"""Entity resolution evaluation and reporting tools.

Helps validate matching quality and tune thresholds by providing:
- Step-by-step match analysis
- Confidence scoring
- Manual review helpers
- Accuracy metrics
"""

from collections import defaultdict
from dataclasses import dataclass
from typing import Literal

import structlog

from src.models import Person

logger = structlog.get_logger()

MatchStrategy = Literal["exact", "cross_source", "romaji", "similarity", "ai_assisted"]


@dataclass(frozen=True)
class MatchDecision:
    """A single entity resolution match decision."""

    source_id: str
    canonical_id: str
    strategy: MatchStrategy
    confidence: float  # 0.0-1.0
    reason: str
    source_person: Person
    canonical_person: Person


@dataclass
class ResolutionReport:
    """Complete entity resolution report."""

    total_persons: int
    total_matches: int
    match_rate: float
    matches_by_strategy: dict[MatchStrategy, int]
    decisions: list[MatchDecision]
    ambiguous_cases: list[tuple[Person, Person, float]]  # (p1, p2, similarity)


def evaluate_match_pair(
    p1: Person,
    p2: Person,
    canonical_map: dict[str, str],
    strategy: MatchStrategy,
) -> MatchDecision | None:
    """Evaluate a single match decision.

    Args:
        p1: First person
        p2: Second person
        canonical_map: Current resolution mapping
        strategy: Matching strategy used

    Returns:
        MatchDecision if matched, None otherwise
    """
    # Check if p2 was mapped to p1
    if canonical_map.get(p2.id) == p1.id:
        # Determine confidence based on strategy
        confidence_map = {
            "exact": 1.0,
            "cross_source": 0.95,
            "romaji": 0.90,
            "similarity": 0.85,
            "ai_assisted": 0.85,
        }
        confidence = confidence_map.get(strategy, 0.8)

        # Generate reason
        reason = _generate_match_reason(p1, p2, strategy)

        return MatchDecision(
            source_id=p2.id,
            canonical_id=p1.id,
            strategy=strategy,
            confidence=confidence,
            reason=reason,
            source_person=p2,
            canonical_person=p1,
        )

    return None


def _generate_match_reason(p1: Person, p2: Person, strategy: MatchStrategy) -> str:
    """Generate human-readable reason for match."""
    if strategy == "exact":
        return f"Exact name match: {p1.name_ja or p1.name_en}"
    elif strategy == "cross_source":
        return f"Cross-source exact match: {p1.name_ja or p1.name_en}"
    elif strategy == "romaji":
        return f"Romaji normalization: {p1.name_en} ≈ {p2.name_en}"
    elif strategy == "similarity":
        return f"High similarity: {p1.name_ja or p1.name_en} ≈ {p2.name_ja or p2.name_en}"
    elif strategy == "ai_assisted":
        return f"AI decision: {p1.name_ja or p1.name_en} ≈ {p2.name_ja or p2.name_en}"
    return "Unknown strategy"


def generate_resolution_report(
    persons: list[Person],
    canonical_map: dict[str, str],
    strategy_breakdown: dict[MatchStrategy, dict[str, str]],
) -> ResolutionReport:
    """Generate comprehensive resolution report.

    Args:
        persons: All persons
        canonical_map: Complete resolution mapping
        strategy_breakdown: {strategy: {source_id: canonical_id}}

    Returns:
        ResolutionReport with detailed analysis
    """
    persons_by_id = {p.id: p for p in persons}

    # Build decision list
    decisions = []
    for strategy, matches in strategy_breakdown.items():
        for source_id, canonical_id in matches.items():
            if source_id in persons_by_id and canonical_id in persons_by_id:
                decision = MatchDecision(
                    source_id=source_id,
                    canonical_id=canonical_id,
                    strategy=strategy,
                    confidence=0.95 if strategy in ["exact", "cross_source"] else 0.85,
                    reason=_generate_match_reason(
                        persons_by_id[canonical_id],
                        persons_by_id[source_id],
                        strategy,
                    ),
                    source_person=persons_by_id[source_id],
                    canonical_person=persons_by_id[canonical_id],
                )
                decisions.append(decision)

    # Calculate statistics
    matches_by_strategy = {
        strategy: len(matches) for strategy, matches in strategy_breakdown.items()
    }

    total_matches = len(canonical_map)
    match_rate = total_matches / len(persons) if persons else 0.0

    return ResolutionReport(
        total_persons=len(persons),
        total_matches=total_matches,
        match_rate=match_rate,
        matches_by_strategy=matches_by_strategy,
        decisions=decisions,
        ambiguous_cases=[],
    )


def format_resolution_report(report: ResolutionReport) -> str:
    """Format resolution report as readable text."""
    lines = []

    lines.append("=" * 80)
    lines.append("ENTITY RESOLUTION REPORT")
    lines.append("=" * 80)
    lines.append("")

    # Summary
    lines.append(f"Total persons: {report.total_persons}")
    lines.append(f"Total matches: {report.total_matches}")
    lines.append(f"Match rate: {report.match_rate * 100:.1f}%")
    lines.append(f"Unique persons after resolution: {report.total_persons - report.total_matches}")
    lines.append("")

    # Breakdown by strategy
    lines.append("Matches by strategy:")
    for strategy, count in report.matches_by_strategy.items():
        pct = (count / report.total_matches * 100) if report.total_matches > 0 else 0
        lines.append(f"  {strategy:15s}: {count:4d} ({pct:5.1f}%)")
    lines.append("")

    # Sample decisions (first 20)
    lines.append("Sample match decisions (first 20):")
    lines.append("-" * 80)
    for i, decision in enumerate(report.decisions[:20], 1):
        lines.append(f"{i}. [{decision.strategy}] {decision.source_id} → {decision.canonical_id}")
        lines.append(f"   Source:    {decision.source_person.name_ja or ''} ({decision.source_person.name_en or ''})")
        lines.append(
            f"   Canonical: {decision.canonical_person.name_ja or ''} ({decision.canonical_person.name_en or ''})"
        )
        lines.append(f"   Reason: {decision.reason}")
        lines.append(f"   Confidence: {decision.confidence:.2f}")
        lines.append("")

    if len(report.decisions) > 20:
        lines.append(f"... and {len(report.decisions) - 20} more matches")
        lines.append("")

    lines.append("=" * 80)

    return "\n".join(lines)


def export_matches_for_review(
    report: ResolutionReport,
    output_file: str,
    min_confidence: float = 0.0,
    max_confidence: float = 1.0,
) -> None:
    """Export matches to CSV for manual review.

    Args:
        report: Resolution report
        output_file: Output CSV path
        min_confidence: Minimum confidence to include
        max_confidence: Maximum confidence to include (for reviewing uncertain matches)
    """
    import csv

    filtered = [
        d for d in report.decisions if min_confidence <= d.confidence <= max_confidence
    ]

    with open(output_file, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(
            [
                "Source ID",
                "Canonical ID",
                "Strategy",
                "Confidence",
                "Source Name JA",
                "Source Name EN",
                "Canonical Name JA",
                "Canonical Name EN",
                "Reason",
                "Correct? (Y/N)",
            ]
        )

        for decision in filtered:
            writer.writerow(
                [
                    decision.source_id,
                    decision.canonical_id,
                    decision.strategy,
                    f"{decision.confidence:.2f}",
                    decision.source_person.name_ja or "",
                    decision.source_person.name_en or "",
                    decision.canonical_person.name_ja or "",
                    decision.canonical_person.name_en or "",
                    decision.reason,
                    "",  # Empty column for manual annotation
                ]
            )

    logger.info(
        "exported_matches_for_review",
        file=output_file,
        count=len(filtered),
        min_confidence=min_confidence,
        max_confidence=max_confidence,
    )


def calculate_precision_from_review(review_csv: str) -> dict[str, float]:
    """Calculate precision from manually reviewed CSV.

    Args:
        review_csv: Path to CSV with "Correct? (Y/N)" column filled

    Returns:
        {strategy: precision} for each strategy
    """
    import csv

    correct_by_strategy: dict[str, int] = defaultdict(int)
    total_by_strategy: dict[str, int] = defaultdict(int)

    with open(review_csv, encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            strategy = row["Strategy"]
            is_correct = row["Correct? (Y/N)"].strip().upper()

            if is_correct in ("Y", "N"):
                total_by_strategy[strategy] += 1
                if is_correct == "Y":
                    correct_by_strategy[strategy] += 1

    precision = {}
    for strategy in total_by_strategy:
        if total_by_strategy[strategy] > 0:
            precision[strategy] = correct_by_strategy[strategy] / total_by_strategy[strategy]

    return precision
