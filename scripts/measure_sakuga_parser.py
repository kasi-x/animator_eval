"""Measure parse quality on cached sakuga atwiki person pages.

Usage:
    pixi run python scripts/measure_sakuga_parser.py [--data-dir data/sakuga]

Reads discovered_pages.json + cache/*.html.gz, parses every person page,
and reports: total / regex_ok / llm_fallback / failed / credit distribution.
Target: regex_ok / total >= 0.70
"""
from __future__ import annotations

import argparse
import gzip
import json
from collections import Counter
from pathlib import Path

from src.scrapers.parsers.sakuga_atwiki import _llm_fallback, parse_person_page


def _load_html(cache_path: Path) -> str | None:
    if not cache_path.exists():
        return None
    return gzip.decompress(cache_path.read_bytes()).decode()


def main(data_dir: Path) -> None:
    discovered_path = data_dir / "discovered_pages.json"
    if not discovered_path.exists():
        print(f"Not found: {discovered_path}")
        return

    pages = json.loads(discovered_path.read_text(encoding="utf-8"))
    persons = [p for p in pages if p.get("page_kind") == "person"]
    print(f"Person pages: {len(persons)}")

    total = regex_ok = llm_ok = failed = 0
    credit_counts: list[int] = []

    for page in persons:
        pid = page["id"]
        html = _load_html(data_dir / "cache" / f"{pid}.html.gz")
        if html is None:
            continue
        total += 1

        # Patch out LLM to measure regex-only rate
        import unittest.mock as mock

        llm_called = False
        original = _llm_fallback

        def mock_llm(text: str) -> list:
            nonlocal llm_called
            llm_called = True
            return original(text)

        with mock.patch("src.scrapers.parsers.sakuga_atwiki._llm_fallback", side_effect=mock_llm):
            result = parse_person_page(html, page_id=pid)

        n = len(result.credits)
        credit_counts.append(n)

        if n > 0 and not llm_called:
            regex_ok += 1
        elif n > 0 and llm_called:
            llm_ok += 1
        else:
            failed += 1

    if total == 0:
        print("No cached person pages found. Run discover first.")
        return

    print(f"\n{'='*50}")
    print(f"Total parsed :  {total}")
    print(f"Regex OK     :  {regex_ok}  ({regex_ok/total:.1%})")
    print(f"LLM fallback :  {llm_ok}   ({llm_ok/total:.1%})")
    print(f"Failed (0)   :  {failed}   ({failed/total:.1%})")
    print("\nCredit count distribution:")
    dist = Counter(credit_counts)
    for k in sorted(dist):
        bar = "█" * min(dist[k], 40)
        print(f"  {k:4d}: {dist[k]:4d}  {bar}")
    if credit_counts:
        avg = sum(credit_counts) / len(credit_counts)
        print(f"\nAvg credits/page: {avg:.1f}")
    if regex_ok / total < 0.70:
        print(f"\n⚠ regex_ok/total={regex_ok/total:.1%} < 0.70 — regex rules need tuning")
    else:
        print(f"\n✓ regex_ok/total={regex_ok/total:.1%} >= 0.70")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--data-dir", default="data/sakuga", type=Path)
    args = parser.parse_args()
    main(args.data_dir)
