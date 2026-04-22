#!/usr/bin/env python3
"""Generate HTML and PDF exports for all briefs.

Usage:
    python scripts/generate_exports.py              # HTML only
    python scripts/generate_exports.py --format pdf # HTML + PDF (requires wkhtmltopdf)
    python scripts/generate_exports.py --brief policy --format html
"""

import sys
import argparse
from pathlib import Path

import structlog

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.report_generators.export import render_brief_html, generate_pdf_from_html

log = structlog.get_logger(__name__)


def export_brief(brief_id: str, format: str = "html") -> bool:
    """Export a single brief.
    
    Returns:
        True if successful, False otherwise
    """
    log.info("export_start", brief_id=brief_id, format=format)
    
    # Generate HTML
    output_dir = "result/html"
    html_path = render_brief_html(brief_id, output_dir)
    
    if not html_path:
        log.error("export_failed", brief_id=brief_id, reason="HTML rendering failed")
        return False
    
    log.info("html_generated", file=html_path)
    
    # Generate PDF if requested
    if format == "pdf":
        pdf_path = html_path.replace(".html", ".pdf")
        if generate_pdf_from_html(html_path, pdf_path):
            log.info("pdf_generated", file=pdf_path)
        else:
            log.warning("pdf_skipped", brief_id=brief_id, reason="wkhtmltopdf not available")
    
    return True


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Generate HTML/PDF exports")
    parser.add_argument("--brief", choices=["policy", "hr", "business"], help="Only export specific brief")
    parser.add_argument("--format", choices=["html", "pdf"], default="html", help="Output format")
    
    args = parser.parse_args()
    
    briefs = [args.brief] if args.brief else ["policy", "hr", "business"]
    
    log.info("export_session_start", briefs=briefs, format=args.format)
    
    results = {}
    for brief_id in briefs:
        success = export_brief(brief_id, args.format)
        results[brief_id] = "✅" if success else "❌"
    
    # Summary
    print("\n" + "="*70)
    print("EXPORT SUMMARY")
    print("="*70)
    for brief_id, status in results.items():
        html_file = f"result/html/{brief_id}_brief.html"
        pdf_file = f"result/html/{brief_id}_brief.pdf"
        print(f"\n{status} {brief_id.upper()}")
        print(f"   HTML: {html_file}")
        if args.format == "pdf":
            exists = "✓" if Path(pdf_file).exists() else "✗"
            print(f"   PDF:  {pdf_file} {exists}")
    print("\n" + "="*70 + "\n")
    
    success_count = sum(1 for s in results.values() if "✅" in s)
    total = len(results)
    
    if success_count == total:
        log.info("all_exports_complete", count=total)
        sys.exit(0)
    else:
        log.error("some_exports_failed", success=success_count, total=total)
        sys.exit(1)


if __name__ == "__main__":
    main()
