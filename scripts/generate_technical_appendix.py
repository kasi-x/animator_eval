#!/usr/bin/env python3
"""Generate technical appendix from catalog and component reports.

This script:
1. Loads the technical appendix catalog
2. Validates all reports (file accessibility, cross-references)
3. Generates unified appendix JSON
4. Creates index for quick lookup by brief + category

Usage:
    python scripts/generate_technical_appendix.py
    python scripts/generate_technical_appendix.py --validate-only
    python scripts/generate_technical_appendix.py --format html
"""

import sys
import json
import argparse
from pathlib import Path

import structlog

# Add parent to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from scripts.report_generators.technical_appendix import (
    TechnicalAppendix,
    create_default_appendix,
)

log = structlog.get_logger(__name__)


def generate_appendix(output_dir: str = "result/json") -> tuple[bool, dict]:
    """Generate technical appendix.
    
    Returns:
        (success, metadata)
    """
    log.info("appendix_generation_start", mode="full")
    
    try:
        # Create default appendix with all reports
        appendix = create_default_appendix(result_dir=output_dir)
        
        # Validate
        is_valid, errors = appendix.validate()
        if not is_valid:
            for error in errors:
                log.warning("appendix_validation_warning", issue=error)
        
        # Export as JSON
        output_path = Path(output_dir) / "technical_appendix.json"
        appendix_dict = appendix.to_dict()
        
        with open(output_path, "w") as f:
            json.dump(appendix_dict, f, indent=2)
        
        log.info(
            "appendix_generated",
            file=str(output_path),
            reports=len(appendix.reports),
            valid=is_valid,
        )
        
        return True, appendix_dict
    
    except Exception as e:
        log.exception("appendix_generation_error", error=str(e))
        return False, {}


def validate_appendix(catalog_path: str = "scripts/report_generators/technical_appendix_catalog.json") -> tuple[bool, dict]:
    """Validate existing appendix catalog and reports.
    
    Returns:
        (all_valid, validation_results)
    """
    log.info("appendix_validation_start", mode="validate_only")
    
    try:
        appendix = TechnicalAppendix.load_from_catalog(catalog_path)
        is_valid, errors = appendix.validate()
        
        results = {
            "valid": is_valid,
            "total_reports": len(appendix.reports),
            "errors": errors,
        }
        
        if is_valid:
            log.info("appendix_validation_passed", reports=len(appendix.reports))
        else:
            log.error("appendix_validation_failed", error_count=len(errors))
        
        return is_valid, results
    
    except Exception as e:
        log.exception("appendix_validation_error", error=str(e))
        return False, {"error": str(e)}


def print_index(appendix_dict: dict) -> None:
    """Print brief index of appendix contents."""
    print("\n" + "="*70)
    print("TECHNICAL APPENDIX INDEX")
    print("="*70)
    
    metadata = appendix_dict.get("metadata", {})
    print(f"\nTotal Reports: {metadata.get('total_reports', 0)}")
    print(f"Active Reports: {metadata.get('active_reports', 0)}")
    
    print("\n📚 By Category:")
    by_cat = appendix_dict.get("reports_by_category", {})
    for category, reports in sorted(by_cat.items()):
        if reports:
            print(f"  {category}: {len(reports)} reports")
            for report in reports[:3]:  # Show first 3
                print(f"    • {report.get('title')}")
            if len(reports) > 3:
                print(f"    ... and {len(reports) - 3} more")
    
    print("\n📍 Cross-References to Main Briefs:")
    xrefs = appendix_dict.get("cross_references", {})
    for brief, info in sorted(xrefs.items()):
        total = info.get("total", 0)
        if total > 0:
            by_cat = info.get("by_category", {})
            print(f"  {brief}: {total} reports")
            for cat, count in sorted(by_cat.items()):
                if count > 0:
                    print(f"    • {cat}: {count}")
    
    print("\n" + "="*70 + "\n")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Generate or validate technical appendix")
    parser.add_argument("--validate-only", action="store_true", help="Only validate (no generation)")
    parser.add_argument("--format", choices=["json", "html", "pdf"], default="json", help="Output format")
    parser.add_argument("--output-dir", default="result/json", help="Output directory for JSON files")
    parser.add_argument("--no-index", action="store_true", help="Suppress index output")
    
    args = parser.parse_args()
    
    try:
        if args.validate_only:
            log.info("technical_appendix_validate_mode")
            is_valid, results = validate_appendix()
            print(f"\n✅ Validation passed" if is_valid else f"\n❌ Validation failed")
            print(f"   {results['total_reports']} reports, {len(results.get('errors', []))} issues")
            if results.get('errors'):
                print("\nErrors:")
                for error in results['errors']:
                    print(f"  • {error}")
            sys.exit(0 if is_valid else 1)
        
        else:
            log.info("technical_appendix_generate_mode")
            success, appendix_dict = generate_appendix(output_dir=args.output_dir)
            
            if success:
                if not args.no_index:
                    print_index(appendix_dict)
                log.info("all_appendix_tasks_complete")
                sys.exit(0)
            else:
                log.error("appendix_generation_failed")
                sys.exit(1)
    
    except KeyboardInterrupt:
        log.warning("appendix_generation_interrupted")
        sys.exit(130)
    except Exception as e:
        log.exception("unexpected_error", error=str(e))
        sys.exit(1)
