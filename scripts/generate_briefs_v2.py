#!/usr/bin/env python3
"""Orchestrator for generating all report briefs (v2 architecture).

Combines policy, HR, and business briefs with optional technical appendix.
Validates all briefs against method gates and vocabulary enforcement.

Usage:
    python scripts/generate_briefs_v2.py
    python scripts/generate_briefs_v2.py --validate-only
    python scripts/generate_briefs_v2.py --export-html
"""

import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

import structlog
from scripts.report_generators.briefs.policy_brief import generate_policy_brief
from scripts.report_generators.briefs.hr_brief import generate_hr_brief
from scripts.report_generators.briefs.business_brief import generate_business_brief

log = structlog.get_logger(__name__)

JSON_DIR = Path("result/json")
BRIEFS = {
    "policy": {
        "generator": generate_policy_brief,
        "filename": "policy_brief.json",
        "description": "Industry policy brief for policymakers",
    },
    "hr": {
        "generator": generate_hr_brief,
        "filename": "hr_brief.json",
        "description": "Studio operations & HR brief for managers",
    },
    "business": {
        "generator": generate_business_brief,
        "filename": "business_brief.json",
        "description": "Market opportunities & innovation brief for investors",
    },
}


def generate_all_briefs(validate_only: bool = False) -> dict:
    """Generate all briefs with comprehensive validation.
    
    Args:
        validate_only: If True, skip generation (only validate existing)
    
    Returns:
        Dict with generation results: {brief_id: {status, file, sections, gates}}
    """
    results = {}
    
    for brief_id, config in BRIEFS.items():
        log.info("brief_generate_start", brief_id=brief_id, description=config["description"])
        
        try:
            # Generate brief
            brief_dict = config["generator"]()
            
            if not brief_dict:
                log.error("brief_generation_failed", brief_id=brief_id, reason="generator_returned_empty")
                results[brief_id] = {
                    "status": "failed",
                    "reason": "Generator validation failed",
                    "file": None,
                }
                continue
            
            # Save to JSON
            output_path = JSON_DIR / config["filename"]
            output_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(output_path, "w", encoding="utf-8") as f:
                json.dump(brief_dict, f, ensure_ascii=False, indent=2)
            
            # Collect metrics
            sections = len(brief_dict.get("sections", {}))
            gates = len(brief_dict.get("method_gates", []))
            
            log.info(
                "brief_generated",
                brief_id=brief_id,
                file=str(output_path),
                sections=sections,
                gates=gates,
                size_kb=output_path.stat().st_size / 1024,
            )
            
            results[brief_id] = {
                "status": "success",
                "file": str(output_path),
                "sections": sections,
                "gates": gates,
                "size_kb": output_path.stat().st_size / 1024,
            }
            
        except Exception as e:
            log.exception("brief_generation_error", brief_id=brief_id)
            results[brief_id] = {
                "status": "error",
                "reason": str(e),
                "file": None,
            }
    
    return results


def validate_briefs() -> tuple[bool, dict]:
    """Validate all generated briefs.
    
    Returns:
        (all_valid, results_dict)
    """
    results = {}
    all_valid = True
    
    for brief_id, config in BRIEFS.items():
        brief_file = JSON_DIR / config["filename"]
        
        if not brief_file.exists():
            log.warning("brief_not_found", brief_id=brief_id, file=str(brief_file))
            results[brief_id] = {"status": "not_found"}
            all_valid = False
            continue
        
        try:
            with open(brief_file, "r", encoding="utf-8") as f:
                brief_dict = json.load(f)
            
            # Check required fields
            errors = []
            
            if "metadata" not in brief_dict:
                errors.append("Missing metadata")
            
            if "sections" not in brief_dict or not brief_dict["sections"]:
                errors.append("Missing sections")
            
            if "method_gates" not in brief_dict or not brief_dict["method_gates"]:
                errors.append("Missing method gates")
            
            # Validate sections
            for section_id, section in brief_dict.get("sections", {}).items():
                if not section.get("findings"):
                    errors.append(f"Section '{section_id}' missing findings")
                if not section.get("interpretation"):
                    errors.append(f"Section '{section_id}' missing interpretation")
            
            if errors:
                log.warning("brief_validation_failed", brief_id=brief_id, errors=errors)
                results[brief_id] = {"status": "invalid", "errors": errors}
                all_valid = False
            else:
                log.info("brief_validation_passed", brief_id=brief_id)
                results[brief_id] = {
                    "status": "valid",
                    "sections": len(brief_dict.get("sections", {})),
                    "gates": len(brief_dict.get("method_gates", [])),
                }
        
        except Exception as e:
            log.exception("brief_validation_error", brief_id=brief_id)
            results[brief_id] = {"status": "error", "reason": str(e)}
            all_valid = False
    
    return all_valid, results


def print_summary(results: dict) -> None:
    """Print generation/validation summary."""
    print("\n" + "="*70)
    print("REPORT BRIEFS GENERATION SUMMARY")
    print("="*70)
    
    for brief_id, result in results.items():
        status = result.get("status", "unknown").upper()
        symbol = "✅" if status == "SUCCESS" else "❌" if status == "ERROR" else "⚠️"
        
        print(f"\n{symbol} {brief_id.upper()}")
        print(f"   Status: {status}")
        
        if result.get("file"):
            print(f"   File: {result['file']}")
        
        if result.get("sections"):
            print(f"   Sections: {result['sections']}")
        
        if result.get("gates"):
            print(f"   Method gates: {result['gates']}")
        
        if result.get("size_kb"):
            print(f"   Size: {result['size_kb']:.1f} KB")
        
        if result.get("errors"):
            for error in result["errors"]:
                print(f"   ⚠️  {error}")
        
        if result.get("reason"):
            print(f"   Reason: {result['reason']}")
    
    print("\n" + "="*70)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Generate all report briefs (v2)")
    parser.add_argument("--validate-only", action="store_true", help="Only validate existing briefs")
    parser.add_argument("--no-summary", action="store_true", help="Suppress summary output")
    
    args = parser.parse_args()
    
    if args.validate_only:
        log.info("brief_validation_start", mode="validate_only")
        all_valid, results = validate_briefs()
        if not args.no_summary:
            print_summary(results)
        sys.exit(0 if all_valid else 1)
    
    # Generate all briefs
    log.info("brief_generation_start", mode="full")
    results = generate_all_briefs()
    
    # Validate
    log.info("brief_validation_start", mode="after_generation")
    all_valid, val_results = validate_briefs()
    
    # Merge validation results into generation results
    for brief_id, val_result in val_results.items():
        if brief_id in results and results[brief_id].get("status") == "success":
            # Update validation status only (keep generation metadata)
            results[brief_id]["validation_status"] = val_result.get("status")
            if val_result.get("status") == "valid":
                results[brief_id]["status"] = "valid"
            else:
                results[brief_id]["status"] = val_result.get("status")
                if val_result.get("errors"):
                    results[brief_id]["errors"] = val_result["errors"]
    
    if not args.no_summary:
        print_summary(results)
    
    # Exit with appropriate code
    success_count = sum(1 for r in results.values() if r.get("status") in ("success", "valid"))
    total = len(results)
    
    if success_count == total:
        log.info("all_briefs_complete", generated=total)
        sys.exit(0)
    else:
        log.error("some_briefs_failed", generated=success_count, total=total)
        sys.exit(1)
