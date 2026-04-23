#!/usr/bin/env python3
"""Show before/after diffs for report briefs and appendix.

This script compares generated reports with their previous versions
to help review and validate changes.

Usage:
    python scripts/report_diff.py
    python scripts/report_diff.py --format summary
    python scripts/report_diff.py --format detailed
"""

import json
import argparse
from typing import Optional

import structlog

log = structlog.get_logger(__name__)


def load_json(file_path: str) -> Optional[dict]:
    """Load JSON file safely."""
    try:
        with open(file_path) as f:
            return json.load(f)
    except Exception as e:
        log.warning("load_error", file=file_path, error=str(e))
        return None


def compare_dicts(old: dict, new: dict, path: str = "") -> list[str]:
    """Recursively compare two dictionaries and return diffs."""
    diffs = []
    
    all_keys = set(old.keys()) | set(new.keys())
    
    for key in sorted(all_keys):
        current_path = f"{path}.{key}" if path else key
        
        old_val = old.get(key)
        new_val = new.get(key)
        
        if old_val == new_val:
            continue
        
        if isinstance(old_val, dict) and isinstance(new_val, dict):
            diffs.extend(compare_dicts(old_val, new_val, current_path))
        elif key not in old:
            diffs.append(f"  + {current_path}: {type(new_val).__name__}")
        elif key not in new:
            diffs.append(f"  - {current_path}: (removed)")
        else:
            diffs.append(f"  ~ {current_path}: {type(old_val).__name__} → {type(new_val).__name__}")
    
    return diffs


def show_brief_diff(brief_id: str, brief_type: str = "generated") -> None:
    """Show diff for a single brief."""
    file_path = f"result/json/{brief_id}_brief.json"
    
    new_data = load_json(file_path)
    if not new_data:
        return
    
    # Try to load from git
    try:
        import subprocess
        old_json = subprocess.check_output(
            ["git", "show", f"HEAD:{file_path}"],
            stderr=subprocess.DEVNULL,
            text=True
        )
        old_data = json.loads(old_json)
    except Exception:
        log.warning("no_git_version", file=file_path)
        old_data = None
    
    print(f"\n📋 {brief_id.upper()} Brief")
    print("=" * 60)
    
    if old_data is None:
        print(f"  (No prior version in git)")
        print(f"  • Sections: {len(new_data.get('sections', {}))}")
        print(f"  • Method gates: {len(new_data.get('method_gates', []))}")
        print(f"  • Generated: {new_data.get('generated_at', 'N/A')}")
        return
    
    diffs = compare_dicts(old_data, new_data)
    
    if not diffs:
        print("  ✅ No changes")
    else:
        print(f"  {len(diffs)} changes:")
        for diff in diffs[:10]:  # Show first 10
            print(f"    {diff}")
        if len(diffs) > 10:
            print(f"    ... and {len(diffs) - 10} more")


def show_appendix_diff() -> None:
    """Show diff for technical appendix."""
    file_path = "result/json/technical_appendix.json"
    
    new_data = load_json(file_path)
    if not new_data:
        return
    
    # Try to load from git
    try:
        import subprocess
        old_json = subprocess.check_output(
            ["git", "show", f"HEAD:{file_path}"],
            stderr=subprocess.DEVNULL,
            text=True
        )
        old_data = json.loads(old_json)
    except Exception:
        log.warning("no_git_version", file=file_path)
        old_data = None
    
    print(f"\n📚 Technical Appendix")
    print("=" * 60)
    
    if old_data is None:
        print(f"  (No prior version in git)")
        metadata = new_data.get('metadata', {})
        print(f"  • Total reports: {metadata.get('total_reports', 0)}")
        print(f"  • Active reports: {metadata.get('active_reports', 0)}")
        print(f"  • Generated: {metadata.get('generated_at', 'N/A')}")
        return
    
    # Compare metadata
    old_meta = old_data.get('metadata', {})
    new_meta = new_data.get('metadata', {})
    
    if old_meta.get('total_reports') != new_meta.get('total_reports'):
        print(f"  • Reports: {old_meta.get('total_reports')} → {new_meta.get('total_reports')}")
    
    if old_meta.get('active_reports') != new_meta.get('active_reports'):
        print(f"  • Active: {old_meta.get('active_reports')} → {new_meta.get('active_reports')}")
    
    # Compare report counts by category
    old_by_cat = old_data.get('reports_by_category', {})
    new_by_cat = new_data.get('reports_by_category', {})
    
    changes = []
    for cat in set(old_by_cat.keys()) | set(new_by_cat.keys()):
        old_count = len(old_by_cat.get(cat, []))
        new_count = len(new_by_cat.get(cat, []))
        if old_count != new_count:
            changes.append(f"    • {cat}: {old_count} → {new_count}")
    
    if changes:
        print("  Category changes:")
        for change in changes:
            print(change)
    else:
        print("  ✅ No structural changes")


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="Show report diffs")
    parser.add_argument("--format", choices=["summary", "detailed"], default="summary")
    parser.add_argument("--brief", choices=["policy", "hr", "business"], help="Only show specific brief")
    parser.add_argument("--appendix-only", action="store_true", help="Only show appendix")
    
    args = parser.parse_args()
    
    print("\n" + "=" * 60)
    print("REPORT CHANGES (Compared to HEAD)")
    print("=" * 60)
    
    if not args.appendix_only:
        if args.brief:
            show_brief_diff(args.brief)
        else:
            for brief_id in ["policy", "hr", "business"]:
                show_brief_diff(brief_id)
    
    if not args.brief:
        show_appendix_diff()
    
    print("\n" + "=" * 60 + "\n")


if __name__ == "__main__":
    main()
