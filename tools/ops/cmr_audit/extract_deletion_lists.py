#!/usr/bin/env python3
"""
Extract deletion lists from completeness audit JSONL files.

Takes any number of JSONL files and produces a consolidated list of DISP-S1
products to delete, grouped by the reason they need to be deleted.

Usage:
    python extract_deletion_lists.py file1.jsonl file2.jsonl ... [--output FILE]

    # Output to stdout (reasons with products)
    python extract_deletion_lists.py completeness_*.jsonl

    # Summary format (products with reasons - each product listed once)
    python extract_deletion_lists.py completeness_*.jsonl --format summary

    # Output to file
    python extract_deletion_lists.py completeness_*.jsonl --output deletion_list.txt

    # Output as JSON
    python extract_deletion_lists.py completeness_*.jsonl --format json --output deletion_list.json

    # Just the unique product IDs (for piping)
    python extract_deletion_lists.py completeness_*.jsonl --format list
"""

import argparse
import json
import re
import sys
from collections import defaultdict
from pathlib import Path


def extract_deletion_lists_from_jsonl(filepath):
    """Extract deletion lists from a JSONL file."""
    deletion_lists = defaultdict(set)

    with open(filepath) as f:
        for line in f:
            line = line.strip()
            if not line:
                continue

            try:
                obj = json.loads(line)
            except json.JSONDecodeError as e:
                print(f"Warning: Could not parse line in {filepath}: {e}", file=sys.stderr)
                continue

            if obj.get('type') != 'frame_report':
                continue

            report = obj.get('report', {})
            dl = report.get('deletion_lists', {})

            # Stale products (grouped by reason)
            stale_products = dl.get('disp_s1_stale_products', {})
            for reason, products in stale_products.items():
                key = f"stale: {reason}"
                for product in products:
                    deletion_lists[key].add(product)

            # Products by anomaly reason
            products_by_reason = dl.get('disp_s1_products_by_reason', {})
            for reason, products in products_by_reason.items():
                key = f"anomaly: {reason}"
                for product in products:
                    deletion_lists[key].add(product)

            # Products found despite untriggerable K-cycle
            untriggerable_products = dl.get('disp_s1_products_found_despite_untriggerable', [])
            if untriggerable_products:
                key = "found_despite_untriggerable"
                for product in untriggerable_products:
                    deletion_lists[key].add(product)

            # Duplicate products
            duplicate_products = dl.get('disp_s1_duplicate_products', [])
            if duplicate_products:
                key = "duplicate"
                for product in duplicate_products:
                    deletion_lists[key].add(product)

    return deletion_lists


def merge_deletion_lists(all_lists):
    """Merge multiple deletion lists into one."""
    merged = defaultdict(set)
    for dl in all_lists:
        for reason, products in dl.items():
            merged[reason].update(products)
    return merged


def format_text_output(deletion_lists):
    """Format deletion lists as text output."""
    lines = []
    lines.append("=" * 80)
    lines.append("DISP-S1 DELETION LIST")
    lines.append("=" * 80)
    lines.append("")

    # Calculate totals
    all_products = set()
    for products in deletion_lists.values():
        all_products.update(products)

    lines.append(f"Total unique products to delete: {len(all_products)}")
    lines.append("")

    # Define order for reasons (stale first, then anomalies, then others)
    def sort_key(reason):
        if reason.startswith("stale:"):
            return (0, reason)
        elif reason.startswith("anomaly:"):
            return (1, reason)
        elif reason == "duplicate":
            return (2, reason)
        elif reason == "found_despite_untriggerable":
            return (3, reason)
        else:
            return (4, reason)

    sorted_reasons = sorted(deletion_lists.keys(), key=sort_key)

    for reason in sorted_reasons:
        products = sorted(deletion_lists[reason])
        lines.append("-" * 80)
        lines.append(f"{reason.upper()} ({len(products)})")
        lines.append("-" * 80)
        for product in products:
            lines.append(product)
        lines.append("")

    return "\n".join(lines)


def format_json_output(deletion_lists):
    """Format deletion lists as JSON output."""
    # Convert sets to sorted lists for JSON serialization
    output = {
        "total_unique_products": len(set().union(*deletion_lists.values())) if deletion_lists else 0,
        "by_reason": {reason: sorted(products) for reason, products in deletion_lists.items()},
        "all_products": sorted(set().union(*deletion_lists.values())) if deletion_lists else []
    }
    return json.dumps(output, indent=2)


def format_list_output(deletion_lists):
    """Format as a simple list of all unique products (no grouping)."""
    all_products = set()
    for products in deletion_lists.values():
        all_products.update(products)
    return "\n".join(sorted(all_products))


def format_summary_output(deletion_lists):
    """Format with products grouped by their reasons (inverse of text format)."""
    # Build product -> reasons mapping
    product_to_reasons = defaultdict(set)
    for reason, products in deletion_lists.items():
        for product in products:
            product_to_reasons[product].add(reason)

    lines = []
    lines.append("=" * 80)
    lines.append("DISP-S1 DELETION LIST (by product)")
    lines.append("=" * 80)
    lines.append("")
    lines.append(f"Total unique products to delete: {len(product_to_reasons)}")
    lines.append("")

    # Count products by primary reason (stale > anomaly > duplicate > other)
    def get_primary_reason(reasons):
        for r in reasons:
            if r.startswith("stale:"):
                return r
        for r in reasons:
            if r == "duplicate":
                return r
        for r in reasons:
            if r == "found_despite_untriggerable":
                return r
        for r in reasons:
            if r.startswith("anomaly:"):
                return r
        return sorted(reasons)[0] if reasons else "unknown"

    # Group products by primary reason for organization
    products_by_primary = defaultdict(list)
    for product, reasons in product_to_reasons.items():
        primary = get_primary_reason(reasons)
        products_by_primary[primary].append((product, reasons))

    # Sort primary reasons
    def sort_key(reason):
        if reason.startswith("stale:"):
            return (0, reason)
        elif reason == "duplicate":
            return (1, reason)
        elif reason == "found_despite_untriggerable":
            return (2, reason)
        elif reason.startswith("anomaly:"):
            return (3, reason)
        else:
            return (4, reason)

    sorted_primaries = sorted(products_by_primary.keys(), key=sort_key)

    for primary in sorted_primaries:
        products = products_by_primary[primary]
        lines.append("-" * 80)
        lines.append(f"{primary.upper()} ({len(products)} products)")
        lines.append("-" * 80)

        for product, reasons in sorted(products, key=lambda x: x[0]):
            lines.append(product)
            # Show additional reasons (not the primary)
            other_reasons = sorted(r for r in reasons if r != primary)
            if other_reasons:
                # Consolidate anomaly reasons
                anomaly_indices = []
                other_non_anomaly = []
                for r in other_reasons:
                    if r.startswith("anomaly: unexpected sensing time"):
                        # Extract index from reason
                        match = re.search(r'idx (\d+)', r)
                        if match:
                            anomaly_indices.append(int(match.group(1)))
                        else:
                            other_non_anomaly.append(r)
                    else:
                        other_non_anomaly.append(r)

                if anomaly_indices:
                    # Consolidate consecutive indices into ranges
                    anomaly_indices.sort()
                    ranges = []
                    start = anomaly_indices[0]
                    end = start
                    for idx in anomaly_indices[1:]:
                        if idx == end + 1:
                            end = idx
                        else:
                            ranges.append((start, end))
                            start = idx
                            end = idx
                    ranges.append((start, end))

                    range_strs = []
                    for s, e in ranges:
                        if s == e:
                            range_strs.append(str(s))
                        else:
                            range_strs.append(f"{s}-{e}")
                    lines.append(f"    + anomaly: unexpected sensing time (indices: {', '.join(range_strs)})")

                for r in other_non_anomaly:
                    lines.append(f"    + {r}")
        lines.append("")

    return "\n".join(lines)


def main():
    parser = argparse.ArgumentParser(
        description="Extract deletion lists from completeness audit JSONL files.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    # Output to stdout
    python extract_deletion_lists.py completeness_*.jsonl

    # Output to file
    python extract_deletion_lists.py completeness_*.jsonl --output deletion_list.txt

    # Output as JSON
    python extract_deletion_lists.py completeness_*.jsonl --format json --output deletion_list.json

    # Output just the product IDs (for piping to other tools)
    python extract_deletion_lists.py completeness_*.jsonl --format list
"""
    )
    parser.add_argument(
        "jsonl_files",
        nargs="+",
        help="JSONL files from completeness audit"
    )
    parser.add_argument(
        "--output", "-o",
        help="Output file (default: stdout)"
    )
    parser.add_argument(
        "--format", "-f",
        choices=["text", "json", "list", "summary"],
        default="text",
        help="Output format: text (reasons with products), summary (products with reasons), json, or list (just product IDs)"
    )

    args = parser.parse_args()

    # Process all JSONL files
    all_deletion_lists = []
    for filepath in args.jsonl_files:
        path = Path(filepath)
        if not path.exists():
            print(f"Warning: File not found: {filepath}", file=sys.stderr)
            continue
        if not path.suffix == '.jsonl':
            print(f"Warning: Skipping non-JSONL file: {filepath}", file=sys.stderr)
            continue

        print(f"Processing: {filepath}", file=sys.stderr)
        dl = extract_deletion_lists_from_jsonl(filepath)
        all_deletion_lists.append(dl)

    if not all_deletion_lists:
        print("Error: No valid JSONL files processed.", file=sys.stderr)
        sys.exit(1)

    # Merge all deletion lists
    merged = merge_deletion_lists(all_deletion_lists)

    # Format output
    if args.format == "json":
        output = format_json_output(merged)
    elif args.format == "list":
        output = format_list_output(merged)
    elif args.format == "summary":
        output = format_summary_output(merged)
    else:
        output = format_text_output(merged)

    # Write output
    if args.output:
        with open(args.output, 'w') as f:
            f.write(output)
        print(f"Output written to: {args.output}", file=sys.stderr)
    else:
        print(output)


if __name__ == "__main__":
    main()
