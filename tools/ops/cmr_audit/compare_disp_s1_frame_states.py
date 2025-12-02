#!/usr/bin/env python
"""
Compare frame states between a CMR audit output JSON and a batch proc JSON.

Usage:
    python compare_disp_s1_frame_states.py <cmr_audit_output.json> <batch_proc.json>

Example:
    python compare_disp_s1_frame_states.py frame_states_output.json batch_proc.json
"""

import json
import sys
import argparse


def load_json(filepath):
    """Load JSON file and return contents."""
    with open(filepath, 'r') as f:
        return json.load(f)


def compare_frame_states(cmr_audit_path, batch_proc_path):
    """
    Compare frame states between CMR audit output and batch proc.

    Args:
        cmr_audit_path: Path to CMR audit output JSON (from --output-frame-states)
        batch_proc_path: Path to batch proc JSON file
    """
    # Load both files
    cmr_audit = load_json(cmr_audit_path)
    batch_proc = load_json(batch_proc_path)

    # Extract frame_states from both
    cmr_states = cmr_audit.get('frame_states', {})
    batch_states = batch_proc.get('frame_states', {})

    # Get k value
    cmr_k = cmr_audit.get('k', 15)
    batch_k = batch_proc.get('k', 15)

    if cmr_k != batch_k:
        print(f"WARNING: k values differ! CMR audit: {cmr_k}, Batch proc: {batch_k}")
        print()

    # Get all frame IDs from both sources
    all_frames = sorted(set(list(cmr_states.keys()) + list(batch_states.keys())), key=lambda x: int(x))

    # Print header
    print("=" * 100)
    print("FRAME STATE COMPARISON")
    print("=" * 100)
    print(f"CMR Audit File:  {cmr_audit_path}")
    print(f"Batch Proc File: {batch_proc_path}")
    print(f"K value: {cmr_k}")
    print("=" * 100)
    print()

    # Print comparison table header
    print(f"{'Frame ID':>10} | {'CMR State':>12} | {'Batch State':>12} | {'Difference':>12} | {'K-Cycles Diff':>14} | {'Status':<20}")
    print("-" * 100)

    # Track statistics
    stats = {
        'match': 0,
        'cmr_ahead': 0,
        'batch_ahead': 0,
        'only_in_cmr': 0,
        'only_in_batch': 0,
    }

    total_diff = 0

    for frame_id in all_frames:
        cmr_val = cmr_states.get(frame_id)
        batch_val = batch_states.get(frame_id)

        if cmr_val is None:
            status = "ONLY IN BATCH"
            diff_str = "N/A"
            kcycle_diff_str = "N/A"
            stats['only_in_batch'] += 1
            cmr_str = "—"
            batch_str = str(batch_val)
        elif batch_val is None:
            status = "ONLY IN CMR"
            diff_str = "N/A"
            kcycle_diff_str = "N/A"
            stats['only_in_cmr'] += 1
            cmr_str = str(cmr_val)
            batch_str = "—"
        else:
            cmr_str = str(cmr_val)
            batch_str = str(batch_val)
            diff = cmr_val - batch_val
            total_diff += abs(diff)
            kcycle_diff = diff / cmr_k

            if diff == 0:
                status = "MATCH ✓"
                stats['match'] += 1
                diff_str = "0"
                kcycle_diff_str = "0"
            elif diff > 0:
                status = "CMR AHEAD"
                stats['cmr_ahead'] += 1
                diff_str = f"+{diff}"
                kcycle_diff_str = f"+{kcycle_diff:.1f}"
            else:
                status = "BATCH AHEAD"
                stats['batch_ahead'] += 1
                diff_str = str(diff)
                kcycle_diff_str = f"{kcycle_diff:.1f}"

        print(f"{frame_id:>10} | {cmr_str:>12} | {batch_str:>12} | {diff_str:>12} | {kcycle_diff_str:>14} | {status:<20}")

    # Print summary
    print("-" * 100)
    print()
    print("=" * 100)
    print("SUMMARY")
    print("=" * 100)
    print(f"Total frames compared: {len(all_frames)}")
    print(f"  Matching:            {stats['match']}")
    print(f"  CMR ahead:           {stats['cmr_ahead']}")
    print(f"  Batch ahead:         {stats['batch_ahead']}")
    print(f"  Only in CMR audit:   {stats['only_in_cmr']}")
    print(f"  Only in batch proc:  {stats['only_in_batch']}")
    print()

    if stats['cmr_ahead'] > 0 or stats['batch_ahead'] > 0:
        print("INTERPRETATION:")
        if stats['cmr_ahead'] > 0:
            print(f"  - {stats['cmr_ahead']} frame(s) have MORE products in CMR than batch proc expects.")
            print("    This means the batch proc is behind and should be updated to continue from where CMR left off.")
        if stats['batch_ahead'] > 0:
            print(f"  - {stats['batch_ahead']} frame(s) have FEWER products in CMR than batch proc expects.")
            print("    This could mean jobs were submitted but products weren't published to CMR,")
            print("    or the batch proc was manually advanced.")
        print()
        print("To update batch proc with CMR frame states, copy the 'frame_states' from the CMR audit output.")

    print("=" * 100)

    return stats


def main():
    parser = argparse.ArgumentParser(
        description='Compare frame states between CMR audit output and batch proc JSON files.',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python compare_disp_s1_frame_states.py frame_states_output.json batch_proc.json

The script compares:
    - frame_states from CMR audit (what products exist in CMR)
    - frame_states from batch proc (what the batch processor thinks has been processed)

A positive difference means CMR has more products than batch proc expects.
A negative difference means batch proc is ahead of what's in CMR.
        """
    )
    parser.add_argument('cmr_audit_json', help='Path to CMR audit output JSON (from --output-frame-states)')
    parser.add_argument('batch_proc_json', help='Path to batch proc JSON file')

    args = parser.parse_args()

    try:
        compare_frame_states(args.cmr_audit_json, args.batch_proc_json)
    except FileNotFoundError as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Error parsing JSON: {e}", file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
