#!/usr/bin/env python3
"""
Analyze DISP-S1 frame completeness for k-cycle processing.

Cross-references the consistent burst database with CSLC coverage audit results
to identify frames with incomplete sensing times that block DISP-S1 processing.

Usage:
    python disp_frame_completeness.py <consistent_db> <jsonl_file> [options]

Examples:
    # Check all frames with default k=15
    python disp_frame_completeness.py consistent-db.json coverage.jsonl

    # Check specific frame with k=10
    python disp_frame_completeness.py consistent-db.json coverage.jsonl --frame 4596 --k 10

    # Show only incomplete frames
    python disp_frame_completeness.py consistent-db.json coverage.jsonl --incomplete-only
"""

import argparse
import json
import sys
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path


def load_consistent_database(db_path):
    """
    Load consistent burst database.

    Returns dict: frame_id -> {
        'burst_ids': set of burst IDs,
        'sensing_times': list of datetime objects (sorted)
    }
    """
    frames = {}

    with open(db_path) as f:
        data = json.load(f).get("data", {})

    for frame_id, frame_data in data.items():
        # Get burst IDs - support both field names
        burst_ids = frame_data.get("burst_id_list") or frame_data.get("burst_ids", [])
        # Normalize to uppercase with underscores (matching JSONL format)
        burst_ids = {bid.upper().replace("-", "_") for bid in burst_ids}

        # Get sensing times
        sensing_times = []
        for st in frame_data.get("sensing_time_list", []):
            try:
                # Parse ISO format datetime
                if isinstance(st, str):
                    # Handle various formats
                    st = st.replace("Z", "+00:00")
                    if "+" not in st and len(st) == 19:
                        st = st + "+00:00"
                    dt = datetime.fromisoformat(st)
                    sensing_times.append(dt)
            except Exception:
                continue

        sensing_times.sort()

        frames[int(frame_id)] = {
            'burst_ids': burst_ids,
            'sensing_times': sensing_times,
        }

    return frames


def load_jsonl_coverage(jsonl_path):
    """
    Load JSONL coverage audit and build lookup structures.

    Returns:
        found_bursts: set of (burst_id, date_str) tuples that have CSLCs
        missing_bursts: dict mapping (burst_id, date_str) -> slc_native_id
    """
    found_bursts = set()
    missing_bursts = {}  # (burst_id, date) -> slc_native_id

    with open(jsonl_path) as f:
        for line in f:
            record = json.loads(line)
            if record.get("_type") != "chunk_result":
                continue

            # Process found bursts
            for burst in record.get("found", []):
                burst_id = burst.get("burst_id", "").upper().replace("-", "_")
                acq_time = burst.get("acquisition_time", "")[:10]  # YYYY-MM-DD
                if burst_id and acq_time:
                    found_bursts.add((burst_id, acq_time))

            # Process missing bursts - track SLC ID
            for burst in record.get("missing", []):
                burst_id = burst.get("burst_id", "").upper().replace("-", "_")
                acq_time = burst.get("acquisition_time", "")[:10]
                slc_id = burst.get("slc_native_id", "")
                if burst_id and acq_time:
                    missing_bursts[(burst_id, acq_time)] = slc_id

    return found_bursts, missing_bursts


def check_sensing_time_completeness(frame_id, frame_data, found_bursts, missing_bursts):
    """
    Check completeness of each sensing time for a frame.

    Returns list of dicts with sensing time status:
    {
        'sensing_time': datetime,
        'date': str (YYYY-MM-DD),
        'total_bursts': int,
        'found_bursts': int,
        'missing_bursts': int,
        'unknown_bursts': int (not in audit data),
        'complete': bool,
        'missing_burst_ids': list,
        'missing_slc_ids': set of SLC IDs that need reprocessing
    }
    """
    results = []
    burst_ids = frame_data['burst_ids']

    for sensing_time in frame_data['sensing_times']:
        date_str = sensing_time.strftime('%Y-%m-%d')

        found = 0
        missing = 0
        unknown = 0
        missing_burst_ids = []
        missing_slc_ids = set()

        for burst_id in burst_ids:
            key = (burst_id, date_str)
            if key in found_bursts:
                found += 1
            elif key in missing_bursts:
                missing += 1
                missing_burst_ids.append(burst_id)
                slc_id = missing_bursts[key]
                if slc_id:
                    missing_slc_ids.add(slc_id)
            else:
                # Burst not in audit data - could be outside audit date range
                unknown += 1

        results.append({
            'sensing_time': sensing_time,
            'date': date_str,
            'total_bursts': len(burst_ids),
            'found_bursts': found,
            'missing_bursts': missing,
            'unknown_bursts': unknown,
            'complete': (found == len(burst_ids)),
            'missing_burst_ids': missing_burst_ids,
            'missing_slc_ids': missing_slc_ids,
        })

    return results


def find_processable_k_cycles(sensing_results, k):
    """
    Find which k-cycles can be processed (have k consecutive complete sensing times).

    Returns list of dicts:
    {
        'cycle_index': int (0-based index of the k-cycle),
        'start_sensing_time': datetime,
        'end_sensing_time': datetime,
        'complete': bool,
        'complete_count': int (how many of k are complete),
        'incomplete_indices': list of indices within the cycle that are incomplete,
        'missing_slc_ids': set of SLC IDs needed to complete this cycle
    }
    """
    cycles = []
    num_sensing_times = len(sensing_results)

    # Calculate number of complete k-cycles
    num_cycles = num_sensing_times // k

    for cycle_idx in range(num_cycles):
        start_idx = cycle_idx * k
        end_idx = start_idx + k

        cycle_results = sensing_results[start_idx:end_idx]
        complete_count = sum(1 for r in cycle_results if r['complete'])
        incomplete_indices = [i for i, r in enumerate(cycle_results) if not r['complete']]

        # Collect all SLC IDs needed for this cycle
        missing_slc_ids = set()
        for r in cycle_results:
            missing_slc_ids.update(r.get('missing_slc_ids', set()))

        cycles.append({
            'cycle_index': cycle_idx,
            'start_sensing_time': cycle_results[0]['sensing_time'],
            'end_sensing_time': cycle_results[-1]['sensing_time'],
            'complete': (complete_count == k),
            'complete_count': complete_count,
            'incomplete_indices': incomplete_indices,
            'missing_slc_ids': missing_slc_ids,
        })

    # Handle remaining sensing times (partial cycle)
    remaining = num_sensing_times % k
    if remaining > 0:
        start_idx = num_cycles * k
        cycle_results = sensing_results[start_idx:]
        complete_count = sum(1 for r in cycle_results if r['complete'])
        incomplete_indices = [i for i, r in enumerate(cycle_results) if not r['complete']]

        # Collect SLC IDs for partial cycle
        missing_slc_ids = set()
        for r in cycle_results:
            missing_slc_ids.update(r.get('missing_slc_ids', set()))

        cycles.append({
            'cycle_index': num_cycles,
            'start_sensing_time': cycle_results[0]['sensing_time'],
            'end_sensing_time': cycle_results[-1]['sensing_time'],
            'complete': False,  # Partial cycle is never complete
            'complete_count': complete_count,
            'incomplete_indices': incomplete_indices,
            'missing_slc_ids': missing_slc_ids,
            'partial': True,
            'partial_size': remaining,
        })

    return cycles


def analyze_frame(frame_id, frame_data, found_bursts, missing_bursts, k):
    """Analyze a single frame's completeness."""
    sensing_results = check_sensing_time_completeness(
        frame_id, frame_data, found_bursts, missing_bursts
    )

    k_cycles = find_processable_k_cycles(sensing_results, k)

    # Calculate summary stats
    total_sensing_times = len(sensing_results)
    complete_sensing_times = sum(1 for r in sensing_results if r['complete'])
    processable_cycles = sum(1 for c in k_cycles if c['complete'] and not c.get('partial'))

    return {
        'frame_id': frame_id,
        'num_bursts': len(frame_data['burst_ids']),
        'total_sensing_times': total_sensing_times,
        'complete_sensing_times': complete_sensing_times,
        'incomplete_sensing_times': total_sensing_times - complete_sensing_times,
        'processable_k_cycles': processable_cycles,
        'total_k_cycles': len([c for c in k_cycles if not c.get('partial')]),
        'sensing_results': sensing_results,
        'k_cycles': k_cycles,
    }


def print_frame_report(analysis, k, verbose=False, show_slcs=False):
    """Print detailed report for a frame."""
    frame_id = analysis['frame_id']

    print("=" * 100)
    print(f"Frame {frame_id}")
    print("=" * 100)
    print(f"Bursts in frame: {analysis['num_bursts']}")
    print(f"Sensing times: {analysis['complete_sensing_times']}/{analysis['total_sensing_times']} complete")
    print(f"K-cycles (k={k}): {analysis['processable_k_cycles']}/{analysis['total_k_cycles']} processable")
    print()

    # Collect all SLCs needed across incomplete cycles
    all_missing_slcs = set()
    for cycle in analysis['k_cycles']:
        if not cycle['complete'] and not cycle.get('partial'):
            all_missing_slcs.update(cycle.get('missing_slc_ids', set()))

    if all_missing_slcs:
        print(f"SLCs needed for reprocessing: {len(all_missing_slcs)} unique")
    print()

    # Show k-cycle summary
    print(f"K-cycle status:")
    print("-" * 100)
    for cycle in analysis['k_cycles']:
        idx = cycle['cycle_index']
        start = cycle['start_sensing_time'].strftime('%Y-%m-%d')
        end = cycle['end_sensing_time'].strftime('%Y-%m-%d')
        num_slcs = len(cycle.get('missing_slc_ids', set()))

        if cycle.get('partial'):
            status = f"PARTIAL ({cycle['partial_size']}/{k})"
            complete_str = f"{cycle['complete_count']}/{cycle['partial_size']}"
        else:
            status = "✓ COMPLETE" if cycle['complete'] else f"✗ INCOMPLETE ({num_slcs} SLCs needed)"
            complete_str = f"{cycle['complete_count']}/{k}"

        print(f"  Cycle {idx}: {start} to {end} - {complete_str} complete - {status}")
    print()

    # Show SLCs needed for each incomplete cycle
    if show_slcs:
        incomplete_cycles = [c for c in analysis['k_cycles']
                           if not c['complete'] and not c.get('partial') and c.get('missing_slc_ids')]
        if incomplete_cycles:
            print("SLCs needed per incomplete k-cycle:")
            print("-" * 100)
            for cycle in incomplete_cycles:
                idx = cycle['cycle_index']
                start = cycle['start_sensing_time'].strftime('%Y-%m-%d')
                end = cycle['end_sensing_time'].strftime('%Y-%m-%d')
                slcs = sorted(cycle['missing_slc_ids'])
                print(f"  Cycle {idx} ({start} to {end}): {len(slcs)} SLCs")
                for slc in slcs[:10]:  # Limit to first 10
                    print(f"    {slc}")
                if len(slcs) > 10:
                    print(f"    ... and {len(slcs) - 10} more")
            print()

        # Show all unique SLCs
        if all_missing_slcs:
            print(f"All unique SLCs needed ({len(all_missing_slcs)}):")
            print("-" * 100)
            for slc in sorted(all_missing_slcs)[:30]:
                print(f"  {slc}")
            if len(all_missing_slcs) > 30:
                print(f"  ... and {len(all_missing_slcs) - 30} more")
            print()

    if verbose:
        # Show incomplete sensing times
        incomplete = [r for r in analysis['sensing_results'] if not r['complete']]
        if incomplete:
            print(f"Incomplete sensing times ({len(incomplete)}):")
            print("-" * 100)
            for r in incomplete[:20]:  # Limit output
                missing_count = r['missing_bursts']
                unknown_count = r['unknown_bursts']
                status_parts = []
                if missing_count > 0:
                    status_parts.append(f"{missing_count} missing")
                if unknown_count > 0:
                    status_parts.append(f"{unknown_count} not in audit")
                status = ", ".join(status_parts)
                print(f"  {r['date']}: {r['found_bursts']}/{r['total_bursts']} found ({status})")
                if r['missing_burst_ids'] and len(r['missing_burst_ids']) <= 5:
                    print(f"    Missing: {', '.join(r['missing_burst_ids'])}")
                if r.get('missing_slc_ids'):
                    print(f"    SLCs: {', '.join(sorted(r['missing_slc_ids']))}")

            if len(incomplete) > 20:
                print(f"  ... and {len(incomplete) - 20} more")
            print()


def main():
    parser = argparse.ArgumentParser(
        description="Analyze DISP-S1 frame completeness for k-cycle processing",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s consistent-db.json coverage.jsonl
  %(prog)s consistent-db.json coverage.jsonl --frame 4596 --k 10
  %(prog)s consistent-db.json coverage.jsonl --incomplete-only --summary
  %(prog)s consistent-db.json coverage.jsonl --frame 10859 --show-slcs
  %(prog)s consistent-db.json coverage.jsonl --output-slcs slcs_to_reprocess.txt
        """,
    )
    parser.add_argument("consistent_db", help="Path to consistent burst database JSON")
    parser.add_argument("jsonl_file", help="JSONL file from cmr_audit_burst_coverage.py")
    parser.add_argument("--frame", type=int, action="append", dest="frames",
                        help="Specific frame ID(s) to analyze (can specify multiple)")
    parser.add_argument("--k", type=int, default=15,
                        help="K-cycle size for DISP-S1 processing (default: 15)")
    parser.add_argument("--incomplete-only", action="store_true",
                        help="Only show frames with incomplete k-cycles")
    parser.add_argument("--summary", action="store_true",
                        help="Show summary only, not detailed per-frame reports")
    parser.add_argument("--verbose", "-v", action="store_true",
                        help="Show detailed incomplete sensing times")
    parser.add_argument("--show-slcs", action="store_true",
                        help="Show SLCs that need reprocessing for each incomplete k-cycle")
    parser.add_argument("--json", action="store_true",
                        help="Output results as JSON")
    parser.add_argument("--output-missing", type=Path, metavar="FILE",
                        help="Write list of missing burst/date combinations to CSV")
    parser.add_argument("--output-slcs", type=Path, metavar="FILE",
                        help="Write list of SLCs to reprocess (one per line)")
    args = parser.parse_args()

    # Load data
    print(f"Loading consistent database: {args.consistent_db}", file=sys.stderr)
    frames = load_consistent_database(args.consistent_db)
    print(f"  Loaded {len(frames)} frames", file=sys.stderr)

    print(f"Loading JSONL coverage: {args.jsonl_file}", file=sys.stderr)
    found_bursts, missing_bursts = load_jsonl_coverage(args.jsonl_file)
    print(f"  Found: {len(found_bursts):,} burst/date combinations", file=sys.stderr)
    print(f"  Missing: {len(missing_bursts):,} burst/date combinations", file=sys.stderr)
    print(file=sys.stderr)

    # Filter frames if specific ones requested
    if args.frames:
        frames = {fid: data for fid, data in frames.items() if fid in args.frames}
        if not frames:
            print(f"No matching frames found for: {args.frames}", file=sys.stderr)
            sys.exit(1)

    # Analyze frames
    results = []
    all_missing = []  # Collect all missing burst/date for output
    all_slcs_needed = set()  # Collect all SLCs needed for reprocessing

    for frame_id in sorted(frames.keys()):
        analysis = analyze_frame(
            frame_id, frames[frame_id], found_bursts, missing_bursts, args.k
        )

        # Collect missing bursts and SLCs
        for sr in analysis['sensing_results']:
            if sr['missing_bursts'] > 0:
                for burst_id in sr['missing_burst_ids']:
                    all_missing.append({
                        'frame_id': frame_id,
                        'burst_id': burst_id,
                        'date': sr['date'],
                    })
            all_slcs_needed.update(sr.get('missing_slc_ids', set()))

        # Filter if incomplete-only
        if args.incomplete_only:
            if analysis['processable_k_cycles'] == analysis['total_k_cycles']:
                continue

        results.append(analysis)

    # Output
    if args.json:
        # JSON output - convert datetimes and sets to strings/lists
        json_results = []
        for r in results:
            jr = {**r}
            jr['sensing_results'] = [
                {**sr,
                 'sensing_time': sr['sensing_time'].isoformat(),
                 'missing_slc_ids': sorted(sr.get('missing_slc_ids', set()))}
                for sr in r['sensing_results']
            ]
            jr['k_cycles'] = [
                {**c,
                 'start_sensing_time': c['start_sensing_time'].isoformat(),
                 'end_sensing_time': c['end_sensing_time'].isoformat(),
                 'missing_slc_ids': sorted(c.get('missing_slc_ids', set()))}
                for c in r['k_cycles']
            ]
            json_results.append(jr)
        print(json.dumps(json_results, indent=2))

    elif args.summary:
        # Summary output
        print("=" * 100)
        print(f"DISP-S1 Frame Completeness Summary (k={args.k})")
        print("=" * 100)
        print()
        print(f"{'Frame':<10} {'Bursts':<8} {'Sensing Times':<20} {'K-cycles':<20} {'Status'}")
        print("-" * 100)

        total_processable = 0
        total_cycles = 0

        for r in results:
            sensing = f"{r['complete_sensing_times']}/{r['total_sensing_times']}"
            cycles = f"{r['processable_k_cycles']}/{r['total_k_cycles']}"
            status = "✓ OK" if r['processable_k_cycles'] == r['total_k_cycles'] else "✗ INCOMPLETE"
            print(f"{r['frame_id']:<10} {r['num_bursts']:<8} {sensing:<20} {cycles:<20} {status}")

            total_processable += r['processable_k_cycles']
            total_cycles += r['total_k_cycles']

        print("-" * 100)
        print(f"Total: {len(results)} frames, {total_processable}/{total_cycles} k-cycles processable")
        print()

    else:
        # Detailed output
        for r in results:
            print_frame_report(r, args.k, args.verbose, args.show_slcs)

    # Write missing bursts to file if requested
    if args.output_missing and all_missing:
        with open(args.output_missing, 'w') as f:
            f.write("frame_id,burst_id,date\n")
            for m in all_missing:
                f.write(f"{m['frame_id']},{m['burst_id']},{m['date']}\n")
        print(f"Wrote {len(all_missing)} missing burst/date combinations to {args.output_missing}",
              file=sys.stderr)

    # Write SLCs to file if requested
    if args.output_slcs and all_slcs_needed:
        with open(args.output_slcs, 'w') as f:
            for slc in sorted(all_slcs_needed):
                f.write(f"{slc}\n")
        print(f"Wrote {len(all_slcs_needed)} unique SLCs to {args.output_slcs}",
              file=sys.stderr)

    # Print summary of SLCs if any were found
    if all_slcs_needed and not args.json:
        print(f"\nTotal unique SLCs needing reprocessing: {len(all_slcs_needed)}", file=sys.stderr)


if __name__ == "__main__":
    main()
