#!/usr/bin/env python3
import sys
import re
from collections import defaultdict
from datetime import datetime

# ANSI Colors
RED     = "\033[91m"
GREEN   = "\033[92m"
YELLOW  = "\033[93m"
CYAN    = "\033[96m"
BOLD    = "\033[1m"
RESET   = "\033[0m"


def parse_revision_time(line):
    """Extract Revision Time from 2nd column (YYYY-MM-DDTHH:MM:SS.sssZ)."""
    try:
        parts = [p.strip() for p in line.split(",")]
        revision_time_str = parts[1].split(".")[0]  # remove milliseconds
        return datetime.strptime(revision_time_str, "%Y-%m-%dT%H:%M:%S")
    except Exception:
        return None


def extract_granule_start_ts(granule_id):
    """Extract start timestamp from granule_id (YYYYMMDDTHHMMSSZ)."""
    m = re.search(r"_([0-9]{8}T[0-9]{6}Z)_", granule_id)
    if m:
        return datetime.strptime(m.group(1), "%Y%m%dT%H%M%SZ")
    return None


def clean_and_report(input_file, report_file, cleaned_file, early_s1cd_file):
    duplicate_key_regex = re.compile(
        r"(OPERA_L2_CSLC-S1_[A-Z0-9-]+_\d{8}T\d{4})"
    )

    pattern_map = defaultdict(list)
    early_entries = []
    all_lines = []

    s1c_cutoff = datetime(2025, 5, 20)
    s1d_cutoff = datetime(2026, 6, 13)

    print(f"{CYAN}Reading file: {input_file}{RESET}")

    with open(input_file, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            all_lines.append(line)
            granule_id = line.split(",")[0]

            # --- Duplicate detection key ---
            match = duplicate_key_regex.match(granule_id)
            if match:
                pattern = match.group(1)
                revision_dt = parse_revision_time(line)
                pattern_map[pattern].append((line, revision_dt))
            else:
                print(f"{YELLOW}Warning: No pattern matched for{RESET} {granule_id}")

            # --- Early S1C detection ---
            if "_S1C_" in granule_id:
                start_ts = extract_granule_start_ts(granule_id)
                if start_ts and start_ts < s1c_cutoff:
                    early_entries.append(line)

            # --- Early S1D detection ---
            if "_S1D_" in granule_id:
                start_ts = extract_granule_start_ts(granule_id)
                if start_ts and start_ts < s1d_cutoff:
                    early_entries.append(line)

    # --- Determine duplicates to remove (keep latest Revision Time) ---
    duplicates_to_remove = set()
    for pattern, entries in pattern_map.items():
        if len(entries) > 1:
            # Sort by Revision Time descending → keep latest
            entries_sorted = sorted(entries, key=lambda x: x[1] or datetime.min, reverse=True)
            to_keep = entries_sorted[0][0]
            for e, _ in entries_sorted[1:]:
                duplicates_to_remove.add(e)

    # Combine with early S1C entries
    lines_to_remove = duplicates_to_remove.union(early_entries)

    # --- Write cleaned file ---
    cleaned_lines = [line for line in all_lines if line not in lines_to_remove]
    with open(cleaned_file, "w") as f:
        for line in cleaned_lines:
            f.write(line + "\n")

    # --- Write early S1CD file ---
    if early_entries:
        with open(early_s1cd_file, "w") as f:
            for line in early_entries:
                f.write(line + "\n")
        print(f"{CYAN}Early S1CD entries written to: {early_s1cd_file}{RESET}")

    # --- Write report file ---
    duplicate_groups = 0
    total_duplicates_removed = 0

    with open(report_file, "w") as out:
        out.write("# Duplicate and Early S1CD Report\n\n")

        for pattern, entries in pattern_map.items():
            if len(entries) > 1:
                duplicate_groups += 1
                entries_sorted = sorted(entries, key=lambda x: x[1] or datetime.min, reverse=True)
                out.write(f"\n# Duplicate Pattern: {pattern}\n")
                out.write(f"# Kept (latest Revision Time): {entries_sorted[0][0]}\n")
                for e, _ in entries_sorted[1:]:
                    out.write(e + "\n")
                    total_duplicates_removed += 1

        if early_entries:
            out.write("\n# Early S1C/D Data Detected (before 2025-05-20 (C) 2026-06-13 (D))\n")
            for e in early_entries:
                out.write(e + "\n")

    # --- Console summary ---
    print(f"\n{CYAN}{BOLD}=== Summary ==={RESET}")
    print(f"{RED}Duplicate groups: {duplicate_groups}{RESET}")
    print(f"{RED}Total duplicates removed (earlier Revision Time): {total_duplicates_removed}{RESET}")
    print(f"{RED}Early S1C/D entries removed: {len(early_entries)}{RESET}")
    print(f"{CYAN}Cleaned file written to: {cleaned_file}{RESET}")
    print(f"{CYAN}Report file written to: {report_file}{RESET}")


if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: python clean_keep_latest_revision_early_s1c_save.py <input_file> <report_file> <cleaned_file> <early_s1cd_file>")
        sys.exit(1)

    clean_and_report(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
  
