#!/usr/bin/env python3
"""
Analyze DISP-S1 RunConfig files to extract regular CSLC acquisition dates and 
Compressed CSLC reference dates, then identify any overlaps that might cause 
the SAS to miscount the number of images.

Based on official OPERA filename regex patterns:
- Regular CSLC: OPERA_L2_CSLC-S1_<burst_id>_<acquisition_ts>_<creation_ts>_...
- Compressed CSLC: OPERA_L2_COMPRESSED-CSLC-S1_<frame_id>_<burst_id>_<ref_date_time>_<first_date_time>_<last_date_time>_<creation_ts>_...
"""

import sys
import yaml
import re
from pathlib import Path
from collections import defaultdict
from datetime import datetime


def parse_cslc_filename(filename):
    """Parse a regular CSLC filename to extract acquisition date and burst ID."""
    # OPERA_L2_CSLC-S1_T124-264517-IW1_20170829T043943Z_20240428T171455Z_S1B_VV_v1.1.h5
    # Format: OPERA_L2_CSLC-S1_T{burst-id}_{acq_date}T{time}_{proc_date}T{time}_{sat}_{pol}_v{ver}.h5
    # The burst ID format is T###-######-IW# so we match that specifically
    match = re.search(r'OPERA_L2_CSLC-S1_(T\d{3}-\d{6}-IW\d)_(\d{8})T', filename)
    if match:
        return {
            'acquisition_date': match.group(2),  # The acquisition date
            'burst_id': match.group(1)           # The burst ID
        }
    return None


def parse_compressed_cslc_filename(filename):
    """
    Parse a compressed CSLC filename to extract reference date, frame ID, and burst ID.
    
    Format per official regex:
    OPERA_L2_COMPRESSED-CSLC-S1_F#####_BURST_<ref_date_time>_<first_date_time>_<last_date_time>_<creation_ts>_POL_VERSION
    
    Example:
    OPERA_L2_COMPRESSED-CSLC-S1_F08882_T034-071055-IW1_20200110T000000Z_20190702T000000Z_20200110T000000Z_20251025T114442Z_VV_v1.0.h5
                                ↑ frame      ↑ burst         ↑ ref_date      ↑ first_date    ↑ last_date     ↑ creation
    """
    # Match frame ID, burst ID, and all 4 date positions
    match = re.search(r'COMPRESSED-CSLC-S1_(F\d+)_(T[\w-]+)_(\d{8})T\d+Z_(\d{8})T\d+Z_(\d{8})T\d+Z_(\d{8})T', filename)
    if match:
        return {
            'frame_id': match.group(1),          # Frame ID (e.g., F08882)
            'burst_id': match.group(2),          # Burst ID (e.g., T034-071055-IW1)
            'reference_date': match.group(3),    # ref_date_time (the phase reference date)
            'first_date': match.group(4),        # first_date_time (start of acquisition range)
            'last_date': match.group(5),         # last_date_time (end of acquisition range)
            'creation_date': match.group(6)      # creation_ts (production date)
        }
    return None


def analyze_runconfig(runconfig_path):
    """Analyze a RunConfig file for CSLC date overlaps."""
    
    print(f"\n{'='*80}")
    print(f"Analyzing: {runconfig_path}")
    print(f"{'='*80}\n")
    
    with open(runconfig_path, 'r') as f:
        config = yaml.safe_load(f)
    
    # Get the cslc_file_list - try both PGE and SAS sections
    cslc_files = None
    try:
        # Try SAS section first (RunConfig_sas.yaml)
        cslc_files = config['input_file_group']['cslc_file_list']
    except (KeyError, TypeError):
        try:
            # Try PGE section (RunConfig.yaml)
            cslc_files = config['RunConfig']['Groups']['PGE']['InputFilesGroup']['InputFilePaths']
        except (KeyError, TypeError):
            try:
                # Try SAS section in full RunConfig
                cslc_files = config['RunConfig']['Groups']['SAS']['input_file_group']['cslc_file_list']
            except (KeyError, TypeError):
                print("ERROR: Could not find CSLC file list in config")
                print("Tried paths:")
                print("  - input_file_group/cslc_file_list")
                print("  - RunConfig/Groups/PGE/InputFilesGroup/InputFilePaths")
                print("  - RunConfig/Groups/SAS/input_file_group/cslc_file_list")
                return
    
    regular_cslc_dates = set()
    compressed_cslc_info = []
    burst_ids = set()
    frame_ids = set()
    
    # Debug: check total files
    print(f"DEBUG: Processing {len(cslc_files)} total files from config")
    
    regular_count = 0
    compressed_count = 0
    skipped_count = 0
    
    # Process each file
    for filepath in cslc_files:
        filename = Path(filepath).name
        
        if 'COMPRESSED-CSLC-S1' in filename:
            # This is a compressed CSLC
            compressed_count += 1
            info = parse_compressed_cslc_filename(filename)
            if info:
                compressed_cslc_info.append(info)
                burst_ids.add(info['burst_id'])
                frame_ids.add(info['frame_id'])
        elif 'CSLC-S1_T' in filename and 'STATIC' not in filename:
            # This is a regular CSLC
            regular_count += 1
            info = parse_cslc_filename(filename)
            if info:
                regular_cslc_dates.add(info['acquisition_date'])
                burst_ids.add(info['burst_id'])
            else:
                print(f"DEBUG: Failed to parse date from: {filename}")
        else:
            skipped_count += 1
            if skipped_count <= 3:  # Only show first few
                print(f"DEBUG: Skipped (not regular CSLC): {filename[:80]}")
    
    print(f"DEBUG: Found {regular_count} regular CSLCs, {compressed_count} compressed CSLCs, {skipped_count} skipped")
    print()
    
    # Print frame and burst information
    print(f"{'='*80}")
    print("FRAME AND BURST INFORMATION:")
    print(f"{'='*80}")
    if frame_ids:
        frame_list = sorted(frame_ids)
        print(f"Frame ID(s): {', '.join(frame_list)}")
        if len(frame_list) > 1:
            print(f"  ⚠️  WARNING: Multiple frame IDs found!")
    else:
        print("Frame ID: Not found (no compressed CSLCs)")
    print(f"Number of unique bursts: {len(burst_ids)}")
    if burst_ids:
        burst_list = sorted(burst_ids)
        print(f"Burst IDs:")
        for i, burst in enumerate(burst_list, 1):
            print(f"  {i:2d}. {burst}")
    print()
    
    # Sort dates for display
    regular_dates_sorted = sorted(regular_cslc_dates)
    
    # Get unique compressed CSLC reference dates
    compressed_ref_dates = set()
    for info in compressed_cslc_info:
        compressed_ref_dates.add(info['reference_date'])
    compressed_ref_dates_sorted = sorted(compressed_ref_dates)
    
    # Print results
    print(f"Regular CSLC Acquisition Dates ({len(regular_dates_sorted)} unique):")
    print("-" * 80)
    for i, date in enumerate(regular_dates_sorted, 1):
        date_obj = datetime.strptime(date, '%Y%m%d')
        print(f"  {i:2d}. {date} ({date_obj.strftime('%Y-%m-%d')})")
    
    print(f"\nCompressed CSLC Reference Dates ({len(compressed_ref_dates_sorted)} unique):")
    print("-" * 80)
    if compressed_cslc_info:
        for i, ref_date in enumerate(compressed_ref_dates_sorted, 1):
            # Find an example with this reference date
            example = next(info for info in compressed_cslc_info if info['reference_date'] == ref_date)
            ref_obj = datetime.strptime(ref_date, '%Y%m%d')
            first_obj = datetime.strptime(example['first_date'], '%Y%m%d')
            last_obj = datetime.strptime(example['last_date'], '%Y%m%d')
            print(f"  {i:2d}. {ref_date} ({ref_obj.strftime('%Y-%m-%d')})")
            print(f"      Acquisition Range: {example['first_date']} to {example['last_date']}")
            print(f"                        ({first_obj.strftime('%Y-%m-%d')} to {last_obj.strftime('%Y-%m-%d')})")
    else:
        print("  (None found)")
    
    print(f"\nCompressed CSLC Files per Reference Date:")
    print("-" * 80)
    ref_date_counts = defaultdict(int)
    for info in compressed_cslc_info:
        ref_date_counts[info['reference_date']] += 1
    for ref_date in compressed_ref_dates_sorted:
        print(f"  {ref_date}: {ref_date_counts[ref_date]} files")
    
    # Check for overlaps
    overlaps = regular_cslc_dates.intersection(compressed_ref_dates)
    
    print(f"\n{'='*80}")
    print("OVERLAP ANALYSIS:")
    print(f"{'='*80}")
    
    if overlaps:
        overlaps_sorted = sorted(overlaps)
        print(f"\n⚠️  FOUND {len(overlaps)} DATE OVERLAP(S)! ⚠️\n")
        
        for overlap_date in overlaps_sorted:
            date_obj = datetime.strptime(overlap_date, '%Y%m%d')
            print(f"  Date {overlap_date} ({date_obj.strftime('%Y-%m-%d')}) appears in:")
            print(f"    ✓ Regular CSLC acquisition dates (cycle #{regular_dates_sorted.index(overlap_date) + 1} of {len(regular_dates_sorted)})")
            print(f"    ✓ Compressed CSLC reference date ({ref_date_counts[overlap_date]} compressed files)")
            print()
        
        print("This overlap may cause the SAS to miscount and report:")
        print(f"  'Processing {len(regular_dates_sorted) + len(overlaps)} SLCs + {len(compressed_cslc_info) - len(overlaps)} compressed SLCs'")
        print("instead of:")
        print(f"  'Processing {len(regular_dates_sorted)} SLCs + {len(compressed_cslc_info)} compressed SLCs'")
        
    else:
        print("\n✓ No overlaps found between regular CSLC dates and compressed CSLC reference dates")
    
    print(f"\n{'='*80}")
    print("SUMMARY:")
    print(f"{'='*80}")
    if frame_ids:
        print(f"  Frame ID(s):             {', '.join(sorted(frame_ids))}")
    print(f"  Number of bursts:        {len(burst_ids)}")
    print(f"  Regular CSLCs:           {len(regular_dates_sorted)} unique dates")
    print(f"  Compressed CSLCs:        {len(compressed_cslc_info)} files ({len(compressed_ref_dates_sorted)} unique ref dates)")
    print(f"  Total files in config:   {len(cslc_files)}")
    print(f"  Date overlaps:           {len(overlaps)}")
    
    if overlaps:
        print(f"\n  Expected SAS report:     'Processing {len(regular_dates_sorted)} SLCs + {len(compressed_cslc_info)} compressed SLCs'")
        print(f"  Actual SAS report:       'Processing {len(regular_dates_sorted) + len(overlaps)} SLCs + {len(compressed_cslc_info) - len(overlaps) * ref_date_counts[list(overlaps)[0]]} compressed SLCs' (INCORRECT)")
    
    print(f"{'='*80}\n")


def main():
    if len(sys.argv) < 2:
        print("Usage: python analyze_cslc_dates_from_disp_s1_runconfig.py <RunConfig.yaml> [<RunConfig2.yaml> ...]")
        print("\nExample:")
        print("  python analyze_cslc_dates_from_disp_s1_runconfig.py RunConfig_sas.yaml")
        print("  python analyze_cslc_dates_from_disp_s1_runconfig.py another_example/RunConfig_sas.yaml")
        sys.exit(1)
    
    for runconfig_path in sys.argv[1:]:
        if not Path(runconfig_path).exists():
            print(f"ERROR: File not found: {runconfig_path}")
            continue
        
        try:
            analyze_runconfig(runconfig_path)
        except Exception as e:
            print(f"ERROR processing {runconfig_path}: {e}")
            import traceback
            traceback.print_exc()


if __name__ == '__main__':
    main()

