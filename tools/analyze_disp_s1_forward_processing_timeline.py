#!/usr/bin/env python3
"""
Analyze DISP-S1 forward processing timeline from run configuration files.

This script:
1. Reads all run config files in a directory
2. Groups them by frame ID
3. For each frame, creates a timeline showing:
   - Job progression through time
   - CSLC coverage with k=15, m=6 ministack parameters
   - Where CCSLCs are created (save_compressed_slc = True)
   - Verification that forward processing is progressing correctly
   - Detection of date overlaps between regular CSLCs and compressed CSLC references

Usage:
    python analyze_disp_s1_forward_processing_timeline.py <run_configs_directory>

Examples:
    python analyze_disp_s1_forward_processing_timeline.py /path/to/run_configs
    python analyze_disp_s1_forward_processing_timeline.py .
"""

import sys
import yaml
import re
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Set, Tuple, Optional

try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    from matplotlib.patches import Rectangle
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    print("WARNING: matplotlib not available. Swimlane diagrams will not be generated.")
    print("Install with: pip install matplotlib")


class JobInfo:
    """Information about a single DISP-S1 job from a run config."""

    def __init__(self, config_path: Path):
        self.config_path = config_path
        self.frame_id: Optional[str] = None
        self.start_date: Optional[str] = None
        self.end_date: Optional[str] = None
        self.regular_cslc_dates: Set[str] = set()
        self.compressed_cslc_ref_dates: Set[str] = set()  # Reference dates from compressed CSLCs
        self.compressed_cslc_count: int = 0
        self.saves_ccslc: bool = False
        self.job_id: Optional[str] = None
        self.bursts: Set[str] = set()  # Set of burst IDs (e.g., "T034-071049-IW1")
        self.regular_cslc_count: int = 0  # Total number of regular CSLC files

    def parse_filename(self) -> bool:
        """Extract frame_id and date range from filename."""
        # Format: OPERA_L3_DISP-S1_IW_F08882_VV_20190527T002635Z_20190608T002635Z_v1.0_20251110T162302Z.rc.yaml
        match = re.search(r'F(\d+)_VV_(\d{8}T\d{6}Z)_(\d{8}T\d{6}Z)', self.config_path.name)
        if match:
            self.frame_id = match.group(1)
            self.start_date = match.group(2)
            self.end_date = match.group(3)
            self.job_id = f"F{self.frame_id}_{self.start_date}_{self.end_date}"
            return True
        return False

    def parse_config(self) -> bool:
        """Parse the run config YAML file."""
        try:
            with open(self.config_path, 'r') as f:
                config = yaml.safe_load(f)

            # Get CSLC file list
            try:
                cslc_files = config['RunConfig']['Groups']['PGE']['InputFilesGroup']['InputFilePaths']
            except (KeyError, TypeError):
                try:
                    cslc_files = config['RunConfig']['Groups']['SAS']['input_file_group']['cslc_file_list']
                except (KeyError, TypeError):
                    print(f"WARNING: Could not find CSLC file list in {self.config_path.name}")
                    return False

            # Parse CSLC files
            for filepath in cslc_files:
                filename = Path(filepath).name

                if 'COMPRESSED-CSLC-S1' in filename:
                    self.compressed_cslc_count += 1
                    # Extract burst ID and reference date from compressed CSLC
                    # Format: OPERA_L2_COMPRESSED-CSLC-S1_F08882_T034-071049-IW1_20170326T000000Z_...
                    match = re.search(r'F\d+_(T\d{3}-\d{6}-IW\d)_(\d{8})T', filename)
                    if match:
                        self.bursts.add(match.group(1))
                        self.compressed_cslc_ref_dates.add(match.group(2))
                elif 'CSLC-S1_T' in filename and 'STATIC' not in filename:
                    # Regular CSLC: extract acquisition date and burst ID
                    match = re.search(r'CSLC-S1_(T\d{3}-\d{6}-IW\d)_(\d{8})T', filename)
                    if match:
                        self.bursts.add(match.group(1))
                        self.regular_cslc_dates.add(match.group(2))
                        self.regular_cslc_count += 1

            # Get save_compressed_slc parameter
            try:
                self.saves_ccslc = config['RunConfig']['Groups']['SAS']['product_path_group']['save_compressed_slc']
            except (KeyError, TypeError):
                pass

            # Get frame_id from config to verify
            try:
                config_frame_id = str(config['RunConfig']['Groups']['SAS']['input_file_group']['frame_id'])
                # Compare as integers to handle zero-padding (e.g., "08882" vs "8882")
                if self.frame_id and int(self.frame_id) != int(config_frame_id):
                    print(f"WARNING: Frame ID mismatch in {self.config_path.name}: "
                          f"filename={self.frame_id}, config={config_frame_id}")
            except (KeyError, TypeError, ValueError):
                pass

            return True

        except Exception as e:
            print(f"ERROR parsing {self.config_path.name}: {e}")
            return False

    def get_date_range_str(self) -> str:
        """Get a formatted date range string."""
        if self.start_date and self.end_date:
            start = datetime.strptime(self.start_date[:8], '%Y%m%d').strftime('%Y-%m-%d')
            end = datetime.strptime(self.end_date[:8], '%Y%m%d').strftime('%Y-%m-%d')
            return f"{start} to {end}"
        return "Unknown"


def create_swimlane_diagram(frame_id: str, jobs: List[JobInfo], output_dir: Path):
    """
    Create a swimlane diagram showing CSLC dates and CCSLC reference dates for each job.

    Args:
        frame_id: The frame ID
        jobs: List of JobInfo objects (already sorted by start date)
        output_dir: Directory to save the diagram
    """
    if not HAS_MATPLOTLIB:
        print("  Skipping swimlane diagram (matplotlib not available)")
        return

    fig, ax = plt.subplots(figsize=(16, max(8, len(jobs) * 0.4)))

    # Convert dates to datetime objects for plotting
    all_dates = []
    for job in jobs:
        all_dates.extend([datetime.strptime(d, '%Y%m%d') for d in job.regular_cslc_dates])
        all_dates.extend([datetime.strptime(d, '%Y%m%d') for d in job.compressed_cslc_ref_dates])

    if not all_dates:
        print("  No dates to plot")
        return

    min_date = min(all_dates)
    max_date = max(all_dates)

    # Track what features we actually have data for
    has_regular = any(job.regular_cslc_dates for job in jobs)
    has_compressed = any(job.compressed_cslc_ref_dates for job in jobs)
    has_overlaps = any(job.regular_cslc_dates & job.compressed_cslc_ref_dates for job in jobs)
    has_ccslc_creation = any(job.saves_ccslc for job in jobs)

    # Plot each job as a horizontal lane
    for job_idx, job in enumerate(jobs):
        y_pos = len(jobs) - job_idx  # Reverse so job #1 is at top

        # Plot regular CSLC dates as blue circles
        if job.regular_cslc_dates:
            cslc_dates = sorted([datetime.strptime(d, '%Y%m%d') for d in job.regular_cslc_dates])
            ax.scatter(cslc_dates, [y_pos] * len(cslc_dates),
                      c='steelblue', s=30, alpha=0.7, marker='o',
                      label='_nolegend_', zorder=3)

        # Plot compressed CSLC reference dates as orange diamonds
        if job.compressed_cslc_ref_dates:
            ccslc_dates = sorted([datetime.strptime(d, '%Y%m%d') for d in job.compressed_cslc_ref_dates])
            ax.scatter(ccslc_dates, [y_pos] * len(ccslc_dates),
                      c='orange', s=50, alpha=0.7, marker='D',
                      label='_nolegend_', zorder=3)

        # Highlight overlaps between regular CSLC and compressed CSLC reference dates
        overlaps = job.regular_cslc_dates & job.compressed_cslc_ref_dates
        if overlaps:
            overlap_dates = sorted([datetime.strptime(d, '%Y%m%d') for d in overlaps])
            ax.scatter(overlap_dates, [y_pos] * len(overlap_dates),
                      c='red', s=100, alpha=0.9, marker='X', edgecolors='darkred', linewidths=2,
                      label='_nolegend_', zorder=5)

        # Highlight jobs that create CCSLCs with a green background
        if job.saves_ccslc:
            ax.axhspan(y_pos - 0.4, y_pos + 0.4, alpha=0.2, color='green', zorder=0)
            # Add a marker on the right side
            ax.text(1.01, y_pos, '✓ Creates CCSLC', transform=ax.get_yaxis_transform(),
                   fontsize=8, color='green', fontweight='bold', va='center')

    # Format axes
    ax.set_ylim(0.5, len(jobs) + 0.5)
    ax.set_yticks(range(1, len(jobs) + 1))
    ax.set_yticklabels([f"Job #{len(jobs) - i}" for i in range(len(jobs))], fontsize=8)
    ax.set_ylabel('Job Number', fontsize=10, fontweight='bold')

    ax.set_xlabel('Date', fontsize=10, fontweight='bold')
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax.xaxis.set_major_locator(mdates.MonthLocator(interval=max(1, (max_date - min_date).days // 365)))
    plt.xticks(rotation=45, ha='right')

    ax.grid(True, axis='x', alpha=0.3, linestyle='--')
    ax.set_axisbelow(True)

    # Create custom legend with all items (even if not present in data)
    from matplotlib.lines import Line2D
    from matplotlib.patches import Patch
    legend_elements = [
        Line2D([0], [0], marker='o', color='w', markerfacecolor='steelblue',
               markersize=8, alpha=0.7, label='Regular CSLC dates'),
        Line2D([0], [0], marker='D', color='w', markerfacecolor='orange',
               markersize=9, alpha=0.7, label='Compressed CSLC ref dates (input)'),
        Patch(facecolor='green', alpha=0.2, label='Job creates CCSLC (output)'),
        Line2D([0], [0], marker='X', color='w', markerfacecolor='red',
               markersize=10, markeredgecolor='darkred', markeredgewidth=2,
               alpha=0.9, label='DATE OVERLAP (Error!)'),
    ]

    # Add legend outside plot area on the right
    ax.legend(handles=legend_elements, loc='upper left', bbox_to_anchor=(1.02, 1),
             fontsize=9, framealpha=0.95, edgecolor='gray', title='Legend', title_fontsize=10)

    # Title
    ax.set_title(f'Frame F{frame_id} - DISP-S1 Forward Processing Timeline\n'
                f'CSLC Acquisition Dates and Compressed CSLC Reference Dates',
                fontsize=12, fontweight='bold', pad=20)

    # Adjust layout to prevent label cutoff and accommodate legend
    plt.tight_layout()

    # Save figure with bbox_inches='tight' to include the legend
    output_file = output_dir / f"frame_{frame_id}_timeline.png"
    plt.savefig(output_file, dpi=150, bbox_inches='tight', facecolor='white')
    plt.close()

    print(f"  Swimlane diagram saved to: {output_file}")


def analyze_frame_timeline(frame_id: str, jobs: List[JobInfo], output_dir: Path, k: int = 15, m: int = 6):
    """
    Analyze and display the timeline for a single frame.

    Args:
        frame_id: The frame ID
        jobs: List of JobInfo objects for this frame
        output_dir: Directory to save visualization outputs
        k: Number of images in a ministack (default 15)
        m: Ministack overlap (default 6)
    """
    # Sort jobs by start date
    jobs.sort(key=lambda j: j.start_date if j.start_date else '')

    print(f"\n{'='*100}")
    print(f"FRAME F{frame_id} - Forward Processing Timeline")
    print(f"{'='*100}")

    # Collect all unique bursts and dates across all jobs
    all_bursts = set()
    all_dates = set()
    for job in jobs:
        all_bursts.update(job.bursts)
        all_dates.update(job.regular_cslc_dates)
    all_dates_sorted = sorted(all_dates)

    print(f"\nFrame Composition:")
    print(f"  Number of Bursts: {len(all_bursts)}")
    if all_bursts:
        # Show first few burst IDs as examples
        burst_examples = sorted(all_bursts)[:5]
        print(f"  Example Bursts: {', '.join(burst_examples)}")
        if len(all_bursts) > 5:
            print(f"  ... and {len(all_bursts) - 5} more")

    print(f"\nMinistack Parameters: k={k} (acquisition dates per ministack), m={m} (overlap)")
    print(f"  Note: Each acquisition date has {len(all_bursts)} bursts processed in parallel")
    print(f"\nTotal Jobs: {len(jobs)}")
    print(f"Total Unique CSLC Dates Across All Jobs: {len(all_dates_sorted)}")

    if all_dates_sorted:
        first_date = datetime.strptime(all_dates_sorted[0], '%Y%m%d').strftime('%Y-%m-%d')
        last_date = datetime.strptime(all_dates_sorted[-1], '%Y%m%d').strftime('%Y-%m-%d')
        print(f"Date Range: {first_date} to {last_date}")

    # Display each job in timeline order
    print(f"\n{'-'*100}")
    print(f"Job Timeline (Chronological Order):")
    print(f"{'-'*100}")

    ccslc_jobs = []

    for i, job in enumerate(jobs, 1):
        print(f"\nJob #{i}: {job.get_date_range_str()}")
        print(f"  Config: {job.config_path.name}")
        print(f"  Regular CSLCs: {job.regular_cslc_count} files = {len(job.regular_cslc_dates)} dates × {len(job.bursts)} bursts")
        print(f"  Compressed CSLCs (input): {job.compressed_cslc_count} files ({len(job.compressed_cslc_ref_dates)} unique ref dates)")

        if job.saves_ccslc:
            print(f"  ✓ CREATES CCSLCs (save_compressed_slc = True)")
            ccslc_jobs.append(i)
        else:
            print(f"  - Does not create CCSLCs (save_compressed_slc = False)")

        # Show date coverage for this job
        if job.regular_cslc_dates:
            job_dates = sorted(job.regular_cslc_dates)
            first = datetime.strptime(job_dates[0], '%Y%m%d').strftime('%Y-%m-%d')
            last = datetime.strptime(job_dates[-1], '%Y%m%d').strftime('%Y-%m-%d')
            print(f"  CSLC Date Coverage: {first} to {last} ({len(job_dates)} dates)")

            # Calculate expected ministack info
            # With k=15 and m=6, each ministack processes k acquisition dates
            # Each date has len(job.bursts) bursts processed in parallel
            # New dates per ministack = k - m = 15 - 6 = 9
            n_dates = len(job_dates)
            n_bursts = len(job.bursts)
            if n_dates >= k:
                n_ministacks_per_burst = 1 + (n_dates - k) // (k - m) + (1 if (n_dates - k) % (k - m) > 0 else 0)
                print(f"  Expected Ministacks: ~{n_ministacks_per_burst} per burst × {n_bursts} bursts = ~{n_ministacks_per_burst * n_bursts} total")

    # Summary
    print(f"\n{'='*100}")
    print(f"SUMMARY FOR FRAME F{frame_id}")
    print(f"{'='*100}")
    print(f"Total Jobs: {len(jobs)}")
    print(f"Jobs Creating CCSLCs: {len(ccslc_jobs)}")
    if ccslc_jobs:
        print(f"  Job numbers: {', '.join(f'#{n}' for n in ccslc_jobs)}")
    print(f"Bursts in Frame: {len(all_bursts)}")
    print(f"Total Unique CSLC Dates: {len(all_dates_sorted)}")
    print(f"Total CSLC Products: {len(all_dates_sorted)} dates × {len(all_bursts)} bursts = {len(all_dates_sorted) * len(all_bursts)} expected files")

    # Count total overlaps
    total_overlaps = 0
    jobs_with_overlaps = []
    for i, job in enumerate(jobs, 1):
        overlaps = job.regular_cslc_dates & job.compressed_cslc_ref_dates
        if overlaps:
            total_overlaps += len(overlaps)
            jobs_with_overlaps.append(i)

    if total_overlaps > 0:
        print(f"\n⚠️  DATE OVERLAP WARNING:")
        print(f"  {len(jobs_with_overlaps)} job(s) with overlaps: {jobs_with_overlaps}")
        print(f"  Total overlap instances: {total_overlaps}")
        print(f"  This may cause SAS to miscount the number of images!")
    else:
        print(f"\n✓ No date overlaps detected (regular CSLC dates vs. compressed CSLC ref dates)")

    # Verify forward processing progression
    print(f"\n{'-'*100}")
    print(f"Forward Processing Verification:")
    print(f"{'-'*100}")

    issues = []

    # Check 1: Jobs should be in chronological order
    for i in range(len(jobs) - 1):
        if jobs[i].start_date and jobs[i+1].start_date:
            if jobs[i].start_date > jobs[i+1].start_date:
                issues.append(f"Jobs out of order: Job #{i+1} starts after Job #{i+2}")

    # Check 2: Each job should have overlapping dates with previous job (for continuity)
    for i in range(len(jobs) - 1):
        overlap = jobs[i].regular_cslc_dates & jobs[i+1].regular_cslc_dates
        if overlap:
            print(f"✓ Job #{i+1} → Job #{i+2}: {len(overlap)} overlapping CSLC dates (good for continuity)")
        else:
            issues.append(f"No overlap between Job #{i+1} and Job #{i+2} (potential gap)")

    # Check 3: Overlaps between regular CSLC dates and compressed CSLC reference dates
    print(f"\nChecking for date overlaps (regular CSLC vs. compressed CSLC reference dates):")
    for i, job in enumerate(jobs, 1):
        overlaps = job.regular_cslc_dates & job.compressed_cslc_ref_dates
        if overlaps:
            overlaps_sorted = sorted(overlaps)
            issues.append(f"Job #{i} has {len(overlaps)} date overlap(s) between regular CSLCs and compressed CSLC ref dates")
            print(f"⚠️  Job #{i}: {len(overlaps)} OVERLAP(S) DETECTED!")
            for overlap_date in overlaps_sorted[:5]:  # Show first 5
                date_obj = datetime.strptime(overlap_date, '%Y%m%d')
                print(f"    - {overlap_date} ({date_obj.strftime('%Y-%m-%d')})")
            if len(overlaps_sorted) > 5:
                print(f"    ... and {len(overlaps_sorted) - 5} more")
        else:
            print(f"✓ Job #{i}: No overlaps")

    # Check 4: CCSLC creation pattern
    if len(ccslc_jobs) > 0:
        print(f"\n✓ CCSLC creation detected in {len(ccslc_jobs)} job(s)")
        print(f"  Pattern: Jobs {ccslc_jobs} create compressed CSLCs for archival/efficiency")
    else:
        print(f"\n⚠  No jobs create CCSLCs (all have save_compressed_slc = False)")

    # Check 5: Accumulated dates should grow
    print(f"\nChecking date accumulation (forward progression):")
    accumulated_dates = set()
    for i, job in enumerate(jobs, 1):
        prev_count = len(accumulated_dates)
        accumulated_dates.update(job.regular_cslc_dates)
        new_dates = len(accumulated_dates) - prev_count
        if new_dates > 0:
            print(f"✓ Job #{i} adds {new_dates} new CSLC date(s) (total: {len(accumulated_dates)})")
        else:
            print(f"⚠  Job #{i} adds no new CSLC dates (all dates already seen)")

    if issues:
        print(f"\n⚠️  ISSUES DETECTED:")
        for issue in issues:
            print(f"  - {issue}")
    else:
        print(f"\n✓ Forward processing appears to be progressing correctly!")

    print(f"\n{'='*100}\n")

    # Generate swimlane diagram
    print(f"Generating swimlane diagram...")
    create_swimlane_diagram(frame_id, jobs, output_dir)


def main():
    # Get run_configs directory from command line
    if len(sys.argv) < 2:
        print("Usage: python analyze_disp_s1_forward_processing_timeline.py <run_configs_directory>")
        print("\nExample:")
        print("  python analyze_disp_s1_forward_processing_timeline.py /path/to/run_configs")
        print("  python analyze_disp_s1_forward_processing_timeline.py .")
        print("\nThe script will:")
        print("  - Read all OPERA_L3_DISP-S1_*.yaml files in the directory")
        print("  - Group them by frame ID")
        print("  - Analyze forward processing timeline for each frame")
        print("  - Generate swimlane diagrams in timeline_diagrams/ subdirectory")
        print("  - Check for date overlaps between regular CSLCs and compressed CSLC references")
        sys.exit(1)

    run_configs_dir = Path(sys.argv[1])

    if not run_configs_dir.exists():
        print(f"ERROR: Directory does not exist: {run_configs_dir}")
        sys.exit(1)

    if not run_configs_dir.is_dir():
        print(f"ERROR: {run_configs_dir} is not a directory")
        sys.exit(1)

    print(f"Analyzing run configs in: {run_configs_dir.absolute()}")
    print(f"Looking for: OPERA_L3_DISP-S1_*.yaml files\n")

    # Find all run config files
    yaml_files = list(run_configs_dir.glob("OPERA_L3_DISP-S1_*.yaml"))

    if not yaml_files:
        print(f"ERROR: No run config files found in {run_configs_dir}")
        print(f"Expected files matching pattern: OPERA_L3_DISP-S1_*.yaml")
        sys.exit(1)

    print(f"Found {len(yaml_files)} run config files")

    # Parse all jobs
    jobs_by_frame: Dict[str, List[JobInfo]] = defaultdict(list)

    for yaml_file in yaml_files:
        job = JobInfo(yaml_file)

        if not job.parse_filename():
            print(f"WARNING: Could not parse filename: {yaml_file.name}")
            continue

        if not job.parse_config():
            continue

        jobs_by_frame[job.frame_id].append(job)

    # Analyze each frame
    if not jobs_by_frame:
        print("ERROR: No valid jobs found")
        sys.exit(1)

    print(f"\nFound {len(jobs_by_frame)} frame(s): {', '.join(f'F{fid}' for fid in sorted(jobs_by_frame.keys()))}")

    # Create output directory for visualizations
    output_dir = run_configs_dir / "timeline_diagrams"
    output_dir.mkdir(exist_ok=True)
    print(f"\nVisualization output directory: {output_dir.absolute()}\n")

    # Analyze each frame's timeline
    for frame_id in sorted(jobs_by_frame.keys()):
        analyze_frame_timeline(frame_id, jobs_by_frame[frame_id], output_dir)


if __name__ == '__main__':
    main()
