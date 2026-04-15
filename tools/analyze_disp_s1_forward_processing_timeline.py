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
from datetime import datetime, timedelta
from typing import Dict, List, Set, Tuple, Optional

try:
    import matplotlib.pyplot as plt
    import matplotlib.dates as mdates
    import matplotlib.patches as mpatches
    HAS_MATPLOTLIB = True
except ImportError:
    HAS_MATPLOTLIB = False
    print("WARNING: matplotlib not available. Swimlane diagrams will not be generated.")
    print("Install with: pip install matplotlib")


class JobInfo:
    """Information about a single DISP-S1 job from a run config."""

    def __init__(self, config_path: Path, failed: bool = False):
        self.config_path = config_path
        self.failed = failed
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
        self.compressed_cslc_details: Set[Tuple[str, str]] = set()  # (first_date, last_date) tuples

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
        # For failed jobs loaded via --failed, frame_id/dates come from config content
        if self.failed:
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
                    # Extract burst ID, reference date, first_date, last_date from compressed CSLC
                    # Format: OPERA_L2_COMPRESSED-CSLC-S1_F14883_T056-119061-IW1_20240922T000000Z_20240407T000000Z_20240922T000000Z_...
                    match = re.search(r'F\d+_(T\d{3}-\d{6}-IW\d)_(\d{8})T\d+Z_(\d{8})T\d+Z_(\d{8})T', filename)
                    if match:
                        self.bursts.add(match.group(1))
                        self.compressed_cslc_ref_dates.add(match.group(2))
                        self.compressed_cslc_details.add((match.group(3), match.group(4)))
                    else:
                        # Fallback: old 2-group regex
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

            # Get frame_id from config to verify (or set for failed jobs)
            try:
                config_frame_id = str(config['RunConfig']['Groups']['SAS']['input_file_group']['frame_id'])
                if self.frame_id and int(self.frame_id) != int(config_frame_id):
                    print(f"WARNING: Frame ID mismatch in {self.config_path.name}: "
                          f"filename={self.frame_id}, config={config_frame_id}")
                if not self.frame_id:
                    self.frame_id = config_frame_id
            except (KeyError, TypeError, ValueError):
                pass

            # For failed jobs, derive start_date and end_date from CSLC dates
            if self.failed and self.regular_cslc_dates:
                sorted_dates = sorted(self.regular_cslc_dates)
                if not self.start_date:
                    self.start_date = sorted_dates[0] + "T000000Z"
                if not self.end_date:
                    self.end_date = sorted_dates[-1] + "T000000Z"
                self.job_id = f"F{self.frame_id}_{self.start_date}_{self.end_date}_FAILED"

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
    Create an extended two-panel swimlane diagram for DISP-S1 forward processing.

    Left panel:  older CCSLCs shown as labeled rounded blocks per job row.
    Right panel: most-recent CCSLC as a translucent date-range span behind
                 CSLC diamonds, with filtered-overlap markers, trigger CSLC,
                 DISP-S1 output square, rotation/transition annotations, and
                 per-job count labels.

    Args:
        frame_id: The frame ID
        jobs: List of JobInfo objects (already sorted by start date)
        output_dir: Directory to save the diagram
    """
    if not HAS_MATPLOTLIB:
        print("  Skipping swimlane diagram (matplotlib not available)")
        return

    from matplotlib.lines import Line2D

    num_jobs = len(jobs)
    if num_jobs == 0:
        print("  No jobs to plot")
        return

    # ── 1. Data preparation ──────────────────────────────────────────────────

    # Collect all unique CCSLCs (first_date, last_date) across every job
    all_ccslc_set: Set[Tuple[str, str]] = set()
    for job in jobs:
        all_ccslc_set.update(job.compressed_cslc_details)

    if not all_ccslc_set:
        print("  No compressed CSLC details available for extended diagram")
        return

    # Sort by last_date (then first_date) and assign sequential names
    all_ccslcs_sorted = sorted(all_ccslc_set, key=lambda x: (x[1], x[0]))
    ccslc_names: Dict[Tuple[str, str], str] = {}
    for idx, key in enumerate(all_ccslcs_sorted, 1):
        ccslc_names[key] = f"CCSLC {idx}"

    n_unique = len(all_ccslcs_sorted)

    # Color palette: blue shades for older CCSLCs, orange for the newest per group
    blue_shades = ['#1f4e79', '#2e75b6', '#4472C4', '#5b9bd5', '#9dc3e6',
                   '#b4d4e7', '#c5dff0', '#d6eaf8']

    def _ccslc_color(key: Tuple[str, str], is_newest: bool) -> str:
        if is_newest:
            return '#e07020'
        idx = all_ccslcs_sorted.index(key)
        return blue_shades[idx % len(blue_shades)]

    # Per-job derived data
    job_y = [num_jobs - i for i in range(num_jobs)]

    # For each job determine: most-recent CCSLC, older CCSLCs, filtered overlaps
    job_newest: List[Optional[Tuple[str, str]]] = []
    job_older: List[List[Tuple[str, str]]] = []
    job_filtered: List[List[str]] = []  # filtered overlap dates (YYYYMMDD)

    # Max CSLC date count across all jobs = k (the full unfiltered window)
    max_cslc_dates = max(len(j.regular_cslc_dates) for j in jobs)

    for job in jobs:
        details = job.compressed_cslc_details
        if not details:
            job_newest.append(None)
            job_older.append([])
            job_filtered.append([])
            continue

        sorted_details = sorted(details, key=lambda x: (x[1], x[0]))
        newest = sorted_details[-1]
        older = sorted_details[:-1]
        job_newest.append(newest)
        job_older.append(older)

        # Infer filtered overlaps: a CCSLC's last_date is "filtered" if it
        # is absent from regular_cslc_dates but would have been in the
        # expected window.  When a job has fewer dates than k (the max),
        # we extend the observed range by one sensing interval to catch
        # edge-of-window filtering (e.g., when the filtered date would
        # have been the oldest CSLC, removing it shifts the earliest date
        # forward and hides the overlap from a naive range check).
        filtered = []
        if job.regular_cslc_dates:
            sorted_reg = sorted(job.regular_cslc_dates)
            earliest, latest = sorted_reg[0], sorted_reg[-1]
            if len(sorted_reg) < max_cslc_dates:
                # Fewer dates than k — a date may have been filtered at
                # the edge of the window.  Extend by the minimum sensing
                # interval (not the first two dates, which may span a gap).
                if len(sorted_reg) >= 2:
                    intervals = [
                        (datetime.strptime(sorted_reg[i+1], '%Y%m%d') -
                         datetime.strptime(sorted_reg[i], '%Y%m%d')).days
                        for i in range(len(sorted_reg) - 1)
                    ]
                    interval_days = min(intervals)
                else:
                    interval_days = 12  # default Sentinel-1 repeat cycle
                dt0 = datetime.strptime(sorted_reg[0], '%Y%m%d')
                extended_earliest = (dt0 - timedelta(days=interval_days)).strftime('%Y%m%d')
                extended_latest = (datetime.strptime(latest, '%Y%m%d')
                                   + timedelta(days=interval_days)).strftime('%Y%m%d')
            else:
                extended_earliest, extended_latest = earliest, latest
            for _first, last in sorted_details:
                if extended_earliest <= last <= extended_latest and last not in job.regular_cslc_dates:
                    filtered.append(last)
        job_filtered.append(filtered)

    # Detect rotation points: consecutive jobs where compressed_cslc_details differs
    rotation_indices: List[int] = []  # index i means rotation between job i and i+1
    for i in range(num_jobs - 1):
        if jobs[i].compressed_cslc_details != jobs[i + 1].compressed_cslc_details:
            rotation_indices.append(i)

    # Detect overlap→no-overlap transitions
    transition_indices: List[int] = []
    for i in range(num_jobs - 1):
        if job_filtered[i] and not job_filtered[i + 1]:
            transition_indices.append(i)

    # Group consecutive jobs by their newest CCSLC for span drawing
    span_groups: List[Tuple[Tuple[str, str], int, int]] = []  # (ccslc_key, start_idx, end_idx)
    cur_key = job_newest[0]  # may be None for jobs without input CCSLCs
    cur_start = 0
    for i in range(1, num_jobs):
        if job_newest[i] != cur_key:
            span_groups.append((cur_key, cur_start, i - 1))
            cur_key = job_newest[i]
            cur_start = i
    span_groups.append((cur_key, cur_start, num_jobs - 1))

    # Determine how many older-CCSLC columns we need
    max_older = max((len(o) for o in job_older), default=0)

    # ── 2. Create figure ─────────────────────────────────────────────────────

    left_ratio = max(1.0, max_older * 0.25) if max_older > 0 else 0.5
    fig, (ax_left, ax_right) = plt.subplots(
        1, 2, sharey=True,
        figsize=(26, max(8, num_jobs * 1.1)),
        gridspec_kw={'width_ratios': [left_ratio, 5], 'wspace': 0.02}
    )

    # ── 3. Left panel: older CCSLCs as labeled blocks ────────────────────────

    bar_width = 0.9
    bar_height = 0.7
    x_positions = [0.5 + i for i in range(max(max_older, 1))]

    for ji, job in enumerate(jobs):
        y = job_y[ji]
        for ci, ccslc_key in enumerate(job_older[ji]):
            if ci >= len(x_positions):
                break
            x = x_positions[ci]
            color = _ccslc_color(ccslc_key, False)
            first_str = f"{ccslc_key[0][:4]}-{ccslc_key[0][4:6]}-{ccslc_key[0][6:]}"
            last_str = f"{ccslc_key[1][:4]}-{ccslc_key[1][4:6]}-{ccslc_key[1][6:]}"
            label_text = f"{first_str}\n\u2192\n{last_str}"

            rect = mpatches.FancyBboxPatch(
                (x - bar_width / 2, y - bar_height / 2), bar_width, bar_height,
                boxstyle="round,pad=0.03",
                facecolor=color, edgecolor='white',
                linewidth=0.8, alpha=0.85, zorder=3
            )
            ax_left.add_patch(rect)
            ax_left.text(x, y, label_text,
                         ha='center', va='center', fontsize=5, color='white',
                         fontweight='bold', zorder=4, linespacing=0.85)

    # Left panel header
    ax_left.text(
        (max_older) / 2 + 0.5 if max_older > 0 else 0.5, num_jobs + 1.5,
        f'Older {max_older} CCSLCs' if max_older > 0 else 'CCSLCs',
        ha='center', va='center', fontsize=10, fontweight='bold', color='#1f4e79')
    ax_left.text(
        (max_older) / 2 + 0.5 if max_older > 0 else 0.5, num_jobs + 1.0,
        '(no CSLC overlap)',
        ha='center', va='center', fontsize=8, color='#888888', style='italic')

    # Rotation annotations on left panel
    for ri in rotation_indices:
        rot_y = (job_y[ri] + job_y[ri + 1]) / 2
        ax_left.axhline(y=rot_y, color='#e07020', linestyle='--', linewidth=1.5,
                        alpha=0.7, xmin=0.02, xmax=0.98)
        # Determine what changed
        old_set = jobs[ri].compressed_cslc_details
        new_set = jobs[ri + 1].compressed_cslc_details
        dropped = old_set - new_set
        added = new_set - old_set
        parts = []
        for d in sorted(dropped, key=lambda x: x[1]):
            parts.append(f"{ccslc_names.get(d, '?')} dropped")
        for a in sorted(added, key=lambda x: x[1]):
            parts.append(f"{ccslc_names.get(a, '?')} added")
        rot_label = 'rotation: ' + ', '.join(parts) if parts else 'CCSLC rotation'
        ax_left.text(
            (max_older) / 2 + 0.5 if max_older > 0 else 0.5, rot_y + 0.25,
            rot_label, ha='center', va='center', fontsize=6.5, color='#e07020',
            fontweight='bold',
            bbox=dict(boxstyle='round,pad=0.2', facecolor='#fff3e0',
                      edgecolor='#e07020', alpha=0.9))

    # Mark CCSLC-generating jobs on left panel
    ccslc_gen_counter = 0
    for ji, job in enumerate(jobs):
        if job.saves_ccslc:
            ccslc_gen_counter += 1
            y = job_y[ji]
            n_products = len(getattr(job, 'output_product_dates', set()))
            product_note = f'\n   ({n_products} products)' if n_products > 1 else ''
            gen_label = f'\u2190 generates\n   CCSLC {ccslc_gen_counter}{product_note}'
            ax_left.text(max_older + 0.8 if max_older > 0 else 1.3, y, gen_label,
                         ha='left', va='center', fontsize=7, color='#e07020',
                         fontweight='bold')

    # Temporal arrow at bottom
    x_arrow_start = 0.2
    x_arrow_end = max_older + 0.2 if max_older > 0 else 1.2
    ax_left.annotate('', xy=(x_arrow_end, -0.15), xytext=(x_arrow_start, -0.15),
                     arrowprops=dict(arrowstyle='->', color='#888888', linewidth=1))
    ax_left.text((x_arrow_start + x_arrow_end) / 2, -0.45,
                 'older \u2192 newer', ha='center', va='center',
                 fontsize=7, color='#888888', style='italic')

    ax_left.set_xlim(-0.2, max_older + 1.8 if max_older > 0 else 2.0)
    ax_left.set_xticks([])
    ax_left.spines['top'].set_visible(False)
    ax_left.spines['right'].set_visible(False)
    ax_left.spines['bottom'].set_visible(False)
    ax_left.set_yticks(job_y)
    ax_left.set_yticklabels(
        [f"Job {i + 1} ({datetime.strptime(j.end_date[:8], '%Y%m%d').strftime('%m/%d')})"
         if j.end_date else f"Job {i + 1}"
         for i, j in enumerate(jobs)],
        fontsize=8)

    # ── 4. Right panel: most-recent CCSLC span + CSLC diamonds ───────────────

    ax = ax_right

    # Compute date extent from regular CSLC dates
    all_reg_dates = []
    for job in jobs:
        all_reg_dates.extend(
            datetime.strptime(d, '%Y%m%d') for d in job.regular_cslc_dates)
    if not all_reg_dates:
        print("  No regular CSLC dates to plot")
        plt.close()
        return

    x_min = min(all_reg_dates) - timedelta(days=20)
    x_max = max(all_reg_dates) + timedelta(days=50)

    # Draw CCSLC spans behind job rows
    for ccslc_key, grp_start, grp_end in span_groups:
        if ccslc_key is None:
            continue
        first_dt = datetime.strptime(ccslc_key[0], '%Y%m%d')
        last_dt = datetime.strptime(ccslc_key[1], '%Y%m%d')
        c_start = mdates.date2num(first_dt)
        c_end = mdates.date2num(last_dt)
        c_width = c_end - c_start

        is_newest_overall = (ccslc_key == all_ccslcs_sorted[-1])
        span_fc = '#FFE0C0' if is_newest_overall else '#BDD7EE'
        span_ec = '#e07020' if is_newest_overall else '#4472C4'

        for ji in range(grp_start, grp_end + 1):
            y = job_y[ji]
            rect = mpatches.FancyBboxPatch(
                (c_start, y - 0.35), c_width, 0.7,
                boxstyle="round,pad=0.02",
                facecolor=span_fc, edgecolor=span_ec,
                linewidth=1.0, linestyle='--', alpha=0.35, zorder=1)
            ax.add_patch(rect)

    # Top bracket labels for each span group, staggered vertically to avoid overlap
    bracket_y = num_jobs + 1.0
    label_idx = 0
    for ccslc_key, grp_start, grp_end in span_groups:
        if ccslc_key is None:
            continue
        first_dt = datetime.strptime(ccslc_key[0], '%Y%m%d')
        last_dt = datetime.strptime(ccslc_key[1], '%Y%m%d')
        c_start = mdates.date2num(first_dt)
        c_end = mdates.date2num(last_dt)
        name = ccslc_names.get(ccslc_key, '?')
        first_str = first_dt.strftime('%Y-%m-%d')
        last_str = last_dt.strftime('%Y-%m-%d')

        is_newest_overall = (ccslc_key == all_ccslcs_sorted[-1])
        lbl_color = '#e07020' if is_newest_overall else '#2e75b6'
        lbl_fc = '#FFE0C0' if is_newest_overall else '#BDD7EE'
        lbl_ec = '#e07020' if is_newest_overall else '#4472C4'

        # Stagger labels vertically so they don't overlap
        label_y = num_jobs + 1.5 + (label_idx % 2) * 1.2
        label_idx += 1

        ax.text(c_start + (c_end - c_start) / 2, label_y,
                f'Most recent CCSLC \u2014 {name}:  {first_str} \u2192 {last_str}',
                ha='center', va='center', fontsize=9, fontweight='bold',
                color=lbl_color,
                bbox=dict(boxstyle='round,pad=0.4', facecolor=lbl_fc,
                          edgecolor=lbl_ec, alpha=0.7))

        # Bracket lines (aligned with staggered label)
        bracket_y_this = label_y - 0.5
        ax.plot([c_start, c_start], [bracket_y_this, bracket_y_this - 0.15],
                color=lbl_ec, linewidth=1, zorder=2)
        ax.plot([c_end, c_end], [bracket_y_this, bracket_y_this - 0.15],
                color=lbl_ec, linewidth=1, zorder=2)
        ax.plot([c_start, c_end], [bracket_y_this, bracket_y_this],
                color=lbl_ec, linewidth=1, zorder=2)

    # Draw CSLC diamonds, trigger, filtered overlap, DISP output per job
    SZ = 100
    for ji, job in enumerate(jobs):
        y = job_y[ji]
        trigger_date_str = job.end_date[:8] if job.end_date else None
        trigger_dt = datetime.strptime(trigger_date_str, '%Y%m%d') if trigger_date_str else None

        if job.failed:
            # Failed job: faded diamonds with red edge
            for d in sorted(job.regular_cslc_dates):
                dt = datetime.strptime(d, '%Y%m%d')
                x = mdates.date2num(dt)
                ax.scatter(x, y, marker='D', s=SZ, c='#FFCCCC',
                           edgecolors='#CC0000', linewidths=0.8, alpha=0.7, zorder=10)
            # Filtered overlap markers (same as successful jobs)
            for fd in job_filtered[ji]:
                fd_dt = datetime.strptime(fd, '%Y%m%d')
                x_f = mdates.date2num(fd_dt)
                ax.scatter(x_f, y, marker='D', s=SZ + 20, c='#FF0000',
                           edgecolors='darkred', linewidths=1.2, zorder=10)
                ax.scatter(x_f, y, marker='x', s=80, c='white',
                           linewidths=2.5, zorder=11)
            # Red X marker instead of green output square
            if trigger_dt:
                x_out = mdates.date2num(trigger_dt + timedelta(days=8))
                ax.scatter(x_out, y, marker='X', s=SZ + 60, c='#CC0000',
                           edgecolors='darkred', linewidths=1.5, zorder=10)
        else:
            # Don't show trigger highlight for consolidated multi-product jobs (e.g., historical batch)
            n_products = len(getattr(job, 'output_product_dates', set()))
            is_single_product = n_products <= 1

            for d in sorted(job.regular_cslc_dates):
                dt = datetime.strptime(d, '%Y%m%d')
                x = mdates.date2num(dt)
                if is_single_product and trigger_dt and dt == trigger_dt:
                    ax.scatter(x, y, marker='D', s=SZ + 40, c='#FFD700',
                               edgecolors='#B8860B', linewidths=1.3, zorder=10)
                else:
                    ax.scatter(x, y, marker='D', s=SZ, c='#ED7D31',
                               edgecolors='black', linewidths=0.8, zorder=10)

            # Filtered overlap markers (red diamond + white X)
            for fd in job_filtered[ji]:
                fd_dt = datetime.strptime(fd, '%Y%m%d')
                x_f = mdates.date2num(fd_dt)
                ax.scatter(x_f, y, marker='D', s=SZ + 20, c='#FF0000',
                           edgecolors='darkred', linewidths=1.2, zorder=10)
                ax.scatter(x_f, y, marker='x', s=80, c='white',
                           linewidths=2.5, zorder=11)

            # DISP-S1 output square (trigger date + 8 days offset for visibility)
            if trigger_dt:
                x_out = mdates.date2num(trigger_dt + timedelta(days=8))
                ax.scatter(x_out, y, marker='s', s=SZ + 40, c='#70AD47',
                           edgecolors='black', linewidths=1, zorder=10)

    # Vertical dashed lines at unique overlap (last_date) values
    overlap_dates_shown: Set[str] = set()
    for fi_list in job_filtered:
        overlap_dates_shown.update(fi_list)
    for od in sorted(overlap_dates_shown):
        od_dt = datetime.strptime(od, '%Y%m%d')
        ax.axvline(x=mdates.date2num(od_dt), color='#CC0000',
                   linestyle=':', linewidth=1.5, alpha=0.35, zorder=0)
        od_str = f"{od[:4]}-{od[4:6]}-{od[6:]}"
        # Find which CCSLC this belongs to
        ccslc_label = ''
        for key in all_ccslcs_sorted:
            if key[1] == od:
                ccslc_label = f"\n{ccslc_names[key]} last_date"
                break
        ax.text(mdates.date2num(od_dt), -0.5,
                f'{od_str}{ccslc_label}\n(filtered)',
                ha='center', va='center', fontsize=7, color='#CC0000',
                fontweight='bold',
                bbox=dict(boxstyle='round,pad=0.3', facecolor='#fff0f0',
                          edgecolor='#CC0000', alpha=0.9))

    # Transition annotations (overlap→no-overlap)
    for ti in transition_indices:
        sep_y = (job_y[ti] + job_y[ti + 1]) / 2
        ax.axhline(y=sep_y, color='#2e7d32', linestyle='--', linewidth=1.2,
                   alpha=0.5, zorder=0)
        label_x = mdates.date2num(x_max - timedelta(days=40))
        ax.text(label_x, sep_y + 0.25,
                '\u2191 overlap exists    \u2193 window moved past overlap, no overlap',
                ha='center', va='center', fontsize=7, color='#2e7d32',
                fontweight='bold',
                bbox=dict(boxstyle='round,pad=0.2', facecolor='#e8f5e9',
                          edgecolor='#2e7d32', alpha=0.9))

    # Rotation annotations on right panel
    for ri in rotation_indices:
        sep_y = (job_y[ri] + job_y[ri + 1]) / 2
        ax.axhline(y=sep_y, color='#e07020', linestyle='--', linewidth=1.2,
                   alpha=0.5, zorder=0)
        label_x = mdates.date2num(x_max - timedelta(days=40))
        old_newest = job_newest[ri]
        new_newest = job_newest[ri + 1]
        old_name = ccslc_names.get(old_newest, '?') if old_newest else '?'
        new_name = ccslc_names.get(new_newest, '?') if new_newest else '?'
        ax.text(label_x, sep_y + 0.25,
                f'\u2191 {old_name} is most recent    \u2193 {new_name} generated, now most recent',
                ha='center', va='center', fontsize=7, color='#e07020',
                fontweight='bold',
                bbox=dict(boxstyle='round,pad=0.2', facecolor='#fff3e0',
                          edgecolor='#e07020', alpha=0.9))

    # Per-job count annotations
    for ji, job in enumerate(jobs):
        y = job_y[ji]
        trigger_dt = datetime.strptime(job.end_date[:8], '%Y%m%d') if job.end_date else None
        x_r = mdates.date2num((trigger_dt or x_max) + timedelta(days=20))
        n_older = len(job_older[ji])
        n_cslcs = len(job.regular_cslc_dates)
        n_filtered = len(job_filtered[ji])

        n_ccslcs = n_older + (1 if job_newest[ji] is not None else 0)
        count_text = f'{n_ccslcs} CCSLCs + {n_cslcs} CSLCs' if n_ccslcs else f'{n_cslcs} CSLCs'
        if job.failed:
            count_text += ' \u2717 FAILED'
            color = '#CC0000'
            weight = 'bold'
        elif n_filtered > 0:
            count_text += f' ({n_filtered} filtered)'
            color = '#555555'
            weight = 'normal'
        elif job.saves_ccslc:
            count_text += ' (no overlap)\n\u2192 generates CCSLC'
            color = '#2e7d32'
            weight = 'bold'
        else:
            count_text += ' (no overlap)'
            color = '#2e7d32'
            weight = 'bold'

        ax.text(x_r, y, count_text, fontsize=6, va='center',
                color=color, fontweight=weight, linespacing=1.3)

    # ── 5. Right panel formatting ────────────────────────────────────────────

    ax.xaxis.set_major_locator(mdates.MonthLocator())
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%b %Y'))
    plt.setp(ax.xaxis.get_majorticklabels(), rotation=45, ha='right', fontsize=9)
    ax.set_xlim(mdates.date2num(x_min), mdates.date2num(x_max))

    ax.set_ylim(-1.2, num_jobs + 2.3)
    for y in job_y:
        ax.axhline(y=y - 0.5, color='#e8e8e8', linewidth=0.5, zorder=0)

    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_visible(False)
    ax.set_yticks([])

    # ── 6. Legend ─────────────────────────────────────────────────────────────

    legend_elements = []
    # Add span patches for each unique most-recent CCSLC
    seen_newest = set()
    for ccslc_key, grp_start, grp_end in span_groups:
        if ccslc_key is None or ccslc_key in seen_newest:
            continue
        seen_newest.add(ccslc_key)
        name = ccslc_names.get(ccslc_key, '?')
        is_newest_overall = (ccslc_key == all_ccslcs_sorted[-1])
        span_fc = '#FFE0C0' if is_newest_overall else '#BDD7EE'
        span_ec = '#e07020' if is_newest_overall else '#4472C4'
        job_range = f"jobs {grp_start + 1}\u2013{grp_end + 1}"
        legend_elements.append(
            mpatches.Patch(facecolor=span_fc, edgecolor=span_ec, alpha=0.5,
                           linestyle='--', label=f'{name} coverage (most recent, {job_range})'))

    legend_elements.append(
        mpatches.Patch(facecolor='#4472C4', edgecolor='black',
                       label='Older CCSLCs (left panel)'))
    legend_elements.append(
        Line2D([0], [0], marker='D', color='w', markerfacecolor='#ED7D31',
               markeredgecolor='black', markersize=10, label='CSLC'))
    legend_elements.append(
        Line2D([0], [0], marker='D', color='w', markerfacecolor='#FF0000',
               markeredgecolor='darkred', markersize=10,
               label='CSLC at CCSLC last_date (filtered)'))
    legend_elements.append(
        Line2D([0], [0], marker='D', color='w', markerfacecolor='#FFD700',
               markeredgecolor='#B8860B', markersize=10,
               label='Trigger CSLC (new acquisition)'))
    legend_elements.append(
        mpatches.Patch(facecolor='#70AD47', edgecolor='black',
                       label='DISP-S1 output'))
    legend_elements.append(
        Line2D([0], [0], marker='X', color='w', markerfacecolor='#CC0000',
               markeredgecolor='darkred', markersize=10,
               label='FAILED (no product)'))

    ax.legend(handles=legend_elements, loc='upper right', fontsize=8,
              framealpha=0.95, edgecolor='#cccccc',
              bbox_to_anchor=(1.0, 1.15))

    # ── 7. Title ─────────────────────────────────────────────────────────────

    all_bursts: Set[str] = set()
    for job in jobs:
        all_bursts.update(job.bursts)

    fig.suptitle(
        f'DISP-S1 Forward Processing \u2014 Frame {frame_id} \u00b7 '
        f'{len(all_bursts)} bursts/frame \u00b7 '
        f'{num_jobs} jobs \u00b7 {n_unique} unique CCSLCs',
        fontsize=13, fontweight='bold', y=0.99)

    # ── 8. Save ──────────────────────────────────────────────────────────────

    output_file = output_dir / f"frame_{frame_id}_timeline.png"
    plt.savefig(output_file, dpi=150, bbox_inches='tight',
                facecolor='white', edgecolor='none')
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
    # Sort jobs by end date (secondary/sensing date) for temporal order
    jobs.sort(key=lambda j: (j.end_date or '', j.start_date or ''))

    # Consolidate jobs with identical input CSLC date sets into a single entry.
    # This merges the multiple output products from a single PGE invocation
    # (e.g., the historical batch produces 14 products from one job).
    consolidated = []
    for job in jobs:
        date_key = frozenset(job.regular_cslc_dates)
        merged = False
        for existing in consolidated:
            if frozenset(existing.regular_cslc_dates) == date_key:
                # Same inputs — merge output dates and preserve CCSLC flag
                existing.output_product_dates = getattr(existing, 'output_product_dates', set())
                existing.output_product_dates.add(job.end_date[:8] if job.end_date else '')
                if not hasattr(job, 'output_product_dates'):
                    existing.output_product_dates.add(existing.end_date[:8] if existing.end_date else '')
                # Keep the latest end_date for display
                if (job.end_date or '') > (existing.end_date or ''):
                    existing.end_date = job.end_date
                existing.saves_ccslc = existing.saves_ccslc or job.saves_ccslc
                existing.compressed_cslc_count = max(existing.compressed_cslc_count, job.compressed_cslc_count)
                existing.compressed_cslc_ref_dates |= job.compressed_cslc_ref_dates
                existing.compressed_cslc_details |= job.compressed_cslc_details
                merged = True
                break
        if not merged:
            job.output_product_dates = {job.end_date[:8] if job.end_date else ''}
            consolidated.append(job)

    if len(consolidated) < len(jobs):
        print(f"\nConsolidated {len(jobs)} RunConfigs into {len(consolidated)} unique jobs "
              f"(jobs with identical input dates merged)")

    jobs = consolidated

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
        n_products = len(getattr(job, 'output_product_dates', {job.end_date[:8] if job.end_date else ''}))
        product_label = f" ({n_products} output products)" if n_products > 1 else ""
        print(f"\nJob #{i}: {job.get_date_range_str()}{product_label}")
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
    import argparse
    parser = argparse.ArgumentParser(
        description="Analyze DISP-S1 forward processing timeline from RunConfig files."
    )
    parser.add_argument("run_configs_dir", type=Path,
                        help="Directory containing OPERA_L3_DISP-S1_*.rc.yaml files")
    parser.add_argument("--failed", type=Path, default=None,
                        help="Directory containing RunConfig.yaml files from failed (triaged) jobs")
    args = parser.parse_args()

    run_configs_dir = args.run_configs_dir

    if not run_configs_dir.exists() or not run_configs_dir.is_dir():
        print(f"ERROR: {run_configs_dir} does not exist or is not a directory")
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

    # Load failed job RunConfigs if provided
    if args.failed and args.failed.is_dir():
        failed_files = list(args.failed.glob("*.yaml")) + list(args.failed.glob("*.yml"))
        print(f"Found {len(failed_files)} failed job RunConfig files in {args.failed}")
        for yaml_file in failed_files:
            job = JobInfo(yaml_file, failed=True)
            if not job.parse_filename():
                pass  # Expected — failed jobs skip filename parsing
            if not job.parse_config():
                continue
            if job.frame_id:
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
