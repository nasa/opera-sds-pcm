#!/usr/bin/env python3
"""Render a DISP-S1 campaign as an acquisition timeline with accountability overlaid.

This is the same picture the burst database gives you -- one tick per sensing date,
grouped into historical and forward phases, with the compressed CSLC lineage starts
and k-set boundaries marked -- except that every tick is coloured by what actually
happened to it:

    published   a DISP-S1 product exists for that date
    running     a job is in flight
    FAILED      a job failed and no product came out
    pending     expected, nothing has run yet
    no_run      the burst database never expected anything here

A frame whose earliest outstanding unit has failed is flagged STUCK, because
everything behind it gates on a compressed CSLC that will never publish.

Consumes the JSON emitted by disp_s1_campaign_status.py --json, so it runs
anywhere with matplotlib and does not have to run on Mozart:

    # on Mozart
    python tools/disp_s1_campaign_status.py --json > status.json
    # anywhere
    python tools/plot_disp_s1_campaign_status.py status.json campaign_status.png
"""

import json
import sys
from datetime import datetime

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Patch
from matplotlib.lines import Line2D

# phase identity (the burst database's view)
HIST, FWD = "#1f4fd8", "#0d8f7a"
# accountability (what actually happened)
DONE, RUNNING, FAILED, PENDING, SKIP = "#2e7d32", "#e8a33d", "#c0392b", "#c9ced6", "#eceef1"
LINEAGE = "#c2410c"
INK, MUTED, GRID = "#1a1d21", "#6b7280", "#eef0f3"

STATUS_COLOR = {"done": DONE, "running": RUNNING, "failed": FAILED,
                "pending": PENDING, "skipped": SKIP}
# a failure has to be visible at a glance, so it gets height as well as colour
STATUS_SPAN = {"done": (0.30, 0.74), "running": (0.30, 0.74), "failed": (0.22, 0.86),
               "pending": (0.42, 0.62), "skipped": (0.46, 0.58)}
STATUS_LW = {"done": 2.0, "running": 2.0, "failed": 2.6, "pending": 1.4, "skipped": 1.2}


def d(s):
    return datetime.strptime(s[:8], "%Y%m%d")


def draw_frame(ax, fr):
    sensing = fr.get("sensing") or []
    if not sensing:
        ax.text(0.5, 0.5, "no phase information (frame quarantined)", transform=ax.transAxes,
                ha="center", va="center", color=FAILED, fontsize=11)
        return

    # gap annotations, so the reason the frame is phased at all stays visible
    for a, b in zip(sensing, sensing[1:]):
        span = (d(b["date"]) - d(a["date"])).days
        if span > 400:
            mid = d(a["date"]) + (d(b["date"]) - d(a["date"])) / 2
            ax.annotate("%.1f yr gap" % (span / 365.25), xy=(mid, 0.52),
                        ha="center", va="center", fontsize=8.6, color=MUTED, zorder=4,
                        bbox=dict(boxstyle="round,pad=0.28", fc="white", ec=MUTED, lw=0.8))

    for e in sensing:
        lo, hi = STATUS_SPAN[e["status"]]
        ax.vlines(d(e["date"]), lo, hi, color=STATUS_COLOR[e["status"]],
                  lw=STATUS_LW[e["status"]], zorder=3)
        if e["lineage_start"]:
            ax.plot(d(e["date"]), 0.90, marker="v", ms=6.5, color=LINEAGE,
                    zorder=5, clip_on=False)
        if e["boundary"]:
            # filled once the compressed CSLC has published; hollow until then
            ax.plot(d(e["date"]), 0.16, marker="^", ms=6, color=LINEAGE, zorder=5,
                    clip_on=False,
                    markerfacecolor=LINEAGE if e["boundary_published"] else "white")

    # One x per failed RUN of dates, not per date -- the unit of failure is the job,
    # which owns a whole k-set. Fifteen overlapping markers just make noise.
    run = []
    for e in sensing + [None]:
        if e is not None and e["status"] == "failed":
            run.append(e)
            continue
        if run:
            a, b = mdates.date2num(d(run[0]["date"])), mdates.date2num(d(run[-1]["date"]))
            ax.plot(a + (b - a) / 2, 0.94, marker="x", ms=9, mew=2.4,
                    color=FAILED, zorder=6, clip_on=False)
            run = []

    # phase name under its own span. Adjacent short phases collide on a long axis,
    # so drop every colliding label to a second row rather than overprinting.
    span = mdates.date2num(d(sensing[-1]["date"])) - mdates.date2num(d(sensing[0]["date"]))
    rows, last = [], []
    for ph in fr["phases"]:
        seg = [e for e in sensing if e["phase"] == ph["label"]]
        if not seg:
            continue
        a, b = d(seg[0]["date"]), d(seg[-1]["date"])
        mid = mdates.date2num(a) + (mdates.date2num(b) - mdates.date2num(a)) / 2
        row = 0
        while row < len(last) and abs(mid - last[row]) < 0.085 * span:
            row += 1
        while len(last) <= row:
            last.append(float("-inf"))
        last[row] = mid
        rows.append((mid, row, ph["label"]))

    for mid, row, label in rows:
        col = HIST if label.startswith("historical_") else (
            FWD if label.startswith("forward_") else MUTED)
        ax.text(mid, 0.075 - 0.062 * row, label, ha="center", va="bottom", fontsize=8.6,
                color=col, fontweight="bold")


def main():
    status = json.load(open(sys.argv[1]))
    out = sys.argv[2] if len(sys.argv) > 2 else "campaign_status.png"
    frames = status["frames"]

    fig, axes = plt.subplots(len(frames), 1, figsize=(15.5, 2.7 * len(frames) + 2.0),
                             squeeze=False, sharex=True)
    axes = [a[0] for a in axes]
    fig.patch.set_facecolor("white")

    for ax, fr in zip(axes, frames):
        draw_frame(ax, fr)
        title = ("Frame %s  —  %d/%d products (%d%%), %d/%d compressed CSLCs, "
                 "%d/%d units done, %d bursts"
                 % (fr["frame"], fr["products"], fr["expected"], fr["pct"],
                    fr["ccslc"], fr["ccslc_expected"], fr["units_done"],
                    fr["units_total"], fr["bursts"]))
        ax.set_title(title, loc="left", fontsize=11, fontweight="bold", color=INK, pad=8)
        if fr.get("stuck"):
            ax.set_title("   STUCK on %s" % (fr.get("blocked_on") or "a failed job"),
                         loc="right", fontsize=10.5, fontweight="bold", color=FAILED, pad=8)
        ax.set_ylim(0, 1)
        ax.set_yticks([])
        for s in ("left", "right", "top"):
            ax.spines[s].set_visible(False)
        ax.spines["bottom"].set_color("#c9ced6")
        ax.grid(axis="x", color=GRID, lw=0.8, zorder=1)

    axes[-1].xaxis.set_major_locator(mdates.YearLocator())
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    axes[-1].tick_params(labelsize=10, colors=MUTED)

    handles = [
        Line2D([0], [0], color=DONE, lw=2.6, label="published (product exists)"),
        Line2D([0], [0], color=RUNNING, lw=2.6, label="running"),
        Line2D([0], [0], color=FAILED, lw=2.8, label="FAILED — no product"),
        Line2D([0], [0], color=PENDING, lw=2.0, label="pending"),
        Line2D([0], [0], color=SKIP, lw=2.0, label="no_run (never expected)"),
        Line2D([0], [0], color=LINEAGE, lw=0, marker="v", ms=6.5,
               label="new compressed CSLC lineage"),
        Line2D([0], [0], color=LINEAGE, lw=0, marker="^", ms=6,
               label="k-set boundary (filled = published)"),
    ]
    fig.legend(handles=handles, loc="upper center",
               bbox_to_anchor=(0.5, 0.055 if len(frames) > 1 else 0.10),
               ncol=4, frameon=False, fontsize=9.3)

    pct = int(status["products"] / status["products_expected"] * 100) \
        if status["products_expected"] else 0
    head = ("%s — %d/%d products (%d%%), %d/%d compressed CSLCs"
            % (status["label"], status["products"], status["products_expected"], pct,
               status["ccslc"], status["ccslc_expected"]))
    fig.suptitle(head, x=0.007, ha="left", fontsize=13.5, fontweight="bold", color=INK, y=0.987)
    sub = ("Expectation is the processing-mode burst database; state is product existence "
           "and job status.")
    if status.get("stuck_frames"):
        sub += ("   STUCK: frame(s) %s"
                % ", ".join(str(f) for f in status["stuck_frames"]))
    fig.text(0.007, 0.952, sub, fontsize=9.6,
             color=FAILED if status.get("stuck_frames") else MUTED, ha="left")

    fig.tight_layout(rect=[0, 0.075, 1, 0.935], h_pad=2.0)
    fig.savefig(out, dpi=170, facecolor="white")
    print("wrote", out)


if __name__ == "__main__":
    main()
