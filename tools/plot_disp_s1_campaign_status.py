#!/usr/bin/env python3
"""Render a phased DISP-S1 campaign as a status timeline: done / running / pending.

Consumes the JSON emitted by disp_s1_campaign_status.py --json, so it can be run
off-cluster:

    # on Mozart
    python tools/disp_s1_campaign_status.py --json > status.json
    # anywhere
    python plot_campaign_status.py status.json campaign_status.png
"""

import json
import sys
from datetime import datetime

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Patch, Rectangle

DONE, RUNNING, PENDING, SKIP = "#2e7d32", "#e8a33d", "#c9ced6", "#d1495b"
INK, MUTED = "#1a1d21", "#6b7280"
COL = {"done": DONE, "running": RUNNING, "pending": PENDING, "skipped": SKIP}
NAMES = {25278: "Okmok", 33065: "Unimak", 24726: "Arizona", 17235: "17235"}


def d(s):
    return datetime.strptime(s[:8], "%Y%m%d")


def main():
    status = json.load(open(sys.argv[1]))
    out = sys.argv[2] if len(sys.argv) > 2 else "campaign_status.png"
    frames = status["frames"]

    fig, axes = plt.subplots(len(frames), 1, figsize=(15, 2.9 * len(frames) + 1.8),
                             squeeze=False)
    axes = [a[0] for a in axes]
    fig.patch.set_facecolor("white")

    # common date range across every frame so the rows are comparable
    alld = []
    for fr in frames:
        for ph in fr["phases"]:
            for u in ph["units"]:
                for part in u["dates"].split(".."):
                    alld.append(d(part))
    lo, hi = min(alld), max(alld)

    for ax, fr in zip(axes, frames):
        ax.xaxis_date()
        fid = fr["frame"]
        for ph in fr["phases"]:
            for u in ph["units"]:
                span = u["dates"].split("..")
                a = d(span[0]); b = d(span[-1])
                if a == b:
                    b = d(span[-1])
                ax.add_patch(Rectangle((mdates.date2num(a), 0.30),
                                       max(mdates.date2num(b) - mdates.date2num(a), 6),
                                       0.42, facecolor=COL.get(u["status"], PENDING),
                                       edgecolor="white", lw=1.2, zorder=3))
            # phase label under its span
            first = d(ph["units"][0]["dates"].split("..")[0])
            last = d(ph["units"][-1]["dates"].split("..")[-1])
            mid = mdates.date2num(first) + (mdates.date2num(last) - mdates.date2num(first)) / 2
            ax.text(mid, 0.16, ph["label"], ha="center", va="top", fontsize=8.6, color=MUTED)

        pct = int(fr["products"] / fr["expected"] * 100) if fr["expected"] else 0
        ax.set_title("Frame %s (%s) — %d/%d products (%d%%), %d/%d compressed CSLCs, %d bursts"
                     % (fid, NAMES.get(fid, ""), fr["products"], fr["expected"], pct,
                        fr["ccslc"], fr["ccslc_expected"], fr["bursts"]),
                     loc="left", fontsize=11, fontweight="bold", color=INK, pad=12)
        ax.set_xlim(mdates.date2num(lo) - 60, mdates.date2num(hi) + 60)
        ax.set_ylim(0, 1)
        ax.set_yticks([])
        for s in ("left", "right", "top"):
            ax.spines[s].set_visible(False)
        ax.spines["bottom"].set_color("#c9ced6")
        ax.xaxis.set_major_locator(mdates.YearLocator())
        ax.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
        ax.grid(axis="x", color="#eef0f3", lw=0.8, zorder=0)
        ax.tick_params(labelsize=9.5, colors=MUTED)

    handles = [Patch(facecolor=DONE, label="done"),
               Patch(facecolor=RUNNING, label="running now"),
               Patch(facecolor=PENDING, label="pending"),
               Patch(facecolor=SKIP, label="no_run (skipped)")]
    fig.legend(handles=handles, loc="upper center", bbox_to_anchor=(0.5, 0.055),
               ncol=4, frameon=False, fontsize=9.5)

    pct = int(status["products"] / status["products_expected"] * 100) if status["products_expected"] else 0
    fig.suptitle("%s — %d/%d products (%d%%), %d/%d compressed CSLCs"
                 % (status["label"], status["products"], status["products_expected"], pct,
                    status["ccslc"], status["ccslc_expected"]),
                 x=0.007, ha="left", fontsize=13.5, fontweight="bold", color=INK, y=0.985)

    fig.tight_layout(rect=[0, 0.06, 1, 0.94], h_pad=2.4)
    fig.savefig(out, dpi=170, facecolor="white")
    print("wrote", out)


if __name__ == "__main__":
    main()
