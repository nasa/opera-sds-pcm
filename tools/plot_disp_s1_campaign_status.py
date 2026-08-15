#!/usr/bin/env python3
"""Render a DISP-S1 campaign as an acquisition timeline with accountability overlaid.

This is the same picture the burst database gives you -- one tick per sensing date on a
real-time axis, grouped into historical and forward phases, with compressed CSLC lineage
starts and k-set boundaries marked -- except that every tick is coloured by what actually
happened to it, and a job row underneath shows the work itself.

Four lanes, top to bottom:

    sensing ticks   one per acquisition, coloured by accountability state
    job row         one mark per JOB, with a constant-size chip at its end
    boundary marks  the compressed CSLC each k-set job publishes
    blackout rail   the seasons the labeler deliberately excludes

A frame whose earliest outstanding unit has failed is flagged STUCK, because everything
behind it gates on a compressed CSLC that will never publish.

Consumes the JSON emitted by disp_s1_campaign_status.py --json, so it runs anywhere with
matplotlib and does not have to run on Mozart:

    # on Mozart
    python tools/disp_s1_campaign_status.py --json > status.json
    # anywhere
    python tools/plot_disp_s1_campaign_status.py status.json campaign_status.png
"""

import json
import sys
from datetime import datetime, timedelta

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Patch, Rectangle
from matplotlib.lines import Line2D

# Phase identity, matching the Key concepts diagram. Used for the phase labels only --
# in THIS figure the tick colour is accountability state, not phase.
HIST, FWD = "#1f4fd8", "#0d8f7a"

# Lineage and boundary marks are STRUCTURE, not status, so they are drawn in ink. The
# Key concepts diagram draws them in warm orange, which here would sit ~3 OKLab units
# from the failure colour -- indistinguishable, on the one mark that must not be misread.
LINEAGE = "#1a1d21"

# Accountability. done and failed are separated by LUMINANCE, not only hue: the
# conventional mid-green/mid-red pair is equiluminant (1.06:1), so under deuteranopia
# (~8% of men), in greyscale print, or on a washed-out projector, "published" and
# "failed" are literally the same mark -- and that is the one distinction this figure
# exists to make. Failure is the darkest, thickest, tallest mark and additionally
# carries an x and a job chip, so it survives every channel being taken away.
DONE, RUNNING, FAILED = "#3f9e4d", "#e8a33d", "#a4161a"
# pending and no_run are separated by LANE as well as tone: pending sits in the middle
# of the tick field, no_run on the baseline. "Not yet" and "never" are the two states an
# operator must not confuse, and two greys alone could not carry that.
PENDING, SKIP = "#9aa3b0", "#cfd4dc"
BLACKOUT, GAP = "#b9c0cb", "#e3e6ea"
INK, MUTED, GRID = "#1a1d21", "#6b7280", "#eef0f3"

STATUS_COLOR = {"done": DONE, "running": RUNNING, "failed": FAILED,
                "pending": PENDING, "skipped": SKIP}
STATUS_SPAN = {"done": (0.44, 0.88), "running": (0.44, 0.88), "failed": (0.38, 0.96),
               "pending": (0.55, 0.77), "skipped": (0.30, 0.37)}
STATUS_LW = {"done": 2.2, "running": 2.2, "failed": 3.0, "pending": 1.3, "skipped": 1.4}

JOB_TOP, JOB_BOT = 0.25, 0.15          # the job row, below the sensing ticks
JOB_MID = (JOB_TOP + JOB_BOT) / 2
BOUNDARY_Y = 0.075                     # boundary triangles, below the job row


def d(s):
    return datetime.strptime(s[:8], "%Y%m%d")


def num(s):
    return mdates.date2num(d(s))


def draw_blackout(ax, fr):
    """A rail along the bottom, not a full-height band.

    A blackout is a season the labeler deliberately excludes -- which is WHY a chunk can
    be too short to fill a k-set. It is context, not a failure. On a high-latitude frame
    the windows cover ~65% of the axis, so full-height bands invert figure and ground:
    the panel becomes stripes and the narrow white gaps read as the bands. A rail keeps
    the tick field white and still shows the seasonal rhythm.

    Windows are inclusive to 23:59:59, so the rail is extended a day to cover the
    final day rather than stopping one day short of it.
    """
    for start, end in fr.get("blackout") or []:
        ax.axvspan(d(start), d(end) + timedelta(days=1), ymin=0.0, ymax=0.045,
                   color=BLACKOUT, zorder=0, lw=0)


def draw_gaps(ax, sensing):
    """The multi-year acquisition gaps, drawn as the strong mark they are.

    A gap is why the frame is phased at all: it forced the chunk split and the lineage
    reset. The annual blackout is routine by comparison. Drawing the gap as a hatched
    region and the blackout as a thin rail keeps that ranking visible -- an empty
    stretch of axis then reads unambiguously as one or the other.
    """
    for a, b in zip(sensing, sensing[1:]):
        days = (d(b["date"]) - d(a["date"])).days
        if days <= 400:
            continue
        # hatch only, no fill: the gap must be unmistakable against the blackout rail,
        # but it is empty space and should not outweigh the marks that carry state
        ax.axvspan(d(a["date"]), d(b["date"]), ymin=0.10, ymax=0.99, facecolor="none",
                   edgecolor=GAP, hatch="///", lw=0.0, zorder=1)
        mid = d(a["date"]) + (d(b["date"]) - d(a["date"])) / 2
        ax.annotate("%.1f yr acquisition gap" % (days / 365.25), xy=(mid, 0.66),
                    ha="center", va="center", fontsize=8.6, color=MUTED, zorder=4,
                    bbox=dict(boxstyle="round,pad=0.30", fc="white", ec=MUTED, lw=0.8))


def draw_jobs(ax, fr, sensing):
    """One mark per JOB, in the unit's status colour, with a constant-size chip.

    A historical k-set is a SINGLE SCIFLO_L3_DISP_S1_hist job over 15 sensing dates, so
    it draws as a BRACKET -- a rule with end caps at its first and last date. Not a
    filled bar: a k-set can span years of wall clock (frame 33065's second k-set covers
    3.0 years, because snow blackouts suppress acquisition for most of each year) while
    the job itself runs about eight hours. A filled bar claims a duration off by four
    orders of magnitude; a bracket says "these dates are one job", which is true.

    Bracket width therefore varies 6x with no information in it, which would make a
    failure on a short k-set quieter than a success on a long one. The chip at each
    job's end fixes that: every job, historical or forward, gets the same size mark, so
    scanning the job row is scanning jobs rather than scanning acquisition cadence.
    """
    by_pos = {e["position"]: e for e in sensing}
    cap = (JOB_TOP - JOB_BOT) / 2
    # Surface gap between consecutive jobs, in DAYS scaled to the axis. A fixed 1-day
    # inset is ~0.6 px on an 11-year axis, so abutting brackets fused and the chip on
    # one job touched the start cap of the next.
    span_days = num(sensing[-1]["date"]) - num(sensing[0]["date"])
    inset = max(3.0, span_days * 0.004)
    for ph in fr["phases"]:
        for u in ph["units"]:
            positions = [p for p in u.get("positions", []) if p in by_pos]
            if not positions:
                continue
            first, last = by_pos[positions[0]], by_pos[positions[-1]]
            colour = STATUS_COLOR.get(u["status"], PENDING)
            pending = u["status"] == "pending"

            if u.get("kind") == "no_run":
                # no_run owes no jobs at all -- a flat pale ground, said rather than left
                # blank. Nothing else in this figure uses a filled ground strip.
                a, b = num(first["date"]), num(last["date"])
                ax.add_patch(Rectangle((a, JOB_BOT), max(b - a, 3), JOB_TOP - JOB_BOT,
                                       facecolor=SKIP, alpha=0.45, lw=0, zorder=2))
                continue

            if u.get("kind") == "historical":
                a, b = num(first["date"]), num(last["date"])
                lw = 1.2 if pending else 2.0
                right = max(b - inset, a + inset + 2)
                ax.hlines(JOB_MID, a + inset, right, color=colour, lw=lw, zorder=3)
                for x in (a + inset, right):
                    ax.vlines(x, JOB_MID - cap, JOB_MID + cap, color=colour, lw=lw,
                              zorder=3)
                # the compressed CSLC boundary sits at the job's right cap, which is
                # exactly where that job publishes it
                ax.plot(d(last["date"]), BOUNDARY_Y, marker="^", ms=6.5, color=LINEAGE,
                        zorder=5, clip_on=False,
                        markerfacecolor=LINEAGE if last.get("boundary_published")
                        else "white")
            # The chip sits ON the bracket's right cap, inside the inset, so it never
            # reaches the next job's start cap and two jobs never read as one.
            chip = (max(num(last["date"]) - inset, num(first["date"]) + inset + 2)
                    if u.get("kind") == "historical" else num(last["date"]))
            ax.plot(chip, JOB_MID, marker="s", ms=4.8, color=colour, mew=1.1, zorder=6,
                    clip_on=False, markerfacecolor="white" if pending else colour,
                    markeredgecolor=colour)


def draw_frame(ax, fr):
    sensing = fr.get("sensing") or []
    if not sensing:
        ax.text(0.5, 0.5, "no phase information (frame quarantined)", transform=ax.transAxes,
                ha="center", va="center", color=FAILED, fontsize=11)
        return

    draw_blackout(ax, fr)
    draw_gaps(ax, sensing)

    for e in sensing:
        lo, hi = STATUS_SPAN[e["status"]]
        ax.vlines(d(e["date"]), lo, hi, color=STATUS_COLOR[e["status"]],
                  lw=STATUS_LW[e["status"]], zorder=3)
        if e["lineage_start"]:
            # Every historical phase begins a compressed CSLC lineage at its first date.
            # The post-gap ones additionally ABANDON the previous one -- the event
            # lineage_transitions records -- marked with a cut line rather than a
            # different fill, because fill already means "published" on the boundary
            # triangle and one figure cannot have it mean two things.
            ax.plot(d(e["date"]), 0.99, marker="v", ms=7, color=LINEAGE,
                    markerfacecolor=LINEAGE, zorder=6, clip_on=False)
            if e.get("lineage_reset"):
                ax.vlines(d(e["date"]), 0.10, 1.0, color=LINEAGE, lw=1.1,
                          linestyle=(0, (3, 3)), zorder=2)

    draw_jobs(ax, fr, sensing)

    # One x per failed RUN of dates, not per date -- the unit of failure is the job,
    # which owns a whole k-set. Fifteen overlapping markers just make noise.
    run = []
    for e in sensing + [None]:
        if e is not None and e["status"] == "failed":
            run.append(e)
            continue
        if run:
            a, b = num(run[0]["date"]), num(run[-1]["date"])
            ax.plot(a + (b - a) / 2, 0.99, marker="x", ms=9, mew=2.4,
                    color=FAILED, zorder=6, clip_on=False)
            run = []

    # Phase name under its own span. Adjacent short phases collide on a long axis, so
    # drop every colliding label to a second row rather than overprinting.
    span = num(sensing[-1]["date"]) - num(sensing[0]["date"])
    rows, last = [], []
    for ph in fr["phases"]:
        seg = [e for e in sensing if e["phase"] == ph["label"]]
        if not seg:
            continue
        a, b = num(seg[0]["date"]), num(seg[-1]["date"])
        mid = a + (b - a) / 2
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
        ax.text(mid, -0.055 - 0.055 * row, label, ha="center", va="bottom", fontsize=8.6,
                color=col, fontweight="bold")


def legend_handles(has_blackout):
    handles = [
        Line2D([0], [0], color=DONE, lw=2.4, label="published (product exists)"),
        Line2D([0], [0], color=RUNNING, lw=2.4, label="running"),
        Line2D([0], [0], color=FAILED, lw=3.2, label="FAILED — no product"),
        Line2D([0], [0], color=PENDING, lw=1.6, label="pending"),
        Line2D([0], [0], color=SKIP, lw=1.8, label="no_run (never expected)"),
        Patch(facecolor=GAP, edgecolor="#dcdfe4", hatch="///",
              label="multi-year acquisition gap"),
        Line2D([0], [0], color=MUTED, lw=1.8, marker="s", ms=4.6, markevery=[1],
               label="job row: bracket = one k-set job, chip = one job"),
        Line2D([0], [0], color=LINEAGE, lw=0, marker="v", ms=7,
               label="lineage start (dashed rule = reset after a gap)"),
        Line2D([0], [0], color=LINEAGE, lw=0, marker="^", ms=6.5,
               label="k-set boundary (filled = compressed CSLC published)"),
    ]
    if has_blackout:
        handles.insert(5, Patch(facecolor=BLACKOUT, label="blackout season (bottom rail)"))
    return handles


def main():
    status = json.load(open(sys.argv[1]))
    out = sys.argv[2] if len(sys.argv) > 2 else "campaign_status.png"
    frames = status["frames"]

    # Header and legend are laid out in INCHES converted to figure fractions. Fixed
    # fractions are tuned to one figure height and collide on any other -- a one-frame
    # campaign overprinted its own subtitle.
    height = 3.1 * len(frames) + 2.4
    fig, axes = plt.subplots(len(frames), 1, figsize=(15.5, height),
                             squeeze=False, sharex=True)
    axes = [a[0] for a in axes]
    fig.patch.set_facecolor("white")

    for ax, fr in zip(axes, frames):
        draw_frame(ax, fr)
        title = ("Frame %s  —  %d/%d products (%d%%), %d/%d compressed CSLCs, "
                 "%d/%d jobs done, %d bursts"
                 % (fr["frame"], fr["products"], fr["expected"], fr["pct"],
                    fr["ccslc"], fr["ccslc_expected"], fr["units_done"],
                    fr["units_total"], fr["bursts"]))
        ax.set_title(title, loc="left", fontsize=11, fontweight="bold", color=INK, pad=12)
        if fr.get("stuck"):
            ax.set_title("   STUCK on %s" % (fr.get("blocked_on") or "a failed job"),
                         loc="right", fontsize=10.5, fontweight="bold", color=FAILED,
                         pad=12)
        ax.set_ylim(-0.16, 1.06)
        ax.set_yticks([])
        for s in ("left", "right", "top"):
            ax.spines[s].set_visible(False)
        ax.spines["bottom"].set_visible(False)
        ax.grid(axis="x", color=GRID, lw=0.8, zorder=0)

    axes[-1].xaxis.set_major_locator(mdates.YearLocator())
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
    axes[-1].tick_params(labelsize=10, colors=MUTED, pad=14)

    has_blackout = any(fr.get("blackout") for fr in frames)
    fig.legend(handles=legend_handles(has_blackout), loc="upper center",
               bbox_to_anchor=(0.5, 1.30 / height), ncol=3, frameon=False, fontsize=9.3)

    pct = int(status["products"] / status["products_expected"] * 100) \
        if status["products_expected"] else 0
    fig.suptitle("%s — %d/%d products (%d%%), %d/%d compressed CSLCs"
                 % (status["label"], status["products"], status["products_expected"],
                    pct, status["ccslc"], status["ccslc_expected"]),
                 x=0.007, ha="left", fontsize=13.5, fontweight="bold", color=INK,
                 y=1 - 0.30 / height)

    sub = ("Ticks are accountability state, not phase. Expectation is the processing-mode "
           "burst database; state is product existence and job status.")
    if status.get("stuck_frames"):
        sub += "   STUCK: frame(s) %s" % ", ".join(str(f) for f in status["stuck_frames"])
    fig.text(0.007, 1 - 0.58 / height, sub, fontsize=9.6,
             color=FAILED if status.get("stuck_frames") else MUTED, ha="left")

    # Which ancillary files this picture was computed from. Both drift independently of
    # the campaign, and a band drawn or not drawn changes how an empty stretch reads.
    prov = status.get("provenance") or {}
    bo = prov.get("blackout")
    line = "burst database: %s     blackout: %s" % (
        prov.get("burst_db", "unknown"),
        bo if bo else "NOT AVAILABLE — no rails drawn, absence here means nothing")
    fig.text(0.007, 1 - 0.80 / height, line, fontsize=8.4,
             color=FAILED if not bo else MUTED, ha="left")

    fig.tight_layout(rect=[0, 1.60 / height, 1, 1 - 0.98 / height], h_pad=2.6)
    fig.savefig(out, dpi=170, facecolor="white")
    print("wrote", out)


if __name__ == "__main__":
    main()
