#!/usr/bin/env python3
"""Render a DISP-S1 forward ledger: one lane per frame, one mark per sensing date coloured by its outcome.

Lanes are grouped by track. Frames that start years apart (a catch-up that starts each
frame where its earlier work ended) are drawn in separate panels, and a panel's time axis is
cut into the periods in which its frames have dates, so the empty years are not drawn.
Each k-boundary is marked under its date (filled once its CCSLC set has published), and the
right margin says, per frame, how many L3s it has, what is waiting, and where and why it
stopped.

Consumes the JSON written by disp_s1_forward_ledger.py --json, so it runs anywhere with
matplotlib and does not have to run on Mozart:

    # on Mozart
    python tools/disp_s1_forward_ledger.py --region 0 --cmr --json ledger.json
    # anywhere
    python tools/plot_disp_s1_forward_ledger.py ledger.json ledger.png
"""

import json
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta

# Reference palette (validated light-mode slots): blue = produced, green = produced by the historical
# SCIFLO, gray = no product by design, amber = needs an operator, red = unexplained.
SURFACE, INK, INK2, GRID, BAND = "#fcfcfb", "#0b0b0b", "#52514e", "#e4e2dc", "#f3f2ee"
GRAY, MUTED, BLUE, GREEN = "#c9c7bf", "#9a988f", "#2a78d6", "#1baf7a"
WARNING, CRITICAL = "#fab219", "#d03b3b"

# outcome -> (marker, face, edge, size, z, legend label)
STYLE = {
    "produced": ("o", BLUE, BLUE, 34, 4, "L3 produced (forward)"),
    "produced (batch forward)": ("o", GREEN, GREEN, 34, 4, "L3 produced (forward SCIFLO, batch forward phase)"),
    "produced (historical)": ("s", GREEN, GREEN, 26, 4, "L3 produced (historical SCIFLO)"),
    "in progress": ("o", SURFACE, BLUE, 34, 4, "fired, L3 in progress"),
    "in progress (historical)": ("s", SURFACE, GREEN, 26, 4, "historical SCIFLO in progress"),
    "superseded": ("o", GRAY, GRAY, 14, 2, "superseded (no forward product)"),
    "excluded (burst DB)": ("x", INK2, INK2, 22, 3, "excluded by burst DB / blackout"),
    "blackout": ("x", INK2, INK2, 22, 3, None),
    "historical date (burst DB)": ("x", GRAY, GRAY, 16, 2, "partial CSC on a historical date (not used)"),
    "skipped": ("X", SURFACE, WARNING, 70, 6, "skipped by operator (reason recorded)"),
    "partial (passed)": ("x", MUTED, MUTED, 22, 3, "partial date, already passed"),
    "stopped": ("X", WARNING, INK, 90, 6, "stopped: partial date"),
    "behind a stop": ("x", WARNING, WARNING, 20, 3, "behind a stop (no KSC yet)"),
    "held behind a gap": ("o", SURFACE, WARNING, 22, 3, "held behind a partial date"),
    "window filling": ("o", SURFACE, MUTED, 16, 2, "waiting (window, CCSLC, inputs, catalog)"),
    "awaiting CCSLC": ("o", SURFACE, MUTED, 16, 2, None),
    "awaiting inputs": ("o", SURFACE, MUTED, 16, 2, None),
    "pending": ("o", SURFACE, MUTED, 16, 2, None),
    "not whitelisted": ("s", SURFACE, INK2, 20, 3, "region not whitelisted"),
    "UNEXPLAINED": ("D", CRITICAL, INK, 60, 7, "UNEXPLAINED"),
}
PRODUCED = ("produced", "produced (batch forward)", "produced (historical)")
# dates further apart than this are not drawn on one continuous time axis
PANEL_GAP_DAYS = 120
WAITING = ("in progress", "in progress (historical)", "window filling", "awaiting CCSLC", "awaiting inputs", "pending")


def d(s):
    return datetime.strptime(s, "%Y%m%d")


def short_cause(text):
    n = "".join(ch for ch in text.split(":", 1)[-1].split("burst")[0] if ch.isdigit())
    if text.startswith("OPS production gap"):
        return f"OPS CSLC gap ({n} bursts)"
    if text.startswith("acquisition gap"):
        return f"not imaged ({n} bursts)"
    if text.startswith("filled since"):
        return "filled since"
    return text[:40]


def lanes_by_track(frames):
    """[(y, frame, track)] grouped by track, and [(track, first y, last y)]."""
    by_track = defaultdict(list)
    for f, info in frames.items():
        by_track[info.get("track")].append(int(f))
    lanes, spans, y = [], [], 0.0
    for track in sorted(by_track, key=lambda t: (t is None, t or 0)):
        y += 0.6
        y0 = y
        for f in sorted(by_track[track]):
            lanes.append((y, f, track))
            y += 1
        spans.append((track, y0, y - 1))
    return lanes, spans, y


def frame_label(info, counts, rows):
    bits = [f"{sum(counts.get(o, 0) for o in PRODUCED)} L3"]
    waiting = sum(counts.get(o, 0) for o in WAITING)
    if waiting:
        bits.append(f"{waiting} waiting")
    if counts.get("skipped"):
        bits.append(f"{counts['skipped']} skipped")
    stop = next((r for r in sorted(rows, key=lambda r: r["date"]) if r["outcome"] == "stopped"), None)
    if stop:
        cause = short_cause(stop.get("root_cause") or "") if stop.get("root_cause") else "partial"
        bits.append(f"stop {stop['date'][:4]}-{stop['date'][4:6]}-{stop['date'][6:]}: {cause}")
    if counts.get("behind a stop"):
        bits.append(f"{counts['behind a stop']} behind it")
    if counts.get("held behind a gap"):
        bits.append(f"{counts['held behind a gap']} held")
    if counts.get("UNEXPLAINED"):
        bits.append(f"{counts['UNEXPLAINED']} UNEXPLAINED")
    if not info.get("in_burst_db", True):
        bits = ["not in the burst database"]
    return " · ".join(bits)


def clusters(dates, gap_days=PANEL_GAP_DAYS):
    """[(first, last)] for runs of YYYYMMDD dates with no hole longer than gap_days."""
    out = []
    for x in sorted(set(dates)):
        if out and d(x) <= d(out[-1][1]) + timedelta(days=gap_days):
            out[-1][1] = x
        else:
            out.append([x, x])
    return [tuple(c) for c in out]


def panels_by_period(frames, rows_by_frame, gap_days=PANEL_GAP_DAYS):
    """[([frame], [(first date, last date)])]: one panel per group of frames that start near each other.

    A catch-up starts each frame where its earlier work ended, so the frames of one run can
    start years apart, and one frame can have dates years apart (forward dates at its start,
    a ministack after a large gap). On one time axis most of the figure would be the empty
    years in between. Frames whose first dates lie within gap_days of each other share a
    panel, and a panel's time axis is cut into the periods in which its frames have dates.
    A frame with no dates yet goes in the latest panel.
    """
    first = {int(f): min(r["date"] for r in rows_by_frame[int(f)]) for f in frames if rows_by_frame.get(int(f))}
    panels = []
    for lo, hi in clusters(first.values(), gap_days):
        fs = sorted(f for f, x in first.items() if lo <= x <= hi)
        panels.append((fs, clusters((r["date"] for f in fs for r in rows_by_frame[f]), gap_days)))
    idle = sorted(int(f) for f in frames if int(f) not in first)
    if idle:
        if not panels:
            panels.append(([], []))
        panels[-1][0].extend(idle)
    return panels


def main(path, out):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D

    L = json.load(open(path))
    rows, frames = L["rows"], L["frames"]
    gen = L["generated"][:16].replace("T", " ") + "Z"
    rows_by_frame = defaultdict(list)
    for r in rows:
        rows_by_frame[int(r["frame"])].append(r)
    until = d(L["until"]) if L.get("until") else (max(d(r["date"]) for r in rows) if rows else datetime.now())
    end = d(L["burst_db_assessed_end"]) if L.get("burst_db_assessed_end") else None
    pad, near = timedelta(days=12), timedelta(days=PANEL_GAP_DAYS)

    # one panel per group of frames, stacked; heights in inches so lanes keep their size
    panels = []
    for fs, periods in panels_by_period(frames, rows_by_frame):
        lanes, spans, height = lanes_by_track({str(f): frames[str(f)] for f in fs})
        periods = [[d(lo), d(hi)] for lo, hi in periods] or [[until - timedelta(days=180), until]]
        panels.append((periods, lanes, spans, height, 0.4 + 0.34 * max(len(lanes), 1)))
    # the ledger's until date is drawn on the latest panel when that panel's dates reach near it
    if panels and until <= panels[-1][0][-1][1] + near:
        panels[-1][0][-1][1] = max(until, panels[-1][0][-1][1])
    top, bottom, between = 2.1, 0.6, 0.85
    left, width, cut = 0.08, 0.70, 0.012
    fig_h = top + sum(p[-1] for p in panels) + between * (len(panels) - 1) + bottom
    fig = plt.figure(figsize=(16, fig_h))
    fig.patch.set_facecolor(SURFACE)

    y_in = fig_h - top
    for periods, lanes, spans, height, h_in in panels:
        y_in -= h_in
        ypos = {f: yy for yy, f, _ in lanes}
        # the panel's periods side by side, each as wide as it is long, none thinner than a tenth
        days = [(hi - lo).days + 24 for lo, hi in periods]
        share = [max(n / sum(days), 0.1) for n in days]
        avail = width - cut * (len(periods) - 1)
        x_fig = left
        for j, (lo, hi) in enumerate(periods):
            w = avail * share[j] / sum(share)
            ax = fig.add_axes([x_fig, y_in / fig_h, w, h_in / fig_h])
            x_fig += w + cut
            x0, x1 = lo - pad, hi + pad
            ax.set_facecolor(SURFACE)
            ax.set_xlim(x0, x1)
            for _, ya, yb in spans[1::2]:
                ax.axhspan(ya - 0.5, yb + 0.5, color=BAND, zorder=0, lw=0)
            ax.grid(axis="x", color=GRID, lw=0.6, zorder=0)
            for sp in ("top", "right", "left"):
                ax.spines[sp].set_visible(False)
            ax.spines["bottom"].set_color(GRID)
            ax.tick_params(colors=INK2, labelsize=9, length=0)
            ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=2, maxticks=max(3, round(10 * w / width))))
            ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ax.xaxis.get_major_locator()))
            if x0 < until < x1:
                ax.axvline(until, color=INK2, lw=0.8, ls=(0, (2, 2)), zorder=1)
                ax.text(until, -0.1, f" until\n {until:%Y-%m-%d}", fontsize=8, color=INK2, va="bottom")
            if end and x0 < end < x1:
                ax.axvline(end, color=INK2, lw=0.8, ls=(0, (1, 3)), zorder=1)
                ax.text(end, -0.1, "burst DB \nsurvey end ", fontsize=8, color=INK2, va="bottom", ha="right")
            if j:                                           # the time axis is cut here
                ax.spines["left"].set_visible(True)
                ax.spines["left"].set_color(INK2)
                ax.spines["left"].set_linestyle((0, (1, 2)))

            for f in ypos:
                for r in rows_by_frame[f]:
                    x = d(r["date"])
                    if r["outcome"] not in STYLE or not lo <= x <= hi:
                        continue
                    mk, fc, ec, sz, z, _ = STYLE[r["outcome"]]
                    if mk == "x":
                        ax.scatter([x], [ypos[f]], marker=mk, s=sz, c=fc, linewidths=1.0, zorder=z)
                    else:
                        ax.scatter([x], [ypos[f]], marker=mk, s=sz, facecolors=fc, edgecolors=ec, linewidths=1.0,
                                   zorder=z)
                    if r.get("boundary"):
                        done = r.get("boundary_ok", r["outcome"] == "produced")
                        ax.scatter([x], [ypos[f] - 0.42], marker="v", s=22, facecolors=INK if done else SURFACE,
                                   edgecolors=INK, linewidths=0.8, zorder=5)
                if j == len(periods) - 1:
                    info = frames[str(f)]
                    counts = (info.get("summary") or {}).get("counts", {})
                    ax.text(x1, ypos[f], "  " + frame_label(info, counts, rows_by_frame[f]), fontsize=7.5,
                            va="center", ha="left", color=CRITICAL if counts.get("UNEXPLAINED") else INK2,
                            clip_on=False)

            ax.set_yticks([yy for yy, _, _ in lanes])
            ax.set_yticklabels([f"{f}" + (f"  T{t:03d}" if t is not None else "") for _, f, t in lanes] if not j
                               else [], fontsize=8.5, color=INK)
            ax.set_ylim(height, -0.4)
            if not j and (len(panels) > 1 or len(periods) > 1):
                when = ", ".join(f"{a:%Y-%m-%d} to {b:%Y-%m-%d}" for a, b in periods)
                ax.set_title(f"{len(lanes)} frame{'' if len(lanes) == 1 else 's'} · {when}", loc="left",
                             fontsize=10, fontweight="bold", color=INK, pad=14)
        y_in -= between

    handles = [Line2D([], [], marker=mk, ls="", markerfacecolor=fc, markeredgecolor=ec, markersize=7, label=lab)
               for mk, fc, ec, _, _, lab in STYLE.values() if lab]
    handles.append(Line2D([], [], marker="v", ls="", markerfacecolor=INK, markeredgecolor=INK, markersize=6,
                          label="k-boundary: CCSLC set published (hollow = pending)"))
    fig.legend(handles=handles, loc="upper center", ncol=4, frameon=False, fontsize=8.5,
               bbox_to_anchor=(0.5, 1 - 0.55 / fig_h))

    tot = Counter()
    for info in frames.values():
        s = info.get("summary") or {}
        tot.update(s.get("counts", {}))
        tot.update({k: s.get(k, 0) or 0 for k in ("l3_grq", "l3_s3", "l3_delivered")})
    fig.suptitle(f"DISP-S1 forward ledger at {gen}", fontsize=14, fontweight="bold", color=INK, x=0.5,
                 y=1 - 0.12 / fig_h)
    fig.text(0.5, 1 - 0.36 / fig_h,
             f"{sum(tot[o] for o in PRODUCED)} L3 produced ({tot['l3_grq']} in GRQ, "
             f"{tot['l3_s3']} in S3, {tot['l3_delivered']} delivered) · "
             f"{sum(tot[o] for o in WAITING)} waiting · {tot['skipped']} skipped · {tot['stopped']} stopped · "
             f"{tot['held behind a gap']} held · {tot['UNEXPLAINED']} unexplained",
             ha="center", fontsize=10, color=INK2)
    fig.savefig(out, dpi=150, facecolor=SURFACE)
    print(f"wrote {out}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2])
