#!/usr/bin/env python3
"""Render a DISP-S1 forward ledger: one lane per frame, one mark per sensing date coloured by its outcome.

Lanes are grouped by track. Each k-boundary is marked under its date (filled once its CCSLC
set has published), and the right margin says, per frame, how many L3s it has, what is
waiting, and where and why it stopped.

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
    "superseded": ("o", GRAY, GRAY, 14, 2, "superseded (no forward product)"),
    "excluded (burst DB)": ("x", INK2, INK2, 22, 3, "excluded by burst DB / blackout"),
    "blackout": ("x", INK2, INK2, 22, 3, None),
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
WAITING = ("in progress", "window filling", "awaiting CCSLC", "awaiting inputs", "pending")


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


def main(path, out):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.dates as mdates
    import matplotlib.pyplot as plt
    from matplotlib.lines import Line2D

    L = json.load(open(path))
    rows, frames = L["rows"], L["frames"]
    gen = L["generated"][:16].replace("T", " ") + "Z"
    lanes, spans, height = lanes_by_track(frames)
    ypos = {f: yy for yy, f, _ in lanes}
    rows_by_frame = defaultdict(list)
    for r in rows:
        rows_by_frame[int(r["frame"])].append(r)

    dates = [d(r["date"]) for r in rows]
    until = d(L["until"]) if L.get("until") else (max(dates) if dates else datetime.now())
    x0 = (min(dates) if dates else until - timedelta(days=180)) - timedelta(days=12)
    x1 = max([until] + dates) + timedelta(days=12)

    fig, ax = plt.subplots(figsize=(16, 2.6 + 0.34 * max(len(lanes), 1)))
    fig.patch.set_facecolor(SURFACE)
    ax.set_facecolor(SURFACE)
    ax.set_xlim(x0, x1)
    for _, ya, yb in spans[1::2]:
        ax.axhspan(ya - 0.5, yb + 0.5, color=BAND, zorder=0, lw=0)
    ax.grid(axis="x", color=GRID, lw=0.6, zorder=0)
    for s in ("top", "right", "left"):
        ax.spines[s].set_visible(False)
    ax.spines["bottom"].set_color(GRID)
    ax.tick_params(colors=INK2, labelsize=9, length=0)
    ax.xaxis.set_major_locator(mdates.AutoDateLocator(minticks=4, maxticks=10))
    ax.xaxis.set_major_formatter(mdates.ConciseDateFormatter(ax.xaxis.get_major_locator()))
    ax.axvline(until, color=INK2, lw=0.8, ls=(0, (2, 2)), zorder=1)
    ax.text(until, -0.1, f" until\n {until:%Y-%m-%d}", fontsize=8, color=INK2, va="bottom")
    if L.get("burst_db_assessed_end"):
        end = d(L["burst_db_assessed_end"])
        if x0 < end < x1:
            ax.axvline(end, color=INK2, lw=0.8, ls=(0, (1, 3)), zorder=1)
            ax.text(end, -0.1, " burst DB\n survey end", fontsize=8, color=INK2, va="bottom")

    for r in rows:
        f = int(r["frame"])
        if f not in ypos or r["outcome"] not in STYLE:
            continue
        mk, fc, ec, sz, z, _ = STYLE[r["outcome"]]
        x = d(r["date"])
        if mk == "x":
            ax.scatter([x], [ypos[f]], marker=mk, s=sz, c=fc, linewidths=1.0, zorder=z)
        else:
            ax.scatter([x], [ypos[f]], marker=mk, s=sz, facecolors=fc, edgecolors=ec, linewidths=1.0, zorder=z)
        if r.get("boundary"):
            done = r.get("boundary_ok", r["outcome"] == "produced")
            ax.scatter([x], [ypos[f] - 0.42], marker="v", s=22, facecolors=INK if done else SURFACE,
                       edgecolors=INK, linewidths=0.8, zorder=5)

    for f, info in frames.items():
        f = int(f)
        if f not in ypos:
            continue
        counts = (info.get("summary") or {}).get("counts", {})
        ax.text(x1, ypos[f], "  " + frame_label(info, counts, rows_by_frame[f]), fontsize=7.5, va="center",
                ha="left", color=CRITICAL if counts.get("UNEXPLAINED") else INK2, clip_on=False)

    ax.set_yticks([yy for yy, _, _ in lanes])
    ax.set_yticklabels([f"{f}" + (f"  T{t:03d}" if t is not None else "") for _, f, t in lanes],
                       fontsize=8.5, color=INK)
    ax.set_ylim(height, -0.4)

    handles = [Line2D([], [], marker=mk, ls="", markerfacecolor=fc, markeredgecolor=ec, markersize=7, label=lab)
               for mk, fc, ec, _, _, lab in STYLE.values() if lab]
    handles.append(Line2D([], [], marker="v", ls="", markerfacecolor=INK, markeredgecolor=INK, markersize=6,
                          label="k-boundary: CCSLC set published (hollow = pending)"))
    fig.legend(handles=handles, loc="upper center", ncol=4, frameon=False, fontsize=8.5,
               bbox_to_anchor=(0.5, 1 - 0.55 / fig.get_figheight()))

    tot = Counter()
    for info in frames.values():
        s = info.get("summary") or {}
        tot.update(s.get("counts", {}))
        tot.update({k: s.get(k, 0) or 0 for k in ("l3_grq", "l3_s3", "l3_delivered")})
    fig.suptitle(f"DISP-S1 forward ledger at {gen}", fontsize=14, fontweight="bold", color=INK, x=0.5,
                 y=1 - 0.12 / fig.get_figheight())
    fig.text(0.5, 1 - 0.36 / fig.get_figheight(),
             f"{sum(tot[o] for o in PRODUCED)} L3 produced ({tot['l3_grq']} in GRQ, "
             f"{tot['l3_s3']} in S3, {tot['l3_delivered']} delivered) · "
             f"{sum(tot[o] for o in WAITING)} waiting · {tot['skipped']} skipped · {tot['stopped']} stopped · "
             f"{tot['held behind a gap']} held · {tot['UNEXPLAINED']} unexplained",
             ha="center", fontsize=10, color=INK2)
    fig.subplots_adjust(left=0.08, right=0.78, top=1 - 1.6 / fig.get_figheight(), bottom=0.6 / fig.get_figheight())
    fig.savefig(out, dpi=150, facecolor=SURFACE)
    print(f"wrote {out}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        sys.exit(__doc__)
    main(sys.argv[1], sys.argv[2])
