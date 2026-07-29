"""Unit tests for the DISP-S1 CCSLC-rotation projection (pure logic).

Covers the out-of-order premature-finalization bug plus the cases that could
otherwise strand a KSC on a CCSLC that never publishes (aged-out, supersession,
greenfield). Stdlib-only target; no ES / PCM environment needed.
"""
from datetime import datetime, timedelta

from data_subscriber.cslc.disp_s1_rotation import (
    compute_projected_pending_boundaries as proj,
)

K, M = 15, 3  # m-1 = 2: a KSC compresses the 2 most-recent CCSLCs
_START = datetime(2016, 10, 20)
DATES = [(_START + timedelta(days=12 * i)).strftime("%Y%m%d") for i in range(60)]
B1, B2, B3 = DATES[14], DATES[29], DATES[44]  # CCSLC1/2/3 last_dates


def test_out_of_order_bug_marks_missing_boundary_pending():
    # KSC between B2 and B3, only CCSLC1 published -> must wait for B2.
    assert proj(DATES, {B1}, K, M, DATES[35]) == [B2]


def test_steady_state_no_pending():
    assert proj(DATES, {B1, B2}, K, M, DATES[35]) == []


def test_aged_out_boundary_not_pending():
    # Far-ahead KSC: 2 most-recent boundaries are B2,B3; B1 aged out -> not
    # returned, so it can never strand the KSC on an unpublishable CCSLC.
    assert proj(DATES, {B1}, K, M, DATES[55]) == sorted([B2, B3])


def test_supersession_via_published_not_pending():
    # A superseded boundary is superseded because its CCSLC exists (published)
    # -> excluded from pending.
    assert proj(DATES, {B1, B2}, K, M, DATES[35]) == []


def test_greenfield_no_anchor_returns_empty():
    assert proj(DATES, set(), K, M, DATES[10]) == []


def test_m1_requires_no_compressed_cslcs():
    assert proj(DATES, {B1}, K, 1, DATES[35]) == []


def test_missed_acquisition_boundary_tracks_count():
    # Drop a non-boundary date; the boundary is the 15th acquisition after the
    # anchor (positional), a different calendar date -- correct DISP-S1 rule.
    dates_gap = DATES[:20] + DATES[21:]
    assert proj(dates_gap, {B1}, K, M, dates_gap[34]) == [dates_gap[14 + K]]
