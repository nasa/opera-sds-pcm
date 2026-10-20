"""The tools that read a burst coverage JSONL must honour chunk_error and unverified records.

An audit that skipped a chunk or could not verify a burst exits 2; the status, query and
DISP-S1 completeness readers must say so too, and never put an unverified burst's SLC on the
reprocessing list.
"""
import json
import subprocess
import sys
from pathlib import Path

import pytest

TOOLS = Path(__file__).resolve().parents[5] / "tools" / "ops" / "cmr_audit"  # repo root
SLC_MISSING = "S1A_IW_SLC__1SDV_20260623T133605_20260623T133632_065097_083497_2D92-SLC"
SLC_UNVERIFIED = "S1A_IW_SLC__1SDV_20260623T133540_20260623T133607_065097_083497_5CE6-SLC"
SLC_FOUND = "S1A_IW_SLC__1SDV_20260623T133515_20260623T133543_065097_083497_DD3F-SLC"


def _burst(pattern, slc, **extra):
    opera = pattern.replace("-", "_")
    return {"burst_id": opera, "burst_pattern": pattern, "acquisition_time": "2026-06-23T13:36:10+00:00",
            "platform": "S1A", "polarization": "VV", "slc_native_id": slc, **extra}


@pytest.fixture
def report(tmp_path):
    found = [_burst("T100-213513-IW1", SLC_FOUND, opera_product_id="OPERA_L2_CSLC-S1_T100-213513-IW1_x"),
             _burst("T100-213513-IW2", SLC_FOUND, opera_product_id="OPERA_L2_CSLC-S1_T100-213513-IW2_x")]
    missing = [_burst("T100-213518-IW2", SLC_MISSING)]
    unverified = [_burst("T100-213515-IW2", SLC_UNVERIFIED, reason="CMR status 500 after 3 attempts")]
    records = [
        {"_type": "metadata", "start_datetime": "2026-06-23T00:00:00+00:00", "end_datetime": "2026-06-25T00:00:00+00:00",
         "chunk_days": 1},
        {"_type": "chunk_result", "chunk_start": "2026-06-23T00:00:00+00:00", "chunk_end": "2026-06-24T00:00:00+00:00",
         "product_type": "CSLC-S1", "found_count": 2, "missing_count": 1, "unverified_count": 1, "slc_count": 3,
         "found": found, "missing": missing, "unverified": unverified},
        {"_type": "chunk_error", "chunk_start": "2026-06-24T00:00:00+00:00", "chunk_end": "2026-06-25T00:00:00+00:00",
         "error": "CMR SLC listing incomplete: 70 of 441 items returned"},
        {"_type": "summary", "total_slcs": 3, "failed_chunks": [{"chunk_start": "2026-06-24T00:00:00+00:00"}],
         "unverified_days": ["2026-06-23"], "products": {}},
    ]
    path = tmp_path / "coverage.jsonl"
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n")
    return path


def _run(script, *args):
    out = subprocess.run([sys.executable, str(TOOLS / script), *map(str, args)], capture_output=True, text=True)
    assert out.returncode == 0, out.stderr
    return out.stdout, out.stderr


# --- burst_coverage_status.py ------------------------------------------------------------------------

def test_status_counts_the_skipped_chunk_in_progress_and_reports_it(report):
    stdout, _ = _run("burst_coverage_status.py", report)
    assert "Progress:   2/2 chunks (100.0%), 1 skipped" in stdout
    assert "Skipped chunks" in stdout and "2026-06-24 to 2026-06-25: CMR SLC listing incomplete" in stdout
    assert "Unverified: 1" in stdout
    assert "Coverage:   66.67% (of verified bursts)" in stdout


def test_status_keeps_unverified_slcs_off_the_reprocess_list(report, tmp_path):
    out = tmp_path / "slcs.txt"
    stdout, _ = _run("burst_coverage_status.py", report, "--show-slcs", "--output-slcs", out)
    assert out.read_text().splitlines() == [SLC_MISSING]
    assert "SLCs with unverified bursts only (1 unique" in stdout and SLC_UNVERIFIED in stdout
    assert "Not written: 1 SLCs with unverified bursts only" in stdout


def test_status_by_day_shows_an_unverified_column(report):
    stdout, _ = _run("burst_coverage_status.py", report, "--by-day")
    row = next(line for line in stdout.splitlines() if "2026-06-23" in line and "%" in line)
    assert row.split()[1:5] == ["2026-06-23", "2", "1", "1"]


# --- burst_coverage_query.py -------------------------------------------------------------------------

def test_query_shows_unverified_bursts_for_an_slc(report):
    stdout, _ = _run("burst_coverage_query.py", report, "5CE6")
    assert "1 unverified" in stdout
    assert "? UNVERIF" in stdout
    assert "Unverified burst IDs (CMR gave no complete answer" in stdout
    assert "T100_213515_IW2" in stdout and "CMR status 500" in stdout


def test_query_list_and_json_include_unverified(report):
    stdout, _ = _run("burst_coverage_query.py", report, "5CE6", "--list-slcs")
    assert "1 UNVERIFIED" in stdout
    stdout, _ = _run("burst_coverage_query.py", report, "5CE6", "--json")
    data = json.loads(stdout)["slcs"][SLC_UNVERIFIED]
    assert data["unverified_count"] == 1 and data["unverified"][0]["burst_id"] == "T100_213515_IW2"


# --- disp_frame_completeness.py ----------------------------------------------------------------------

@pytest.fixture
def consistent_db(tmp_path):
    db = {"data": {"26690": {"burst_id_list": ["t100_213513_iw1", "t100_213513_iw2", "t100_213515_iw2", "t100_213518_iw2"],
                            "sensing_time_list": ["2026-06-23T13:36:10"]}}}
    path = tmp_path / "cbdb.json"
    path.write_text(json.dumps(db))
    return path


def test_completeness_reports_unverified_apart_from_missing(report, consistent_db, tmp_path):
    out = tmp_path / "reprocess.txt"
    stdout, stderr = _run("disp_frame_completeness.py", consistent_db, report, "--k", "1", "--show-slcs", "--verbose",
                          "--output-slcs", out)
    assert "Unverified: 1 burst/date combinations" in stderr
    assert "SLCs needed for reprocessing: 1 unique" in stdout
    assert "SLCs unverified, re-check before deciding: 1 unique" in stdout
    assert "1 missing, 1 unverified" in stdout
    assert out.read_text().splitlines() == [SLC_MISSING]
    unverified_file = tmp_path / "reprocess-unverified.txt"
    assert unverified_file.read_text().splitlines() == [SLC_UNVERIFIED]
    assert "Total unique SLCs unverified" in stderr


def test_completeness_json_carries_unverified_ids(report, consistent_db):
    stdout, _ = _run("disp_frame_completeness.py", consistent_db, report, "--k", "1", "--json")
    frame = json.loads(stdout)[0]
    sr = frame["sensing_results"][0]
    assert sr["missing_slc_ids"] == [SLC_MISSING]
    assert sr["unverified_slc_ids"] == [SLC_UNVERIFIED]
    assert sr["unverified_bursts"] == 1 and sr["unknown_bursts"] == 0
    assert frame["k_cycles"][0]["unverified_slc_ids"] == [SLC_UNVERIFIED]
    assert frame["unverified_sensing_times"] == 1
