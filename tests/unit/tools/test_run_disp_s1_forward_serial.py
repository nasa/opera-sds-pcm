"""How the serialized forward driver resolves a date whose cycle state config can never yield a k-cycle state config.

The k-cycle evaluator rule fires only on a complete CSC, so a date the consistent burst database excludes (a pass that
covers only part of the frame) or a blacked-out date never gets a KSC. The driver must treat those as terminal no-fires
at once rather than wait out the KSC timeout and stop the walk.
"""

from unittest.mock import MagicMock, patch

import pytest

import tools.run_disp_s1_forward_serial as fwd

FRAME = 9154
DATE = 20260120


@pytest.fixture(autouse=True)
def no_sleep():
    # A terminal answer must come from the first poll; any sleep means the driver kept waiting.
    with patch.object(fwd.time, "sleep", side_effect=AssertionError("driver waited instead of resolving the date")):
        yield


def resolve(csc, ksc=None, ksc_timeout_s=600):
    with patch.object(fwd, "get_ksc", return_value=ksc), patch.object(fwd, "get_csc", return_value=csc):
        return fwd.wait_for_disposition(MagicMock(), FRAME, DATE, ksc_timeout_s, poll_s=30, incomplete_settle_s=300)


def test_db_excluded_csc_is_a_terminal_no_fire():
    disp, meta = resolve({"db_excluded": True, "blackout": False})
    assert disp == "no-fire-db-excluded"
    assert meta is None
    assert disp in fwd.NOFIRE


def test_blacked_out_csc_is_still_a_terminal_no_fire():
    disp, _ = resolve({"blackout": True})
    assert disp == "no-fire-blackout"


def test_an_ordinary_csc_without_a_ksc_keeps_waiting_until_the_timeout():
    # zero timeout: the loop never polls, and the final snapshot still has no KSC
    disp, _ = resolve({"db_excluded": False, "blackout": False}, ksc_timeout_s=0)
    assert disp == "fail-no-ksc"


def test_a_ksc_still_wins_over_the_csc_flags():
    fired = {"is_complete": True, "compressed_cslc_final": True, "save_compressed_cslc": False, "region_id": "0"}
    with patch.object(fwd, "WHITELIST", None):
        disp, meta = resolve({"db_excluded": True}, ksc=fired)
    assert disp == "fire"
    assert meta is fired


def test_get_csc_reads_the_exclusion_flag():
    es = MagicMock()
    es.es.search.return_value = {"hits": {"hits": [{"_source": {"metadata": {"db_excluded": True}}}]}}
    assert fwd.get_csc(es, FRAME, DATE) == {"db_excluded": True}
    assert "metadata.db_excluded" in es.es.search.call_args.kwargs["body"]["_source"]


def test_wait_for_date_advances_past_an_excluded_date():
    with patch.object(fwd, "wait_for_disposition", return_value=("no-fire-db-excluded", None)), \
         patch.object(fwd, "l3_count", return_value=0):
        r = fwd.wait_for_date(MagicMock(), FRAME, fwd.datetime(2026, 1, 20), base_l3=0, base_ccslc=0,
                              mode="boundary-serial", ksc_timeout_s=600, l3_timeout_s=600, ccslc_timeout_s=600,
                              poll_s=30, incomplete_settle_s=300)
    assert r["ok"] is True
    assert r["fired"] is False
    assert r["disp"] == "no-fire-db-excluded"


def test_skipped_csc_is_a_terminal_no_fire():
    disp, meta = resolve({"skipped": True, "skip_reason": "bursts never imaged"})
    assert disp == "no-fire-skipped"
    assert meta is None
    assert disp in fwd.NOFIRE


def test_skip_wins_over_a_stale_ksc_written_before_it():
    # A KSC that was waiting on its window when the date was skipped never fires.
    stale = {"is_complete": False, "compressed_cslc_final": False, "cycles_complete": 2, "cycles_expected": 15,
             "completeness_reason": "K-window incomplete"}
    disp, _ = resolve({"skipped": True}, ksc=stale)
    assert disp == "no-fire-skipped"


def test_get_csc_reads_the_skip_flag():
    es = MagicMock()
    es.es.search.return_value = {"hits": {"hits": [{"_source": {"metadata": {"skipped": True}}}]}}
    assert fwd.get_csc(es, FRAME, DATE) == {"skipped": True}
    assert "metadata.skipped" in es.es.search.call_args.kwargs["body"]["_source"]
