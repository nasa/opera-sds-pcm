"""Batch proc CLI: collection choices and the validation and view of DISP-NI batch procs.

A gslc_query_hist batch proc is checked against the DISP-NI frame database before it is stored,
and that database is loaded only for such batch procs, so venues without one keep working.
"""
import argparse
import sys
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from data_subscriber.gslc.gslc_utils import process_disp_ni_frame_db

# pcm_batch reaches packages that only exist on-cluster.
for _name in ("tabulate", "hysds", "hysds.celery", "hysds_commons",
              "pcm_commons", "pcm_commons.query", "pcm_commons.query.ancillary_utility"):
    sys.modules.setdefault(_name, MagicMock())

# It also reads the deployed .sds config and the DISP-S1 burst database while
# being imported. Both are stubbed for the duration of the import only, so the
# real implementations stay in place for every other test in the session.
with patch("util.conf_util.SettingsConf", MagicMock()), \
        patch("data_subscriber.cslc_utils.localize_disp_frame_burst_hist",
              MagicMock(return_value=({}, {}, {}))), \
        patch("opera_commons.es_connection.get_grq_es", MagicMock()):
    from tools import pcm_batch


def _collection_choices():
    """The --collection-shortname choices, which live on the 'create' subcommand."""
    for action in pcm_batch.create_parser()._actions:
        if isinstance(action, argparse._SubParsersAction):
            for subparser in action.choices.values():
                for sub_action in subparser._actions:
                    if sub_action.dest == "collection":
                        return sub_action.choices
    raise AssertionError("no --collection-shortname argument found")


@pytest.mark.parametrize("collection", ["SENTINEL-1C_SLC", "SENTINEL-1D_SLC"])
def test_batch_procs_can_be_created_for_the_newer_s1_platforms(collection):
    """Without these, no batch proc -- and so no on-demand or historical SLC
    processing -- can be created for an S1C or S1D date range."""
    assert collection in _collection_choices()


def test_the_original_collections_are_still_accepted():
    choices = _collection_choices()
    for collection in ("HLSL30", "HLSS30", "SENTINEL-1A_SLC", "SENTINEL-1B_SLC"):
        assert collection in choices


DISP_NI_DB = str(Path(__file__).parents[1] / "data_subscriber" / "test_data" / "disp_ni_consistent_db.json")


def _gslc_proc(**overrides):
    proc = {"enabled": True, "label": "DISP-NI Historical", "processing_mode": "historical",
            "include_regions": "north_america_opera", "exclude_regions": "",
            "data_start_date": "2025-07-01T00:00:00", "data_end_date": "2026-10-01T00:00:00",
            "k": 15, "m": 6, "frames": [26410, [12419, 12419]], "wait_between_acq_cycles_mins": 10,
            "job_type": "gslc_query_hist", "job_queue": "opera-job_worker-gslc_data_query_hist",
            "download_job_queue": "opera-job_worker-gslc_data_download_hist", "chunk_size": 1}
    proc.update(overrides)
    return proc


@pytest.fixture
def disp_ni_frame_db(monkeypatch):
    loader = MagicMock(return_value=process_disp_ni_frame_db(DISP_NI_DB))
    monkeypatch.setattr(pcm_batch, "localize_disp_ni_frame_db", loader)
    return loader


def test_a_valid_disp_ni_batch_proc_is_accepted(disp_ni_frame_db):
    assert pcm_batch._validate_proc(_gslc_proc()) is True


@pytest.mark.parametrize("overrides, message", [
    ({"frames": [26410, 99999]}, "Frame 99999 not found in DISP-NI frame database"),
    ({"frames": [30000]}, "Frame 30000 has no sensing datetimes"),
    ({"frames": [[26411, 26410]]}, "Frame range must be in ascending order"),
    ({"k": None}, "k must be a positive integer"),
    ({"m": 0}, "m must be a positive integer"),
    ({"processing_mode": "forward"}, "supports historical processing only"),
], ids=["unknown_frame", "frame_without_dates", "bad_range", "no_k", "zero_m", "forward_mode"])
def test_an_invalid_disp_ni_batch_proc_is_rejected(disp_ni_frame_db, overrides, message):
    result = pcm_batch._validate_proc(_gslc_proc(**overrides))

    assert result is not True
    assert message in result


@pytest.mark.parametrize("job_type", ["cslc_query_hist", "hls_query"])
def test_the_disp_ni_frame_database_is_only_loaded_for_disp_ni_batch_procs(disp_ni_frame_db, job_type):
    pcm_batch._validate_proc({"job_type": job_type, "frames": [], "k": 15, "m": 6})

    disp_ni_frame_db.assert_not_called()


def test_blocked_frames_lists_waiting_frames_with_hours_waited_and_skips_cleared_ones():
    proc = {"frame_blocked_since": {"26410": "2026-10-20T00:00:00", "12419": None, "3": "2026-10-19T12:00:00"}}

    assert pcm_batch.blocked_frames(proc, datetime(2026, 10, 20, 12, 0, 0)) == ["3: 24h", "26410: 12h"]
    assert pcm_batch.blocked_frames({}, datetime(2026, 10, 20)) == []
