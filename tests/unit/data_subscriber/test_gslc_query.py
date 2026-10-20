"""Historical NISAR GSLC query for one DISP-NI k-set.

The query job must hand the download job exactly one GSLC per NISAR frame and date of the k-set,
all in one mode and polarization (ICS 2.6.1), and must fail rather than download an incomplete
k-set. NISAR frames are routinely acquired in several modes on one date, and reprocessed granules
appear under a new counter, so both are resolved here. CMR is mocked at async_cmr_posts.
"""

from datetime import datetime, timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from data_subscriber.cmr import CMR_TIME_FORMAT, Collection
from data_subscriber.gslc.gslc_query import GslcCmrQuery
from data_subscriber.gslc.gslc_utils import process_disp_ni_frame_db
from data_subscriber.parser import create_parser

TEST_DATA = Path(__file__).parent / "test_data"
DISP_NI_DB = str(TEST_DATA / "disp_ni_consistent_db.json")
FRAME_DB = process_disp_ni_frame_db(DISP_NI_DB)

FRAME = 26410
TWO_NISAR_FRAME_FRAME = 12419


def make_settings(preference=("4005_DHDH",)):
    return {"SHORTNAME_FILTERS": {}, "RELEASE_VERSION": "test-release",
            "DISP_NI": {"GSLC_MODE_POLARIZATION_PREFERENCE": list(preference)}}


def window(frame_id, position, k):
    dates = FRAME_DB[frame_id].sensing_datetimes
    return ((dates[position] - timedelta(minutes=30)).strftime(CMR_TIME_FORMAT),
            (dates[position + k - 1] + timedelta(minutes=30)).strftime(CMR_TIME_FORMAT))


def make_args(frame_id=FRAME, position=0, k=3, m=1, extra=()):
    start, end = window(frame_id, position, k)
    return create_parser().parse_args([
        "query", f"--collection-shortname={Collection.NISAR_GSLC}", "--processing-mode=historical",
        f"--frame-id={frame_id}", f"--start-date={start}", f"--end-date={end}", f"--k={k}", f"--m={m}",
        "--use-temporal", "--job-queue=opera-job_worker-gslc_data_download_hist", "--release-version=test-release",
        "--chunk-size=1", "--provider=ASF", *extra])


def gslc_id(nisar_frame, cycle, sensing: datetime, mode="4005_DHDH", counter="001"):
    track, direction, frame = nisar_frame.split("_")
    start = sensing.strftime("%Y%m%dT%H%M%S")
    end = (sensing + timedelta(seconds=17)).strftime("%Y%m%dT%H%M%S")
    return f"NISAR_L2_PR_GSLC_{cycle:03d}_{track}_{direction}_{frame}_{mode}_A_{start}_{end}_P05023_N_F_J_{counter}"


def make_item(granule_id, sensing: datetime):
    return {
        "meta": {"native-id": granule_id, "revision-id": 1, "revision-date": "2026-08-06T16:18:39.087Z",
                 "provider-id": "ASF"},
        "umm": {
            "GranuleUR": granule_id,
            "TemporalExtent": {"RangeDateTime": {"BeginningDateTime": sensing.strftime("%Y-%m-%dT%H:%M:%S.000Z")}},
            "DataGranule": {"ProductionDateTime": "2026-08-06T00:00:00.000Z"},
            "Platforms": [{"ShortName": "NISAR"}],
            "SpatialExtent": {"HorizontalSpatialDomain": {"Geometry": {"GPolygons": [{"Boundary": {"Points": [
                {"Latitude": 19.0, "Longitude": -155.0}, {"Latitude": 19.0, "Longitude": -154.0},
                {"Latitude": 20.0, "Longitude": -154.0}, {"Latitude": 19.0, "Longitude": -155.0}]}}]}}},
            "RelatedUrls": [{"URL": f"https://nisar.asf.earthdatacloud.nasa.gov/{granule_id}/{granule_id}.h5"},
                            {"URL": f"s3://asf-nisar/{granule_id}/{granule_id}.h5"}],
        },
    }


def k_set_items(frame_id=FRAME, position=0, k=3, mode="4005_DHDH"):
    """One GSLC per NISAR frame and date of the k-set. Cycles count from the frame's first date."""
    frame = FRAME_DB[frame_id]
    items = []
    for dt in frame.sensing_datetimes[position:position + k]:
        cycle = 4 + (dt - frame.sensing_datetimes[0]).days // 12
        for nisar_frame in sorted(frame.nisar_frames):
            items.append(make_item(gslc_id(nisar_frame, cycle, dt, mode), dt))
    return items


def cycle_of(dt, frame_id=FRAME):
    return 4 + (dt - FRAME_DB[frame_id].sensing_datetimes[0]).days // 12


def run(args, items, settings=None):
    """Run the query job with CMR answering with these items. Returns (result, submit mock, es_conn)."""
    es_conn = MagicMock()
    query = GslcCmrQuery(args, None, es_conn, "cmr.earthdata.nasa.gov", "job-1", settings or make_settings(),
                         disp_ni_frame_db_file=DISP_NI_DB)
    with patch("data_subscriber.cmr.async_cmr_posts", new_callable=AsyncMock, return_value=[{"items": items}]), \
            patch("data_subscriber.query.submit_download_job", return_value="download-job-1") as submit:
        result = query.run_query()
    return result, submit, es_conn


def catalog_docs(es_conn):
    return [c.kwargs for c in es_conn.process_url.call_args_list]


def test_one_download_job_covers_all_k_batch_ids():
    k = 15
    result, submit, es_conn = run(make_args(k=k), k_set_items(k=k))

    submit.assert_called_once()
    kwargs = submit.call_args.kwargs
    batch_ids = next(p["value"] for p in kwargs["params"] if p["name"] == "batch_ids").split()[1:]
    cycles = [cycle_of(dt) for dt in FRAME_DB[FRAME].sensing_datetimes[:k]]
    expected = [f"f{FRAME}_a{cycle}" for cycle in cycles]
    assert batch_ids == expected
    assert kwargs["product_type"] == "gslc"
    assert kwargs["job_queue"] == "opera-job_worker-gslc_data_download_hist"
    assert kwargs["release_version"] == "test-release"
    assert kwargs["job_name"] == f"job-WF-gslc_download-frame-{FRAME}-cycles-{cycles[0]}-to-{cycles[-1]}"
    assert result["success"] == ["download-job-1"]
    assert sorted(c.args[0] for c in es_conn.mark_download_job_id.call_args_list) == sorted(expected)


def test_no_schedule_download_catalogs_without_submitting():
    result, submit, es_conn = run(make_args(extra=["--no-schedule-download"]), k_set_items())

    submit.assert_not_called()
    assert len(result["download_granules"]) == 3
    assert es_conn.process_url.call_count == 3


def test_catalog_docs_carry_the_fields_the_download_job_needs():
    _, _, es_conn = run(make_args(m=2), k_set_items())

    doc = catalog_docs(es_conn)[0]
    first = FRAME_DB[FRAME].sensing_datetimes[0]
    assert doc["frame_id"] == FRAME
    assert doc["nisar_frame"] == "151_A_011"
    assert doc["acquisition_cycle"] == 4
    assert doc["acquisition_ts"] == first
    assert doc["mode"] == "4005"
    assert doc["polarization"] == "DHDH"
    assert doc["download_batch_id"] == f"f{FRAME}_a4"
    assert doc["unique_id"] == f"f{FRAME}_a4_151_A_011"
    assert doc["k"] == 3
    assert doc["m"] == 2


def test_a_missing_date_fails_the_job_so_it_is_retried():
    items = k_set_items()
    del items[1]

    with pytest.raises(RuntimeError, match="GSLC dates don't match frame DB"):
        run(make_args(), items)


def test_a_date_missing_one_of_the_frames_nisar_frames_fails_the_job():
    items = [i for i in k_set_items(TWO_NISAR_FRAME_FRAME)
             if not i["umm"]["GranuleUR"].startswith("NISAR_L2_PR_GSLC_005_071_D_101_")]
    assert len(items) == 5

    with pytest.raises(RuntimeError, match="GSLC missing a NISAR frame"):
        run(make_args(TWO_NISAR_FRAME_FRAME), items)


def test_a_frame_made_of_two_nisar_frames_gets_one_doc_per_nisar_frame_and_date():
    result, submit, _ = run(make_args(TWO_NISAR_FRAME_FRAME), k_set_items(TWO_NISAR_FRAME_FRAME))

    assert len(result["download_granules"]) == 6
    batch_ids = next(p["value"] for p in submit.call_args.kwargs["params"] if p["name"] == "batch_ids").split()[1:]
    assert len(batch_ids) == 3


def test_a_reprocessed_granule_replaces_the_original():
    items = k_set_items()
    first = FRAME_DB[FRAME].sensing_datetimes[0]
    reprocessed = gslc_id("151_A_011", 4, first, counter="002")
    items.append(make_item(reprocessed, first))

    result, _, _ = run(make_args(), items)

    granule_ids = [g["granule_id"] for g in result["download_granules"]]
    assert len(granule_ids) == 3
    assert reprocessed in granule_ids


def test_other_modes_on_the_same_date_are_dropped():
    items = k_set_items() + k_set_items(mode="2005_QPDH")[:1]

    result, _, _ = run(make_args(), items)

    assert {(g["mode"], g["polarization"]) for g in result["download_granules"]} == {("4005", "DHDH")}
    assert len(result["download_granules"]) == 3


def test_the_next_preference_is_used_when_a_date_lacks_the_first():
    items = k_set_items()[1:] + k_set_items(mode="2005_DHDH")

    result, _, _ = run(make_args(), items, make_settings(("4005_DHDH", "2005_DHDH")))

    assert {g["mode"] for g in result["download_granules"]} == {"2005"}
    assert len(result["download_granules"]) == 3


def test_no_mode_common_to_every_date_fails_the_job():
    items = k_set_items()[1:] + k_set_items(mode="2005_DHDH")[:1]

    with pytest.raises(RuntimeError, match="No common GSLC mode/pol"):
        run(make_args(), items, make_settings(("4005_DHDH", "2005_DHDH")))


def test_a_date_the_frame_database_excludes_is_not_part_of_the_k_set():
    frame = FRAME_DB[FRAME]
    # Cycle 7 (2025-12-14) falls inside the second k-set's window but is not in the frame database
    excluded = frame.sensing_datetimes[0] + timedelta(days=36)
    assert excluded not in frame.sensing_datetimes
    items = k_set_items(position=1) + [make_item(gslc_id("151_A_011", 7, excluded), excluded)]

    result, _, _ = run(make_args(position=1), items)

    assert 7 not in {g["acquisition_cycle"] for g in result["download_granules"]}
    assert len(result["download_granules"]) == 3


def test_the_query_asks_cmr_for_the_frames_nisar_frames_over_the_window():
    query = GslcCmrQuery(make_args(TWO_NISAR_FRAME_FRAME), None, MagicMock(), "cmr.earthdata.nasa.gov", "job-1",
                         make_settings(), disp_ni_frame_db_file=DISP_NI_DB)

    with patch("data_subscriber.cmr.async_cmr_posts", new_callable=AsyncMock, return_value=[{"items": []}]) as posts:
        with pytest.raises(RuntimeError, match="GSLC dates don't match frame DB"):
            query.run_query()

    request_body = "".join(posts.call_args.args[1])
    assert "native-id[]=NISAR_L2_*_GSLC_*_071_D_100_*" in request_body
    assert "native-id[]=NISAR_L2_*_GSLC_*_071_D_101_*" in request_body
    assert "temporal" in request_body


def test_a_frame_not_in_the_frame_database_fails_the_job():
    args = make_args()
    args.frame_id = "99999"

    with pytest.raises(RuntimeError, match="Frame not in DISP-NI frame DB"):
        run(args, [])


@pytest.mark.parametrize("change", [["--processing-mode=forward"], ["--frame-id"]],
                         ids=["forward_mode", "no_frame_id"])
def test_only_historical_queries_of_one_frame_are_accepted(change):
    args = make_args()
    if change == ["--frame-id"]:
        args.frame_id = None
    else:
        args.proc_mode = "forward"

    with pytest.raises(AssertionError):
        GslcCmrQuery(args, None, MagicMock(), "cmr", "job-1", make_settings(), disp_ni_frame_db_file=DISP_NI_DB)


def test_grq_is_not_a_gslc_provider():
    args = make_args()
    args.provider = "GRQ"
    query = GslcCmrQuery(args, None, MagicMock(), "cmr", "job-1", make_settings(), disp_ni_frame_db_file=DISP_NI_DB)

    with pytest.raises(ValueError, match="GRQ provider not supported"):
        query._get_query_func()
