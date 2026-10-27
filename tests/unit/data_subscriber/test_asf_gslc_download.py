"""Download of one DISP-NI k-set and submission of its SCIFLO_L3_DISP_NI job.

The download job is the only place a k-set's GSLCs, the compressed GSLCs of the frame's earlier
k-sets, its GUNWs and the frequency/polarization they share come together, so the PGE job's inputs are
only as right as the product metadata built here. A k-set submitted without a compressed GSLC it needs
would break the ministack lineage, so a missing one must fail the job instead. GUNWs are optional: a
k-set without an unbroken chain of them runs without, and the SAS estimates the ionosphere itself.
"""

import argparse
import hashlib
from datetime import timedelta
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from data_subscriber.gslc.asf_gslc_download import AsfDaacGslcDownload
from data_subscriber.gslc.gslc_utils import process_disp_ni_frame_db

DISP_NI_DB = str(Path(__file__).parent / "test_data" / "disp_ni_consistent_db.json")
FRAME_DB = process_disp_ni_frame_db(DISP_NI_DB)
FRAME = 26410
K = 3
DATES = FRAME_DB[FRAME].sensing_datetimes
SETTINGS = {"DATASET_BUCKET": "opera-rs", "RELEASE_VERSION": "fallback",
            "DISP_NI": {"GUNW_MODE_POLARIZATION_PREFERENCE": {"4005_DHDH": ["4000_SH"]}}}


def cycle_of(position):
    return 4 + (DATES[position] - DATES[0]).days // 12


def make_doc(position, m=1, polarization="DHDH"):
    dt = DATES[position]
    cycle = cycle_of(position)
    granule_id = (f"NISAR_L2_PR_GSLC_{cycle:03d}_151_A_011_4005_{polarization}_A_{dt:%Y%m%dT%H%M%S}_"
                  f"{dt + timedelta(seconds=17):%Y%m%dT%H%M%S}_P05023_N_F_J_001")
    return {
        "granule_id": granule_id,
        "unique_id": f"f{FRAME}_a{cycle}_151_A_011",
        "nisar_frame": "151_A_011",
        "download_batch_id": f"f{FRAME}_a{cycle}",
        "s3_url": f"s3://asf-nisar/{granule_id}/{granule_id}.h5",
        "https_url": f"https://nisar.asf/{granule_id}/{granule_id}.h5",
        "acquisition_ts": dt.isoformat(),
        "acquisition_cycle": cycle,
        "mode": "4005",
        "polarization": polarization,
        "k": K,
        "m": m,
        "bounding_box": [{"lat": 19.0 + position, "lon": -155.0}, {"lat": 20.0 + position, "lon": -154.0}],
    }


class FakeEs:
    """Answers compressed-GSLC lookups from published (frame_id, last date) pairs."""

    def __init__(self, published=()):
        self.published = set(published)

    def query(self, index=None, body=None):
        frame_term, date_range = body["query"]["bool"]["must"]
        day = date_range["range"]["metadata.last_date_time"]["gte"][:10]
        if (frame_term["term"]["metadata.frame_id"], day) not in self.published:
            return []
        name = f"OPERA_L2_COMPRESSED-GSLC-NI_4000_{FRAME}_SH_{day.replace('-', '')}T000000Z"
        prefix = f"s3://lts/products/GSLC_COMPRESSED/{name}/{name}"
        return [{"_source": {"metadata": {"product_s3_paths": [f"{prefix}.h5", f"{prefix}.iso.xml"]}}}]


def make_es_conn(docs, published=()):
    es_conn = MagicMock()
    es_conn.get_download_granule_revision.side_effect = \
        lambda batch_id: [d for d in docs if d["download_batch_id"] == batch_id]
    es_conn.es_util = FakeEs(published)
    return es_conn


def gunw_item(reference_cycle, secondary_cycle, mode="4000_SH"):
    """A GUNW as CMR lists it: with its QA_STATS.h5 and a browse image next to the GUNW itself."""

    granule_id = (f"NISAR_L2_PR_GUNW_{reference_cycle:03d}_151_A_011_{secondary_cycle:03d}_{mode}_"
                  f"20251108T155041_20251108T155058_20251120T155041_20251120T155058_P05023_N_P_J_001")
    urls = [f"{prefix}/{granule_id}/{granule_id}{suffix}"
            for prefix in ("https://nisar.asf/GUNW", "s3://asf-nisar-gunw")
            for suffix in (".h5", "_QA_STATS.h5", "_LATLON.png")]
    return {"umm": {"GranuleUR": granule_id, "RelatedUrls": [{"URL": url} for url in urls]}}


def gunw_s3_url(item):
    granule_id = item["umm"]["GranuleUR"]
    return f"s3://asf-nisar-gunw/{granule_id}/{granule_id}.h5"


def run(docs, published=(), transfer_protocol="auto", gunw_items=(), settings=None):
    args = argparse.Namespace(batch_ids=[d["download_batch_id"] for d in reversed(docs)], proc_mode="historical",
                              transfer_protocol=transfer_protocol, release_version="test-release")
    es_conn = make_es_conn(docs, published)
    with patch("data_subscriber.gslc.asf_gslc_download.try_submit_mozart_job", return_value="sciflo-1") as submit, \
            patch("data_subscriber.gslc.asf_gslc_download.SettingsConf") as settings_conf, \
            patch("data_subscriber.gslc.asf_gslc_download.async_query_cmr_v2", new_callable=AsyncMock,
                  return_value=list(gunw_items)) as cmr_query:
        settings_conf.return_value.cfg = settings or SETTINGS
        result = AsfDaacGslcDownload("ASF-NISAR-GSLC", disp_ni_frame_db_file=DISP_NI_DB).run_download(
            args, "edl-token", es_conn, None, None, None, "cmr.earthdata.nasa.gov", "download-job-1")
    run.cmr_query = cmr_query
    return result, submit, es_conn


def product_paths(submit):
    return submit.call_args.kwargs["product"]["_source"]["metadata"]["product_paths"]


@pytest.fixture(autouse=True)
def in_tmp_dir(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)   # BaseDownload creates a downloads directory in the working directory


def test_first_k_set_submits_its_gslcs_from_asf_s3_with_no_compressed_gslcs():
    docs = [make_doc(p) for p in range(K)]

    result, submit, es_conn = run(docs)

    assert result == "sciflo-1"
    kwargs = submit.call_args.kwargs
    metadata = kwargs["product"]["_source"]["metadata"]
    gslc_paths = [d["s3_url"] for d in docs]
    assert metadata["product_paths"] == {"L2_GSLC_NI": gslc_paths, "L2_GSLC_COMPRESSED": [], "L2_GUNW_NI": []}
    assert metadata["frame_id"] == FRAME
    assert (metadata["frequency"], metadata["polarization"]) == ("frequencyA", "HH")
    assert metadata["save_compressed_slc"] is True
    assert metadata["processing_mode"] == "historical"
    assert metadata["bounding_box"] == [-155.0, 19.0, -154.0, 20.0 + K - 1]
    assert kwargs["job_spec"] == "job-SCIFLO_L3_DISP_NI:test-release"
    assert kwargs["job_queue"] == "opera-job_worker-sciflo-l3_disp_ni"
    assert kwargs["payload_hash"] == hashlib.md5("".join(sorted(gslc_paths)).encode()).hexdigest()
    assert {p["name"]: p["value"] for p in kwargs["params"]}["dataset_type"] == "L2_GSLC_NI"
    assert sorted(c.args[0] for c in es_conn.mark_product_as_downloaded.call_args_list) == \
        sorted(d["unique_id"] for d in docs)


def test_later_k_set_adds_the_compressed_gslc_of_the_k_set_before_it():
    docs = [make_doc(p, m=2) for p in range(K, 2 * K)]
    last_day = DATES[K - 1].strftime("%Y-%m-%d")

    _, submit, _ = run(docs, published=[(FRAME, last_day)])

    compressed = submit.call_args.kwargs["product"]["_source"]["metadata"]["product_paths"]["L2_GSLC_COMPRESSED"]
    assert len(compressed) == 1
    assert compressed[0].endswith(f"_{last_day.replace('-', '')}T000000Z.h5")


def test_a_missing_compressed_gslc_fails_the_job_before_anything_is_submitted():
    docs = [make_doc(p, m=2) for p in range(K, 2 * K)]

    with pytest.raises(RuntimeError, match="Compressed GSLC not found"):
        _, submit, es_conn = run(docs)


def test_a_k_set_mixing_polarizations_is_refused():
    docs = [make_doc(0), make_doc(1, polarization="DVDV"), make_doc(2)]

    with pytest.raises(RuntimeError, match="GSLC k-set mixes polarizations"):
        run(docs)


def test_https_downloads_and_reuploads_the_gslcs(tmp_path):
    docs = [make_doc(p) for p in range(K)]

    def fake_download(self, url, token, target_dirpath, **kwargs):
        path = Path(target_dirpath) / Path(url).name
        path.write_text("gslc")
        return path

    def fake_upload(bucket, key_prefix, files):
        return [f"s3://{bucket}/{key_prefix}/{Path(f).name}" for f in files]

    with patch.object(AsfDaacGslcDownload, "download_asf_product", fake_download), \
            patch("data_subscriber.gslc.asf_gslc_download.concurrent_s3_client_try_upload_file",
                  side_effect=fake_upload) as upload:
        _, submit, _ = run(docs, transfer_protocol="https")

    gslc_paths = submit.call_args.kwargs["product"]["_source"]["metadata"]["product_paths"]["L2_GSLC_NI"]
    assert all(p.startswith("s3://opera-rs/tmp/disp_ni/") for p in gslc_paths)
    assert upload.call_args.kwargs["bucket"] == "opera-rs"
    assert not list((tmp_path / "downloads").iterdir())   # local copies removed after upload


def test_a_k_set_with_an_unbroken_chain_gets_its_gunws_from_asf_s3():
    docs = [make_doc(p) for p in range(K)]
    cycles = [cycle_of(p) for p in range(K)]
    chain = [gunw_item(a, b) for a, b in zip(cycles, cycles[1:])]

    _, submit, _ = run(docs, gunw_items=chain + [gunw_item(cycles[-1], cycles[-1] + 1)])

    assert product_paths(submit)["L2_GUNW_NI"] == [gunw_s3_url(item) for item in chain]
    files = [f["FileName"] for f in submit.call_args.kwargs["product"]["_source"]["metadata"]["Files"]]
    assert all(Path(gunw_s3_url(item)).name in files for item in chain)
    kwargs = run.cmr_query.call_args.kwargs
    assert kwargs["collection"] == "NISAR_L2_GUNW_PROVISIONAL_V1"
    assert kwargs["native_id_patterns"] == ["NISAR_L2_*_GUNW_*_151_A_011_*"]
    assert kwargs["timerange"].start_date < DATES[0].strftime("%Y-%m-%dT%H:%M:%SZ")
    assert kwargs["timerange"].end_date > DATES[K - 1].strftime("%Y-%m-%dT%H:%M:%SZ")


def test_a_k_set_with_a_gap_in_its_gunws_runs_without_any():
    docs = [make_doc(p) for p in range(K)]
    cycles = [cycle_of(p) for p in range(K)]

    result, submit, _ = run(docs, gunw_items=[gunw_item(cycles[0], cycles[1])])

    assert result == "sciflo-1"
    assert product_paths(submit)["L2_GUNW_NI"] == []


def test_gunws_of_an_unconfigured_mode_are_not_used():
    docs = [make_doc(p) for p in range(K)]
    cycles = [cycle_of(p) for p in range(K)]

    _, submit, _ = run(docs, gunw_items=[gunw_item(a, b, mode="2000_SH") for a, b in zip(cycles, cycles[1:])])

    assert product_paths(submit)["L2_GUNW_NI"] == []


def test_no_gunw_mode_configured_for_the_gslcs_skips_the_gunw_query():
    docs = [make_doc(p) for p in range(K)]

    _, submit, _ = run(docs, settings={**SETTINGS, "DISP_NI": {}})

    run.cmr_query.assert_not_called()
    assert product_paths(submit)["L2_GUNW_NI"] == []


def test_https_downloads_and_reuploads_the_gunws_too():
    docs = [make_doc(p) for p in range(K)]
    cycles = [cycle_of(p) for p in range(K)]
    chain = [gunw_item(a, b) for a, b in zip(cycles, cycles[1:])]

    def fake_download(self, url, token, target_dirpath, **kwargs):
        path = Path(target_dirpath) / Path(url).name
        path.write_text("h5")
        return path

    with patch.object(AsfDaacGslcDownload, "download_asf_product", fake_download), \
            patch("data_subscriber.gslc.asf_gslc_download.concurrent_s3_client_try_upload_file",
                  side_effect=lambda bucket, key_prefix, files: [f"s3://{bucket}/{key_prefix}/{Path(f).name}"
                                                                 for f in files]):
        _, submit, _ = run(docs, transfer_protocol="https", gunw_items=chain)

    gunw_paths = product_paths(submit)["L2_GUNW_NI"]
    assert [Path(p).name for p in gunw_paths] == [Path(gunw_s3_url(item)).name for item in chain]
    assert all(p.startswith("s3://opera-rs/tmp/disp_ni/") for p in gunw_paths)
