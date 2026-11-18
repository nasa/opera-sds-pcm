"""Polarization handling in RtcBatchEvaluator.evaluate for current (post) granule sets."""
import logging
from unittest.mock import MagicMock

from data_subscriber.rtc_for_dist.rtc_batch_evaluator import RtcBatchEvaluator, Args

DOWNLOAD_BATCH_ID = "p18TVT_2_S1A_a100"
PRODUCT_ID = "18TVT_2"

VV_VH = ("VV", "VH")
HH_HV = ("HH", "HV")

BURST_A = "T018-000001-IW1"
BURST_B = "T018-000002-IW2"
BURST_C = "T018-000003-IW3"


def _granule(burst_id, acquisition_ts, polarizations):
    granule_id = f"OPERA_L2_RTC-S1_{burst_id}_{acquisition_ts}Z_20260105T000000Z_S1A_30_v1.0"
    urls = [f"s3://bucket/{granule_id}/{granule_id}_{p}.tif" for p in polarizations]
    urls += [f"https://host/{granule_id}/{granule_id}_{p}.tif" for p in polarizations]
    return {
        "granule_id": granule_id,
        "download_batch_id": DOWNLOAD_BATCH_ID,
        "batch_id": DOWNLOAD_BATCH_ID,
        "product_id": PRODUCT_ID,
        "polarization": list(polarizations),
        "filtered_urls": urls,
    }


def _current(burst_id, polarizations):
    return _granule(burst_id, "20260101T225300", polarizations)


def _baselines(burst_id, polarizations):
    return [_granule(burst_id, f"2025010{day}T225300", polarizations) for day in (1, 7)]


def _evaluate(current_granules, baseline_granules):
    dist_dependency = MagicMock()
    dist_dependency.should_wait_previous_run.return_value = (False, None, None)
    evaluator = RtcBatchEvaluator(
        logger=logging.getLogger(__name__),
        download_batch_id_to_k_granules={DOWNLOAD_BATCH_ID: baseline_granules},
        batch_id_to_current_granules={DOWNLOAD_BATCH_ID: current_granules},
        download_batch_id_to_job_submittable={DOWNLOAD_BATCH_ID: True},
        args=Args(proc_mode="forward"),
        dist_dependency=dist_dependency,
        es_conn=MagicMock(),
        settings={},
    )
    evaluator.evaluate(current_granules, None)
    current_urls = evaluator.usable_batch_id_to_current_urls_map.get(DOWNLOAD_BATCH_ID, [])
    baseline_urls = evaluator.batch_id_to_baseline_urls_map.get(DOWNLOAD_BATCH_ID, [])
    return current_urls, baseline_urls


def _suffixes(urls):
    return sorted(url.rsplit("_", 1)[-1] for url in urls)


def test_evaluate__when_all_current_granules_dual_pol_vv_vh():
    # ARRANGE
    current = [_current(BURST_A, VV_VH), _current(BURST_B, VV_VH)]
    baseline = _baselines(BURST_A, VV_VH) + _baselines(BURST_B, VV_VH)

    # ACT
    current_urls, baseline_urls = _evaluate(current, baseline)

    # ASSERT
    assert len(current_urls) == 4
    assert len(baseline_urls) == 8
    assert all(url.startswith("s3://") for url in current_urls + baseline_urls)


def test_evaluate__when_dual_pol_vv_vh_batch_has_one_single_pol_vv_granule():
    # ARRANGE
    current = [_current(BURST_A, VV_VH), _current(BURST_B, VV_VH), _current(BURST_C, ("VV",))]
    baseline = _baselines(BURST_A, VV_VH) + _baselines(BURST_B, VV_VH) + _baselines(BURST_C, VV_VH)

    # ACT
    current_urls, baseline_urls = _evaluate(current, baseline)

    # ASSERT
    # the single-pol granule is not used as a current input, so its burst is dropped from both sets
    assert len(current_urls) == 4
    assert len(baseline_urls) == 8
    assert not any(BURST_C in url for url in current_urls + baseline_urls)
    assert _suffixes(current_urls) == ["VH.tif", "VH.tif", "VV.tif", "VV.tif"]


def test_evaluate__when_dual_pol_hh_hv_batch_has_one_single_pol_hh_granule():
    # ARRANGE
    current = [_current(BURST_A, ("HH",)), _current(BURST_B, HH_HV), _current(BURST_C, HH_HV)]
    baseline = _baselines(BURST_A, HH_HV) + _baselines(BURST_B, HH_HV) + _baselines(BURST_C, HH_HV)

    # ACT
    current_urls, baseline_urls = _evaluate(current, baseline)

    # ASSERT
    assert len(current_urls) == 4
    assert len(baseline_urls) == 8
    assert not any(BURST_A in url for url in current_urls + baseline_urls)
    assert _suffixes(current_urls) == ["HH.tif", "HH.tif", "HV.tif", "HV.tif"]
    assert _suffixes(baseline_urls) == ["HH.tif"] * 4 + ["HV.tif"] * 4


def test_evaluate__when_all_current_granules_single_pol():
    # ARRANGE
    current = [_current(BURST_A, ("VV",)), _current(BURST_B, ("VV",))]
    baseline = _baselines(BURST_A, VV_VH) + _baselines(BURST_B, VV_VH)

    # ACT
    current_urls, baseline_urls = _evaluate(current, baseline)

    # ASSERT
    assert current_urls == []


def test_evaluate__when_current_granules_mix_dual_pol_vv_vh_and_hh_hv():
    # ARRANGE
    current = [_current(BURST_A, VV_VH), _current(BURST_B, HH_HV)]
    baseline = _baselines(BURST_A, VV_VH) + _baselines(BURST_B, HH_HV)

    # ACT
    current_urls, baseline_urls = _evaluate(current, baseline)

    # ASSERT
    assert len(current_urls) == 4
    assert len(baseline_urls) == 8


def test_evaluate__when_current_granules_mix_dual_pol_and_baselines_have_both_polarizations():
    # ARRANGE
    current = [_current(BURST_A, VV_VH), _current(BURST_B, HH_HV), _current(BURST_C, VV_VH)]
    # every burst has baseline granules in both polarizations
    baseline = [g for burst_id in (BURST_A, BURST_B, BURST_C) for g in (
        _baselines(burst_id, VV_VH)
        + [_granule(burst_id, f"2025010{day}T225300", HH_HV) for day in (3, 5)]
    )]

    # ACT
    current_urls, baseline_urls = _evaluate(current, baseline)

    # ASSERT
    # each burst keeps its own polarization, in its current and in its baseline URLs
    assert len(current_urls) == 6
    assert len(baseline_urls) == 12
    for burst_id, expected_suffixes in [(BURST_A, ["VH.tif", "VV.tif"]), (BURST_B, ["HH.tif", "HV.tif"]),
                                        (BURST_C, ["VH.tif", "VV.tif"])]:
        assert _suffixes(url for url in current_urls if burst_id in url) == expected_suffixes
        assert _suffixes(url for url in baseline_urls if burst_id in url) == sorted(expected_suffixes * 2)
