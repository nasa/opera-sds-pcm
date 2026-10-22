"""Polarization handling in BaselineGranuleRetriever.retrieve_baseline_granules (baseline/pre granule selection)."""
import logging
from datetime import datetime, timedelta
from unittest.mock import MagicMock

from data_subscriber.dist_s1_utils import basic_decorate_granule
from data_subscriber.rtc_for_dist.baseline_granule_retriever import BaselineGranuleRetriever, Args
from data_subscriber.rtc_for_dist.rtc_batch_evaluator import RtcBatchEvaluator, Args as RtcBatchEvaluatorArgs

PRODUCT_ID = "18TVT_2"
DOWNLOAD_BATCH_ID = "p18TVT_2_S1A_a365"

VV_VH = ("VV", "VH")
HH_HV = ("HH", "HV")

BURST_A = "T018-000001-IW1"
BURST_B = "T018-000002-IW2"
BURST_C = "T018-000003-IW3"
BURST_D = "T018-000004-IW1"
BURSTS = [BURST_A, BURST_B, BURST_C, BURST_D]

POST_ACQUISITION = datetime(2026, 1, 1, 22, 53, 0)
K_OFFSETS_AND_COUNTS = [(365, 3)]
WINDOW_DELTA_DAYS = 60


def _granule(burst_id, acquisition_dt, polarizations):
    granule_id = (f"OPERA_L2_RTC-S1_{burst_id}_{acquisition_dt.strftime('%Y%m%dT%H%M%S')}Z"
                  f"_{(acquisition_dt + timedelta(days=4)).strftime('%Y%m%dT%H%M%S')}Z_S1A_30_v1.0")
    return {
        "granule_id": granule_id,
        "polarization": list(polarizations),
        "related_urls": [],
        "revision_id": 1,
        "filtered_urls": [f"s3://bucket/{granule_id}/{granule_id}_{p}.tif" for p in polarizations],
    }


def _baseline_pass(n):
    """Acquisition time of the n-th 12-day pass before the start of the 365-day look-back window."""
    return POST_ACQUISITION - timedelta(days=365 + 12 * n)


def _current_granules(burst_to_polarizations):
    current_granules = []
    for burst_id, polarizations in burst_to_polarizations.items():
        granule = _granule(burst_id, POST_ACQUISITION, polarizations)
        basic_decorate_granule(granule)
        granule["product_id"] = PRODUCT_ID
        granule["download_batch_id"] = DOWNLOAD_BATCH_ID
        granule["batch_id"] = DOWNLOAD_BATCH_ID
        current_granules.append(granule)
    return current_granules


def _query_func_factory(pool):
    """Fake CMR: return the pool granules of the queried bursts whose acquisition time is in the temporal range."""
    def query_func_factory(use_async=True, secondary=True, args=None):
        bursts = [s.removeprefix("OPERA_L2_RTC-S1_").rstrip("*") for s in args.native_id.split("&native-id[]=")]

        async def query(date_time_range, _now):
            start = datetime.strptime(date_time_range.start_date, "%Y-%m-%dT%H:%M:%SZ")
            end = datetime.strptime(date_time_range.end_date, "%Y-%m-%dT%H:%M:%SZ")
            return [_granule(burst_id, acquisition_dt, polarizations)
                    for burst_id in bursts
                    for acquisition_dt, polarizations in pool.get(burst_id, [])
                    if start <= acquisition_dt <= end]
        return query
    return query_func_factory


def _retrieve(pool, current_granules):
    retriever = BaselineGranuleRetriever(
        logger=logging.getLogger(__name__),
        args=Args(),
        k_offsets_counts=K_OFFSETS_AND_COUNTS,
        product_to_bursts={PRODUCT_ID: set(BURSTS)},
        window_delta_days=WINDOW_DELTA_DAYS,
        settings={},
        bursts_to_products={burst_id: {PRODUCT_ID} for burst_id in BURSTS},
        query_func_factory=_query_func_factory(pool),
    )
    return retriever.retrieve_baseline_granules(PRODUCT_ID, current_granules, Args(), K_OFFSETS_AND_COUNTS, verbose=False)


def _baselines_by_burst(baseline_granules):
    """burst_id -> sorted list of (pass number, polarization) of the selected baseline granules."""
    result = {}
    for granule in baseline_granules:
        n = (POST_ACQUISITION - timedelta(days=365) - granule["acquisition_ts"]).days // 12
        result.setdefault(granule["burst_id"], []).append((n, tuple(granule["polarization"])))
    return {burst_id: sorted(v) for burst_id, v in result.items()}


def test_retrieve_baseline_granules__when_current_granules_mix_dual_pol_vv_vh_and_hh_hv():
    # ARRANGE
    # each burst has one pass in the other polarization; the k=3 cap must skip it and reach back to the next pass
    pool = {
        burst_id: [(_baseline_pass(n), other_pol if n == 3 else own_pol) for n in range(1, 6)]
        for burst_id, own_pol, other_pol in [(BURST_A, VV_VH, HH_HV), (BURST_B, VV_VH, HH_HV),
                                             (BURST_C, HH_HV, VV_VH), (BURST_D, HH_HV, VV_VH)]
    }
    current = _current_granules({BURST_A: VV_VH, BURST_B: VV_VH, BURST_C: HH_HV, BURST_D: HH_HV})

    # ACT
    baseline_granules = _retrieve(pool, current)

    # ASSERT
    assert _baselines_by_burst(baseline_granules) == {
        BURST_A: [(1, VV_VH), (2, VV_VH), (4, VV_VH)],
        BURST_B: [(1, VV_VH), (2, VV_VH), (4, VV_VH)],
        BURST_C: [(1, HH_HV), (2, HH_HV), (4, HH_HV)],
        BURST_D: [(1, HH_HV), (2, HH_HV), (4, HH_HV)],
    }


def test_retrieve_baseline_granules__when_all_vv_vh_and_baseline_passes_alternate_polarization():
    # ARRANGE
    # T19MCR-like: the baseline passes alternate VV+VH / HH+HV over all bursts
    pool = {
        burst_id: [(_baseline_pass(n), HH_HV if n in (1, 3) else VV_VH) for n in range(1, 6)]
        for burst_id in BURSTS
    }
    current = _current_granules({burst_id: VV_VH for burst_id in BURSTS})

    # ACT
    baseline_granules = _retrieve(pool, current)

    # ASSERT
    assert _baselines_by_burst(baseline_granules) == {
        burst_id: [(2, VV_VH), (4, VV_VH), (5, VV_VH)] for burst_id in BURSTS
    }


def test_retrieve_baseline_granules__when_dual_pol_batch_has_one_single_pol_granule():
    # ARRANGE
    pool = {burst_id: [(_baseline_pass(n), VV_VH) for n in range(1, 6)] for burst_id in BURSTS}
    current = _current_granules({BURST_A: VV_VH, BURST_B: VV_VH, BURST_C: VV_VH, BURST_D: ("VV",)})

    # ACT
    baseline_granules = _retrieve(pool, current)

    # ASSERT
    # the dual-pol bursts get their baselines. (BURST_D's current granule is not usable, so the
    # batch evaluator drops that burst whatever baselines it has.)
    baselines_by_burst = _baselines_by_burst(baseline_granules)
    for burst_id in [BURST_A, BURST_B, BURST_C]:
        assert baselines_by_burst[burst_id] == [(1, VV_VH), (2, VV_VH), (3, VV_VH)]
    assert all(tuple(g["polarization"]) == VV_VH for g in baseline_granules)


def test_retrieve_baseline_granules__when_no_current_granule_is_dual_pol():
    # ARRANGE
    pool = {burst_id: [(_baseline_pass(n), VV_VH) for n in range(1, 6)] for burst_id in BURSTS}
    current = _current_granules({BURST_A: ("VV",), BURST_B: ("VV",)})

    # ACT
    baseline_granules = _retrieve(pool, current)

    # ASSERT
    assert baseline_granules == []


def test_retrieve_baseline_granules_then_evaluate__when_current_granules_mix_dual_pol_vv_vh_and_hh_hv():
    # ARRANGE
    # each burst has baseline passes in both polarizations (in the window: HH+HV at 1, 3, 5 and VV+VH at 2, 4)
    pool = {
        burst_id: [(_baseline_pass(n), HH_HV if n % 2 else VV_VH) for n in range(1, 6)]
        for burst_id in BURSTS
    }
    current = _current_granules({BURST_A: VV_VH, BURST_B: VV_VH, BURST_C: HH_HV, BURST_D: HH_HV})
    baseline_granules = _retrieve(pool, current)

    dist_dependency = MagicMock()
    dist_dependency.should_wait_previous_run.return_value = (False, None, None)
    evaluator = RtcBatchEvaluator(
        logger=logging.getLogger(__name__),
        download_batch_id_to_k_granules={DOWNLOAD_BATCH_ID: baseline_granules},
        batch_id_to_current_granules={DOWNLOAD_BATCH_ID: current},
        download_batch_id_to_job_submittable={DOWNLOAD_BATCH_ID: True},
        args=RtcBatchEvaluatorArgs(proc_mode="forward"),
        dist_dependency=dist_dependency,
        es_conn=MagicMock(),
        settings={},
    )

    # ACT
    evaluator.evaluate(current, None)

    # ASSERT
    current_urls = evaluator.usable_batch_id_to_current_urls_map.get(DOWNLOAD_BATCH_ID, [])
    baseline_urls = evaluator.batch_id_to_baseline_urls_map.get(DOWNLOAD_BATCH_ID, [])
    expected_current_urls = [url for g in current for url in g["filtered_urls"]]
    expected_baseline_urls = [
        url
        for burst_id, own_pol, passes in [(BURST_A, VV_VH, (2, 4)), (BURST_B, VV_VH, (2, 4)),
                                          (BURST_C, HH_HV, (1, 3, 5)), (BURST_D, HH_HV, (1, 3, 5))]
        for n in passes
        for url in _granule(burst_id, _baseline_pass(n), own_pol)["filtered_urls"]
    ]
    assert sorted(current_urls) == sorted(expected_current_urls)
    assert sorted(baseline_urls) == sorted(expected_baseline_urls)
