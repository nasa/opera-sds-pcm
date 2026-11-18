"""Dedup of current (post) RTC granules in RtcForDistCmrQuery._run_query_func."""
import logging
from datetime import datetime, timedelta
from types import SimpleNamespace

from data_subscriber.rtc_for_dist.rtc_for_dist_query import RtcForDistCmrQuery

PRODUCT_ID = "18TVT_2"
BURST_A = "T018-000001-IW1"
BURST_B = "T018-000002-IW2"
BURSTS = [BURST_A, BURST_B]

POST_ACQUISITION = datetime(2026, 1, 1, 22, 53, 0)


def _granule_id(burst_id, acquisition_dt, production_dt):
    return (f"OPERA_L2_RTC-S1_{burst_id}_{acquisition_dt:%Y%m%dT%H%M%S}Z"
            f"_{production_dt:%Y%m%dT%H%M%S}Z_S1A_30_v1.0")


def _run_query_func(granule_ids):
    """Run a reprocessing-mode query whose fake CMR returns granule_ids."""
    query = RtcForDistCmrQuery.__new__(RtcForDistCmrQuery)  # skip __init__ (burst DB, ES, CMR)
    query.logger = logging.getLogger(__name__)
    query.args = SimpleNamespace(proc_mode="reprocessing",
                                 product_id_time=[f"{PRODUCT_ID},{POST_ACQUISITION:%Y%m%dT%H%M%SZ}"])
    query.product_to_bursts = {PRODUCT_ID: set(BURSTS)}
    query.bursts_to_products = {burst_id: {PRODUCT_ID} for burst_id in BURSTS}

    def get_query_func(use_async=True, args=None):
        async def query_func(_date_time_range, _now):
            return [{"granule_id": granule_id} for granule_id in granule_ids]
        return query_func
    query._get_query_func = get_query_func

    return query._run_query_func(None, None)


def test_run_query_func__when_post_twins_are_1_second_apart__then_latest_production_is_kept():
    # ARRANGE
    # RTCs of a burst in the same acquisition minute are one image processed more than once
    newer = _granule_id(BURST_A, POST_ACQUISITION, POST_ACQUISITION + timedelta(days=2))
    twin = _granule_id(BURST_A, POST_ACQUISITION + timedelta(seconds=1), POST_ACQUISITION + timedelta(days=1))
    other = _granule_id(BURST_B, POST_ACQUISITION, POST_ACQUISITION + timedelta(days=1))

    # ACT
    granules = _run_query_func([twin, newer, other])

    # ASSERT
    assert sorted(g["granule_id"] for g in granules) == sorted([newer, other])


def test_run_query_func__when_post_duplicates_share_the_second__then_latest_production_is_kept():
    # ARRANGE
    older = _granule_id(BURST_A, POST_ACQUISITION, POST_ACQUISITION + timedelta(days=1))
    newer = _granule_id(BURST_A, POST_ACQUISITION, POST_ACQUISITION + timedelta(days=2))
    other = _granule_id(BURST_B, POST_ACQUISITION, POST_ACQUISITION + timedelta(days=1))

    # ACT
    granules = _run_query_func([older, newer, other])

    # ASSERT
    assert sorted(g["granule_id"] for g in granules) == sorted([newer, other])
