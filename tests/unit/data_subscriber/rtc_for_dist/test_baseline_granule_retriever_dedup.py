"""Dedup of RTC granules (unique_latest_granules: one per burst and acquisition minute) and its use for baseline granules."""
import logging
from datetime import datetime, timedelta

from data_subscriber.dist_s1_utils import basic_decorate_granule
from data_subscriber.rtc_for_dist.baseline_granule_retriever import unique_latest_granules, \
    BaselineGranuleRetriever, Args

PRODUCT_ID = "18TVT_2"
DOWNLOAD_BATCH_ID = "p18TVT_2_S1A_a365"
VV_VH = ("VV", "VH")
BURST_A = "T018-000001-IW1"
BURST_B = "T018-000002-IW2"
BURSTS = [BURST_A, BURST_B]

POST_ACQUISITION = datetime(2026, 1, 1, 22, 53, 0)
K_OFFSETS_AND_COUNTS = [(365, 3)]
WINDOW_DELTA_DAYS = 60


def _decorated_granule(burst_id, acquisition_ts, production_ts):
    granule_id = f"OPERA_L2_RTC-S1_{burst_id}_{acquisition_ts:%Y%m%dT%H%M%S}Z_{production_ts:%Y%m%dT%H%M%S}Z_S1A_30_v1.0"
    return {"granule_id": granule_id, "burst_id": burst_id,
            "acquisition_ts": acquisition_ts, "production_ts": production_ts}


def test_unique_latest_granules__when_same_second__then_keeps_latest_production():
    older = _decorated_granule("T123-123456-IW1", datetime(2026, 1, 1, 12, 34, 56), datetime(2026, 1, 2))
    newer = _decorated_granule("T123-123456-IW1", datetime(2026, 1, 1, 12, 34, 56), datetime(2026, 1, 3))

    assert unique_latest_granules([older, newer]) == [newer]
    assert unique_latest_granules([newer, older]) == [newer]


def test_unique_latest_granules__when_reprocessed_twins_are_1_second_apart__then_keeps_latest_production():
    # ARRANGE
    # One acquisition processed three times (CMR, burst T139-297073-IW2); the first run has the acquisition time 1 s later
    first_run = _decorated_granule("T139-297073-IW2", datetime(2024, 2, 25, 5, 37, 38), datetime(2024, 2, 25, 10, 58, 30))
    same_day_rerun = _decorated_granule("T139-297073-IW2", datetime(2024, 2, 25, 5, 37, 37), datetime(2024, 2, 25, 14, 13, 29))
    reprocessed = _decorated_granule("T139-297073-IW2", datetime(2024, 2, 25, 5, 37, 37), datetime(2024, 3, 19, 3, 10, 38))

    # ACT / ASSERT
    assert unique_latest_granules([same_day_rerun, reprocessed, first_run]) == [reprocessed]
    assert unique_latest_granules([first_run, reprocessed, same_day_rerun]) == [reprocessed]


def test_unique_latest_granules__when_different_bursts__then_keeps_both():
    a = _decorated_granule("T123-123456-IW1", datetime(2026, 1, 1, 12, 34, 56), datetime(2026, 1, 2))
    b = _decorated_granule("T123-123456-IW2", datetime(2026, 1, 1, 12, 34, 56), datetime(2026, 1, 3))

    assert unique_latest_granules([a, b]) == [a, b]


def test_unique_latest_granules__when_production_ts_is_str():
    older = _decorated_granule("T123-123456-IW1", datetime(2026, 1, 1, 12, 34, 56), datetime(2026, 1, 2))
    newer = _decorated_granule("T123-123456-IW1", datetime(2026, 1, 1, 12, 34, 56), datetime(2026, 1, 3))
    older["production_ts"] = "2026-01-02T00:00:00"
    newer["production_ts"] = "2026-01-03T00:00:00"

    assert unique_latest_granules([older, newer]) == [newer]


def _granule(burst_id, acquisition_dt, production_dt=None):
    production_dt = production_dt or acquisition_dt + timedelta(days=4)
    granule_id = f"OPERA_L2_RTC-S1_{burst_id}_{acquisition_dt:%Y%m%dT%H%M%S}Z_{production_dt:%Y%m%dT%H%M%S}Z_S1A_30_v1.0"
    return {
        "granule_id": granule_id,
        "polarization": list(VV_VH),
        "related_urls": [],
        "revision_id": 1,
        "filtered_urls": [f"s3://bucket/{granule_id}/{granule_id}_{p}.tif" for p in VV_VH],
    }


def _baseline_pass(n):
    """Acquisition time of the n-th 12-day pass before the start of the 365-day look-back window."""
    return POST_ACQUISITION - timedelta(days=365 + 12 * n)


def _current_granules():
    current_granules = []
    for burst_id in BURSTS:
        granule = _granule(burst_id, POST_ACQUISITION)
        basic_decorate_granule(granule)
        granule["product_id"] = PRODUCT_ID
        granule["download_batch_id"] = DOWNLOAD_BATCH_ID
        granule["batch_id"] = DOWNLOAD_BATCH_ID
        current_granules.append(granule)
    return current_granules


def _retrieve(pool):
    """Retrieve baselines for the current granules from a fake CMR serving pool: burst_id -> [(acquisition_dt[, production_dt])]."""
    def query_func_factory(use_async=True, secondary=True, args=None):
        bursts = [s.removeprefix("OPERA_L2_RTC-S1_").rstrip("*") for s in args.native_id.split("&native-id[]=")]

        async def query(date_time_range, _now):
            start = datetime.strptime(date_time_range.start_date, "%Y-%m-%dT%H:%M:%SZ")
            end = datetime.strptime(date_time_range.end_date, "%Y-%m-%dT%H:%M:%SZ")
            return [_granule(burst_id, acquisition_dt, *production_dt)
                    for burst_id in bursts
                    for acquisition_dt, *production_dt in pool.get(burst_id, [])
                    if start <= acquisition_dt <= end]
        return query

    retriever = BaselineGranuleRetriever(
        logger=logging.getLogger(__name__),
        args=Args(),
        k_offsets_counts=K_OFFSETS_AND_COUNTS,
        product_to_bursts={PRODUCT_ID: set(BURSTS)},
        window_delta_days=WINDOW_DELTA_DAYS,
        settings={},
        bursts_to_products={burst_id: {PRODUCT_ID} for burst_id in BURSTS},
        query_func_factory=query_func_factory,
    )
    return retriever.retrieve_baseline_granules(PRODUCT_ID, _current_granules(), Args(), K_OFFSETS_AND_COUNTS,
                                                verbose=False)


def _granule_ids(baseline_granules, burst_id):
    return sorted(g["granule_id"] for g in baseline_granules if g["burst_id"] == burst_id)


def test_retrieve_baseline_granules__when_baseline_twins_are_1_second_apart__then_latest_production_is_kept():
    # ARRANGE
    # BURST_A's newest baseline pass has a twin 1 s later, processed earlier: one acquisition processed twice.
    # RTCs of a burst in the same acquisition minute are one image, so only the latest production is kept.
    twin_dt = _baseline_pass(1) + timedelta(seconds=1)
    twin_production_dt = _baseline_pass(1) + timedelta(days=2)
    pool = {burst_id: [(_baseline_pass(n),) for n in range(1, 6)] for burst_id in BURSTS}
    pool[BURST_A].append((twin_dt, twin_production_dt))

    # ACT
    baseline_granules = _retrieve(pool)

    # ASSERT
    assert _granule_ids(baseline_granules, BURST_A) == sorted(
        _granule(BURST_A, _baseline_pass(n))["granule_id"] for n in (1, 2, 3))
    assert _granule_ids(baseline_granules, BURST_B) == sorted(
        _granule(BURST_B, _baseline_pass(n))["granule_id"] for n in (1, 2, 3))


def test_retrieve_baseline_granules__when_baseline_duplicates_share_the_second__then_latest_production_is_kept():
    # ARRANGE
    # BURST_A's newest baseline pass was produced twice: same acquisition second, later production time
    reprocessed_production_dt = _baseline_pass(1) + timedelta(days=30)
    pool = {burst_id: [(_baseline_pass(n),) for n in range(1, 6)] for burst_id in BURSTS}
    pool[BURST_A].append((_baseline_pass(1), reprocessed_production_dt))

    # ACT
    baseline_granules = _retrieve(pool)

    # ASSERT
    assert _granule_ids(baseline_granules, BURST_A) == sorted([
        _granule(BURST_A, _baseline_pass(1), reprocessed_production_dt)["granule_id"],
        _granule(BURST_A, _baseline_pass(2))["granule_id"],
        _granule(BURST_A, _baseline_pass(3))["granule_id"],
    ])
