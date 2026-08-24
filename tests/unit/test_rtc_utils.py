import re

from rtc_utils import rtc_product_file_regex, determine_acquisition_cycle_for_rtc_product_file, \
    determine_acquisition_cycle_for_rtc_granule, rtc_granule_regex, dedupe_rtc_es_docs, \
    determine_acquisition_cycle


def test_determine_acquisition_cycle_for_rtc_product_file():
    rtc_product_filename = "OPERA_L2_RTC-S1_T118-252624-IW1_20250512T193408Z_20250513T011557Z_S1A_30_v1.0.h5"
    assert 345 == determine_acquisition_cycle_for_rtc_product_file(rtc_product_filename=rtc_product_filename)


def test_determine_acquisition_cycle_for_rtc_product_file_2():
    rtc_product_filename = "OPERA_L2_RTC-S1_T118-252624-IW1_20250512T193408Z_20250513T011557Z_S1A_30_v1.0.h5"
    match = re.match(rtc_product_file_regex, rtc_product_filename)
    assert 345 == determine_acquisition_cycle_for_rtc_product_file(match_rtc_product_filename=match)

def test_determine_acquisition_cycle_for_rtc_granule():
    granule_id = "OPERA_L2_RTC-S1_T118-252624-IW1_20250512T193408Z_20250513T011557Z_S1A_30_v1.0"
    match = re.match(rtc_granule_regex, granule_id)
    assert 345 == determine_acquisition_cycle_for_rtc_granule(match_granule_id=match)


def test_determine_acquisition_cycle_for_s1d_rtc_granule():
    """S1D once carried a placeholder epoch, which raised instead of indexing."""
    granule_id = "OPERA_L2_RTC-S1_T118-252624-IW1_20250519T193408Z_20250520T011557Z_S1D_30_v1.0"
    match = re.match(rtc_granule_regex, granule_id)
    assert 345 == determine_acquisition_cycle_for_rtc_granule(match_granule_id=match)


def test_s1d_shares_the_cycle_of_the_s1a_pass_seven_days_earlier():
    """S1D flies the S1A ground track slot 7 days behind it, so a burst imaged by
    S1D falls in the same 12-day collection cycle as the S1A pass 7 days before."""
    burst_id = "T118-252624-IW1"
    s1a_acq, s1d_acq = "20250512T193408Z", "20250519T193408Z"
    s1a_id = f"OPERA_L2_RTC-S1_{burst_id}_{s1a_acq}_20250513T011557Z_S1A_30_v1.0"
    s1d_id = f"OPERA_L2_RTC-S1_{burst_id}_{s1d_acq}_20250520T011557Z_S1D_30_v1.0"

    assert (determine_acquisition_cycle(burst_id, s1a_acq, s1a_id)
            == determine_acquisition_cycle(burst_id, s1d_acq, s1d_id))

def test_dedupe_rtc_es_docs__when_es_docs__and_no_filter_path__and_no_sort():
    granule_ids = [
        "OPERA_L2_RTC-S1_T123-123456-IW1_20260101T123458Z_20260101T123456Z_S1A_30_v1.0",
        "OPERA_L2_RTC-S1_T123-123456-IW1_20260101T123457Z_20260101T123456Z_S1A_30_v1.0",
        "OPERA_L2_RTC-S1_T123-123456-IW1_20260101T123456Z_20260101T123456Z_S1A_30_v1.0",
    ]
    rtcs = []
    for granule_id in granule_ids:
        rtcs.append({"_source": {"granule_id": granule_id}})

    result_granule_ids = [
        "OPERA_L2_RTC-S1_T123-123456-IW1_20260101T123458Z_20260101T123456Z_S1A_30_v1.0",
        "OPERA_L2_RTC-S1_T123-123456-IW1_20260101T123457Z_20260101T123456Z_S1A_30_v1.0",
        "OPERA_L2_RTC-S1_T123-123456-IW1_20260101T123456Z_20260101T123456Z_S1A_30_v1.0",
    ]
    result_rtcs = []
    for granule_id in result_granule_ids:
        result_rtcs.append({"_source": {"granule_id": granule_id}})

    assert dedupe_rtc_es_docs(rtcs, filter_path=False, sort=False) == result_rtcs

def test_dedupe_rtc_es_docs__when_es_docs__and_no_filter_path__and_sort():
    granule_ids = [
        "OPERA_L2_RTC-S1_T123-123456-IW1_20260101T123458Z_20260101T123456Z_S1A_30_v1.0",
        "OPERA_L2_RTC-S1_T123-123456-IW1_20260101T123457Z_20260101T123456Z_S1A_30_v1.0",
        "OPERA_L2_RTC-S1_T123-123456-IW1_20260101T123456Z_20260101T123456Z_S1A_30_v1.0",
    ]
    rtcs = []
    for granule_id in granule_ids:
        rtcs.append({"_source": {"granule_id": granule_id}})

    result_granule_ids = [
        "OPERA_L2_RTC-S1_T123-123456-IW1_20260101T123456Z_20260101T123456Z_S1A_30_v1.0",
        "OPERA_L2_RTC-S1_T123-123456-IW1_20260101T123457Z_20260101T123456Z_S1A_30_v1.0",
        "OPERA_L2_RTC-S1_T123-123456-IW1_20260101T123458Z_20260101T123456Z_S1A_30_v1.0",
    ]
    result_rtcs = []
    for granule_id in result_granule_ids:
        result_rtcs.append({"_source": {"granule_id": granule_id}})

    assert dedupe_rtc_es_docs(rtcs, filter_path=False, sort=True) == result_rtcs


def test_dedupe_rtc_es_docs__when_es_docs__and_filter_path__and_no_sort():
    granule_ids = [
        "OPERA_L2_RTC-S1_T123-123456-IW1_20260101T123458Z_20260101T123456Z_S1A_30_v1.0",
        "OPERA_L2_RTC-S1_T123-123456-IW1_20260101T123457Z_20260101T123456Z_S1A_30_v1.0",
        "OPERA_L2_RTC-S1_T123-123456-IW1_20260101T123456Z_20260101T123456Z_S1A_30_v1.0",
    ]
    rtcs = []
    for granule_id in granule_ids:
        rtcs.append({"granule_id": granule_id})

    result_granule_ids = [
        "OPERA_L2_RTC-S1_T123-123456-IW1_20260101T123458Z_20260101T123456Z_S1A_30_v1.0",
        "OPERA_L2_RTC-S1_T123-123456-IW1_20260101T123457Z_20260101T123456Z_S1A_30_v1.0",
        "OPERA_L2_RTC-S1_T123-123456-IW1_20260101T123456Z_20260101T123456Z_S1A_30_v1.0",
    ]
    result_rtcs = []
    for granule_id in result_granule_ids:
        result_rtcs.append({"granule_id": granule_id})

    assert dedupe_rtc_es_docs(rtcs, filter_path=True, sort=False) == result_rtcs

def test_dedupe_rtc_es_docs__when_es_docs__and_filter_path__and_sort():
    granule_ids = [
        "OPERA_L2_RTC-S1_T123-123456-IW1_20260101T123458Z_20260101T123456Z_S1A_30_v1.0",
        "OPERA_L2_RTC-S1_T123-123456-IW1_20260101T123457Z_20260101T123456Z_S1A_30_v1.0",
        "OPERA_L2_RTC-S1_T123-123456-IW1_20260101T123456Z_20260101T123456Z_S1A_30_v1.0",
    ]
    rtcs = []
    for granule_id in granule_ids:
        rtcs.append({"granule_id": granule_id})

    result_granule_ids = [
        "OPERA_L2_RTC-S1_T123-123456-IW1_20260101T123456Z_20260101T123456Z_S1A_30_v1.0",
        "OPERA_L2_RTC-S1_T123-123456-IW1_20260101T123457Z_20260101T123456Z_S1A_30_v1.0",
        "OPERA_L2_RTC-S1_T123-123456-IW1_20260101T123458Z_20260101T123456Z_S1A_30_v1.0",
    ]
    result_rtcs = []
    for granule_id in result_granule_ids:
        result_rtcs.append({"granule_id": granule_id})

    assert dedupe_rtc_es_docs(rtcs, filter_path=True, sort=True) == result_rtcs
