import re

from rtc_utils import rtc_product_file_regex, determine_acquisition_cycle_for_rtc_product_file, \
    determine_acquisition_cycle_for_rtc_granule, rtc_granule_regex


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
