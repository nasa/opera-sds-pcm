from data_subscriber.cmr import _filter_slc_granules


def test__filter_slc_granules__when_has_IW_then_filtered_in():
    # ARRANGE
    granule = {
        "related_urls": ["https://datapool.asf.alaska.edu/SLC/SA/S1A_IW_SLC__1SDV_20221117T004741_20221117T004756_045927_057ED0_CABE.zip"]
    }

    # ACT
    filtered_urls = _filter_slc_granules(granule)

    # ASSERT
    assert filtered_urls


def test__filter_slc_granules__when_not_has_IW__then_filtered_out():
    # ARRANGE
    granule = {
        "related_urls": ["https://datapool.asf.alaska.edu/SLC/SA/S1A_SLC__1SDV_20221117T004741_20221117T004756_045927_057ED0_CABE.zip"]
    }

    # ACT
    filtered_urls = _filter_slc_granules(granule)

    # ASSERT
    assert not filtered_urls

