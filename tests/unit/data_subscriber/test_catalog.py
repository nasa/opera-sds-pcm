
from datetime import date, datetime, timedelta
from unittest import TestCase
from unittest.mock import patch

from data_subscriber.cslc.cslc_catalog import CSLCProductCatalog
from data_subscriber.cslc.cslc_catalog import CSLCStaticProductCatalog
from data_subscriber.hls.hls_catalog import HLSProductCatalog
from data_subscriber.hls.hls_catalog import HLSSpatialProductCatalog
from data_subscriber.rtc.rtc_catalog import RTCProductCatalog
from data_subscriber.slc.slc_catalog import SLCProductCatalog
from data_subscriber.slc.slc_catalog import SLCSpatialProductCatalog


def mock_hls_query(self, **kwargs):
    return [
        {
            "_id": "HLS.S30.T56MPU.2022152T000741.v2.0-r1",
            "_source": {
                "granule_id": "HLS.S30.T56MPU.2022152T000741.v2.0",
                "revision_id": "1",
                "s3_url": "s3://path/to/HLS.S30.T56MPU.2022152T000741.v2.0",
                "https_url": "https://path/to/HLS.S30.T56MPU.2022152T000741.v2.0",
                "filtered_field": "should not be present"
            }
        }
    ]

def mock_rtc_query(self, **kwargs):
    return [
        {
            "_id": "OPERA_L2_RTC-S1_T011-022517-IW1_20231019T111602Z_20231019T214046Z_S1A_30_v1.0-r1",
            "_source": {
                "granule_id": "OPERA_L2_RTC-S1_T011-022517-IW1_20231019T111602Z_20231019T214046Z_S1A_30_v1.0",
                "revision_id": "1",
                "mgrs_set_id_acquisition_ts_cycle_index": "MS_12_16$145",
                "s3_url": "s3://path/to/OPERA_L2_RTC-S1_T011-022517-IW1_20231019T111602Z_20231019T214046Z_S1A_30_v1.0",
                "https_url": "https://path/to/OPERA_L2_RTC-S1_T011-022517-IW1_20231019T111602Z_20231019T214046Z_S1A_30_v1.0",
            }
        },
        {
            "_id": "OPERA_L2_RTC-S1_T011-022517-IW3_20231019T111602Z_20231019T214046Z_S1A_30_v1.0-r1",
            "_source": {
                "granule_id": "OPERA_L2_RTC-S1_T011-022517-IW3_20231019T111602Z_20231019T214046Z_S1A_30_v1.0",
                "revision_id": "1",
                "mgrs_set_id_acquisition_ts_cycle_index": "MS_12_16$146",
                "s3_url": "s3://path/to/OPERA_L2_RTC-S1_T011-022517-IW3_20231019T111602Z_20231019T214046Z_S1A_30_v1.0",
                "https_url": "https://path/to/OPERA_L2_RTC-S1_T011-022517-IW3_20231019T111602Z_20231019T214046Z_S1A_30_v1.0",
            }
        },
    ]

def mock_cslc_query(self, **kwargs):
    return [
        {
            "_id": "11112_15",
            "_source": {
                "k": 4,
                "m": 2
            }
        }
    ]

def test_hls_product_catalog():
    """Tests the HLSProductCatalog class, including functionality inherited from parent ProductCatalog class"""
    hls_product_catalog = HLSProductCatalog()

    # HLSProductCatalog specific tests
    assert hls_product_catalog.NAME == "hls_catalog"
    assert hls_product_catalog.ES_INDEX_PATTERNS == "hls_catalog*"

    assert isinstance(hls_product_catalog.generate_es_index_name(), str)
    assert hls_product_catalog.generate_es_index_name().startswith("hls_catalog-")

    granule, revision = hls_product_catalog.granule_and_revision(es_id="HLS.S30.T56MPU.2022152T000741.v2.0-r1")

    assert granule == "HLS.S30.T56MPU.2022152T000741.v2.0"
    assert revision == "1"

    # ProductCatalog specific tests
    with patch("tests.unit.conftest.MockElasticsearchUtility.query", new=mock_hls_query):
        # Tests for ProductCatalog.get_download_granule_revision()
        result = hls_product_catalog.get_download_granule_revision("HLS.S30.T56MPU.2022152T000741.v2.0-r1")
        expected_result = {
            "_id": "HLS.S30.T56MPU.2022152T000741.v2.0-r1",
            "granule_id": "HLS.S30.T56MPU.2022152T000741.v2.0",
            "revision_id": "1",
            "s3_url": "s3://path/to/HLS.S30.T56MPU.2022152T000741.v2.0",
            "https_url": "https://path/to/HLS.S30.T56MPU.2022152T000741.v2.0"
        }

        assert len(result) == 1
        TestCase().assertDictEqual(result[0], expected_result)

    test_granule = {
        "granule_id": "HLS.S30.T56MPU.2022152T000741.v2.0-r1",
        "provider": "PO.DAAC",
        "production_datetime": str(datetime.now()),
        "provider_date": str(date.today()),
        "short_name": "HLS.S30.T56MPU",
        "identifier": "HLS.S30.T56MPU.2022152T000741.v2.0",
        "bounding_box": [1, 2, 3, 4]
    }

    with patch("tests.unit.conftest.MockElasticsearchUtility.index_document") as mock_index_document:
        # Tests for ProductCatalog.process_granule()
        hls_product_catalog.process_granule(test_granule)
        mock_index_document.assert_called()
        assert mock_index_document.call_args.kwargs["body"]["id"] == "HLS.S30.T56MPU.2022152T000741.v2.0-r1"
        assert mock_index_document.call_args.kwargs["body"]["bounding_box"] == [1, 2, 3, 4]
        assert "creation_timestamp" in mock_index_document.call_args.kwargs["body"]
        assert isinstance(mock_index_document.call_args.kwargs["body"]["creation_timestamp"], datetime)

    with patch("tests.unit.conftest.MockElasticsearchUtility.update_document") as mock_update_document:
        # Tests for ProductCatalog.process_url()
        hls_product_catalog.process_url(
            urls=["s3://path/to/HLS.S30.T56MPU.2022152T000741.v2.0"],
            granule=test_granule,
            job_id="test_hls_job_id",
            query_dt=datetime.now(),
            temporal_extent_beginning_dt=datetime.now(),
            revision_date_dt=datetime.now(),
            revision_id="99",
            additional_kwarg="additional_value"
        )
        mock_update_document.assert_called()
        assert mock_update_document.call_args.kwargs["id"] == "HLS.S30.T56MPU.2022152T000741.v2.0-r99"
        assert "additional_kwarg" in mock_update_document.call_args.kwargs["body"]["doc"]
        assert mock_update_document.call_args.kwargs["body"]["doc"]["additional_kwarg"] == "additional_value"
        assert "s3_url" in mock_update_document.call_args.kwargs["body"]["doc"]
        assert "https_url" not in mock_update_document.call_args.kwargs["body"]["doc"]

        mock_update_document.reset_mock()

        hls_product_catalog.process_url(
            urls=["https://path/to/HLS.S30.T56MPU.2022152T000741.v2.0"],
            granule=test_granule,
            job_id="test_hls_job_id",
            query_dt=datetime.now(),
            temporal_extent_beginning_dt=datetime.now(),
            revision_date_dt=datetime.now()
        )
        mock_update_document.assert_called()
        assert mock_update_document.call_args.kwargs["id"] == "HLS.S30.T56MPU.2022152T000741.v2.0-r1"
        assert "s3_url" not in mock_update_document.call_args.kwargs["body"]["doc"]
        assert "https_url" in mock_update_document.call_args.kwargs["body"]["doc"]

        mock_update_document.reset_mock()

        # Tests for ProductCatalog.mark_product_as_downloaded()
        hls_product_catalog.mark_product_as_downloaded(
            url="s3://path/to/HLS.S30.T56MPU.2022152T000741.v2.0",
            job_id="test_hls_job_id",
            filesize=123,
            doc={"additional_job_ts": str(datetime.now())}
        )
        mock_update_document.assert_called()
        assert mock_update_document.call_args.kwargs["id"] == "HLS.S30.T56MPU.2022152T000741.v2.0"
        assert mock_update_document.call_args.kwargs["body"]["doc"]["downloaded"] == True
        assert mock_update_document.call_args.kwargs["body"]["doc"]["download_job_id"] == "test_hls_job_id"
        assert mock_update_document.call_args.kwargs["body"]["doc"]["metadata"] == {"FileSize": 123}
        assert "additional_job_ts" in mock_update_document.call_args.kwargs["body"]["doc"]

    with patch("tests.unit.conftest.MockElasticsearch.update_by_query") as mock_update_by_query:
        # Tests for ProductCatalog.mark_download_job_id()
        hls_product_catalog.mark_download_job_id(batch_id="test_batch_id", job_id="test_job_id")
        mock_update_by_query.assert_called()
        assert mock_update_by_query.call_args.kwargs["index"] == "hls_catalog*"
        assert mock_update_by_query.call_args.kwargs["body"]["script"]["source"] == "ctx._source.download_job_id = 'test_job_id'"
        assert mock_update_by_query.call_args.kwargs["body"]["query"]["bool"]["must"][0]["term"]["download_batch_id"] == "test_batch_id"

    with patch("tests.unit.conftest.MockElasticsearchUtility.query") as mock_query:
        # Tests for ProductCatalog.get_all_between()
        start_dt = datetime.now()
        end_dt = start_dt + timedelta(seconds=30)

        hls_product_catalog.get_all_between(start_dt, end_dt, use_temporal=False)
        mock_query.assert_called()
        assert "revision_date" in mock_query.call_args.kwargs["body"]["query"]["bool"]["must"][0]["range"]
        assert mock_query.call_args.kwargs["body"]["query"]["bool"]["must"][0]["range"]["revision_date"]["gte"] == start_dt.isoformat()
        assert mock_query.call_args.kwargs["body"]["query"]["bool"]["must"][0]["range"]["revision_date"]["lt"] == end_dt.isoformat()

        mock_query.reset_mock()

        hls_product_catalog.get_all_between(start_dt, end_dt, use_temporal=True)
        mock_query.assert_called()
        assert "temporal_extent_beginning_datetime" in mock_query.call_args.kwargs["body"]["query"]["bool"]["must"][0]["range"]
        assert mock_query.call_args.kwargs["body"]["query"]["bool"]["must"][0]["range"]["temporal_extent_beginning_datetime"]["gte"] == start_dt.isoformat()
        assert mock_query.call_args.kwargs["body"]["query"]["bool"]["must"][0]["range"]["temporal_extent_beginning_datetime"]["lt"] == end_dt.isoformat()

    with patch("tests.unit.conftest.MockIndicesClient.refresh") as mock_refresh:
        # Tests for ProductCatalog.refresh()
        hls_product_catalog.refresh()
        mock_refresh.assert_called()
        assert mock_refresh.call_args.kwargs["index"] == hls_product_catalog.ES_INDEX_PATTERNS

def test_hls_spatial_product_catalog():
    """Tests for functionality specific to the HLSSpatialProductCatalog class"""
    hls_spatial_product_catalog = HLSSpatialProductCatalog()

    assert hls_spatial_product_catalog.NAME == "hls_spatial_catalog"
    assert hls_spatial_product_catalog.ES_INDEX_PATTERNS == "hls_spatial_catalog*"

    assert isinstance(hls_spatial_product_catalog.generate_es_index_name(), str)
    assert hls_spatial_product_catalog.generate_es_index_name().startswith("hls_spatial_catalog-")

def test_slc_product_catalog():
    """Tests for functionality specifc to the SLCProductCatalog class"""
    slc_product_catalog = SLCProductCatalog()

    assert slc_product_catalog.NAME == "slc_catalog"
    assert slc_product_catalog.ES_INDEX_PATTERNS == "slc_catalog*"

    assert isinstance(slc_product_catalog.generate_es_index_name(), str)
    assert slc_product_catalog.generate_es_index_name().startswith("slc_catalog-")

    granule, revision = slc_product_catalog.granule_and_revision(
        es_id="S1A_IW_SLC__1SDV_20220601T000522_20220601T000549_043462_05308F_86F3.zip-r5"
    )

    assert granule == "S1A_IW_SLC__1SDV_20220601T000522_20220601T000549_043462_05308F_86F3-SLC"
    assert revision == "5"

def test_slc_spatial_product_catalog():
    """Tests for functionality specific to the SLCSpatialProductCatalog class"""
    slc_spatial_product_catalog = SLCSpatialProductCatalog()

    assert slc_spatial_product_catalog.NAME == "slc_spatial_catalog"
    assert slc_spatial_product_catalog.ES_INDEX_PATTERNS == "slc_spatial_catalog*"

    assert isinstance(slc_spatial_product_catalog.generate_es_index_name(), str)
    assert slc_spatial_product_catalog.generate_es_index_name().startswith("slc_spatial_catalog-")

def test_rtc_product_catalog():
    """Tests for functionality specific to the RTCProductCatalog class"""
    rtc_product_catalog = RTCProductCatalog()

    assert rtc_product_catalog.NAME == "rtc_catalog"
    assert rtc_product_catalog.ES_INDEX_PATTERNS == "rtc_catalog*"

    assert isinstance(rtc_product_catalog.generate_es_index_name(), str)
    assert rtc_product_catalog.generate_es_index_name().startswith("rtc_catalog-")

    granule, revision = rtc_product_catalog.granule_and_revision(
        es_id="OPERA_L2_RTC-S1_T011-022517-IW3_20231019T111602Z_20231019T214046Z_S1A_30_v1.0-r1"
    )

    assert granule == "OPERA_L2_RTC-S1_T011-022517-IW3_20231019T111602Z_20231019T214046Z_S1A_30_v1.0"
    assert revision == "1"

    with patch("tests.unit.conftest.MockElasticsearchUtility.query", new=mock_rtc_query):
        # Tests for RTCProductCatalog.get_download_granule_revision()
        result = rtc_product_catalog.get_download_granule_revision(mgrs_set_id_acquisition_ts_cycle_index="MS_12_16$146")
        expected_result = {
            "granule_id": "OPERA_L2_RTC-S1_T011-022517-IW3_20231019T111602Z_20231019T214046Z_S1A_30_v1.0",
            "revision_id": "1",
            "mgrs_set_id_acquisition_ts_cycle_index": "MS_12_16$146",
            "s3_url": "s3://path/to/OPERA_L2_RTC-S1_T011-022517-IW3_20231019T111602Z_20231019T214046Z_S1A_30_v1.0",
            "https_url": "https://path/to/OPERA_L2_RTC-S1_T011-022517-IW3_20231019T111602Z_20231019T214046Z_S1A_30_v1.0",
        }

        assert len(result) == 1
        TestCase().assertDictEqual(result[0], expected_result)

        # Tests for RTCProductCatalog.filter_catalog_by_sets()
        results = rtc_product_catalog.filter_catalog_by_sets(
            mgrs_set_id_acquisition_ts_cycle_indexes=["MS_12_16$145", "MS_12_16$146"]
        )
        expected_results = [
            {
                "granule_id": "OPERA_L2_RTC-S1_T011-022517-IW1_20231019T111602Z_20231019T214046Z_S1A_30_v1.0",
                "revision_id": "1",
                "mgrs_set_id_acquisition_ts_cycle_index": "MS_12_16$145",
                "s3_url": "s3://path/to/OPERA_L2_RTC-S1_T011-022517-IW1_20231019T111602Z_20231019T214046Z_S1A_30_v1.0",
                "https_url": "https://path/to/OPERA_L2_RTC-S1_T011-022517-IW1_20231019T111602Z_20231019T214046Z_S1A_30_v1.0",
            },
            {
                "granule_id": "OPERA_L2_RTC-S1_T011-022517-IW3_20231019T111602Z_20231019T214046Z_S1A_30_v1.0",
                "revision_id": "1",
                "mgrs_set_id_acquisition_ts_cycle_index": "MS_12_16$146",
                "s3_url": "s3://path/to/OPERA_L2_RTC-S1_T011-022517-IW3_20231019T111602Z_20231019T214046Z_S1A_30_v1.0",
                "https_url": "https://path/to/OPERA_L2_RTC-S1_T011-022517-IW3_20231019T111602Z_20231019T214046Z_S1A_30_v1.0",
            }
        ]

        assert len(results) == 2
        for result, expected_result in zip(results, expected_results):
            TestCase().assertDictEqual(result, expected_result)

    batch_id_to_product_id_map = {
        "MS_12_16$145": {
            "product_id": [
                {"id": "product1", "download_job_ids": ["id1"]},
                {"id": "product2", "download_job_ids": ["id2"]},
                {"id": "product3", "download_job_ids": ["id3"]}
            ]
        }
    }

    batch_id_to_products_map = {
        "MS_12_16$145": [
            {"id": "product1", "dswx_s1_jobs_ids": ["id1"], "production_datetime": datetime.now(), "creation_timestamp": datetime.now()},
            {"id": "product2", "dswx_s1_jobs_ids": ["id2"], "production_datetime": datetime.now(), "creation_timestamp": datetime.now()},
            {"id": "product3", "dswx_s1_jobs_ids": ["id3"], "production_datetime": datetime.now(), "creation_timestamp": datetime.now()}
        ]
    }

    with patch("elasticsearch.helpers.bulk") as mock_bulk:
        with patch("tests.unit.conftest.MockIndicesClient.refresh") as mock_refresh:
            with patch("tests.unit.conftest.MockElasticsearchUtility.query"):
                # Tests for RTCProductCatalog.mark_products_as_download_job_submitted()
                rtc_product_catalog.mark_products_as_download_job_submitted(batch_id_to_product_id_map)

                mock_bulk.assert_called()
                mock_refresh.assert_called()

                operations = mock_bulk.call_args.args[1]
                assert len(operations) == 3

                mock_bulk.reset_mock()
                mock_refresh.reset_mock()

                # Tests for RTCProductCatalog.mark_products_as_job_submitted()
                rtc_product_catalog.mark_products_as_job_submitted(batch_id_to_products_map)

                mock_bulk.assert_called()
                mock_refresh.assert_called()

                operations = mock_bulk.call_args.args[1]
                assert len(operations) == 3

    with patch("tests.unit.conftest.MockElasticsearchUtility.update_document") as mock_update_document:
        # Tests for RTCProductCatalog.update_granule_index()
        test_granule = {
            "granule_id": "OPERA_L2_RTC-S1_T011-022517-IW3_20231019T111602Z_20231019T214046Z_S1A_30_v1.0",
            "filtered_urls": ["s3://path/to/OPERA_L2_RTC-S1_T011-022517-IW1_20231019T111602Z_20231019T214046Z_S1A_30_v1.0",
                              "https://path/to/OPERA_L2_RTC-S1_T011-022517-IW3_20231019T111602Z_20231019T214046Z_S1A_30_v1.0"],
            "temporal_extent_beginning_datetime": datetime.now().isoformat(),
            "revision_date": date.today().isoformat(),
            "production_datetime": datetime.now()
        }
        rtc_product_catalog.update_granule_index(
            granule=test_granule, job_id="job_id", query_dt=datetime.now(),
            mgrs_set_id_acquisition_ts_cycle_indexes=["MS_12_16$146"], additional_kwarg="additional_value"
        )
        mock_update_document.assert_called()
        assert mock_update_document.call_args.kwargs["body"]["doc"]["id"] == "OPERA_L2_RTC-S1_T011-022517-IW3_20231019T111602Z_20231019T214046Z_S1A_30_v1.0$MS_12_16$146"
        assert "additional_kwarg" in mock_update_document.call_args.kwargs["body"]["doc"]
        assert mock_update_document.call_args.kwargs["body"]["doc"]["additional_kwarg"] == "additional_value"
        assert mock_update_document.call_args.kwargs["body"]["doc"]["https_urls"] == ["https://path/to/OPERA_L2_RTC-S1_T011-022517-IW3_20231019T111602Z_20231019T214046Z_S1A_30_v1.0"]
        assert mock_update_document.call_args.kwargs["body"]["doc"]["s3_urls"] == ["s3://path/to/OPERA_L2_RTC-S1_T011-022517-IW1_20231019T111602Z_20231019T214046Z_S1A_30_v1.0"]

def test_clsc_product_catalog():
    """Tests for functionality specific to the CSLCProductCatalog class"""
    cslc_product_catalog = CSLCProductCatalog()

    assert cslc_product_catalog.NAME == "cslc_catalog"
    assert cslc_product_catalog.ES_INDEX_PATTERNS == "cslc_catalog*"

    assert isinstance(cslc_product_catalog.generate_es_index_name(), str)
    assert cslc_product_catalog.generate_es_index_name().startswith("cslc_catalog-")

    with patch("tests.unit.conftest.MockElasticsearchUtility.query") as mock_query:
        # Tests for CSLCProductCatalog.get_unsubmitted_granules()
        cslc_product_catalog.get_unsubmitted_granules()
        mock_query.assert_called()
        assert mock_query.call_args.kwargs["index"] == "cslc_catalog*"
        assert mock_query.call_args.kwargs["body"]["query"]["bool"]["must_not"][0]["exists"]["field"] == "download_job_id"
        assert mock_query.call_args.kwargs["body"]["query"]["bool"]["must"][0]["term"]["processing_mode"] == "forward"

        mock_query.reset_mock()

        cslc_product_catalog.get_unsubmitted_granules(processing_mode="historical")
        mock_query.assert_called()
        assert mock_query.call_args.kwargs["body"]["query"]["bool"]["must"][0]["term"]["processing_mode"] == "historical"

        mock_query.reset_mock()

        # Tests for CSLCProductCatalog.get_submitted_granules()
        cslc_product_catalog.get_submitted_granules(download_batch_id="11114_15")
        mock_query.assert_called()
        assert mock_query.call_args.kwargs["index"] == "cslc_catalog*"
        assert mock_query.call_args.kwargs["body"]["query"]["bool"]["must"][0]["term"]["download_batch_id"] == "11114_15"
        assert mock_query.call_args.kwargs["body"]["query"]["bool"]["must"][1]["exists"]["field"] == "download_job_id"

        mock_query.reset_mock()

        # Tests for CSLCProductCatalog.get_download_granule_revision()
        cslc_product_catalog.get_download_granule_revision(granule_id="11113_15")
        mock_query.assert_called()
        assert mock_query.call_args.kwargs["index"] == "cslc_catalog*"
        assert mock_query.call_args.kwargs["body"]["query"]["bool"]["must"][0]["term"]["download_batch_id"] == "11113_15"

    with patch("tests.unit.conftest.MockElasticsearchUtility.query", new=mock_cslc_query):
        # Tests for CSLCProductCatalog.get_k_and_m()
        k, m = cslc_product_catalog.get_k_and_m(granule_id="11112_15")
        assert k == 4
        assert m == 2

    with patch("tests.unit.conftest.MockElasticsearchUtility.update_document") as mock_update_document:
        # Tests for CSLCProductCatalog.mark_product_as_downloaded()
        cslc_product_catalog.mark_product_as_downloaded(
            url="s3://path/to/OPERA_L2_CSLC-S1_T042-088897-IW1_20240125T140731Z_20240126T091539Z_S1A_VV_v1.0.h5",
            job_id="test_cslc_job_id",
            filesize=321
        )
        mock_update_document.assert_called()
        assert mock_update_document.call_args.kwargs["id"] == "OPERA_L2_CSLC-S1_T042-088897-IW1_20240125T140731Z_20240126T091539Z_S1A_VV_v1.0.h5"
        assert mock_update_document.call_args.kwargs["body"]["doc"]["downloaded"] == True
        assert mock_update_document.call_args.kwargs["body"]["doc"]["download_job_id"] == "test_cslc_job_id"
        assert mock_update_document.call_args.kwargs["body"]["doc"]["metadata"] == {"FileSize": 321}
        assert "latest_download_job_ts" in mock_update_document.call_args.kwargs["body"]["doc"]

def test_cslc_static_product_catalog():
    """Tests for functionality specific to the CSLCStaticProductCatalog class"""
    cslc_static_product_catalog = CSLCStaticProductCatalog()

    assert cslc_static_product_catalog.NAME == "cslc_static_catalog"
    assert cslc_static_product_catalog.ES_INDEX_PATTERNS == "cslc_static_catalog*"

    assert isinstance(cslc_static_product_catalog.generate_es_index_name(), str)
    assert cslc_static_product_catalog.generate_es_index_name().startswith("cslc_static_catalog-")

    granule, revision = cslc_static_product_catalog.granule_and_revision(
        es_id="OPERA_L2_CSLC-S1-STATIC_T042-088897-IW1_20140403_S1A_v1.0.h5-r50"
    )

    assert granule == "OPERA_L2_CSLC-S1-STATIC_T042-088897-IW1_20140403_S1A_v1.0.h5"
    assert revision == "50"
