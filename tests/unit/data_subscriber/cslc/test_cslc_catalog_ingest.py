"""Tests for CSLC-S1 Catalog Ingest."""

import json
import os
import shutil
import sys
import tempfile
import unittest
from collections import defaultdict
from unittest.mock import MagicMock, patch, AsyncMock

# Mock heavy imports to avoid numpy/elasticsearch version issues in local dev.
_mock_cslc_utils = MagicMock()

with patch.dict(sys.modules, {
    "data_subscriber.cslc_utils": _mock_cslc_utils,
    "util.exec_util": MagicMock(),
    "util.ctx_util": MagicMock(),
    "util.conf_util": MagicMock(),
    "data_subscriber.es_conn_util": MagicMock(),
    "data_subscriber.cmr": MagicMock(),
    "tools.ops.cmr_audit.cmr_client": MagicMock(),
    "hysds.celery": MagicMock(),
    "elasticsearch": MagicMock(),
    "elasticsearch.client": MagicMock(),
    "elasticsearch.transport": MagicMock(),
    "elasticsearch.serializer": MagicMock(),
}):
    from data_subscriber.cslc import cslc_catalog_ingest as ingest_mod
    from data_subscriber.cslc.cslc_catalog_ingest import CslcCatalogIngest


class _FakeHistBursts:
    def __init__(self, frame_number, burst_ids, day_indices):
        self.frame_number = frame_number
        self.burst_ids = set(burst_ids)
        self.sensing_datetimes = []
        self.sensing_seconds_since_first = []
        self.sensing_datetime_days_index = day_indices


def _make_umm_item(granule_ur, s3_urls, start_time="2024-08-01T18:31:17Z"):
    """Build a minimal UMM JSON item for testing."""
    related_urls = [{"URL": url, "Type": "GET DATA VIA DIRECT ACCESS"} for url in s3_urls]
    return {
        "umm": {
            "GranuleUR": granule_ur,
            "RelatedUrls": related_urls,
            "TemporalExtent": {
                "RangeDateTime": {
                    "BeginningDateTime": start_time,
                    "EndingDateTime": start_time,
                }
            },
        },
        "meta": {
            "concept-id": "G12345-ASF",
            "revision-id": 1,
        },
    }


class TestCreateDatasets(unittest.TestCase):

    def setUp(self):
        self.orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir)

    def test_creates_vv_dataset(self):
        items = [
            _make_umm_item(
                "OPERA_L2_CSLC-S1_T074-157286-IW3_20240801T183117Z_20240802T021015Z_S1A_VV_v1.1",
                ["s3://bucket/path/to/file.h5"],
            )
        ]
        created = CslcCatalogIngest._create_datasets(items)
        self.assertEqual(created, 1)

        dataset_id = "OPERA_L2_CSLC-S1_T074-157286-IW3_20240801T183117Z_20240802T021015Z_S1A_VV_v1.1"
        self.assertTrue(os.path.isdir(dataset_id))

        met_path = os.path.join(dataset_id, f"{dataset_id}.met.json")
        with open(met_path) as f:
            met = json.load(f)
        self.assertEqual(met["product_s3_paths"], ["s3://bucket/path/to/file.h5"])
        self.assertTrue(met["catalog_ingest"])

        ds_path = os.path.join(dataset_id, f"{dataset_id}.dataset.json")
        with open(ds_path) as f:
            ds = json.load(f)
        self.assertEqual(ds["version"], "1")
        self.assertEqual(ds["starttime"], "2024-08-01T18:31:17Z")

    def test_skips_vh_granules(self):
        items = [
            _make_umm_item(
                "OPERA_L2_CSLC-S1_T074-157286-IW3_20240801T183117Z_20240802T021015Z_S1A_VH_v1.1",
                ["s3://bucket/path/to/file.h5"],
            )
        ]
        created = CslcCatalogIngest._create_datasets(items)
        self.assertEqual(created, 0)

    def test_skips_granule_without_s3_urls(self):
        items = [
            _make_umm_item(
                "OPERA_L2_CSLC-S1_T074-157286-IW3_20240801T183117Z_20240802T021015Z_S1A_VV_v1.1",
                ["https://not-s3/file.h5"],
            )
        ]
        created = CslcCatalogIngest._create_datasets(items)
        self.assertEqual(created, 0)

    def test_skips_duplicate_in_same_run(self):
        items = [
            _make_umm_item(
                "OPERA_L2_CSLC-S1_T074-157286-IW3_20240801T183117Z_20240802T021015Z_S1A_VV_v1.1",
                ["s3://bucket/file.h5"],
            ),
            _make_umm_item(
                "OPERA_L2_CSLC-S1_T074-157286-IW3_20240801T183117Z_20240802T021015Z_S1A_VV_v1.1",
                ["s3://bucket/file.h5"],
            ),
        ]
        created = CslcCatalogIngest._create_datasets(items)
        self.assertEqual(created, 1)

    def test_multiple_vv_granules(self):
        items = [
            _make_umm_item(
                "OPERA_L2_CSLC-S1_T074-157286-IW3_20240801T183117Z_20240802T021015Z_S1A_VV_v1.1",
                ["s3://bucket/file1.h5"],
            ),
            _make_umm_item(
                "OPERA_L2_CSLC-S1_T074-157287-IW1_20240801T183117Z_20240802T021015Z_S1A_VV_v1.1",
                ["s3://bucket/file2.h5"],
            ),
        ]
        created = CslcCatalogIngest._create_datasets(items)
        self.assertEqual(created, 2)

    def test_filters_non_h5_s3_urls(self):
        items = [
            _make_umm_item(
                "OPERA_L2_CSLC-S1_T074-157286-IW3_20240801T183117Z_20240802T021015Z_S1A_VV_v1.1",
                ["s3://bucket/file.h5", "s3://bucket/file.xml"],
            )
        ]
        created = CslcCatalogIngest._create_datasets(items)
        self.assertEqual(created, 1)

        dataset_id = items[0]["umm"]["GranuleUR"]
        met_path = os.path.join(dataset_id, f"{dataset_id}.met.json")
        with open(met_path) as f:
            met = json.load(f)
        self.assertEqual(met["product_s3_paths"], ["s3://bucket/file.h5"])

    def test_single_datetime_temporal(self):
        item = {
            "umm": {
                "GranuleUR": "OPERA_L2_CSLC-S1_T074-157286-IW3_20240801T183117Z_20240802T021015Z_S1A_VV_v1.1",
                "RelatedUrls": [{"URL": "s3://bucket/file.h5", "Type": "GET DATA VIA DIRECT ACCESS"}],
                "TemporalExtent": {
                    "SingleDateTime": "2024-08-01T18:31:17Z",
                },
            },
            "meta": {"concept-id": "G12345-ASF", "revision-id": 1},
        }
        created = CslcCatalogIngest._create_datasets([item])
        self.assertEqual(created, 1)

        dataset_id = item["umm"]["GranuleUR"]
        ds_path = os.path.join(dataset_id, f"{dataset_id}.dataset.json")
        with open(ds_path) as f:
            ds = json.load(f)
        self.assertEqual(ds["starttime"], "2024-08-01T18:31:17Z")


class TestIngest(unittest.TestCase):

    def setUp(self):
        self.orig_dir = os.getcwd()
        self.test_dir = tempfile.mkdtemp()
        os.chdir(self.test_dir)

        self.burst_ids = ["T074-157286-IW3", "T074-157287-IW1"]
        self.frame_to_bursts = defaultdict(lambda: None)
        self.frame_to_bursts[7098] = _FakeHistBursts(7098, self.burst_ids, [0, 6])
        self.burst_to_frames = {b: [7098] for b in self.burst_ids}

    def tearDown(self):
        os.chdir(self.orig_dir)
        shutil.rmtree(self.test_dir)

    def test_skips_unknown_frame(self):
        _mock_cslc_utils.localize_disp_frame_burst_hist.return_value = (
            self.frame_to_bursts, self.burst_to_frames, {}
        )

        with patch.object(ingest_mod, "get_cmr_token",
                          return_value=("cmr.earthdata.nasa.gov", "token", None, None, None)):
            ingester = CslcCatalogIngest(settings={})
            ingester._query_cmr_for_frame = MagicMock(return_value=[])

            # Frame 99999 not in constDB — should be skipped
            ingester.ingest([99999], "2024-01-01T00:00:00Z", "2024-12-31T23:59:59Z")

            ingester._query_cmr_for_frame.assert_not_called()

    def test_calls_query_for_valid_frame(self):
        _mock_cslc_utils.localize_disp_frame_burst_hist.return_value = (
            self.frame_to_bursts, self.burst_to_frames, {}
        )

        items = [
            _make_umm_item(
                "OPERA_L2_CSLC-S1_T074-157286-IW3_20240801T183117Z_20240802T021015Z_S1A_VV_v1.1",
                ["s3://bucket/file.h5"],
            )
        ]

        with patch.object(ingest_mod, "get_cmr_token",
                          return_value=("cmr.earthdata.nasa.gov", "token", None, None, None)):
            ingester = CslcCatalogIngest(settings={})
            ingester._query_cmr_for_frame = MagicMock(return_value=items)

            ingester.ingest([7098], "2024-01-01T00:00:00Z", "2024-12-31T23:59:59Z")

            ingester._query_cmr_for_frame.assert_called_once()
            self.assertTrue(os.path.isdir(
                "OPERA_L2_CSLC-S1_T074-157286-IW3_20240801T183117Z_20240802T021015Z_S1A_VV_v1.1"
            ))


if __name__ == "__main__":
    unittest.main()
