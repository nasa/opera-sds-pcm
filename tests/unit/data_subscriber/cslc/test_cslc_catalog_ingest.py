"""Tests for CSLC-S1 Catalog Ingest."""

import json
import os
import re
import shutil
import sys
import tempfile
import unittest
from collections import defaultdict
from unittest.mock import MagicMock, patch, AsyncMock

# Mock heavy imports to avoid numpy/elasticsearch version issues in local dev.
_mock_cslc_utils = MagicMock()


def _mock_parse_cslc(native_id):
    """Extract burst_id and acquisition_ts from CSLC native ID for testing."""
    m = re.match(
        r"OPERA_L2_CSLC-S1_(?P<burst_id>\w{4}-\w{6}-\w{3})_(?P<acquisition_ts>\d{8}T\d{6}Z)",
        native_id,
    )
    if not m:
        raise ValueError(f"Could not parse {native_id}")
    return m.group("burst_id"), m.group("acquisition_ts")


_mock_cslc_utils.parse_cslc_file_name = _mock_parse_cslc

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


class TestComputeSeededStartDate(unittest.TestCase):
    """OPERA-2467: extend start_date back to seed trailing 14 CSLCs from imported CCSLC."""

    def setUp(self):
        _mock_cslc_utils.localize_disp_frame_burst_hist.return_value = (
            defaultdict(lambda: None), {}, {}
        )

    def _make_ingester(self, hits):
        """Build an ingester whose ES connection returns the given CCSLC ID hits."""
        es_conn = MagicMock()
        es_conn.es.search.return_value = {"hits": {"hits": hits}}
        return CslcCatalogIngest(settings={}, es_conn=es_conn)

    def _ccslc_hit(self, frame_id, ref, first, last, creation, burst="T042-088905-IW1"):
        """Build a fake CCSLC hit with the date-bearing ID pattern."""
        doc_id = (
            f"OPERA_L2_COMPRESSED-CSLC-S1_F{frame_id}_{burst}_"
            f"{ref}T000000Z_{first}T000000Z_{last}T000000Z_{creation}T010150Z_VV_v1.0"
        )
        return {"_id": doc_id}

    def test_no_es_conn_returns_unchanged(self):
        ingester = CslcCatalogIngest(settings={})  # no es_conn
        self.assertEqual(
            ingester._compute_seeded_start_date(11114, "2025-01-01T00:00:00Z"),
            "2025-01-01T00:00:00Z",
        )

    def test_no_ccslc_returns_unchanged(self):
        ingester = self._make_ingester(hits=[])
        self.assertEqual(
            ingester._compute_seeded_start_date(11114, "2025-01-01T00:00:00Z"),
            "2025-01-01T00:00:00Z",
        )

    def test_extends_start_date_when_operator_value_after_seed_cutoff(self):
        # CCSLC last_date=20241202. Seed cutoff = 20241202 - 78 days = 2024-09-15.
        # Operator wants to start at 2024-12-03 (current behavior) — should extend
        # back to 2024-09-15.
        ingester = self._make_ingester(hits=[
            self._ccslc_hit(11114, "20221002", "20220417", "20221002", "20250903"),
            self._ccslc_hit(11114, "20241202", "20240605", "20241202", "20250904"),
        ])
        result = ingester._compute_seeded_start_date(11114, "2024-12-03T00:00:00Z")
        self.assertEqual(result, "2024-09-15T00:00:00Z")

    def test_no_adjustment_when_operator_start_already_before_seed_cutoff(self):
        # Operator start = 2024-01-01 is already earlier than seed cutoff
        # (20241202 - 78d = 2024-09-15) — no adjustment.
        ingester = self._make_ingester(hits=[
            self._ccslc_hit(11114, "20241202", "20240605", "20241202", "20250904"),
        ])
        result = ingester._compute_seeded_start_date(11114, "2024-01-01T00:00:00Z")
        self.assertEqual(result, "2024-01-01T00:00:00Z")

    def test_picks_latest_last_date_when_multiple_ccslcs_present(self):
        # Multiple boundary dates; latest is 20241202.
        ingester = self._make_ingester(hits=[
            self._ccslc_hit(11114, "20221002", "20220417", "20221002", "20250903"),
            self._ccslc_hit(11114, "20230424", "20221014", "20230424", "20250903"),
            self._ccslc_hit(11114, "20241202", "20240605", "20241202", "20250904"),
            self._ccslc_hit(11114, "20231102", "20230506", "20231102", "20250903"),
        ])
        result = ingester._compute_seeded_start_date(11114, "2026-01-01T00:00:00Z")
        # Seed cutoff = 20241202 - 78 days = 2024-09-15
        self.assertEqual(result, "2024-09-15T00:00:00Z")

    def test_es_exception_returns_unchanged_with_warning(self):
        es_conn = MagicMock()
        es_conn.es.search.side_effect = RuntimeError("ES down")
        ingester = CslcCatalogIngest(settings={}, es_conn=es_conn)
        result = ingester._compute_seeded_start_date(11114, "2025-01-01T00:00:00Z")
        self.assertEqual(result, "2025-01-01T00:00:00Z")

    def test_malformed_start_date_returns_unchanged(self):
        ingester = self._make_ingester(hits=[
            self._ccslc_hit(11114, "20241202", "20240605", "20241202", "20250904"),
        ])
        result = ingester._compute_seeded_start_date(11114, "not-a-date")
        self.assertEqual(result, "not-a-date")

    def test_ignores_hits_without_date_pattern(self):
        # A hit whose _id doesn't match the CCSLC date regex is ignored.
        ingester = self._make_ingester(hits=[
            {"_id": "totally-wrong-id-format"},
            self._ccslc_hit(11114, "20241202", "20240605", "20241202", "20250904"),
        ])
        result = ingester._compute_seeded_start_date(11114, "2025-01-01T00:00:00Z")
        self.assertEqual(result, "2024-09-15T00:00:00Z")


if __name__ == "__main__":
    unittest.main()
