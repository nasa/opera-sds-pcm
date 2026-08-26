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


_CCSLC_DOC_ID_DATE_RE = re.compile(
    r"_(\d{8})T\d+Z_(\d{8})T\d+Z_(\d{8})T\d+Z_(\d{8})T\d+Z_"
)


def _mock_parse_ccslc_dates(doc_id):
    """Real implementation matching cslc_utils.parse_ccslc_doc_id_dates so the
    ingest module's date-extraction path is exercised end-to-end in tests."""
    m = _CCSLC_DOC_ID_DATE_RE.search(doc_id)
    return m.groups() if m else None


_mock_cslc_utils.parse_ccslc_doc_id_dates = _mock_parse_ccslc_dates

# The evaluators import latest_cslc_per_burst by name, so the module mock must supply a
# working one or every product-path list becomes a MagicMock. These tests predate the
# deduplication and assert the old sorted-unique semantics, which is exactly what this
# preserves; the selection rule itself is covered by test_latest_cslc_per_burst.py.
_mock_cslc_utils.latest_cslc_per_burst = lambda paths: sorted(set(paths or []))

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

    def test_skips_frame_when_gap_exceeds_threshold(self):
        """A frame that returns False from gap check is skipped before CMR query."""
        _mock_cslc_utils.localize_disp_frame_burst_hist.return_value = (
            self.frame_to_bursts, self.burst_to_frames, {}
        )

        with patch.object(ingest_mod, "get_cmr_token",
                          return_value=("cmr.earthdata.nasa.gov", "token", None, None, None)):
            ingester = CslcCatalogIngest(settings={}, es_conn=MagicMock())
            # Pretend there's an imported CCSLC and the gap is huge.
            ingester._get_latest_ccslc_dates = MagicMock(
                return_value=("20210720", "20211019")
            )
            ingester._get_next_cslc_sensing_date = MagicMock(return_value="20250529")
            ingester._query_cmr_for_frame = MagicMock(return_value=[])

            ingester.ingest([7098], "2025-01-01T00:00:00Z", "2026-01-01T00:00:00Z")

            # Refused → no CMR query, no datasets created
            ingester._query_cmr_for_frame.assert_not_called()

    def test_proceeds_when_gap_within_threshold(self):
        """Small gap allows bootstrap to proceed normally."""
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
            ingester = CslcCatalogIngest(settings={}, es_conn=MagicMock())
            # CCSLC range 2024-06-21..2024-09-15, next CSLC 2024-09-21 → 6 day gap, ok.
            ingester._get_latest_ccslc_dates = MagicMock(
                return_value=("20240621", "20240915")
            )
            ingester._get_next_cslc_sensing_date = MagicMock(return_value="20240921")
            ingester._query_cmr_for_frame = MagicMock(return_value=items)

            ingester.ingest([7098], "2024-09-16T00:00:00Z", "2024-12-31T23:59:59Z")

            ingester._query_cmr_for_frame.assert_called_once()
            # Verify the start_date passed to CMR was extended to the CCSLC's
            # first_date (2024-06-21) so all sensing dates used to build the
            # CCSLC are re-cataloged.
            args, _ = ingester._query_cmr_for_frame.call_args
            self.assertEqual(args[1], "2024-06-21T00:00:00Z")


class TestCheckBootstrapGap(unittest.TestCase):
    """Pre-flight gap check refuses forward bootstrap on multi-year gaps."""

    def setUp(self):
        self.burst_ids = ["T042-088905-IW1"]
        self.frame_to_bursts = defaultdict(lambda: None)
        self.frame_to_bursts[33065] = _FakeHistBursts(33065, self.burst_ids, [0])
        _mock_cslc_utils.localize_disp_frame_burst_hist.return_value = (
            self.frame_to_bursts, {b: [33065] for b in self.burst_ids}, {}
        )

    def _make_ingester(self):
        return CslcCatalogIngest(settings={}, es_conn=MagicMock())

    def test_no_ccslc_allows_bootstrap(self):
        ingester = self._make_ingester()
        allowed, reason = ingester._check_bootstrap_gap(
            33065, None, 730, "cmr.earthdata.nasa.gov", "tok"
        )
        self.assertTrue(allowed)
        self.assertIn("no imported CCSLC", reason)

    def test_small_gap_allows_bootstrap(self):
        ingester = self._make_ingester()
        # Mock next CSLC = 2021-10-25 (6 days after CCSLC last_date 2021-10-19)
        ingester._get_next_cslc_sensing_date = MagicMock(return_value="20211025")
        allowed, reason = ingester._check_bootstrap_gap(
            33065, "20211019", 730, "cmr.earthdata.nasa.gov", "tok"
        )
        self.assertTrue(allowed)
        self.assertIn("6 days", reason)

    def test_gap_exactly_at_threshold_allows(self):
        ingester = self._make_ingester()
        # 730 days after 2020-01-01 = 2021-12-31; gap == threshold should allow
        ingester._get_next_cslc_sensing_date = MagicMock(return_value="20211231")
        allowed, reason = ingester._check_bootstrap_gap(
            33065, "20200101", 730, "cmr.earthdata.nasa.gov", "tok"
        )
        self.assertTrue(allowed)

    def test_gap_exceeds_threshold_refuses(self):
        ingester = self._make_ingester()
        # F33065 from #133: CCSLC last_date 2021-10-19, next CSLC 2025-05-29 → 1318 days
        ingester._get_next_cslc_sensing_date = MagicMock(return_value="20250529")
        allowed, reason = ingester._check_bootstrap_gap(
            33065, "20211019", 730, "cmr.earthdata.nasa.gov", "tok"
        )
        self.assertFalse(allowed)
        self.assertIn("1318 days", reason)
        self.assertIn("historical reprocessing", reason)

    def test_no_next_cslc_refuses(self):
        ingester = self._make_ingester()
        ingester._get_next_cslc_sensing_date = MagicMock(return_value=None)
        allowed, reason = ingester._check_bootstrap_gap(
            33065, "20211019", 730, "cmr.earthdata.nasa.gov", "tok"
        )
        self.assertFalse(allowed)
        self.assertIn("no CSLC found", reason)

    def test_cmr_error_refuses_with_distinguishable_message(self):
        """CMR transient errors produce a refusal whose
        message includes the exception text, so operators can disambiguate
        from a real time-series break."""
        ingester = self._make_ingester()
        ingester._get_next_cslc_sensing_date = MagicMock(
            side_effect=RuntimeError("CMR 503 Service Unavailable")
        )
        allowed, reason = ingester._check_bootstrap_gap(
            33065, "20211019", 730, "cmr.earthdata.nasa.gov", "tok"
        )
        self.assertFalse(allowed)
        # Distinguishable signal: the message says "CMR gap-check query failed"
        # and includes the exception text — different from the "no CSLC found"
        # refusal when CMR is healthy but returns empty results.
        self.assertIn("CMR gap-check query failed", reason)
        self.assertIn("CMR 503", reason)
        self.assertNotIn("no CSLC found", reason)


class TestGetNextCslcSensingDate(unittest.TestCase):
    """CMR query helper for the next CSLC sensing date."""

    def setUp(self):
        self.burst_ids = ["T042-088905-IW1", "T042-088905-IW2"]
        self.frame_to_bursts = defaultdict(lambda: None)
        self.frame_to_bursts[33065] = _FakeHistBursts(33065, self.burst_ids, [0])
        _mock_cslc_utils.localize_disp_frame_burst_hist.return_value = (
            self.frame_to_bursts, {b: [33065] for b in self.burst_ids}, {}
        )

    def _make_ingester(self):
        return CslcCatalogIngest(settings={}, es_conn=MagicMock())

    def test_extracts_sensing_date_from_granule_ur(self):
        ingester = self._make_ingester()
        # Patch asyncio.run on the module to return a single fake granule.
        fake_items = [_make_umm_item(
            "OPERA_L2_CSLC-S1_T042-088905-IW1_20250529T140746Z_20250530T123456Z_S1A_VV_v1.1",
            ["s3://b/file.h5"],
        )]
        with patch.object(ingest_mod, "asyncio") as mock_asyncio:
            mock_asyncio.run.return_value = fake_items
            result = ingester._get_next_cslc_sensing_date(
                33065, "20211019", "cmr.earthdata.nasa.gov", "tok"
            )
        self.assertEqual(result, "20250529")

    def test_returns_none_when_no_granules(self):
        ingester = self._make_ingester()
        with patch.object(ingest_mod, "asyncio") as mock_asyncio:
            mock_asyncio.run.return_value = []
            result = ingester._get_next_cslc_sensing_date(
                33065, "20211019", "cmr.earthdata.nasa.gov", "tok"
            )
        self.assertIsNone(result)

    def test_propagates_cmr_error(self):
        """CMR errors propagate so the caller can distinguish
        a transient outage from a genuine no-granules result."""
        ingester = self._make_ingester()
        with patch.object(ingest_mod, "asyncio") as mock_asyncio:
            mock_asyncio.run.side_effect = RuntimeError("CMR down")
            with self.assertRaises(RuntimeError):
                ingester._get_next_cslc_sensing_date(
                    33065, "20211019", "cmr.earthdata.nasa.gov", "tok"
                )


class TestComputeSeededStartDate(unittest.TestCase):
    """Extend start_date back to the imported CCSLC's first_date so every
    sensing date used to build the CCSLC is re-cataloged."""

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

    def test_extends_start_date_to_ccslc_first_date(self):
        # CCSLC range 2024-06-05..2024-12-02. Operator wants to start at
        # 2024-12-03 — should extend back to the CCSLC's first_date so
        # every sensing date the CCSLC was built from is re-cataloged.
        # Note: ES query sorts by acquisition_cycle desc with size=1, so the
        # first hit is the most-recent boundary.
        ingester = self._make_ingester(hits=[
            self._ccslc_hit(11114, "20241202", "20240605", "20241202", "20250904"),
            self._ccslc_hit(11114, "20221002", "20220417", "20221002", "20250903"),
        ])
        result = ingester._compute_seeded_start_date(11114, "2024-12-03T00:00:00Z")
        self.assertEqual(result, "2024-06-05T00:00:00Z")

    def test_no_adjustment_when_operator_start_already_before_first_date(self):
        # Operator start = 2024-01-01 is already earlier than CCSLC
        # first_date (2024-06-05) — no adjustment.
        ingester = self._make_ingester(hits=[
            self._ccslc_hit(11114, "20241202", "20240605", "20241202", "20250904"),
        ])
        result = ingester._compute_seeded_start_date(11114, "2024-01-01T00:00:00Z")
        self.assertEqual(result, "2024-01-01T00:00:00Z")

    def test_picks_latest_ccslc_when_multiple_present(self):
        # Multiple boundary dates; the ES query sorts by acquisition_cycle
        # desc, so the first hit is the most-recent boundary (20241202).
        # size=1 in the production query means only the first hit is read.
        ingester = self._make_ingester(hits=[
            self._ccslc_hit(11114, "20241202", "20240605", "20241202", "20250904"),
            self._ccslc_hit(11114, "20231102", "20230506", "20231102", "20250903"),
            self._ccslc_hit(11114, "20230424", "20221014", "20230424", "20250903"),
            self._ccslc_hit(11114, "20221002", "20220417", "20221002", "20250903"),
        ])
        result = ingester._compute_seeded_start_date(11114, "2026-01-01T00:00:00Z")
        # Anchors to first_date of the most-recent CCSLC (20240605)
        self.assertEqual(result, "2024-06-05T00:00:00Z")

    def test_cadence_agnostic_seed_for_s1a_only_track(self):
        # Regression: prior implementation used last_date - 78 days, which
        # assumed 6-day S1A+S1B cadence. On 12-day S1A-only tracks the seed
        # only covered ~6 acquisitions instead of 14. Anchoring to first_date
        # captures every sensing date the CCSLC was actually built from.
        ingester = self._make_ingester(hits=[
            # F33039-style: 144-day CCSLC range on 12-day S1A-only cadence.
            self._ccslc_hit(33039, "20211106", "20211106", "20220330", "20250903"),
        ])
        result = ingester._compute_seeded_start_date(33039, "2022-03-30T00:00:00Z")
        # Must seed back to 20211106 (not 20220330 - 78d = 20220111).
        self.assertEqual(result, "2021-11-06T00:00:00Z")

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

    def test_bad_first_hit_returns_unchanged_with_warning(self):
        # With size=1, the top hit (sorted by acquisition_cycle desc) is the
        # only one consulted. If its ID does not match the date pattern,
        # treat the frame as having no imported CCSLC and leave start_date
        # unchanged. Operator sees a warning in the logs.
        ingester = self._make_ingester(hits=[
            {"_id": "totally-wrong-id-format"},
        ])
        result = ingester._compute_seeded_start_date(11114, "2025-01-01T00:00:00Z")
        self.assertEqual(result, "2025-01-01T00:00:00Z")


class TestFilterToCcslcLineage(unittest.TestCase):
    """Drop CMR granules whose sensing date is on or before the latest
    CCSLC's last_date and not present in any historical CCSLC's lineage."""

    def setUp(self):
        _mock_cslc_utils.localize_disp_frame_burst_hist.return_value = (
            defaultdict(lambda: None), {}, {}
        )

    def _ccslc_hit(self, frame_id, ref, first, last, creation,
                   lineage_dates, burst="T117-249922-IW3"):
        """CCSLC hit with date-bearing ID + a lineage list of CSLC filenames
        whose first YYYYMMDDT...Z token is the sensing date."""
        doc_id = (
            f"OPERA_L2_COMPRESSED-CSLC-S1_F{frame_id}_{burst}_"
            f"{ref}T000000Z_{first}T000000Z_{last}T000000Z_{creation}T010150Z_VV_v1.0"
        )
        lineage = [
            f"OPERA_L2_CSLC-S1_{burst}_{sd}T013307Z_20210101T000000Z_S1A_VV_v1.1.h5"
            for sd in lineage_dates
        ]
        return {
            "_id": doc_id,
            "_source": {"metadata": {"lineage": lineage}},
        }

    def _make_ingester(self, hits, enabled=True):
        es_conn = MagicMock()
        es_conn.es.search.return_value = {"hits": {"hits": hits}}
        return CslcCatalogIngest(settings={}, es_conn=es_conn,
                                 filter_to_ccslc_lineage=enabled)

    def _granule(self, sensing_date, burst="T117-249922-IW3"):
        ur = (
            f"OPERA_L2_CSLC-S1_{burst}_"
            f"{sensing_date}T013307Z_20210101T000000Z_S1A_VV_v1.1"
        )
        return {"umm": {"GranuleUR": ur}}

    def test_no_historical_ccslc_returns_unchanged(self):
        ingester = self._make_ingester(hits=[])
        items = [self._granule("20170101"), self._granule("20180101")]
        result = ingester._filter_to_ccslc_lineage(items, 31241)
        self.assertEqual(len(result), 2)

    def test_constructor_flag_disables_filter(self):
        # Filter wired off — ingest() should not invoke _filter_to_ccslc_lineage.
        # We assert by inspecting the constructor-stored flag rather than
        # exercising the full ingest pipeline (which requires CMR mocking).
        ingester = self._make_ingester(hits=[], enabled=False)
        self.assertFalse(ingester.filter_to_ccslc_lineage)

    def test_keeps_forward_acquisition(self):
        # Granule sensing date 20190101 > max_last_date 20180419 — kept as
        # forward acquisition regardless of lineage.
        ingester = self._make_ingester(hits=[
            self._ccslc_hit(31241, "20180419", "20171102", "20180419", "20250903",
                            lineage_dates=["20171102", "20180101", "20180419"]),
        ])
        items = [self._granule("20190101")]
        result = ingester._filter_to_ccslc_lineage(items, 31241)
        self.assertEqual(len(result), 1)

    def test_keeps_lineage_date_within_envelope(self):
        # Sensing date 20171102 <= max_last_date 20180419 and is in lineage — kept.
        ingester = self._make_ingester(hits=[
            self._ccslc_hit(31241, "20180419", "20171102", "20180419", "20250903",
                            lineage_dates=["20171102", "20180101", "20180419"]),
        ])
        items = [self._granule("20171102")]
        result = ingester._filter_to_ccslc_lineage(items, 31241)
        self.assertEqual(len(result), 1)

    def test_drops_date_in_envelope_but_not_in_lineage(self):
        # This is the F31241 smoke-test bug: CMR returned 20161113 (and 6
        # other dates) within the CCSLC envelope, but they were never part
        # of any historical ministack lineage. Filter must drop them.
        ingester = self._make_ingester(hits=[
            self._ccslc_hit(31241, "20180419", "20171102", "20180419", "20250903",
                            lineage_dates=["20171102", "20180101", "20180419"]),
        ])
        items = [
            self._granule("20161113"),  # extra date — must be dropped
            self._granule("20171102"),  # in lineage — kept
            self._granule("20190101"),  # forward — kept
        ]
        result = ingester._filter_to_ccslc_lineage(items, 31241)
        kept_dates = sorted(
            ingest_mod.CslcCatalogIngest._CSLC_SENSING_DATE_RE.search(
                it["umm"]["GranuleUR"]
            ).group(1)
            for it in result
        )
        self.assertEqual(kept_dates, ["20171102", "20190101"])

    def test_unions_lineage_across_multiple_ccslcs(self):
        # Three CCSLCs covering different historical ministacks. max_last_date
        # is the latest. Granules in any ccslc's lineage must be kept.
        ingester = self._make_ingester(hits=[
            self._ccslc_hit(31241, "20171021", "20161020", "20171021", "20250903",
                            lineage_dates=["20161020", "20170518", "20171021"]),
            self._ccslc_hit(31241, "20180419", "20171102", "20180419", "20250903",
                            lineage_dates=["20171102", "20180419"]),
            self._ccslc_hit(31241, "20190108", "20180501", "20190108", "20250903",
                            lineage_dates=["20180501", "20190108"]),
        ])
        items = [
            self._granule("20161020"),  # in CCSLC#1 lineage
            self._granule("20171102"),  # in CCSLC#2 lineage
            self._granule("20180501"),  # in CCSLC#3 lineage
            self._granule("20170729"),  # NOT in any lineage, within envelope
            self._granule("20200101"),  # forward
        ]
        result = ingester._filter_to_ccslc_lineage(items, 31241)
        kept_dates = sorted(
            ingest_mod.CslcCatalogIngest._CSLC_SENSING_DATE_RE.search(
                it["umm"]["GranuleUR"]
            ).group(1)
            for it in result
        )
        self.assertEqual(
            kept_dates,
            ["20161020", "20171102", "20180501", "20200101"],
        )

    def test_empty_lineage_returns_unchanged(self):
        # CCSLCs exist but no parseable lineage entries — filter must no-op
        # rather than drop everything within the envelope, since the lineage
        # envelope is unknown.
        ingester = self._make_ingester(hits=[
            self._ccslc_hit(31241, "20180419", "20171102", "20180419", "20250903",
                            lineage_dates=[]),
        ])
        items = [self._granule("20161113"), self._granule("20180101")]
        result = ingester._filter_to_ccslc_lineage(items, 31241)
        self.assertEqual(len(result), 2)

    def test_es_exception_returns_unchanged(self):
        # Transient ES error — leave items untouched rather than guess.
        es_conn = MagicMock()
        es_conn.es.search.side_effect = RuntimeError("ES down")
        ingester = CslcCatalogIngest(settings={}, es_conn=es_conn,
                                     filter_to_ccslc_lineage=True)
        items = [self._granule("20170101"), self._granule("20180101")]
        result = ingester._filter_to_ccslc_lineage(items, 31241)
        self.assertEqual(len(result), 2)

    def test_unparseable_granule_ur_is_kept(self):
        # Granule UR without a sensing-date token — keep, let downstream surface it.
        ingester = self._make_ingester(hits=[
            self._ccslc_hit(31241, "20180419", "20171102", "20180419", "20250903",
                            lineage_dates=["20171102", "20180419"]),
        ])
        items = [{"umm": {"GranuleUR": "no-sensing-date-token"}}]
        result = ingester._filter_to_ccslc_lineage(items, 31241)
        self.assertEqual(len(result), 1)


if __name__ == "__main__":
    unittest.main()
