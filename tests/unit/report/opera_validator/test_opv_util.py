"""Tests for report/opera_validator/opv_util.py query functions.

Baseline + output_path/output_dir tests for retrieve_r3_products and get_granules_from_query.
"""
import json
import tempfile
from datetime import datetime, timezone
from unittest.mock import patch, MagicMock

import pytest

from report.opera_validator.opv_util import retrieve_r3_products, parallel_fetch


# Sample CMR response matching the umm_json format returned by these functions
SAMPLE_ITEM = {
    "meta": {
        "native-id": "OPERA_L3_DISP-S1_IW_F01234_VV_20250101_20250113_v1.0_20250114T000000Z",
        "revision-id": "1",
        "revision-date": "2025-01-14T00:00:00.000Z"
    },
    "umm": {
        "GranuleUR": "OPERA_L3_DISP-S1_IW_F01234_VV_20250101_20250113_v1.0_20250114T000000Z",
        "TemporalExtent": {
            "RangeDateTime": {
                "BeginningDateTime": "2025-01-01T00:00:00.000Z",
                "EndingDateTime": "2025-01-13T23:59:59.000Z"
            }
        },
        "AdditionalAttributes": [
            {"Name": "FRAME_NUMBER", "Values": ["1234"]}
        ],
        "RelatedUrls": [
            {"URL": "https://example.com/disp.nc", "Type": "GET DATA"}
        ]
    }
}


def make_cmr_page_response(items, hits=None):
    """Create a CMR response matching the JSON structure from requests.get().json()."""
    if hits is None:
        hits = len(items)
    return {"hits": hits, "items": items}


class TestRetrieveR3Products:
    """Baseline tests for retrieve_r3_products."""

    @pytest.fixture
    def mock_requests_get(self):
        with patch("report.opera_validator.opv_util.requests.get") as mock:
            yield mock

    def test_returns_all_items_single_page(self, mock_requests_get):
        """Single page response returns all items."""
        response_mock = MagicMock()
        response_mock.json.return_value = make_cmr_page_response(
            [SAMPLE_ITEM, SAMPLE_ITEM], hits=2
        )
        response_mock.raise_for_status = MagicMock()
        mock_requests_get.return_value = response_mock

        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2025, 1, 14, tzinfo=timezone.utc)

        result = retrieve_r3_products(start, end, "OPS", "OPERA_L3_DISP-S1_V1")

        assert isinstance(result, list)
        assert len(result) == 2
        assert result[0]["meta"]["native-id"] == SAMPLE_ITEM["meta"]["native-id"]

    def test_returns_empty_list_when_no_hits(self, mock_requests_get):
        """Empty response returns empty list."""
        response_mock = MagicMock()
        response_mock.json.return_value = make_cmr_page_response([], hits=0)
        response_mock.raise_for_status = MagicMock()
        mock_requests_get.return_value = response_mock

        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2025, 1, 2, tzinfo=timezone.utc)

        result = retrieve_r3_products(start, end, "OPS", "OPERA_L3_DISP-S1_V1")

        assert result == []

    def test_paginates_across_multiple_pages(self, mock_requests_get):
        """Should paginate when hits > page_size items."""
        page1_response = MagicMock()
        page1_response.json.return_value = make_cmr_page_response(
            [SAMPLE_ITEM] * 1000, hits=1500
        )
        page1_response.raise_for_status = MagicMock()

        page2_response = MagicMock()
        page2_response.json.return_value = make_cmr_page_response(
            [SAMPLE_ITEM] * 500, hits=1500
        )
        page2_response.raise_for_status = MagicMock()

        mock_requests_get.side_effect = [page1_response, page2_response]

        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2025, 1, 14, tzinfo=timezone.utc)

        result = retrieve_r3_products(start, end, "OPS", "OPERA_L3_DISP-S1_V1")

        assert len(result) == 1500
        assert mock_requests_get.call_count == 2


class TestParallelFetch:
    """Baseline tests for the parallel_fetch helper used by get_granules_from_query."""

    @pytest.fixture
    def mock_fetch_with_backoff(self):
        with patch("report.opera_validator.opv_util.fetch_with_backoff") as mock:
            yield mock

    def test_returns_batch_granules(self, mock_fetch_with_backoff):
        """Should return the list of granules from fetch_with_backoff."""
        import multiprocessing
        mock_fetch_with_backoff.return_value = [SAMPLE_ITEM, SAMPLE_ITEM]
        downloaded_batches = multiprocessing.Value('i', 0)

        result = parallel_fetch(
            "https://cmr.earthdata.nasa.gov/search/granules.umm_json",
            {"provider": "ASF", "ShortName[]": "OPERA_L2_RTC-S1_V1"},
            page_num=1,
            page_size=1000,
            downloaded_batches=downloaded_batches
        )

        assert len(result) == 2
        assert downloaded_batches.value == 1

    def test_returns_empty_on_failure(self, mock_fetch_with_backoff):
        """Should return empty list on fetch failure."""
        import multiprocessing
        mock_fetch_with_backoff.side_effect = Exception("Network error")
        downloaded_batches = multiprocessing.Value('i', 0)

        result = parallel_fetch(
            "https://cmr.earthdata.nasa.gov/search/granules.umm_json",
            {"provider": "ASF", "ShortName[]": "OPERA_L2_RTC-S1_V1"},
            page_num=1,
            page_size=1000,
            downloaded_batches=downloaded_batches
        )

        assert result == []
        assert downloaded_batches.value == 1


class TestRetrieveR3ProductsOutputPath:
    """Tests for retrieve_r3_products with output_path (Phase 4 streaming)."""

    @pytest.fixture
    def mock_requests_get(self):
        with patch("report.opera_validator.opv_util.requests.get") as mock:
            yield mock

    def test_output_path_writes_jsonl(self, mock_requests_get):
        """When output_path is set, items should be written to JSONL file."""
        response_mock = MagicMock()
        response_mock.json.return_value = make_cmr_page_response(
            [SAMPLE_ITEM, SAMPLE_ITEM], hits=2
        )
        response_mock.raise_for_status = MagicMock()
        mock_requests_get.return_value = response_mock

        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2025, 1, 14, tzinfo=timezone.utc)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = f"{tmpdir}/results.jsonl"
            result = retrieve_r3_products(start, end, "OPS", "OPERA_L3_DISP-S1_V1",
                                          output_path=output_path)

            # Should return the output_path
            assert result == output_path

            # File should contain JSONL records
            with open(output_path) as f:
                lines = f.readlines()
            assert len(lines) == 2
            record = json.loads(lines[0])
            assert record["meta"]["native-id"] == SAMPLE_ITEM["meta"]["native-id"]

    def test_output_path_paginates_to_file(self, mock_requests_get):
        """Multi-page responses should all be appended to the same JSONL file."""
        page1_response = MagicMock()
        page1_response.json.return_value = make_cmr_page_response(
            [SAMPLE_ITEM] * 1000, hits=1500
        )
        page1_response.raise_for_status = MagicMock()

        page2_response = MagicMock()
        page2_response.json.return_value = make_cmr_page_response(
            [SAMPLE_ITEM] * 500, hits=1500
        )
        page2_response.raise_for_status = MagicMock()

        mock_requests_get.side_effect = [page1_response, page2_response]

        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2025, 1, 14, tzinfo=timezone.utc)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = f"{tmpdir}/results.jsonl"
            result = retrieve_r3_products(start, end, "OPS", "OPERA_L3_DISP-S1_V1",
                                          output_path=output_path)

            assert result == output_path
            with open(output_path) as f:
                lines = f.readlines()
            assert len(lines) == 1500

    def test_without_output_path_returns_list(self, mock_requests_get):
        """Without output_path, should return list as before."""
        response_mock = MagicMock()
        response_mock.json.return_value = make_cmr_page_response(
            [SAMPLE_ITEM], hits=1
        )
        response_mock.raise_for_status = MagicMock()
        mock_requests_get.return_value = response_mock

        start = datetime(2025, 1, 1, tzinfo=timezone.utc)
        end = datetime(2025, 1, 14, tzinfo=timezone.utc)

        result = retrieve_r3_products(start, end, "OPS", "OPERA_L3_DISP-S1_V1")

        assert isinstance(result, list)
        assert len(result) == 1


class TestParallelFetchOutputPath:
    """Tests for parallel_fetch with output_path (Phase 4 streaming)."""

    @pytest.fixture
    def mock_fetch_with_backoff(self):
        with patch("report.opera_validator.opv_util.fetch_with_backoff") as mock:
            yield mock

    def test_output_path_writes_jsonl(self, mock_fetch_with_backoff):
        """When output_path is set, items should be written to JSONL file."""
        import multiprocessing
        mock_fetch_with_backoff.return_value = [SAMPLE_ITEM, SAMPLE_ITEM]
        downloaded_batches = multiprocessing.Value('i', 0)

        with tempfile.TemporaryDirectory() as tmpdir:
            output_path = f"{tmpdir}/page_1.jsonl"
            result = parallel_fetch(
                "https://cmr.earthdata.nasa.gov/search/granules.umm_json",
                {"provider": "ASF", "ShortName[]": "OPERA_L2_RTC-S1_V1"},
                page_num=1,
                page_size=1000,
                downloaded_batches=downloaded_batches,
                output_path=output_path
            )

            # Still returns the batch granules (for progress bar counting)
            assert len(result) == 2

            # File should contain JSONL records
            with open(output_path) as f:
                lines = f.readlines()
            assert len(lines) == 2
            record = json.loads(lines[0])
            assert record["meta"]["native-id"] == SAMPLE_ITEM["meta"]["native-id"]
