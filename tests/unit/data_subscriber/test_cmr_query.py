"""Tests for data_subscriber/cmr.py query functions.

Establishes behavioral baselines for async_query_cmr_v2 and
_async_request_search_cmr_granules before Phase 3 modifications.
"""
import asyncio
import json
from collections import namedtuple
from unittest.mock import patch, AsyncMock

import pytest

from data_subscriber.cmr import (
    async_query_cmr_v2,
    _async_request_search_cmr_granules,
    response_jsons_to_cmr_granules,
    DateTimeRange,
)


# Minimal CMR response page matching the umm_json format
def make_cmr_response(items, hits=None):
    """Create a CMR response JSON structure."""
    if hits is None:
        hits = len(items)
    return {"hits": hits, "items": items}


SAMPLE_ITEM_RTC = {
    "meta": {
        "native-id": "OPERA_L2_RTC-S1_T168-359595-IW3_20250516T053145Z_20250516T155714Z_S1A_30_v1.0",
        "revision-id": "1",
        "revision-date": "2025-05-16T15:57:14.123Z",
        "provider-id": "ASF"
    },
    "umm": {
        "GranuleUR": "OPERA_L2_RTC-S1_T168-359595-IW3_20250516T053145Z_20250516T155714Z_S1A_30_v1.0",
        "TemporalExtent": {
            "RangeDateTime": {
                "BeginningDateTime": "2025-05-16T05:31:45.000Z",
                "EndingDateTime": "2025-05-16T05:31:50.000Z"
            }
        },
        "DataGranule": {"ProductionDateTime": "2025-05-16T10:00:00.000Z"},
        "ProviderDates": [{"Type": "Insert", "Date": "2025-05-16T12:00:00.000Z"}],
        "Platforms": [{"ShortName": "Sentinel-1A"}],
        "SpatialExtent": {
            "HorizontalSpatialDomain": {
                "Geometry": {
                    "GPolygons": [{
                        "Boundary": {
                            "Points": [
                                {"Latitude": 37.0, "Longitude": -122.0},
                                {"Latitude": 37.0, "Longitude": -121.0},
                                {"Latitude": 38.0, "Longitude": -121.0},
                                {"Latitude": 38.0, "Longitude": -122.0}
                            ]
                        }
                    }]
                }
            }
        },
        "RelatedUrls": [
            {"URL": "https://example.com/data.tif", "Type": "GET DATA"}
        ],
        "AdditionalAttributes": [
            {"Name": "POLARIZATION", "Values": ["VV", "VH"]}
        ],
        "InputGranules": [
            "S1A_IW_SLC__1SDV_20250516T053145_20250516T053212_064123_07E456_ABCD.zip"
        ]
    }
}

SAMPLE_ITEM_SIMPLE = {
    "meta": {
        "native-id": "OPERA_L3_DSWx-S1_T55GCQ_20250512T193408Z_20250513T064736Z_S1A_30_v1.0",
        "revision-id": "2",
        "revision-date": "2025-05-13T06:47:36.456Z",
        "provider-id": "POCLOUD"
    },
    "umm": {
        "GranuleUR": "OPERA_L3_DSWx-S1_T55GCQ_20250512T193408Z_20250513T064736Z_S1A_30_v1.0",
        "TemporalExtent": {
            "RangeDateTime": {
                "BeginningDateTime": "2025-05-12T19:34:08.000Z",
                "EndingDateTime": "2025-05-13T06:47:36.000Z"
            }
        },
        "InputGranules": [
            "OPERA_L2_RTC-S1_T118-252625-IW2_20250512T193412Z_20250512T193437Z_S1A_30_v1.0"
        ],
        "RelatedUrls": [
            {"URL": "https://example.com/dswx.tif", "Type": "GET DATA"}
        ]
    }
}


class TestAsyncQueryCmrV2:
    """Tests for async_query_cmr_v2 baseline behavior."""

    @pytest.fixture
    def mock_async_cmr_posts(self):
        """Mock the async_cmr_posts call to avoid network."""
        with patch("data_subscriber.cmr.async_cmr_posts", new_callable=AsyncMock) as mock:
            yield mock

    def test_returns_raw_items_list(self, mock_async_cmr_posts):
        """async_query_cmr_v2 returns a flat list of raw CMR items (convert_results=False)."""
        mock_async_cmr_posts.return_value = [
            make_cmr_response([SAMPLE_ITEM_SIMPLE, SAMPLE_ITEM_SIMPLE])
        ]

        timerange = DateTimeRange("2025-05-12T00:00:00Z", "2025-05-13T00:00:00Z")
        result = asyncio.run(async_query_cmr_v2(
            timerange=timerange,
            provider="POCLOUD",
            collection="OPERA_L3_DSWX-S1_V1"
        ))

        assert isinstance(result, list)
        assert len(result) == 2
        # Items should be raw CMR items (not converted)
        assert result[0]["meta"]["native-id"] == SAMPLE_ITEM_SIMPLE["meta"]["native-id"]

    def test_passes_correct_params_to_cmr_posts(self, mock_async_cmr_posts):
        """Verify the request parameters are correctly constructed."""
        mock_async_cmr_posts.return_value = [make_cmr_response([])]

        timerange = DateTimeRange("2025-05-12T00:00:00Z", "2025-05-13T00:00:00Z")
        asyncio.run(async_query_cmr_v2(
            timerange=timerange,
            provider="ASF",
            collection="OPERA_L2_RTC-S1_V1",
            bbox="-180,-60,180,90"
        ))

        # Verify async_cmr_posts was called
        mock_async_cmr_posts.assert_called_once()
        call_args = mock_async_cmr_posts.call_args
        # First positional arg is the URL
        assert "cmr.earthdata.nasa.gov/search/granules.umm_json" in call_args[0][0]

    def test_returns_empty_list_when_no_results(self, mock_async_cmr_posts):
        """Empty CMR response should return empty list."""
        mock_async_cmr_posts.return_value = [make_cmr_response([])]

        timerange = DateTimeRange("2025-05-12T00:00:00Z", "2025-05-13T00:00:00Z")
        result = asyncio.run(async_query_cmr_v2(
            timerange=timerange,
            provider="ASF",
            collection="OPERA_L2_RTC-S1_V1"
        ))

        assert result == []

    def test_no_timerange_omits_temporal_param(self, mock_async_cmr_posts):
        """When timerange is None, temporal param should not be included."""
        mock_async_cmr_posts.return_value = [make_cmr_response([])]

        asyncio.run(async_query_cmr_v2(
            timerange=None,
            provider="ASF",
            collection="OPERA_L2_RTC-S1_V1"
        ))

        mock_async_cmr_posts.assert_called_once()
        # The request body should not contain "temporal"
        request_body = mock_async_cmr_posts.call_args[0][1][0]  # second positional arg, first item
        assert "temporal" not in request_body


class TestAsyncRequestSearchCmrGranules:
    """Tests for _async_request_search_cmr_granules baseline behavior."""

    @pytest.fixture
    def mock_async_cmr_posts(self):
        with patch("data_subscriber.cmr.async_cmr_posts", new_callable=AsyncMock) as mock:
            yield mock

    def test_returns_raw_items_when_convert_false(self, mock_async_cmr_posts):
        """With convert_results=False, returns flat list of raw items."""
        mock_async_cmr_posts.return_value = [
            make_cmr_response([SAMPLE_ITEM_SIMPLE])
        ]

        result = asyncio.run(_async_request_search_cmr_granules(
            "OPERA_L3_DSWX-S1_V1",
            "https://cmr.earthdata.nasa.gov/search/granules.umm_json",
            [{"provider": "POCLOUD", "ShortName[]": ["OPERA_L3_DSWX-S1_V1"]}],
            convert_results=False
        ))

        assert isinstance(result, list)
        assert len(result) == 1
        assert result[0]["meta"]["native-id"] == SAMPLE_ITEM_SIMPLE["meta"]["native-id"]

    def test_passes_params_as_request_bodies(self, mock_async_cmr_posts):
        """Params should be converted to request bodies via cmr_client.paramss_to_request_body."""
        mock_async_cmr_posts.return_value = [make_cmr_response([])]

        params = {"provider": "ASF", "ShortName[]": ["OPERA_L2_RTC-S1_V1"]}
        asyncio.run(_async_request_search_cmr_granules(
            "OPERA_L2_RTC-S1_V1",
            "https://cmr.earthdata.nasa.gov/search/granules.umm_json",
            [params],
            convert_results=False
        ))

        mock_async_cmr_posts.assert_called_once()
        call_args = mock_async_cmr_posts.call_args
        # Second positional arg should be the request bodies list
        request_bodies = call_args[0][1]
        assert isinstance(request_bodies, list)
        assert len(request_bodies) == 1
        assert "provider" in request_bodies[0]


class TestResponseJsonsToCmrGranules:
    """Tests for response_jsons_to_cmr_granules conversion function."""

    def test_convert_false_returns_flat_items(self):
        """With convert_results=False, returns flat list of items from all pages."""
        response_jsons = [
            make_cmr_response([SAMPLE_ITEM_SIMPLE]),
            make_cmr_response([SAMPLE_ITEM_SIMPLE])
        ]

        result = response_jsons_to_cmr_granules("OPERA_L3_DSWX-S1_V1", response_jsons, convert_results=False)

        assert len(result) == 2
        assert all(item["meta"]["native-id"] == SAMPLE_ITEM_SIMPLE["meta"]["native-id"] for item in result)

    def test_empty_response_returns_empty_list(self):
        """Empty response pages should return empty list."""
        response_jsons = [make_cmr_response([])]

        result = response_jsons_to_cmr_granules("OPERA_L3_DSWX-S1_V1", response_jsons, convert_results=False)

        assert result == []

    def test_multiple_pages_flattened(self):
        """Items from multiple response pages should be flattened into single list."""
        page1 = make_cmr_response([SAMPLE_ITEM_SIMPLE])
        page2 = make_cmr_response([SAMPLE_ITEM_SIMPLE])

        result = response_jsons_to_cmr_granules("OPERA_L3_DSWX-S1_V1", [page1, page2], convert_results=False)

        assert len(result) == 2
