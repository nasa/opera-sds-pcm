"""Sentinel-1D coverage in the CMR audit tools.

Without these, S1D acquisitions are silently absent from the audits: the burst
coverage audit reports them as gaps, and the SLC audit either raises on the S1D
collection short name or filters S1D out server-side.
"""
import asyncio
import sys
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

# cmr_audit_slc reaches osgeo transitively, which is only present on-cluster.
sys.modules.setdefault("osgeo", MagicMock())

from tools.ops.cmr_audit import cmr_audit_burst_coverage, cmr_audit_slc
from tools.ops.cmr_audit.cmr_audit_utils import request_body_supplier

START = datetime(2026, 8, 1, tzinfo=timezone.utc)
END = datetime(2026, 8, 2, tzinfo=timezone.utc)
BBOX = "-180,-60,180,90"


def test_request_body_supplier_accepts_the_s1d_slc_collection():
    body = request_body_supplier("SENTINEL-1D_SLC", "2026-08-01T00:00:00Z",
                                 "2026-08-02T00:00:00Z", "SENTINEL-1D")
    assert "short_name[]=SENTINEL-1D_SLC" in body
    assert "platform[]=SENTINEL-1D" in body


def test_burst_coverage_platform_map_covers_s1d():
    assert cmr_audit_burst_coverage.PLATFORM_MAP["S1D"] == "SENTINEL-1D"


def test_burst_coverage_queries_the_s1d_collection():
    queried = []

    async def fake_query(collection, **kwargs):
        queried.append(collection)
        return set(), {}

    empty_cache = MagicMock()
    empty_cache.get.return_value = None

    with patch.object(cmr_audit_burst_coverage, "async_get_cmr_granules", fake_query), \
            patch.object(cmr_audit_burst_coverage, "get_cache", return_value=empty_cache):
        asyncio.run(cmr_audit_burst_coverage.fetch_slc_granules(START, END, BBOX))

    assert "SENTINEL-1D_SLC" in queried


def test_slc_audit_queries_s1d_by_collection_and_platform():
    captured = {}

    async def fake_query(collection, **kwargs):
        captured["collection"] = collection
        captured["platform"] = kwargs.get("platform_short_name")
        return set(), {}

    with patch.object(cmr_audit_slc, "async_get_cmr_granules", fake_query):
        asyncio.run(cmr_audit_slc.async_get_cmr_granules_slc_s1d(
            temporal_date_start="2026-08-01T00:00:00Z",
            temporal_date_end="2026-08-02T00:00:00Z"))

    assert captured == {"collection": "SENTINEL-1D_SLC", "platform": "SENTINEL-1D"}


# cmr_audit_slc.async_get_cmr() also pins an explicit platform list that S1D was
# missing from, but it is unreachable: the module is deprecated, its entry point
# raises in favour of cmr_audit_burst_coverage, and the module-level `logger` it
# references is only bound on that disabled path. It is left untested rather than
# propped up with a fake global.
