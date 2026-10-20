"""The burst coverage audit must not treat an incomplete CMR listing as the expected set.

CMR can count more granules than it returns (CMR-Hits > items) or answer a slow query with a subset
and a CMR-Timed-Out header. Before this check, such a listing was cached permanently and replayed by
every later run, so the SLCs it lacked were never flagged as missing products.
"""
import asyncio
import json
import os
import sys
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# Cluster-only modules reached through opera_commons.es_connection.
sys.modules.setdefault("hysds", MagicMock())
sys.modules.setdefault("hysds.celery", MagicMock())
os.environ.setdefault("USER", "pytest")

from tools.ops.cmr_audit import cmr_audit_burst_coverage as cov
from tools.ops.cmr_audit.cmr_client import IncompleteCmrResultsError, async_cmr_post

UTC = timezone.utc


def _page(hits, n_items, headers=None):
    """One mocked CMR response page as returned through fetch_post_url."""
    response = AsyncMock()
    response.json.return_value = {"hits": hits, "items": [{"meta": {"native-id": f"g{i}"}} for i in range(n_items)]}
    response.headers = headers or {}
    ctx = AsyncMock()
    ctx.__aenter__.return_value = response
    return ctx


def _run_post(pages, **kwargs):
    mock_fetch = AsyncMock(side_effect=pages)
    _run_post.sleep = AsyncMock()
    with patch("tools.ops.cmr_audit.cmr_client.fetch_post_url", mock_fetch), \
            patch("tools.ops.cmr_audit.cmr_client.asyncio.sleep", _run_post.sleep):
        result = asyncio.run(async_cmr_post("https://cmr/search/granules.umm_json", "q", session=AsyncMock(), **kwargs))
    return result, mock_fetch


# --- cmr_client.async_cmr_post -------------------------------------------------------------------

def test_verify_complete_raises_when_cmr_returns_fewer_items_than_it_counts():
    with pytest.raises(IncompleteCmrResultsError) as excinfo:
        _run_post([_page(3, 2), _page(3, 2), _page(3, 2)], verify_complete=True)
    assert excinfo.value.hits == 3
    assert excinfo.value.items == 2
    assert not excinfo.value.timed_out


def test_verify_complete_retries_from_the_first_page_and_returns_a_complete_listing():
    result, mock_fetch = _run_post([_page(3, 2), _page(3, 3)], verify_complete=True)
    assert len(result) == 1
    assert len(result[0]["items"]) == 3
    assert mock_fetch.await_count == 2
    _run_post.sleep.assert_awaited_once_with(2), "a pause before the retry, not an immediate re-issue"


def test_verify_complete_treats_a_timed_out_response_as_incomplete():
    timed_out = {"CMR-Timed-Out": "true"}
    with pytest.raises(IncompleteCmrResultsError) as excinfo:
        _run_post([_page(2, 2, timed_out)] * 3, verify_complete=True)
    assert excinfo.value.timed_out


def test_missing_hit_count_with_more_pages_is_incomplete():
    # The old code read response_json["hits"] and failed loudly; a silent .get() must not turn a
    # count-less first page into a "complete" (and cached) listing when CMR says there is more.
    with pytest.raises(IncompleteCmrResultsError) as excinfo:
        _run_post([_page(None, 2, {"CMR-Search-After": "[1]"})] * 3, verify_complete=True)
    assert "no hit count" in excinfo.value.reason


def test_missing_hit_count_without_more_pages_is_complete():
    result, mock_fetch = _run_post([_page(None, 2)], verify_complete=True)
    assert len(result[0]["items"]) == 2
    assert mock_fetch.await_count == 1


def test_more_items_than_counted_is_complete():
    # hits comes from page 1 and can drift upward while scrolling; only a shortfall is incomplete.
    result, mock_fetch = _run_post([_page(3, 4)], verify_complete=True)
    assert len(result[0]["items"]) == 4
    assert mock_fetch.await_count == 1


def test_verify_complete_counts_items_across_pages():
    # 2001 hits span two 2000-item pages; CMR sends CMR-Search-After on every non-empty page.
    pages = [_page(2001, 2000, {"CMR-Search-After": "[1]"}), _page(2001, 1, {"CMR-Search-After": "[2]"})]
    result, mock_fetch = _run_post(pages, verify_complete=True)
    assert [len(p["items"]) for p in result] == [2000, 1]
    assert mock_fetch.await_count == 2


def test_default_behaviour_only_warns_on_an_incomplete_listing():
    # The data subscriber shares this client; its behaviour is unchanged.
    result, mock_fetch = _run_post([_page(3, 2)])
    assert len(result) == 1
    assert mock_fetch.await_count == 1


def test_retry_rewinds_the_output_file(tmp_path):
    out = tmp_path / "items.jsonl"
    out.write_text('{"meta": {"native-id": "earlier"}}\n')
    _run_post([_page(2, 1), _page(2, 2)], verify_complete=True, output_path=str(out))
    ids = [json.loads(line)["meta"]["native-id"] for line in out.read_text().splitlines()]
    assert ids == ["earlier", "g0", "g1"]


# --- cmr_audit_utils.async_get_cmr_granules ---------------------------------------------------------

def test_listing_raises_when_one_of_its_windows_is_incomplete():
    from tools.ops.cmr_audit import cmr_audit_utils

    windows = []

    async def one_window(url, body, session, sem, output_path=None, verify_complete=True):
        windows.append(body)
        if len(windows) == 1:
            raise IncompleteCmrResultsError(hits=441, items=70, timed_out=False, pages=1, request=body[-60:])
        return set(), {}

    with patch.object(cmr_audit_utils, "get_cmr_audit_granules", one_window):
        with pytest.raises(IncompleteCmrResultsError):
            asyncio.run(cmr_audit_utils.async_get_cmr_granules(
                "SENTINEL-1A_SLC", "2026-06-23T00:00:00Z", "2026-06-24T00:00:00Z", "SENTINEL-1A", bounding_box="b",
                verify_complete=True))

    assert len(windows) == 2, "both 12-hour windows ran before the failure was raised"


def test_listing_is_best_effort_unless_asked_to_verify():
    # cmr_audit_slc/hls/dswx_s1 call this without handling IncompleteCmrResultsError; they keep the old contract.
    from tools.ops.cmr_audit import cmr_audit_utils

    seen = []

    async def one_window(url, body, session, sem, output_path=None, verify_complete=False):
        seen.append(verify_complete)
        return set(), {}

    with patch.object(cmr_audit_utils, "get_cmr_audit_granules", one_window):
        asyncio.run(cmr_audit_utils.async_get_cmr_granules(
            "SENTINEL-1A_SLC", "2026-06-23T00:00:00Z", "2026-06-24T00:00:00Z", "SENTINEL-1A", bounding_box="b"))
    assert seen == [False, False]


def test_fetch_slc_granules_opts_in_to_verification():
    kwargs_seen = {}

    async def listing(collection, **kwargs):
        kwargs_seen.update(kwargs)
        return set(), {}

    cache = MagicMock()
    cache.get.return_value = None
    with patch.object(cov, "async_get_cmr_granules", listing), patch.object(cov, "get_cache", return_value=cache):
        asyncio.run(cov.fetch_slc_granules(datetime(2026, 6, 23, tzinfo=UTC), datetime(2026, 6, 24, tzinfo=UTC), "b"))
    assert kwargs_seen["verify_complete"] is True


# --- cmr_audit_burst_coverage.fetch_slc_granules ----------------------------------------------------

def test_fetch_slc_granules_does_not_cache_an_incomplete_listing():
    async def incomplete(collection, **kwargs):
        raise IncompleteCmrResultsError(hits=441, items=70, timed_out=False, pages=1, request=collection)

    cache = MagicMock()
    cache.get.return_value = None

    with patch.object(cov, "async_get_cmr_granules", incomplete), patch.object(cov, "get_cache", return_value=cache):
        with pytest.raises(IncompleteCmrResultsError):
            asyncio.run(cov.fetch_slc_granules(datetime(2026, 6, 23, tzinfo=UTC), datetime(2026, 6, 24, tzinfo=UTC), "b"))

    cache.set.assert_not_called()


# --- RequestCache recheck dates ---------------------------------------------------------------------

def _slc_params(start, end):
    return {"collection": "SENTINEL-1A_SLC", "start": start, "end": end, "bbox": "-125,24,-66,50"}


def test_recheck_dates_bypass_cached_slc_listings_that_cover_them(tmp_path):
    cache = cov.RequestCache(tmp_path / "cache", enabled=True, recheck_dates={"2026-06-23"})
    on_date = _slc_params("2026-06-23T00:00:00Z", "2026-06-24T00:00:00Z")
    before = _slc_params("2026-06-22T00:00:00Z", "2026-06-23T00:00:00Z")
    spanning = _slc_params("2026-06-22T00:00:00Z", "2026-06-25T00:00:00Z")
    for params in (on_date, before, spanning):
        cache.set("cmr_slc", params, {"cached": True})

    assert cache.get("cmr_slc", on_date) is None
    assert cache.get("cmr_slc", spanning) is None
    assert cache.get("cmr_slc", before) == {"cached": True}


def test_recheck_dates_bypass_product_lookups_whose_window_ends_on_them(tmp_path):
    # A midnight-spanning SLC looks up date..end_date; the end day counts too.
    cache = cov.RequestCache(tmp_path / "cache", enabled=True, recheck_dates={"2026-06-23"})
    spanning = {"burst": "T100-213518-IW2", "date": "2026-06-22", "end_date": "2026-06-23", "product": "CSLC-S1"}
    before = {"burst": "T100-213518-IW2", "date": "2026-06-21", "end_date": "2026-06-22", "product": "CSLC-S1"}
    on_date = {"burst": "T100-213518-IW2", "date": "2026-06-23", "end_date": "2026-06-23", "product": "CSLC-S1"}
    for params in (spanning, before, on_date):
        cache.set("cmr_opera", params, ["p"])

    assert cache.get("cmr_opera", spanning) is None
    assert cache.get("cmr_opera", on_date) is None
    assert cache.get("cmr_opera", before) == ["p"]


def test_recheck_dates_leave_other_namespaces_alone(tmp_path):
    cache = cov.RequestCache(tmp_path / "cache", enabled=True, recheck_dates={"2026-06-23"})
    cache.set("asf_bursts", {"slc": "x", "start": "2026-06-23T00:00:00Z", "end": "2026-06-24T00:00:00Z"}, ["a"])
    assert cache.get("asf_bursts", {"slc": "x", "start": "2026-06-23T00:00:00Z", "end": "2026-06-24T00:00:00Z"}) == ["a"]


# --- audit loop ------------------------------------------------------------------------------------

def test_audit_skips_the_chunk_and_records_it_when_the_listing_is_incomplete(tmp_path):
    geojson = tmp_path / "aoi.geojson"
    geojson.write_text(json.dumps({"type": "FeatureCollection", "features": [{
        "type": "Feature", "properties": {},
        "geometry": {"type": "Polygon", "coordinates": [[[-115, 33], [-112, 33], [-112, 35], [-115, 35], [-115, 33]]]}}]}))
    out = tmp_path / "audit.jsonl"
    calls = []

    async def listing(start, end, bbox):
        calls.append(start)
        if len(calls) == 1:
            raise IncompleteCmrResultsError(hits=441, items=70, timed_out=False, pages=1, request="SENTINEL-1A_SLC")
        return set(), {}

    with patch.object(cov, "fetch_slc_granules", listing):
        results = asyncio.run(cov.audit_burst_coverage(
            start_datetime=datetime(2026, 6, 23, tzinfo=UTC), end_datetime=datetime(2026, 6, 25, tzinfo=UTC),
            geojson_path=str(geojson), product_types=["CSLC-S1"], polarizations=["VV"],
            low_memory=True, output_path=str(out), chunk_days=1, buffer_deg=0.0))

    assert len(calls) == 2, "the run continues past the failed chunk"
    failed = results["metadata"]["failed_chunks"]
    assert [f["chunk_start"][:10] for f in failed] == ["2026-06-23"]
    assert "70 of 441" in failed[0]["error"]

    records = [json.loads(line) for line in out.read_text().splitlines()]
    kinds = [r["_type"] for r in records]
    assert kinds.count("chunk_error") == 1
    assert kinds.count("chunk_result") == 0
    summary = next(r for r in records if r["_type"] == "summary")
    assert len(summary["failed_chunks"]) == 1


def test_audit_skips_the_chunk_when_cmr_fails_after_retries(tmp_path):
    import aiohttp

    geojson = tmp_path / "aoi.geojson"
    geojson.write_text(json.dumps({"type": "FeatureCollection", "features": [{
        "type": "Feature", "properties": {},
        "geometry": {"type": "Polygon", "coordinates": [[[-115, 33], [-112, 33], [-112, 35], [-115, 35], [-115, 33]]]}}]}))
    out = tmp_path / "audit.jsonl"
    calls = []

    async def listing(start, end, bbox):
        calls.append(start)
        if len(calls) == 1:
            raise aiohttp.ClientResponseError(MagicMock(), (), status=500, message="Internal Server Error")
        return set(), {}

    with patch.object(cov, "fetch_slc_granules", listing):
        results = asyncio.run(cov.audit_burst_coverage(
            start_datetime=datetime(2026, 6, 23, tzinfo=UTC), end_datetime=datetime(2026, 6, 25, tzinfo=UTC),
            geojson_path=str(geojson), product_types=["CSLC-S1"], polarizations=["VV"],
            low_memory=True, output_path=str(out), chunk_days=1, buffer_deg=0.0))

    assert len(calls) == 2, "a CMR 500 on one chunk no longer aborts the run"
    failed = results["metadata"]["failed_chunks"]
    assert len(failed) == 1 and "failed" in failed[0]["error"] and "500" in failed[0]["error"]


def test_audit_surfaces_a_permanent_cmr_rejection_instead_of_skipping(tmp_path):
    # A 400 (bad bbox) would repeat on every chunk; "re-run these dates" would be the wrong advice.
    import aiohttp

    geojson = tmp_path / "aoi.geojson"
    geojson.write_text(json.dumps({"type": "FeatureCollection", "features": [{
        "type": "Feature", "properties": {},
        "geometry": {"type": "Polygon", "coordinates": [[[-115, 33], [-112, 33], [-112, 35], [-115, 35], [-115, 33]]]}}]}))

    async def listing(start, end, bbox):
        raise aiohttp.ClientResponseError(MagicMock(), (), status=400, message="Bad Request")

    with patch.object(cov, "fetch_slc_granules", listing):
        with pytest.raises(aiohttp.ClientResponseError):
            asyncio.run(cov.audit_burst_coverage(
                start_datetime=datetime(2026, 6, 23, tzinfo=UTC), end_datetime=datetime(2026, 6, 25, tzinfo=UTC),
                geojson_path=str(geojson), product_types=["CSLC-S1"], polarizations=["VV"],
                low_memory=True, output_path=str(tmp_path / "audit.jsonl"), chunk_days=1, buffer_deg=0.0))


def test_transient_cmr_errors_are_the_retryable_ones():
    import aiohttp
    from tools.ops.cmr_audit.cmr_client import is_transient_cmr_error

    assert is_transient_cmr_error(aiohttp.ClientResponseError(MagicMock(), (), status=503))
    assert is_transient_cmr_error(aiohttp.ClientResponseError(MagicMock(), (), status=429))
    assert is_transient_cmr_error(asyncio.TimeoutError())
    assert is_transient_cmr_error(aiohttp.ServerTimeoutError())
    assert not is_transient_cmr_error(aiohttp.ClientResponseError(MagicMock(), (), status=400))
    assert not is_transient_cmr_error(aiohttp.ClientResponseError(MagicMock(), (), status=413))


# --- fetch_opera_products ----------------------------------------------------------------------------

class _Resp:
    def __init__(self, payload, headers=None, status=200):
        self.payload, self.headers, self.status = payload, headers or {}, status

    async def json(self):
        return self.payload


class _Ctx:
    def __init__(self, resp):
        self.resp = resp

    async def __aenter__(self):
        return self.resp

    async def __aexit__(self, *args):
        return False


class _Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = 0

    def post(self, *args, **kwargs):
        self.calls += 1
        return _Ctx(self.responses.pop(0))


def _opera_lookup(session):
    cache = MagicMock()
    cache.get.return_value = None
    burst = cov.BurstInfo(100, 213518, "IW2")
    with patch.object(cov, "get_cache", return_value=cache), patch.object(cov.asyncio, "sleep", AsyncMock()):
        found = asyncio.run(cov.fetch_opera_products(
            burst, datetime(2026, 6, 23, 13, 36, tzinfo=UTC), "CSLC-S1", session, asyncio.Semaphore(1)))
    return found, cache


def test_fetch_opera_products_marks_an_incomplete_response_unverified_and_does_not_cache_it():
    item = {"meta": {"native-id": "OPERA_L2_CSLC-S1_T100-213518-IW2_20260623T133610Z_x_S1A_VV_v1.1"}}
    session = _Session([_Resp({"hits": 2, "items": [item]})] * 3)
    lookup, cache = _opera_lookup(session)
    assert lookup.products == {item["meta"]["native-id"]}
    assert not lookup.verified and "1 of 2" in lookup.reason
    assert session.calls == 3
    cache.set.assert_not_called()


def test_fetch_opera_products_marks_a_failed_lookup_unverified():
    session = _Session([_Resp({}, status=500)] * 3)
    lookup, cache = _opera_lookup(session)
    assert lookup.products == set()
    assert not lookup.verified and "500" in lookup.reason
    cache.set.assert_not_called()


def test_fetch_opera_products_caches_a_complete_response():
    item = {"meta": {"native-id": "OPERA_L2_CSLC-S1_T100-213518-IW2_20260623T133610Z_x_S1A_VV_v1.1"}}
    session = _Session([_Resp({"hits": 1, "items": [item]})])
    lookup, cache = _opera_lookup(session)
    assert lookup.products == {item["meta"]["native-id"]} and lookup.verified
    cache.set.assert_called_once()


def test_fetch_opera_products_follows_search_after_when_hits_exceed_a_page():
    # More products than one page holds (same-date duplicates, reprocessed revisions): page through
    # with CMR-Search-After instead of calling the lookup incomplete forever.
    a = {"meta": {"native-id": "OPERA_L2_CSLC-S1_T100-213518-IW2_20260623T133610Z_a_S1A_VV_v1.1"}}
    b = {"meta": {"native-id": "OPERA_L2_CSLC-S1_T100-213518-IW2_20260623T133610Z_b_S1A_VV_v1.1"}}
    session = _Session([_Resp({"hits": 2, "items": [a]}, headers={"CMR-Search-After": "[1]"}),
                        _Resp({"hits": 2, "items": [b]}, headers={"CMR-Search-After": "[2]"})])
    lookup, cache = _opera_lookup(session)
    assert lookup.products == {a["meta"]["native-id"], b["meta"]["native-id"]} and lookup.verified
    assert session.calls == 2
    cache.set.assert_called_once()


# --- unverified bursts are not gaps ----------------------------------------------------------------

def _expected(pol="VV"):
    return cov.ExpectedBurst(cov.BurstInfo(100, 213518, "IW2"), datetime(2026, 6, 23, 13, 36, tzinfo=UTC), "S1A", pol,
                             "S1A_IW_SLC__1SDV_20260623T133605_20260623T133632_065097_083497_2D92-SLC")


def _coverage(lookup_impl):
    async def fake_lookup(*args, **kwargs):
        return lookup_impl()
    with patch.object(cov, "fetch_opera_products", fake_lookup):
        return asyncio.run(cov.check_coverage_for_bursts([_expected()], "CSLC-S1"))


def test_unanswered_lookup_reports_the_burst_as_unverified_not_missing():
    found, missing, unverified = _coverage(lambda: cov.ProductLookup(set(), False, "CMR status 500 after 3 attempts"))
    assert (found, missing) == ([], [])
    assert len(unverified) == 1 and unverified[0]["reason"] == "CMR status 500 after 3 attempts"
    assert unverified[0]["burst_pattern"] == "T100-213518-IW2"


def test_partial_lookup_still_counts_what_it_found():
    pid = "OPERA_L2_CSLC-S1_T100-213518-IW2_20260623T133610Z_x_S1A_VV_v1.1"
    found, missing, unverified = _coverage(lambda: cov.ProductLookup({pid}, False, "CMR returned 1 of 2"))
    assert [f["opera_product_id"] for f in found] == [pid]
    assert (missing, unverified) == ([], [])


def test_lookup_exception_makes_the_group_unverified_instead_of_dropping_it():
    def boom():
        raise RuntimeError("socket closed")
    found, missing, unverified = _coverage(boom)
    assert (found, missing) == ([], [])
    assert len(unverified) == 1 and "socket closed" in unverified[0]["reason"]


def test_verified_lookup_with_no_product_is_missing():
    found, missing, unverified = _coverage(lambda: cov.ProductLookup(set()))
    assert (found, unverified) == ([], [])
    assert len(missing) == 1


def test_audit_records_unverified_days_and_exits_2(tmp_path):
    geojson = tmp_path / "aoi.geojson"
    geojson.write_text(json.dumps({"type": "FeatureCollection", "features": [{
        "type": "Feature", "properties": {},
        "geometry": {"type": "Polygon", "coordinates": [[[-115, 33], [-112, 33], [-112, 35], [-115, 35], [-115, 33]]]}}]}))
    out = tmp_path / "audit.jsonl"
    nid = "S1A_IW_SLC__1SDV_20260623T133605_20260623T133632_065097_083497_2D92-SLC"
    item = {"umm": {"SpatialExtent": {"HorizontalSpatialDomain": {"Geometry": {"GPolygons": [{"Boundary": {"Points": [
        {"Longitude": -114.0, "Latitude": 33.5}, {"Longitude": -113.0, "Latitude": 33.5},
        {"Longitude": -113.0, "Latitude": 34.5}, {"Longitude": -114.0, "Latitude": 34.5}]}}]}}}}}

    async def listing(start, end, bbox):
        return {nid}, {nid: item}

    async def expected(slcs, polarizations):
        return 1, ["one expected burst"]

    async def coverage(expected_bursts, product_type, use_grq=False):
        return [], [], [{**_expected().to_dict(), "reason": "CMR status 500 after 3 attempts"}]

    with patch.object(cov, "fetch_slc_granules", listing), patch.object(cov, "process_slcs_to_expected_bursts", expected), \
            patch.object(cov, "check_coverage_for_bursts", coverage):
        results = asyncio.run(cov.audit_burst_coverage(
            start_datetime=datetime(2026, 6, 23, tzinfo=UTC), end_datetime=datetime(2026, 6, 24, tzinfo=UTC),
            geojson_path=str(geojson), product_types=["CSLC-S1"], polarizations=["VV"],
            low_memory=True, output_path=str(out), chunk_days=1, buffer_deg=0.0))

    assert results["metadata"]["unverified_days"] == ["2026-06-23"]
    product = results["products"]["CSLC-S1"]
    assert (product["missing_count"], product["unverified_count"]) == (0, 1)
    assert cov.exit_code_for(results) == 2

    records = [json.loads(line) for line in out.read_text().splitlines()]
    chunk = next(r for r in records if r["_type"] == "chunk_result")
    assert chunk["unverified_count"] == 1 and chunk["unverified"][0]["reason"].startswith("CMR status 500")
    summary = next(r for r in records if r["_type"] == "summary")
    assert summary["unverified_days"] == ["2026-06-23"]


def test_exit_code_is_1_for_missing_and_0_when_clean():
    base = {"metadata": {"failed_chunks": [], "unverified_days": []}, "products": {"CSLC-S1": {"missing_count": 3}}}
    assert cov.exit_code_for(base) == 1
    base["products"]["CSLC-S1"]["missing_count"] = 0
    assert cov.exit_code_for(base) == 0
