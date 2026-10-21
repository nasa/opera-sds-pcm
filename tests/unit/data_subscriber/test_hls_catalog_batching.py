from unittest.mock import MagicMock, patch

from data_subscriber.hls.hls_catalog import HLSProductCatalog, HLSSpatialProductCatalog


def search_response(hits, total=None):
    return {"hits": {"total": {"value": len(hits) if total is None else total}, "hits": hits}}


def hit(_id, index, creation_timestamp="2026-09-01T00:00:00", **source):
    return {"_id": _id, "_index": index, "_source": {"creation_timestamp": creation_timestamp, **source}}


def test_prefetch_existing_answers_index_lookups_from_memory():
    catalog = HLSProductCatalog()
    catalog.es_util.es.search = MagicMock(return_value=search_response([
        hit("a.B04.tif-r1", "hls_catalog-2026.08", "2026-08-01T00:00:00"),
        hit("a.B04.tif-r1", "hls_catalog-2026.09", "2026-09-01T00:00:00"),  # newest copy wins
        hit("b.B04.tif-r1", "hls_catalog-2026.07"),
    ]))
    catalog._query_existence = MagicMock()

    catalog.prefetch_existing(["a.B04.tif-r1", "b.B04.tif-r1", "c.B04.tif-r1", "a.B04.tif-r1"])

    assert catalog.es_util.es.search.call_count == 1
    assert catalog.es_util.es.search.call_args.kwargs["body"]["query"] == \
        {"ids": {"values": ["a.B04.tif-r1", "b.B04.tif-r1", "c.B04.tif-r1"]}}
    assert catalog._get_index_name_for("a.B04.tif-r1", default="new") == "hls_catalog-2026.09"
    assert catalog._get_index_name_for("b.B04.tif-r1", default="new") == "hls_catalog-2026.07"
    assert catalog._get_index_name_for("c.B04.tif-r1", default="new") == "new"
    catalog._query_existence.assert_not_called()


def test_prefetch_existing_chunks_and_skips_known_ids():
    catalog = HLSProductCatalog()
    catalog.PREFETCH_CHUNK_SIZE = 2
    catalog.es_util.es.search = MagicMock(return_value=search_response([]))

    catalog.prefetch_existing(["a", "b", "c"])
    catalog.prefetch_existing(["a", "d"])

    assert [c.kwargs["body"]["query"]["ids"]["values"] for c in catalog.es_util.es.search.call_args_list] == \
        [["a", "b"], ["c"], ["d"]]


def test_prefetch_existing_falls_back_when_hits_are_truncated():
    catalog = HLSProductCatalog()
    catalog.es_util.es.search = MagicMock(return_value=search_response([hit("a", "hls_catalog-2026.09")], total=5))
    catalog._query_existence = MagicMock(return_value=[{"_index": "hls_catalog-2026.01"}])

    catalog.prefetch_existing(["a", "b"])

    assert catalog._get_index_name_for("a", default="new") == "hls_catalog-2026.01"
    catalog._query_existence.assert_called_once_with("a")


def test_process_granule_uses_prefetch():
    catalog = HLSSpatialProductCatalog()
    catalog.es_util.es.search = MagicMock(return_value=search_response([hit("g1", "hls_spatial_catalog-2026.09")]))
    catalog._query_existence = MagicMock()
    bulk = MagicMock()
    granule = {"granule_id": "g2", "provider": "LPCLOUD", "production_datetime": "x", "short_name": "HLSS30",
               "identifier": "g2", "bounding_box": []}

    catalog.prefetch_existing(["g1", "g2"])
    catalog.process_granule({**granule, "granule_id": "g1"}, bulk=bulk)
    catalog.process_granule(granule, bulk=bulk)

    catalog._query_existence.assert_not_called()
    bulk.add.assert_called_once()
    assert bulk.add.call_args.args[1] == "g2"


def test_mark_download_job_id_by_doc_id_for_cataloged_batch():
    catalog = HLSProductCatalog()
    catalog.es_util.es.search = MagicMock(return_value=search_response([]))
    catalog.es_util.es.update_by_query = MagicMock()
    granule = {"granule_id": "HLS.S30.T56MPU.2022152T000741.v2.0"}
    urls = ["https://x/HLS.S30.T56MPU.2022152T000741.v2.0.B04.tif", "s3://x/HLS.S30.T56MPU.2022152T000741.v2.0.B04.tif"]

    catalog.process_url(urls, granule, "job", None, None, None, bulk=MagicMock(), revision_id=1)

    with patch("data_subscriber.catalog.bulk_helper", return_value=(1, [])) as bulk_helper:
        catalog.mark_download_job_id("HLS.S30.T56MPU.2022152T000741.v2.0-r1", "download-job-1")

    actions = bulk_helper.call_args.args[1]
    assert actions == [{"_op_type": "update", "_index": catalog.generate_es_index_name(),
                        "_id": "HLS.S30.T56MPU.2022152T000741.v2.0.B04.tif-r1",
                        "doc": {"download_job_id": "download-job-1"}}]
    catalog.es_util.es.update_by_query.assert_not_called()


def test_mark_download_job_id_falls_back_to_query_for_unknown_batch():
    catalog = HLSProductCatalog()
    catalog.es_util.es.update_by_query = MagicMock(return_value={"updated": 16})

    catalog.mark_download_job_id("HLS.S30.T56MPU.2022152T000741.v2.0-r1", "download-job-1")

    catalog.es_util.es.update_by_query.assert_called_once()


def test_get_cataloged_granules_by_granule_ids_groups_hits():
    catalog = HLSProductCatalog()
    catalog.es_util.es.search = MagicMock(return_value=search_response([
        hit("g1.B04.tif-r1", "hls_catalog-2026.09", granule_id="g1", download_job_id="j1"),
        hit("g1.B05.tif-r1", "hls_catalog-2026.09", granule_id="g1", download_job_id="j1"),
    ]))

    result = catalog.get_cataloged_granules_by_granule_ids(["g1", "g2"])

    assert catalog.es_util.es.search.call_args.kwargs["body"]["query"] == {"terms": {"granule_id": ["g1", "g2"]}}
    assert len(result["g1"]) == 2
    assert result.get("g2", []) == []
