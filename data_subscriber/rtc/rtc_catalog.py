import logging
from collections import defaultdict
from datetime import datetime

import dateutil
import elasticsearch.helpers
from more_itertools import last, chunked, first

from util.grq_client import get_body
from ..hls.hls_catalog import HLSProductCatalog

null_logger = logging.getLogger("dummy")
null_logger.addHandler(logging.NullHandler())
null_logger.propagate = False

ES_INDEX_PATTERNS = "rtc_catalog*"

class RTCProductCatalog(HLSProductCatalog):
    """
    Class to track products downloaded by daac_data_subscriber.py

    https://github.com/hysds/hysds_commons/blob/develop/hysds_commons/elasticsearch_utils.py
    ElasticsearchUtility methods
        index_document
        get_by_id
        query
        search
        get_count
        delete_by_id
        update_document
    """
    def __init__(self, /, logger=None):
        super().__init__(logger=logger)
        self.ES_INDEX_PATTERNS = ES_INDEX_PATTERNS

    def generate_es_index_name(self):
        return "rtc_catalog-{date}".format(date=datetime.utcnow().strftime("%Y.%m"))

    def filter_query_result(self, query_result):
        return [result["_source"] for result in (query_result or [])]

    def granule_and_revision(self, es_id: str):
        """For 'OPERA_L2_RTC-S1_T011-022517-IW3_20231019T111602Z_20231019T214046Z_S1A_30_v1.0-r1' returns:
        OPERA_L2_RTC-S1_T011-022517-IW3_20231019T111602Z_20231019T214046Z_S1A_30_v1.0 and 1"""
        return es_id[:es_id.rfind("-")], es_id[es_id.rfind("-r")+2:]

    def get_download_granule_revision(self, mgrs_set_id_acquisition_ts_cycle_index: str):
        downloads = self.es.query(
            index=self.ES_INDEX_PATTERNS,
            body={
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"mgrs_set_id_acquisition_ts_cycle_index": mgrs_set_id_acquisition_ts_cycle_index}},
                            {"match": {"mgrs_set_id": first(mgrs_set_id_acquisition_ts_cycle_index.split("$"))}}
                        ]
                    }
                }
            }
        )
        # apply client-side filtering
        downloads[:] = [download for download in downloads if mgrs_set_id_acquisition_ts_cycle_index == download["_source"]["mgrs_set_id_acquisition_ts_cycle_index"]]

        return self.filter_query_result(downloads)

    def filter_catalog_by_sets(self, mgrs_set_id_acquisition_ts_cycle_indexes):
        body = get_body(match_all=False)
        for mgrs_set_id_acquisition_ts_cycle_idx in mgrs_set_id_acquisition_ts_cycle_indexes:
            body["query"]["bool"]["must"].append({"match": {"mgrs_set_id_acquisition_ts_cycle_index": mgrs_set_id_acquisition_ts_cycle_idx}})
            body["query"]["bool"]["must"].append({"match": {"mgrs_set_id": first(mgrs_set_id_acquisition_ts_cycle_idx.split("$"))}})

        es_docs = self.es.query(body=body, index=self.ES_INDEX_PATTERNS)
        logging.info(f"Found {len(es_docs)=}")
        return self.filter_query_result(es_docs)

    def mark_products_as_download_job_submitted(self, batch_id_to_products_map: dict):
        operations = []
        for batch_id, product_id_to_products_map in batch_id_to_products_map.items():
            download_job_dts = datetime.now().isoformat(timespec="seconds").replace("+00:00", "Z")
            for product_id, products in product_id_to_products_map.items():
                docs = products
                doc_id_to_index_cache = self.raw_create_doc_id_to_index_cache(docs)
                for doc in docs:
                    index = last(doc_id_to_index_cache[doc["id"]],
                        self._get_index_name_for(_id=doc["id"], default=self.generate_es_index_name())
                    )
                    operation = {
                        "_op_type": "update",
                        "_index": index,
                        "_type": "_doc",
                        "_id": doc["id"],
                        "doc_as_upsert": True,
                        "doc": {
                            "download_job_ids": doc["download_job_ids"],
                            "latest_download_job_ts": download_job_dts
                        }
                    }
                    operations.append(operation)
        logging.info(f"Marking {set(batch_id_to_products_map.keys())} products as download job-submitted, in bulk")
        elasticsearch.helpers.bulk(self.es.es, operations)
        logging.info(f"Marked {set(batch_id_to_products_map.keys())} products as download job-submitted, in bulk")

        self.logger.info("performing index refresh")
        self.refresh()
        self.logger.info("performed index refresh")

    def raw_create_doc_id_to_index_cache(self, docs):
        body = get_body(match_all=False)
        body["_source"] = {"includes": [], "excludes": []}
        es_docs = []
        # Batch requests for larger number of docs
        # see Elasticsearch documentation  regarding "indices.query.bool.max_clause_count". Minimum is 1024
        for doc_chunk in chunked(docs, 1024):
            for doc in doc_chunk:
                body["query"]["bool"]["should"].append({"match": {"id.keyword": doc["id"]}})
            es_docs.extend(self.es.query(body=body, index=self.ES_INDEX_PATTERNS))
            body["query"]["bool"]["should"] = []
        id_to_index_cache = defaultdict(set)
        for es_doc in es_docs:
            id_to_index_cache[es_doc["_id"]].add(es_doc["_index"])
        return id_to_index_cache

    def mark_products_as_job_submitted(self, batch_id_to_products_map: dict):
        operations = []
        dswx_s1_job_dts = datetime.now().isoformat(timespec="seconds").replace("+00:00", "Z")
        for batch_id, products in batch_id_to_products_map.items():
            docs = product_docs = products
            doc_id_to_index_cache = self.create_doc_id_to_index_cache(docs)
            for doc in docs:
                index = last(
                    doc_id_to_index_cache[doc["id"]],
                    self._get_index_name_for(_id=doc["id"], default=self.generate_es_index_name())
                )
                operation = {
                    "_op_type": "update",
                    "_index": index,
                    "_type": "_doc",
                    "_id": doc["id"],
                    "doc_as_upsert": True,
                    "doc": {
                        "dswx_s1_jobs_ids": doc["dswx_s1_jobs_ids"],
                        "latest_dswx_s1_job_ts": dswx_s1_job_dts
                    }
                }
                operations.append(operation)
        logging.info(f"Marking {set(batch_id_to_products_map.keys())} products as job-submitted, in bulk")
        elasticsearch.helpers.bulk(self.es.es, operations)
        logging.info(f"Marked {set(batch_id_to_products_map.keys())} products as job-submitted, in bulk")

        self.logger.info("performing index refresh")
        self.refresh()
        self.logger.info("performed index refresh")

    def create_doc_id_to_index_cache(self, docs):
        body = get_body(match_all=False)
        body["_source"] = {"includes": [], "excludes": []}
        es_docs = []
        # Batch requests for larger number of docs
        # see Elasticsearch documentation  regarding "indices.query.bool.max_clause_count". Minimum is 1024
        for doc_chunk in chunked(docs, 1024):
            for doc in doc_chunk:
                body["query"]["bool"]["should"].append({"match": {"id.keyword": doc["id"]}})
            es_docs.extend(self.es.query(body=body, index=self.ES_INDEX_PATTERNS))
            body["query"]["bool"]["should"] = []
        id_to_index_cache = defaultdict(set)
        for es_doc in es_docs:
            id_to_index_cache[es_doc["_id"]].add(es_doc["_index"])
        return id_to_index_cache

    def update_granule_index(
            self,
            granule,
            job_id: str,
            query_dt: datetime,
            mgrs_set_id_acquisition_ts_cycle_indexes: list[str],
            **kwargs
    ):
        urls = granule.get("filtered_urls")
        granule_id = granule.get("granule_id")
        temporal_extent_beginning_dt: datetime = dateutil.parser.isoparse(granule["temporal_extent_beginning_datetime"])
        revision_date_dt: datetime = dateutil.parser.isoparse(granule["revision_date"])

        for mgrs_set_id_acquisition_ts_cycle_index in mgrs_set_id_acquisition_ts_cycle_indexes:
            doc = {
                "id": f"{granule_id}${mgrs_set_id_acquisition_ts_cycle_index}",
                "granule_id": granule_id,
                "creation_timestamp": datetime.now(),
                "query_job_id": job_id,
                "query_datetime": query_dt,
                "temporal_extent_beginning_datetime": temporal_extent_beginning_dt,
                "revision_date": revision_date_dt,
                "https_urls": [url for url in urls if "https://" in url],
                "s3_urls": [url for url in urls if "s3://" in url],
                "mgrs_set_id": first(mgrs_set_id_acquisition_ts_cycle_index.split("$")),
                "mgrs_set_id_acquisition_ts_cycle_index": mgrs_set_id_acquisition_ts_cycle_index
            }
            doc.update(kwargs)
            index = self._get_index_name_for(_id=doc['id'], default=self.generate_es_index_name())
            self.es.update_document(index=index, body={"doc_as_upsert": True, "doc": doc}, id=doc['id'])
