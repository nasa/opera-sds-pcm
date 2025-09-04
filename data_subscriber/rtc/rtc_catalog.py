
from collections import defaultdict
from datetime import datetime, timezone
from opera_commons.datetime_utils import parse_iso_datetime

import dateutil
import elasticsearch.helpers
import opensearchpy
from more_itertools import last, chunked

from data_subscriber.catalog import ProductCatalog
from data_subscriber.rtc import mgrs_bursts_collection_db_client
from util.conf_util import SettingsConf
from util.grq_client import get_body

settings = SettingsConf().cfg


class RTCProductCatalog(ProductCatalog):
    """Cataloging class for downloaded Radiometric Terrain Corrected (RTC) products."""
    NAME = "rtc_catalog"
    ES_INDEX_PATTERNS = "rtc_catalog*"

    def process_query_result(self, query_result):
        return [result["_source"] for result in (query_result or [])]

    def granule_and_revision(self, es_id: str):
        """
        For OPERA_L2_RTC-S1_T011-022517-IW3_20231019T111602Z_20231019T214046Z_S1A_30_v1.0-r1 returns:
            OPERA_L2_RTC-S1_T011-022517-IW3_20231019T111602Z_20231019T214046Z_S1A_30_v1.0 and 1
        """
        return es_id[:es_id.rfind("-")], es_id[es_id.rfind("-r")+2:]

    def get_download_granule_revision(self, mgrs_set_id_acquisition_ts_cycle_index: str):
        downloads = self.es_util.query(
            index=self.ES_INDEX_PATTERNS,
            body={
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"mgrs_set_id_acquisition_ts_cycle_index": mgrs_set_id_acquisition_ts_cycle_index}},
                            {"match": {"mgrs_set_id": mgrs_set_id_acquisition_ts_cycle_index.split("$")[0]}}
                        ]
                    }
                }
            }
        )

        # apply client-side filtering
        downloads[:] = [download
                        for download in downloads
                        if mgrs_set_id_acquisition_ts_cycle_index == download["_source"]["mgrs_set_id_acquisition_ts_cycle_index"]]

        return self.process_query_result(downloads)

    def filter_catalog_by_sets(self, mgrs_set_id_acquisition_ts_cycle_indexes):
        body = get_body(match_all=False)
        for mgrs_set_id_acquisition_ts_cycle_idx in mgrs_set_id_acquisition_ts_cycle_indexes:
            body["query"]["bool"]["must"].append({"match": {"mgrs_set_id_acquisition_ts_cycle_index": mgrs_set_id_acquisition_ts_cycle_idx}})
            body["query"]["bool"]["must"].append({"match": {"mgrs_set_id": mgrs_set_id_acquisition_ts_cycle_idx.split("$")[0]}})

        es_docs = self.es_util.query(body=body, index=self.ES_INDEX_PATTERNS)
        self.logger.debug("Found %d", len(es_docs))
        return self.process_query_result(es_docs)

    def mark_products_as_download_job_submitted(self, batch_id_to_products_map: dict):
        operations = []
        mgrs = mgrs_bursts_collection_db_client.cached_load_mgrs_burst_db(filter_land=True)
        for batch_id, product_id_to_products_map in batch_id_to_products_map.items():
            download_job_dts = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")

            mgrs_set_id = batch_id.split("$")[0]
            number_of_bursts_expected = mgrs[mgrs["mgrs_set_id"] == mgrs_set_id].iloc[0]["number_of_bursts"]
            number_of_bursts_actual = len(product_id_to_products_map)
            coverage = int(number_of_bursts_actual / number_of_bursts_expected * 100)

            for product_id, products in product_id_to_products_map.items():
                docs = products
                doc_id_to_index_cache = self.raw_create_doc_id_to_index_cache(docs)
                for doc in docs:
                    index = last(doc_id_to_index_cache[doc["id"]],
                        self._get_index_name_for(_id=doc["id"], default=self.generate_es_index_name())
                    )

                    op_doc = {
                        "download_job_ids": doc["download_job_ids"],
                        "latest_download_job_ts": download_job_dts,
                        "number_of_bursts_expected": number_of_bursts_expected,
                        "number_of_bursts_actual": number_of_bursts_actual,
                        "coverage": coverage
                    }
                    if "elasticsearch" == settings["GRQ_ES_ENGINE"]:
                        operation = {
                            "_op_type": "update",
                            "_index": index,
                            "_type": "_doc",
                            "_id": doc["id"],
                            "doc_as_upsert": True,
                            "doc": op_doc
                        }
                    elif "opensearch" == settings["GRQ_ES_ENGINE"]:
                        operation = {
                            "_op_type": "update",
                            "_index": index,
                            # "_type": "_doc",
                            "_id": doc["id"],
                            "doc_as_upsert": True,
                            "doc": op_doc,
                            # "update": op_doc
                        }
                    operations.append(operation)

        self.logger.info(f"Marking {set(batch_id_to_products_map.keys())} products as download job-submitted, in bulk")

        if "elasticsearch" == settings["GRQ_ES_ENGINE"]:
            elasticsearch.helpers.bulk(self.es_util.es, operations)
        if "opensearch" == settings["GRQ_ES_ENGINE"]:
            opensearchpy.helpers.bulk(self.es_util.es, operations)

        self.logger.debug("Performing index refresh")
        self.refresh()

    def raw_create_doc_id_to_index_cache(self, docs):
        body = get_body(match_all=False)
        body["_source"] = {"includes": [], "excludes": []}
        es_docs = []

        # Batch requests for larger number of docs
        # see Elasticsearch documentation  regarding "indices.query.bool.max_clause_count". Minimum is 1024
        for doc_chunk in chunked(docs, 1024):
            for doc in doc_chunk:
                body["query"]["bool"]["should"].append({"match": {"id.keyword": doc["id"]}})

            es_docs.extend(self.es_util.query(body=body, index=self.ES_INDEX_PATTERNS))
            body["query"]["bool"]["should"] = []

        id_to_index_cache = defaultdict(set)

        for es_doc in es_docs:
            id_to_index_cache[es_doc["_id"]].add(es_doc["_index"])

        return id_to_index_cache

    def mark_products_as_job_submitted(self, batch_id_to_products_map: dict):
        operations = []
        dswx_s1_job_dts = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="seconds")

        for batch_id, products in batch_id_to_products_map.items():
            docs = products
            doc_id_to_index_cache = self.create_doc_id_to_index_cache(docs)
            latest_production_datetime = max(docs, key=lambda doc: doc["production_datetime"])["production_datetime"]
            latest_creation_timestamp = max(docs, key=lambda doc: doc["creation_timestamp"])["creation_timestamp"]

            for doc in docs:
                index = last(
                    doc_id_to_index_cache[doc["id"]],
                    self._get_index_name_for(_id=doc["id"], default=self.generate_es_index_name())
                )
                op_doc = {
                    "dswx_s1_jobs_ids": doc["dswx_s1_jobs_ids"],
                    "latest_dswx_s1_job_ts": dswx_s1_job_dts,
                    "latest_production_datetime": latest_production_datetime,
                    "latest_creation_timestamp": latest_creation_timestamp
                }
                if "elasticsearch" == settings["GRQ_ES_ENGINE"]:
                    operation = {
                        "_op_type": "update",
                        "_index": index,
                        "_type": "_doc",
                        "_id": doc["id"],
                        "doc_as_upsert": True,
                        "doc": op_doc
                    }
                elif "opensearch" == settings["GRQ_ES_ENGINE"]:
                    operation = {
                        "_op_type": "update",
                        "_index": index,
                        # "_type": "_doc",
                        "_id": doc["id"],
                        "doc_as_upsert": True,
                        "doc": op_doc,
                        # "update": op_doc
                    }
                operations.append(operation)

        self.logger.info(f"Marking {set(batch_id_to_products_map.keys())} products as job-submitted, in bulk")
        if "elasticsearch" == settings["GRQ_ES_ENGINE"]:
            elasticsearch.helpers.bulk(self.es_util.es, operations)
        if "opensearch" == settings["GRQ_ES_ENGINE"]:
            opensearchpy.helpers.bulk(self.es_util.es, operations)

        self.logger.debug("Performing index refresh")
        self.refresh()

    def create_doc_id_to_index_cache(self, docs):
        body = get_body(match_all=False)
        body["_source"] = {"includes": [], "excludes": []}
        es_docs = []

        # Batch requests for larger number of docs
        # see Elasticsearch documentation  regarding "indices.query.bool.max_clause_count". Minimum is 1024
        for doc_chunk in chunked(docs, 1024):
            for doc in doc_chunk:
                body["query"]["bool"]["should"].append({"match": {"id.keyword": doc["id"]}})

            es_docs.extend(self.es_util.query(body=body, index=self.ES_INDEX_PATTERNS))
            body["query"]["bool"]["should"] = []

        id_to_index_cache = defaultdict(set)

        for es_doc in es_docs:
            id_to_index_cache[es_doc["_id"]].add(es_doc["_index"])

        return id_to_index_cache

    def update_granule_index(self, granule: dict, job_id: str, query_dt: datetime,
                             mgrs_set_id_acquisition_ts_cycle_indexes: list[str],
                             **kwargs):
        urls = granule.get("filtered_urls")
        granule_id = granule.get("granule_id")
        temporal_extent_beginning_dt: datetime = parse_iso_datetime(granule["temporal_extent_beginning_datetime"])
        revision_date_dt: datetime = parse_iso_datetime(granule["revision_date"])

        for mgrs_set_id_acquisition_ts_cycle_index in mgrs_set_id_acquisition_ts_cycle_indexes:
            doc = {
                "id": f"{granule_id}${mgrs_set_id_acquisition_ts_cycle_index}",
                "granule_id": granule_id,
                "creation_timestamp": datetime.now(timezone.utc),
                "query_job_id": job_id,
                "query_datetime": query_dt,
                "temporal_extent_beginning_datetime": temporal_extent_beginning_dt,
                "revision_date": revision_date_dt,
                "https_urls": [url for url in urls if "https://" in url],
                "s3_urls": [url for url in urls if "s3://" in url],
                "mgrs_set_id": mgrs_set_id_acquisition_ts_cycle_index.split("$")[0],
                "mgrs_set_id_acquisition_ts_cycle_index": mgrs_set_id_acquisition_ts_cycle_index,
                "production_datetime": granule["production_datetime"]
            }
            doc.update(kwargs)
            index = self._get_index_name_for(_id=doc['id'], default=self.generate_es_index_name())

            body = {
                "doc_as_upsert": True,
                "doc": doc
            }

            self.es_util.update_document(index=index, body=body, id=doc['id'])
