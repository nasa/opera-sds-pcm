from collections import defaultdict
from datetime import datetime
from dataclasses import dataclass, asdict

import elasticsearch.helpers
import opensearchpy
from more_itertools import last, chunked

from data_subscriber.catalog import ProductCatalog
from data_subscriber.gcov_utils import join_mgrs_set_id_and_cycle_number
from util.conf_util import SettingsConf
from util.grq_client import get_body

settings = SettingsConf().cfg


@dataclass
class GcovGranule:
    native_id: str
    granule_id: str
    s3_download_url: str
    track_number: int
    frame_number: int
    cycle_number: int
    mgrs_set_id: str
    mgrs_set_ids: list[str]
    mgrs_set_id_cycle_index: str
    revision_dt: datetime
    acquisition_start_time: datetime

class NisarGcovProductCatalog(ProductCatalog):
    """Cataloging class for NISAR GCOV Products to support DSWx-NI triggering."""
    NAME = "nisar_gcov_catalog"
    ES_INDEX_PATTERNS = "nisar_gcov_catalog*"

    def get_download_granule_revision(self, download_batch_id: str):
        return super().get_download_granule_revision(download_batch_id)

    def update_granule_index(self, granule: GcovGranule, job_id: str, query_dt: datetime):
        """
        Catalog a single GCOV granule in Elasticsearch, using a GcovGranule dataclass instance.
        """
        # Start with the dataclass as dict
        doc = asdict(granule)

        # Add catalog-specific fields
        doc.update({
            "id": self._generate_doc_id_by_gcov_granule(granule),
            "creation_timestamp": datetime.now(),
            "query_job_id": job_id,
            "query_datetime": query_dt,
            "s3_urls": [granule.s3_download_url] if granule.s3_download_url else [],
        })

        index = self._get_index_name_for(_id=doc['id'], default=self.generate_es_index_name())
        body = {
            "doc_as_upsert": True,
            "doc": doc
        }
        self.es_util.update_document(index=index, body=body, id=doc['id'])
        return doc

    def get_gcov_products_from_catalog(self, mgrs_set_id: str, cycle_number: int):
        """
        Query for GCOV products using mgrs_set_id and cycle_number.
        """
        query = self.es_util.query(index=self.ES_INDEX_PATTERNS, body={
            "query": {
                "bool": {
                    "must": [
                        {"term": {"mgrs_set_id_cycle_index.keyword": join_mgrs_set_id_and_cycle_number(mgrs_set_id, cycle_number)}}
                    ]
                }
            }
        })
        return query

    def get_related_gcov_products_from_catalog(self, granule: GcovGranule):
        """
        Query for related GCOV products using mgrs_set_id and cycle_number.
        """
        return self.get_gcov_products_from_catalog(granule.mgrs_set_id, granule.cycle_number)

    def granule_and_revision(self, es_id: str):
        return self.es_util.get_document(index=self.ES_INDEX_PATTERNS, id=es_id)

    def process_query_result(self, query_result: list[dict]):
        return [result['_source'] for result in (query_result or [])]


    def mark_products_as_download_job_submitted(
            self,
            batch_id_to_products_map: dict[str, list[GcovGranule]],
            batch_id_to_job_map: dict[str, str],
            batch_id_to_docs_map: dict[str, dict]
    ):
        operations = []
        for batch_id, product_id_to_products_map in batch_id_to_docs_map.items():
            download_job_dts = datetime.now().isoformat(timespec="seconds").replace("+00:00", "Z")

            for product_id, products in product_id_to_products_map.items():
                docs = products
                doc_id_to_index_cache = self.raw_create_doc_id_to_index_cache(docs)
                for doc in docs:
                    doc = {
                        "id": self._generate_doc_id_by_doc(doc),
                        "download_job_ids": [batch_id_to_job_map[batch_id]]
                    }
                    index = last(doc_id_to_index_cache[doc["id"]],
                        self._get_index_name_for(_id=doc["id"], default=self.generate_es_index_name())
                    )

                    op_doc = {
                        "download_job_ids": doc["download_job_ids"],
                        "latest_download_job_ts": download_job_dts,
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

    def raw_create_doc_id_to_index_cache(self, docs: list[dict]):
        body = get_body(match_all=False)
        body["_source"] = {"includes": [], "excludes": []}
        es_docs = []

        # Batch requests for larger number of docs
        # see Elasticsearch documentation  regarding "indices.query.bool.max_clause_count". Minimum is 1024
        for doc_chunk in chunked(docs, 1024):
            for doc in doc_chunk:
                body["query"]["bool"]["should"].append({"match": {"id.keyword": self._generate_doc_id_by_doc(doc)}})

            es_docs.extend(self.es_util.query(body=body, index=self.ES_INDEX_PATTERNS))
            body["query"]["bool"]["should"] = []

        id_to_index_cache = defaultdict(set)

        for es_doc in es_docs:
            id_to_index_cache[es_doc["_id"]].add(es_doc["_index"])

        return id_to_index_cache

    @staticmethod
    def _generate_doc_id_by_gcov_granule(granule: GcovGranule):
        return granule.native_id + "$" + granule.mgrs_set_id

    @staticmethod
    def _generate_doc_id_by_doc(granule: dict):
        return granule["native_id"] + "$" + granule["mgrs_set_id"]
