from datetime import datetime
from pathlib import Path

from data_subscriber import es_conn_util
from data_subscriber.url import form_batch_id

class HLSProductCatalog:
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
        self.logger = logger
        self.es = es_conn_util.get_es_connection(logger)
        self.ES_INDEX = "hls_catalog"

    def create_index(self, delete_old_index=False):
        if delete_old_index is True:
            self.es.es.indices.delete(index=index, ignore=404)
            if self.logger:
                self.logger.info("Deleted old index: {}".format(index))

        self.es.es.indices.create(body={"settings": {"index": {"sort.field": "creation_timestamp", "sort.order": "asc"}},
                                     "mappings": {
                                         "properties": {
                                             "granule_id": {"type": "keyword"},
                                             "revision_id": {"type": "integer"},
                                             "s3_url": {"type": "keyword"},
                                             "https_url": {"type": "keyword"},
                                             "creation_timestamp": {"type": "date"},
                                             "download_datetime": {"type": "date"},
                                             "downloaded": {"type": "boolean"}}}},
                               index=self.ES_INDEX)
        if self.logger:
            self.logger.info("Successfully created index: {}".format(self.ES_INDEX))

    def delete_index(self):
        self.es.es.indices.delete(index=self.ES_INDEX, ignore=404)
        if self.logger:
            self.logger.info("Successfully deleted index: {}".format(self.ES_INDEX))

    def get_all_between(self, start_dt: datetime, end_dt: datetime, use_temporal: bool):
        hls_catalog = self._query_catalog(start_dt, end_dt, use_temporal)
        return [{"_id": catalog_entry["_id"], "granule_id": catalog_entry["_source"].get("granule_id"),
                 "revision_id": catalog_entry["_source"].get("revision_id"),
                 "s3_url": catalog_entry["_source"].get("s3_url"),
                 "https_url": catalog_entry["_source"].get("https_url")}
                for catalog_entry in (hls_catalog or [])]

    @staticmethod
    def _create_doc(
            url: str,
            granule_id: str,
            job_id: str,
            query_dt: datetime,
            temporal_extent_beginning_dt: datetime,
            revision_date_dt: datetime,
            *args,
            **kwargs
    ):
        filename = Path(url).name

        # We're not doing anything w the query result so comment it out to save resources
        # result = self._query_existence(filename)

        doc = {
            "id": form_batch_id(filename, kwargs['revision_id']),
            "granule_id": granule_id,
            "creation_timestamp": datetime.now(),
            "query_job_id": job_id,
            "query_datetime": query_dt,
            "temporal_extent_beginning_datetime": temporal_extent_beginning_dt,
            "revision_date": revision_date_dt
        }

        if "https://" in url:
            doc["https_url"] = url
        elif "s3://" in url:
            doc["s3_url"] = url
        else:
            raise Exception(f"Unrecognized URL format. {url=}")

        doc.update(kwargs)

        return doc

    def process_url(
            self,
            url: str,
            granule_id: str,
            job_id: str,
            query_dt: datetime,
            temporal_extent_beginning_dt: datetime,
            revision_date_dt: datetime,
            *args,
            **kwargs
    ):
        doc = HLSProductCatalog._create_doc(url, granule_id, job_id, query_dt, temporal_extent_beginning_dt, revision_date_dt, *args,
                            **kwargs)

        self.es.update_document(index=self.ES_INDEX, body={"doc_as_upsert": True, "doc": doc}, id=doc['id'])
        return True

    def product_is_downloaded(self, url):
        filename = url.split("/")[-1]
        result = self._query_existence(filename)

        if result:
            return result["_source"]["downloaded"]
        else:
            return False

    def mark_product_as_downloaded(self, id, job_id):
        result = self.es.update_document(
            id=id,
            body={
                "doc_as_upsert": True,
                "doc": {
                    "downloaded": True,
                    "download_datetime": datetime.now(),
                    "download_job_id": job_id,
                }
            },
            index=self.ES_INDEX
        )

        if self.logger:
            self.logger.info(f"Document updated: {result}")

    def _post(self, filename, body):
        result = self.es.index_document(body=body, id=filename)

        if self.logger:
            self.logger.info(f"Document indexed: {result}")

    def _query_existence(self, filename):
        try:
            result = self.es.get_by_id(index=index, id=filename)
            if self.logger:
                self.logger.debug(f"Query result: {result}")

        except:
            result = None
            if self.logger:
                self.logger.debug(f"{filename} does not exist in {index}")

        return result

    def _query_catalog(self, start_dt: datetime, end_dt: datetime, use_temporal: bool):
        range_str = "temporal_extent_beginning_datetime" if use_temporal else "revision_date"
        try:
            result = self.es.query(index=self.ES_INDEX,
                                body={"sort": [{"creation_timestamp": "asc"}],
                                      "query": {"bool": {"must": [{"range": {range_str: {
                                                                      "gte": start_dt.isoformat(),
                                                                      "lt": end_dt.isoformat()}}}]}}})
            if self.logger:
                self.logger.debug(f"Query result: {result}")

        except:
            result = None

        return result
