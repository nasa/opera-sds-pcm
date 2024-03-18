import logging
from datetime import datetime
from pathlib import Path

import elasticsearch
from hysds_commons.elasticsearch_utils import ElasticsearchUtility

from data_subscriber import es_conn_util
from data_subscriber.url import form_batch_id

null_logger = logging.getLogger('dummy')
null_logger.addHandler(logging.NullHandler())
null_logger.propagate = False


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
        self.logger = logger or null_logger
        self.es = es_conn_util.get_es_connection(logger)
        self.ES_INDEX_PATTERNS = "hls_catalog*"

    def generate_es_index_name(self):
        return "hls_catalog-{date}".format(date=datetime.utcnow().strftime("%Y.%m"))

    def filter_query_result(self, query_result):
        return [
            {
                "_id": catalog_entry["_id"],
                "granule_id": catalog_entry["_source"].get("granule_id"),
                "revision_id": catalog_entry["_source"].get("revision_id"),
                "s3_url": catalog_entry["_source"].get("s3_url"),
                "https_url": catalog_entry["_source"].get("https_url")
            }
            for catalog_entry in (query_result or [])
        ]

    def granule_and_revision(self, es_id):
        '''For HLS.S30.T56MPU.2022152T000741.v2.0-r1 returns:
        HLS.S30.T56MPU.2022152T000741.v2.0 and 1 '''
        return es_id.split('-')[0], es_id.split('-r')[1]

    def get_download_granule_revision(self, id):
        granule, revision = self.granule_and_revision(id)
        downloads = self.es.query(
            index=self.ES_INDEX_PATTERNS,
            body={
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"granule_id": granule}},
                            {"term": {"revision_id": revision}}
                        ]
                    }
                }
            }
        )
        return self.filter_query_result(downloads)

    def get_all_between(self, start_dt: datetime, end_dt: datetime, use_temporal: bool):
        downloads = self._query_catalog(start_dt, end_dt, use_temporal)
        return self.filter_query_result(downloads)

    def process_url(
            self,
            urls: list[str],
            granule,
            job_id: str,
            query_dt: datetime,
            temporal_extent_beginning_dt: datetime,
            revision_date_dt: datetime,
            *args,
            **kwargs
    ):
        filename = Path(urls[0]).name

        doc = self.form_document(
            filename=filename,
            granule=granule,
            job_id=job_id,
            query_dt=query_dt,
            temporal_extent_beginning_dt=temporal_extent_beginning_dt,
            revision_date_dt=revision_date_dt,
            revision_id=kwargs['revision_id']
        )

        for url in urls:
            if "https://" in url:
                doc["https_url"] = url

            if "s3://" in url:
                doc["s3_url"] = url

            if "https://" not in url and "s3://" not in url:
                raise Exception(f"Unrecognized URL format. {url=}")

        doc.update(kwargs)

        index = self._get_index_name_for(_id=doc['id'], default=self.generate_es_index_name())

        self.es.update_document(index=index, body={"doc_as_upsert": True, "doc": doc}, id=doc['id'])
        return True

    def form_document(self, filename: str, granule: dict, job_id: str, query_dt: datetime,
                      temporal_extent_beginning_dt: datetime, revision_date_dt: datetime, revision_id):
        return {
            "id": form_batch_id(filename, revision_id),
            "granule_id": granule["granule_id"],
            "creation_timestamp": datetime.now(),
            "query_job_id": job_id,
            "query_datetime": query_dt,
            "temporal_extent_beginning_datetime": temporal_extent_beginning_dt,
            "revision_date": revision_date_dt
        }

    def mark_product_as_downloaded(self, url, job_id, filesize = None):
        filename = url.split("/")[-1]

        doc = {}
        doc["downloaded"] = True
        doc["download_datetime"] = datetime.now()
        doc["download_job_id"] = job_id
        if filesize:
            doc["metadata"] = {"FileSize": filesize}

        index = self._get_index_name_for(_id=filename, default=self.generate_es_index_name())
        result = self.es.update_document(
            id=filename,
            body={
                "doc_as_upsert": True,
                "doc": doc
            },
            index=index
        )

        self.logger.info(f"Document updated: {result}")

    def mark_download_job_id(self, batch_id, job_id):
        "Stores the download_job_id in the catalog for all granules in this batch"

        result = self.es.es.update_by_query(
            index=self.ES_INDEX_PATTERNS,
            body={
                "script": {
                    "source": f"ctx._source.download_job_id = '{job_id}'",
                    "lang": "painless"
                },
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"download_batch_id": batch_id}}
                        ]
                    }
                }
            }
        )

        self.logger.info(f"Document updated: {result}")

    def _get_index_name_for(self, _id, default=None):
        """Gets the index name for the most recent ES doc matching the given _id"""
        if default is None:
            raise

        results = self._query_existence(_id)
        self.logger.debug(f"{results=}")
        if not results:  # EDGECASE: index doesn't exist yet
            index = default
        else:  # reprocessed or revised product. assume reprocessed. update existing record
            if results:  # found results
                index = results[0]["_index"]  # get the ID of the most recent record
            else:
                index = default
        return index

    def _post(self, filename, body):
        result = self.es.index_document(index=self.generate_es_index_name(), body=body, id=filename)
        self.logger.info(f"Document indexed: {result}")

    def _query_existence(self, _id):
        try:
            results = self.es.query(
                index=self.ES_INDEX_PATTERNS,
                #ignore_unavailable=True,  # EDGECASE: index might not exist yet
                body={
                    "query": {"bool": {"must": [{"term": {"_id": _id}}]}},
                    "sort": [{"creation_timestamp": "desc"}],
                    "_source": {"includes": "false", "excludes": []}  # NOTE: returned object is different than when `"includes": []` is used
                },
            )
            self.logger.debug(f"Query results: {results}")

        except:
            self.logger.info(f"{_id} does not exist in {self.ES_INDEX_PATTERNS}")
            results = None

        return results

    def _query_catalog(self, start_dt: datetime, end_dt: datetime, use_temporal: bool):
        fieldname_for_range_filter = "temporal_extent_beginning_datetime" if use_temporal else "revision_date"
        try:
            result = self.es.query(
                index=self.ES_INDEX_PATTERNS,
                #ignore_unavailable=True,  # EDGECASE: index might not exist yet
                body={
                    "sort": [{"creation_timestamp": "asc"}],
                    "query": {
                        "bool": {
                            "must": [
                                {
                                    "range": {
                                        fieldname_for_range_filter: {
                                            "gte": start_dt.isoformat(),
                                            "lt": end_dt.isoformat()
                                        }
                                    }
                                }
                            ]
                        }
                    }
                }
            )
            self.logger.debug(f"Query result: {result}")

        except Exception as e:
            self.logger.error(f"Query Error: {e}")
            result = None

        return result

    def refresh(self):
        """
        Refresh the underlying indices, making recent operations visible to queries.
        See official Elasticsearch documentation on index refreshing.
        """
        es: ElasticsearchUtility = self.es
        es: elasticsearch.Elasticsearch = es.es
        indices_client = es.indices
        indices_client.refresh(index=self.ES_INDEX_PATTERNS)
