
import logging
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from pathlib import Path

import elasticsearch
import backoff
from opensearchpy.helpers import bulk as bulk_helper

from data_subscriber import es_conn_util
from data_subscriber.url import form_batch_id

null_logger = logging.getLogger('dummy')
null_logger.addHandler(logging.NullHandler())
null_logger.propagate = False


class BulkCatalog:
    def __init__(self, logger):
        self._logger = logger
        self._es = es_conn_util.get_es_connection(logger).es
        self._operations = []
        self._complete = False

    def add(self, doc, doc_id, index, op_type='update', doc_as_upsert=True):
        self._operations.append({
            '_op_type': op_type,
            '_index': index,
            '_id': doc_id,
            'doc_as_upsert': doc_as_upsert,
            'doc': doc
        })

        if len(self._operations) % 1000 == 0:
            # Log something occasionally so we know the job is still running
            self._logger.info(f'Added operation {len(self._operations)} to bulk action')

        self._complete = False

    def commit(self, refresh=False):
        if self._complete:
            raise RuntimeError('Bulk operations already completed')

        self._logger.info(f'Performing {len(self._operations):,} bulk operations')
        start_t = datetime.now()
        # TODO: Should there be error checking here?
        bulk_helper(self._es, self._operations)
        self._logger.info(f'Completed {len(self._operations):,} bulk operations in {datetime.now() - start_t}')

        if refresh:
            indices = set([o['_index'] for o in self._operations])

            for index in indices:
                self._logger.info(f'Refreshing index {index}')
                self._es.indices.refresh(index=index, allow_no_indices=True)

        self._complete = True
        self._operations.clear()


class ProductCatalog(ABC):
    """Abstract base class to track products downloaded by daac_data_subscriber.py"""

    # The following constants should be overwritten by inheritors at their class level
    ES_INDEX_PATTERNS = None
    NAME = None
    BATCH_ID_KEYWORD = 'download_batch_id'

    def __init__(self, logger=None):
        self.logger = logger or null_logger
        self.es_util = es_conn_util.get_es_connection(logger)

    def _get_index_name_for(self, _id: str, default: str):
        """Gets the index name for the most recent ES doc matching the given _id"""
        results = self._query_existence(_id)

        if not results:  # EDGECASE: index doesn't exist yet
            index = default
        else:  # reprocessed or revised product. assume reprocessed. update existing record
            if results:  # found results
                index = results[0]["_index"]  # get the ID of the most recent record
            else:
                index = default

        return index

    def _query_existence(self, _id: str):
        results = None

        try:
            results = self.es_util.query(
                index=self.ES_INDEX_PATTERNS,
                body={
                    "query": {"bool": {"must": [{"term": {"_id": _id}}]}},
                    "sort": [{"creation_timestamp": "desc"}],
                    "_source": {"includes": "false", "excludes": []}
                    # NOTE: returned object is different then when `"includes": []` is used
                },
            )
            self.logger.debug(f"Query results: {results}")
        except Exception:
            self.logger.info(f"{_id} does not exist in {self.ES_INDEX_PATTERNS}")

        return results

    @abstractmethod
    def process_query_result(self, query_result: list[dict]):
        pass

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

    def generate_es_index_name(self):
        """Generates the elasticsearch index name for the current product catalog"""
        return "{name}-{date}".format(name=self.NAME, date=datetime.now(timezone.utc).replace(tzinfo=None).strftime("%Y.%m"))

    def get_all_between(self, start_dt: datetime, end_dt: datetime, use_temporal: bool):
        results = []

        fieldname_for_range_filter = "temporal_extent_beginning_datetime" if use_temporal else "revision_date"

        try:
            results = self.es_util.query(
                index=self.ES_INDEX_PATTERNS,
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
            self.logger.debug(f"get_all_between query result: {results}")
        except Exception as err:
            self.logger.error(f"get_all_between query Error: {err}")

        return self.process_query_result(results)

    def get_download_granule_revision(self, granule_id: str):
        granule, revision = self.granule_and_revision(granule_id)

        downloads = self.es_util.query(
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

        return self.process_query_result(downloads)

    @abstractmethod
    def granule_and_revision(self, es_id: str):
        pass

    @backoff.on_exception(backoff.expo, exception=Exception, max_tries=3, factor=10, jitter=None)
    def mark_download_job_id(self, batch_id, job_id):
        """Stores the download_job_id in the catalog for all granules in this batch"""

        result = self.es_util.es.update_by_query(
            index=self.ES_INDEX_PATTERNS,
            body={
                "script": {
                    "source": f"ctx._source.download_job_id = params['job_id']",
                    "params": {
                        "job_id": str(job_id)
                    },
                    "lang": "painless"
                },
                "query": {
                    "bool": {
                        "must": [
                            {"match": {f"{self.BATCH_ID_KEYWORD}.keyword": batch_id}}
                        ]
                    }
                }
            },
            refresh=True # refresh every time so that we don't run into doc version conflicts
        )

        if result["updated"] == 0:
            self.logger.error(f"No documents updated for {batch_id=} {job_id=}")
        else:
            self.logger.info(f"Document updated: {batch_id=} {job_id=} {result}")

    def mark_product_as_downloaded(self, url, job_id, filesize=None, doc=None):
        filename = url.split("/")[-1]

        if not doc:
            doc = {}

        doc["downloaded"] = True
        doc["download_datetime"]= datetime.now()
        doc["download_job_id"] = job_id

        if filesize:
            doc["metadata"] = {"FileSize": filesize}

        index = self._get_index_name_for(_id=filename, default=self.generate_es_index_name())

        result = self.es_util.update_document(
            id=filename,
            body={
                "doc_as_upsert": True,
                "doc": doc
            },
            index=index
        )

        self.logger.info(f"Document updated: {result}")

    def process_granule(self, granule, bulk: BulkCatalog = None):
        if self._query_existence(granule["granule_id"]):
            self.logger.warning(f'Granule {granule["granule_id"]} already exists in DB. No additional indexing needed.')
            return

        doc = {
            "id": granule["granule_id"],
            "provider": granule["provider"],
            "production_datetime": granule["production_datetime"],
            "provider_date": granule.get("provider_date"),
            "short_name": granule["short_name"],
            "product_id": granule["identifier"],
            "bounding_box": granule["bounding_box"],
            "creation_timestamp": datetime.now()
        }

        index = self.generate_es_index_name()

        if bulk is None:
            result = self.es_util.index_document(index=self.generate_es_index_name(), body=doc, id=granule["granule_id"])

            self.logger.debug(f"Granule {granule['granule_id']} indexed: {result}")
        else:
            bulk.add(doc, granule['granule_id'], index)

    def process_url(self, urls: list[str], granule: dict, job_id: str, query_dt: datetime,
                    temporal_extent_beginning_dt: datetime, revision_date_dt: datetime,
                    filename=None, bulk: BulkCatalog = None, *args, **kwargs):

        if filename is None:
            if len(urls) == 0: # This is the case for CSLCProductCatalog and its children
                filename = granule["unique_id"]
            else:
                filename = Path(urls[0]).name

        doc = self.form_document(
            filename=filename,
            granule=granule,
            job_id=job_id,
            query_dt=query_dt,
            temporal_extent_beginning_dt=temporal_extent_beginning_dt,
            revision_date_dt=revision_date_dt,
            revision_id=kwargs.get("revision_id", "1")  # revision_id should not be required according to function signature
        )

        for url in urls:
            if url.startswith("https://"):
                doc["https_url"] = url
            elif url.startswith("s3://"):
                doc["s3_url"] = url
            else:
                raise Exception(f"Unrecognized URL format. {url=}")

        doc.update(kwargs)

        index = self._get_index_name_for(_id=doc['id'], default=self.generate_es_index_name())

        if bulk is None:
            result = self.es_util.update_document(index=index, body={"doc_as_upsert": True, "doc": doc}, id=doc['id'])

            self.logger.debug(f"Document {filename} upserted: {result}")
        else:
            bulk.add(doc, doc['id'], index)

    def refresh(self):
        """
        Refresh the underlying indices, making recent operations visible to queries.
        See official Elasticsearch documentation on index refreshing.
        """
        es: elasticsearch.Elasticsearch = self.es_util.es
        indices_client = es.indices
        indices_client.refresh(index=self.ES_INDEX_PATTERNS)

    def get_cataloged_granule_by_granule_id(self, granule_id):
        query_result = self.es_util.query(
            index=self.ES_INDEX_PATTERNS,
            body={
                "query": {
                    "bool": {
                        "must": [
                            {
                                "term": {
                                    "granule_id": granule_id
                                }
                            }
                        ]
                    }
                }
            }
        )

        self.logger.debug(f'Catalog search result: {query_result}')

        return query_result
