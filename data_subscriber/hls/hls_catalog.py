import logging
from datetime import datetime
from pathlib import Path

from data_subscriber import es_conn_util

null_logger = logging.getLogger('dummy')
null_logger.addHandler(logging.NullHandler())
null_logger.propagate = False

# ES_INDEX = "hls_catalog"  # TODO chrisjrd: replace


def generate_es_index_name():
    return "hls_catalog-{date}".format(date=datetime.utcnow().strftime("%Y.%m.%d.%H%M%S"))


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

    def create_index(self, index="hls_catalog-*", delete_old_index=False):
        # TODO chrisjrd: verify index deletion
        if delete_old_index is True:
            # TODO chrisjrd: commented out because code is potentially unnecessary. remove code before final submission
            # self.es.es.indices.delete(index=index, ignore=404)
            # self.logger.info("Deleted old index: {}".format(index))
            pass

        self.es.es.indices.put_index_template(
            name="hls_catalog_template",
            create=True,
            body={
                "index_patterns": [index],
                "template": {
                    "settings": {
                        "index": {
                            "sort.field": "creation_timestamp",
                            "sort.order": "asc"
                        }
                    },
                    "mappings": {
                        "properties": {
                            "granule_id": {"type": "keyword"},
                            "s3_url": {"type": "keyword"},
                            "https_url": {"type": "keyword"},
                            "creation_timestamp": {"type": "date"},
                            "download_datetime": {"type": "date"},
                            "downloaded": {"type": "boolean"}
                        }
                    }
                }
            }
        )

        self.logger.info("Successfully created index template: {}".format("hls_catalog_template"))

    def delete_index(self):
        # TODO chrisjrd: commented out because code is potentially unnecessary. remove code before final submission
        # index = "hls_catalog-*"
        # # TODO chrisjrd: verify index deletion
        # # TODO chrisjrd: call existing delete_index function
        # self.es.es.indices.delete(index=index, ignore=404)
        # self.logger.info("Successfully deleted index: {}".format(index))
        pass

    def get_all_between(self, start_dt: datetime, end_dt: datetime, use_temporal: bool):
        hls_catalog = self._query_catalog(start_dt, end_dt, use_temporal)
        return [{"s3_url": catalog_entry["_source"].get("s3_url"), "https_url": catalog_entry["_source"].get("https_url")}
                for catalog_entry in (hls_catalog or [])]

    def process_url(
            self,
            urls: list[str],
            granule_id: str,
            job_id: str,
            query_dt: datetime,
            temporal_extent_beginning_dt: datetime,
            revision_date_dt: datetime,
            *args,
            **kwargs
    ):
        filename = Path(urls[0]).name
        doc = {
            "id": filename,
            "granule_id": granule_id,
            "creation_timestamp": datetime.now(),
            "query_job_id": job_id,
            "query_datetime": query_dt,
            "temporal_extent_beginning_datetime": temporal_extent_beginning_dt,
            "revision_date": revision_date_dt
        }

        for url in urls:
            if "https://" in url:
                doc["https_url"] = url

            if "s3://" in url:
                doc["s3_url"] = url

            if "https://" not in url and "s3://" not in url:
                raise Exception(f"Unrecognized URL format. {url=}")

        doc.update(kwargs)

        # TODO chrisjrd: fix update
        results = self._query_existence(filename)
        self.logger.info(f"{results=}")
        if not results:  # EDGECASE: index doesn't exist yet
            index = generate_es_index_name()
        else:  # reprocessed or revised product. assume reprocessed. update existing record
            if results:  # found results
                index = results[0]["_id"]  # get the ID of the most recent record
            else:
                index = generate_es_index_name()

        # TODO chrisjrd: use ID of existing record, when possible
        self.es.update_document(index=index, body={"doc_as_upsert": True, "doc": doc}, id=filename)
        return True

    def mark_product_as_downloaded(self, url, job_id):
        filename = url.split("/")[-1]

        # TODO chrisjrd: fix update
        results = self._query_existence(filename)
        self.logger.info(f"{results=}")
        if not results:  # EDGECASE: index doesn't exist yet
            index = generate_es_index_name()
        else:  # reprocessed or revised product. assume reprocessed. update existing record
            if results:  # found results
                index = results[0]["_id"]  # get the ID of the most recent record
            else:
                index = generate_es_index_name()

        result = self.es.update_document(
            id=filename,
            body={
                "doc_as_upsert": True,
                "doc": {
                    "downloaded": True,
                    "download_datetime": datetime.now(),
                    "download_job_id": job_id,
                }
            },
            index=index  # TODO chrisjrd: find out if we can accurately update an existing record regardless of index date
        )

        self.logger.info(f"Document updated: {result}")

    def _post(self, filename, body):
        result = self.es.index_document(index=generate_es_index_name(), body=body, id=filename)

        self.logger.info(f"Document indexed: {result}")

    def _query_existence(self, filename, index="hls_catalog-*"):
        try:
            results = self.es.query(
                index=index,
                body={
                    "query": {"bool": {"must": [{"term": {"_id": filename}}]}},
                    "sort": [{"creation_timestamp": "desc"}],
                    "_source": {"includes": "false", "excludes": []}
                }
            )
            self.logger.debug(f"Query results: {results}")

        except:
            results = None
            self.logger.debug(f"{filename} does not exist in {index}")

        return results

    def _query_catalog(self, start_dt: datetime, end_dt: datetime, use_temporal: bool, index="hls_catalog-*"):
        range_str = "temporal_extent_beginning_datetime" if use_temporal else "revision_date"
        try:
            result = self.es.query(index=index,
                                body={"sort": [{"creation_timestamp": "asc"}],
                                      "query": {"bool": {"must": [{"range": {range_str: {
                                                                      "gte": start_dt.isoformat(),
                                                                      "lt": end_dt.isoformat()}}}]}}})
            self.logger.debug(f"Query result: {result}")

        except:
            result = None

        return result
