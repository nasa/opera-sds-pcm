from datetime import datetime
from pathlib import Path

from hysds_commons.elasticsearch_utils import ElasticsearchUtility

ES_INDEX = "slc_catalog"


class SLCProductCatalog(ElasticsearchUtility):
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

    def create_index(self, index=ES_INDEX, delete_old_index=False):
        if delete_old_index is True:
            self.es.indices.delete(index=index, ignore=404)
            if self.logger:
                self.logger.info("Deleted old index: {}".format(index))

        self.es.indices.create(body={"settings": {"index": {"sort.field": "creation_timestamp", "sort.order": "asc"}},
                                     "mappings": {
                                         "properties": {
                                             "granule_id": {"type": "keyword"},
                                             # "s3_url": {"type": "keyword"},
                                             "https_url": {"type": "keyword"},
                                             "creation_timestamp": {"type": "date"},
                                             "download_datetime": {"type": "date"},
                                             "downloaded": {"type": "boolean"}}}},
                               index=ES_INDEX)
        if self.logger:
            self.logger.info("Successfully created index: {}".format(ES_INDEX))

    def delete_index(self):
        self.es.indices.delete(index=ES_INDEX, ignore=404)
        if self.logger:
            self.logger.info("Successfully deleted index: {}".format(ES_INDEX))

    def get_all_between(self, start_dt: datetime, end_dt: datetime, use_temporal: bool):
        undownloaded = self._query_undownloaded(start_dt, end_dt, use_temporal)

        return [{  # "s3_url": result['_source']['s3_url'],
            "https_url": result['_source'].get('https_url')}
            for result in (undownloaded or [])]

    def process_url(
            self,
            url: str,
            granule_id: str,
            job_id: str,
            query_dt: datetime,
            temporal_extent_beginning_dt: datetime,
            revision_date_dt: datetime
    ):
        filename = Path(url).name
        result = self._query_existence(filename)
        doc = {
            "id": filename,
            "granule_id": granule_id,
            "creation_timestamp": datetime.now(),
            "query_job_id": job_id,
            "query_datetime": query_dt,
            "temporal_extent_beginning_datetime": temporal_extent_beginning_dt,
            "revision_date": revision_date_dt
        }

        if "https://" in url:
            doc["https_url"] = url
        # elif "s3://" in url:
        # doc["s3_url"] = url
        else:
            raise Exception(f"Unrecognized URL format. {url=}")

        self.update_document(index=ES_INDEX, body={"doc_as_upsert": True, "doc": doc}, id=filename)
        return True

    def product_is_downloaded(self, url):
        filename = url.split('/')[-1]
        result = self._query_existence(filename)

        if result:
            return result["_source"]["downloaded"]
        else:
            return False

    def mark_product_as_downloaded(self, url, job_id):
        filename = url.split('/')[-1]
        result = self.update_document(id=filename,
                                      body={"doc_as_upsert": True,
                                            "doc": {"downloaded": True,
                                                    "download_datetime": datetime.now(),
                                                    "download_job_id": job_id, }},
                                      index=ES_INDEX)

        if self.logger:
            self.logger.info(f"Document updated: {result}")

    def _post(self, filename, body):
        result = self.index_document(index=ES_INDEX, body=body, id=filename)

        if self.logger:
            self.logger.info(f"Document indexed: {result}")

    def _query_existence(self, filename, index=ES_INDEX):
        try:
            result = self.get_by_id(index=index, id=filename)
            if self.logger:
                self.logger.debug(f"Query result: {result}")

        except:
            result = None
            if self.logger:
                self.logger.debug(f"{filename} does not exist in {index}")

        return result

    def _query_undownloaded(self, start_dt: datetime, end_dt: datetime, use_temporal: bool, index=ES_INDEX):
        range_str = "temporal_extent_beginning_datetime" if use_temporal else "revision_date"
        try:
            result = self.query(index=index,
                                body={"sort": [{"creation_timestamp": "asc"}],
                                      "query": {"bool": {"must": [{"range": {range_str: {
                                                                      "gte": start_dt.isoformat(),
                                                                      "lt": end_dt.isoformat()}}}]}}})
            if self.logger:
                self.logger.debug(f"Query result: {result}")

        except:
            result = None

        return result
