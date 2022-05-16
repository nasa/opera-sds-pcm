from datetime import datetime
from pathlib import Path

from hysds_commons.elasticsearch_utils import ElasticsearchUtility

ES_INDEX = "hls_catalog"


class HLSProductCatalog(ElasticsearchUtility):
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

        self.es.indices.create(body={"settings": {"index": {"sort.field": "index_datetime", "sort.order": "asc"}},
                                     "mappings": {
                                         "properties": {
                                             "granule_id": {"type": "keyword"},
                                             "s3_url": {"type": "keyword"},
                                             "https_url": {"type": "keyword"},
                                             "index_datetime": {"type": "date"},
                                             "download_datetime": {"type": "date"},
                                             "downloaded": {"type": "boolean"}}}},
                               index=ES_INDEX)
        if self.logger:
            self.logger.info("Successfully created index: {}".format(ES_INDEX))

    def delete_index(self):
        self.es.indices.delete(index=ES_INDEX, ignore=404)
        if self.logger:
            self.logger.info("Successfully deleted index: {}".format(ES_INDEX))

    def get_all_undownloaded(self):
        undownloaded = self._query_undownloaded()
        return [
            {
                "s3_url": result['_source']['s3_url'],
                "https_url": result['_source']['https_url']
            } for result in undownloaded
        ]

    def process_url(self, url, granule_id, job_id):
        filename = Path(url).name
        result = self._query_existence(filename)
        doc = {
            "granule_id": granule_id,
            "index_datetime": datetime.now(),
            "query_job_id": job_id,
        }

        if "https://" in url:
            doc["https_url"] = url
        elif "s3://" in url:
            doc["s3_url"] = url
        else:
            raise Exception(f"Unrecognized URL format. {url=}")

        if not result:
            doc["downloaded"] = False
            self._post(filename=filename, body=doc)
            return False
        else:
            self.update_document(index=ES_INDEX, body={"doc": doc}, id=filename)
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
        result = self.update_document(
            id=filename,
            body={
                "doc_as_upsert": True,
                "doc": {
                    "downloaded": True,
                    "download_datetime": datetime.now(),
                    "download_job_id": job_id,
                }
            },
            index=ES_INDEX
        )

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

    def _query_undownloaded(self, index=ES_INDEX):
        try:
            result = self.query(index=index,
                                body={"sort": [{"index_datetime": "asc"}], "query": {"match": {"downloaded": False}}})
            if self.logger:
                self.logger.debug(f"Query result: {result}")

        except:
            result = None

        return result
