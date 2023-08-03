from datetime import datetime

from hysds_commons.elasticsearch_utils import ElasticsearchUtility

REFREC_ID = "refrec_id"
ES_INDEX_PATTERNS = ["jobs_accountability_catalog", "jobs_accountability_catalog-*"]
ES_TYPE = "job"
HEADER = "header"


def generate_es_index_name():
    return "jobs_accountability_catalog-{date}".format(date=datetime.utcnow().strftime("%Y.%m.%d.%H%M%S"))  # TODO chrisjrd: update with final suffix


class JobAccountabilityCatalog(ElasticsearchUtility):
    """
    Class to handle ingestion of All products past job-RRST_Accountability contents into ElasticSearch

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

    def post(self, records, header=None):
        """
        Post records into ElasticSearch.

        :param records: A list of Pass Accountability records to ingest.
        :param header: Header information associated with the records.
        If present, they will get included with each record document posted.
        :param index: Specify the index where the documents will get posted to.
        :return:
        """
        for record in records:
            if header:
                record[HEADER] = header
            else:
                record[HEADER] = {}
            if self.logger:
                self.logger.info("record: {}".format(record))
            self.__post_to_es(record, generate_es_index_name())

    def __post_to_es(self, document, index):
        """
        Posts the given document to ElasticSearch.

        :param document: The document to ingest.
        :return:
        """
        _id = document[REFREC_ID]
        result = self.index_document(index=index, id=_id, body=document)
        if self.logger:
            self.logger.info("document indexed: {}".format(result))
