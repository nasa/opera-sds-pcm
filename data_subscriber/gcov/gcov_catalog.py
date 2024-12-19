
from data_subscriber.catalog import ProductCatalog

class NisarGcovProductCatalog(ProductCatalog):
    """Cataloging class for downloaded NISAR GCOV Products."""
    NAME = "nisar_gcov_catalog"
    ES_INDEX_PATTERNS = "nisar_gcov_catalog*"

    def process_query_result(self, query_result):
        return [result['_source'] for result in (query_result or [])]

    def granule_and_revision(self, es_id):
        return es_id.split('-r')[0], es_id.split('-r')[1]
