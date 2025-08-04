from datetime import datetime
from dataclasses import dataclass, asdict

from data_subscriber.catalog import ProductCatalog

@dataclass
class GcovGranule:
    native_id: str
    granule_id: str
    s3_download_url: str
    track_number: int
    frame_number: int
    cycle_number: int
    mgrs_set_id: str
    revision_dt: datetime
    acquisition_start_time: datetime

class NisarGcovProductCatalog(ProductCatalog):
    """Cataloging class for NISAR GCOV Products to support DSWx-NI triggering."""
    NAME = "nisar_gcov_catalog"
    ES_INDEX_PATTERNS = "nisar_gcov_catalog*"

    def get_download_granule_revision(self, download_batch_id: str):
        return super().get_download_granule_revision(download_batch_id)

    def update_granule_index(self, granule: 'GcovGranule', job_id: str, query_dt: datetime):
        """
        Catalog a single GCOV granule in Elasticsearch, using a GcovGranule dataclass instance.
        """
        # Start with the dataclass as dict
        doc = asdict(granule)
        
        # Add catalog-specific fields
        doc.update({
            "id": granule.native_id,
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

    def get_gcov_products_from_catalog(self, mgrs_set_id: str, cycle_number: int):
        """
        Query for GCOV products using mgrs_set_id and cycle_number.
        """
        query = self.es_util.query(index=self.ES_INDEX_PATTERNS, body={
            "query": {
                "bool": {
                    "must": [
                        {"match": {"mgrs_set_id": mgrs_set_id}},
                        {"match": {"cycle_number": cycle_number}}
                    ]
                }
            }
        })
        return query
    
    def get_related_gcov_products_from_catalog(self, granule: 'GcovGranule'):
        """
        Query for related GCOV products using mgrs_set_id and cycle_number.
        """
        return self.get_gcov_products_from_catalog(granule.mgrs_set_id, granule.cycle_number)

    def granule_and_revision(self, es_id: str):
        return self.es_util.get_document(index=self.ES_INDEX_PATTERNS, id=es_id)

    def process_query_result(self, query_result: list[dict]):
        return [result['_source'] for result in (query_result or [])]