
from datetime import datetime

from data_subscriber.catalog import ProductCatalog


class GSLCProductCatalog(ProductCatalog):
    """Cataloging class for NISAR GSLC granules queried for DISP-NI processing."""
    NAME = "gslc_catalog"
    ES_INDEX_PATTERNS = "gslc_catalog*"

    def process_query_result(self, query_result: list[dict]):
        return [result['_source'] for result in (query_result or [])]

    def granule_and_revision(self, es_id: str):
        raise NotImplementedError()

    def form_document(self, filename: str, granule: dict, job_id: str, query_dt: datetime,
                      temporal_extent_beginning_dt: datetime, revision_date_dt: datetime, revision_id):
        return {
            "id": granule["unique_id"],
            "granule_id": granule["granule_id"],
            "creation_timestamp": datetime.now(),
            "query_job_id": job_id,
            "query_datetime": query_dt,
            "temporal_extent_beginning_datetime": temporal_extent_beginning_dt,
            "revision_date": revision_date_dt
        }

    def get_download_granule_revision(self, download_batch_id: str):
        downloads = self.es_util.query(
            index=self.ES_INDEX_PATTERNS,
            body={
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"download_batch_id.keyword": download_batch_id}}
                        ]
                    }
                }
            }
        )

        return self.process_query_result(downloads)
