
from datetime import datetime

from data_subscriber.catalog import ProductCatalog

class KCSLCProductCatalog(ProductCatalog):
    """Cataloging class for downloaded Coregistered Single Look Complex (CSLC) products used for K-satiety purposes."""
    NAME = "k_cslc_catalog"
    ES_INDEX_PATTERNS = "k_cslc_catalog*"

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

    def get_download_granule_revision(self, granule_id: str):
        downloads = self.es_util.query(
            index=self.ES_INDEX_PATTERNS,
            body={
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"download_batch_id": granule_id}}
                        ]
                    }
                }
            }
        )

        return self.process_query_result(downloads)
class CSLCProductCatalog(KCSLCProductCatalog):
    """Cataloging class for downloaded Coregistered Single Look Complex (CSLC) products."""
    NAME = "cslc_catalog"
    ES_INDEX_PATTERNS = "cslc_catalog*"

    def get_unsubmitted_granules(self, processing_mode="forward"):
        """Returns all unsubmitted granules, should be in forward processing mode only"""
        downloads = self.es_util.query(
            index=self.ES_INDEX_PATTERNS,
            body={
                "query": {
                    "bool": {
                        "must_not": [
                            {"exists": {"field": "download_job_id"}}
                        ],
                        "must": [
                            {"term": {"processing_mode": processing_mode}}
                        ]
                    }
                }
            }
        )

        # Convert acquisition_ts to time object for convenience
        for download in downloads:
            download["_source"]["acquisition_ts"] = datetime.strptime(download["_source"]["acquisition_ts"], "%Y-%m-%dT%H:%M:%S")

        return self.process_query_result(downloads)

    def get_submitted_granules(self, download_batch_id: str):
        """Returns all records that match the download_batch_id that also have the download_job_id"""
        downloads = self.es_util.query(
            index=self.ES_INDEX_PATTERNS,
            body={
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"download_batch_id": download_batch_id}},
                            {"exists": {"field": "download_job_id"}}
                        ]
                    }
                }
            }
        )

        return self.process_query_result(downloads)

    def get_k_and_m(self, granule_id: str):
        one_doc = self.es_util.query(
            index=self.ES_INDEX_PATTERNS,
            body={
                "size": 1,
                "query": {
                    "bool": {
                        "must": [
                            {"term": {"download_batch_id": granule_id}}
                        ]
                    }
                }
            }
        )
        k = int(one_doc[0]["_source"]["k"])
        m = int(one_doc[0]["_source"]["m"])

        return k, m

    def mark_product_as_downloaded(self, url, job_id, filesize=None, doc=None):
        if not doc:
            doc = {}

        #TODO: Also want fields like these:
        # "number_of_bursts_expected": number_of_bursts_expected,
        # "number_of_bursts_actual": number_of_bursts_actual,
        doc["latest_download_job_ts"] = datetime.now().isoformat(timespec="seconds").replace("+00:00", "Z")

        super().mark_product_as_downloaded(url, job_id, filesize, doc)


class CSLCStaticProductCatalog(ProductCatalog):
    """Cataloging class for downloaded CSLC Static Layer Products."""
    NAME = "cslc_static_catalog"
    ES_INDEX_PATTERNS = "cslc_static_catalog*"

    def process_query_result(self, query_result):
        return [result['_source'] for result in (query_result or [])]

    def granule_and_revision(self, es_id):
        return es_id.split('-r')[0], es_id.split('-r')[1]
