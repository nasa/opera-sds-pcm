from datetime import datetime, timedelta

import dateutil.parser

from data_subscriber.cslc.cslc_catalog import CSLCProductCatalog

class RTCForDistProductCatalog(CSLCProductCatalog):
    """Cataloging class for cataloging RTC products queried from CMR for DIST-S1 production purposes."""
    NAME = "rtc_for_dist_catalog"
    ES_INDEX_PATTERNS = "rtc_for_dist_catalog*"

    def get_download_granule_revision(self, download_batch_id: str):
        # TODO: Not sure why but we need this explicit call instead of relying on inheritance
        return super().get_download_granule_revision(download_batch_id)

    def form_document(self, filename: str, granule: dict, job_id: str, query_dt: datetime,
                      temporal_extent_beginning_dt: datetime, revision_date_dt: datetime, revision_id):

        m = super().form_document(
            filename, granule, job_id, query_dt, temporal_extent_beginning_dt, revision_date_dt, revision_id
        )

        # Add http_urls and s3_urls to the document
        m["filtered_urls"] = granule.get("filtered_urls", [])
        if granule.get("polarization"):
            m["polarization"] = granule["polarization"]

        m["@timestamp"] = datetime.now()  # needed for opensearch

        return m

    def get_unsubmitted_granules(self, processing_mode="forward"):
        """Returns all unsubmitted granules, should be in forward processing mode only"""
        body = {
            "query": {
                "bool": {
                    "must_not": [
                        {"exists": {"field": "download_job_id"}}
                    ],
                    "must": [
                        {"term": {"processing_mode": processing_mode}},

                    ]
                }
            }
        }

        now = datetime.now()
        if processing_mode == "forward":
            body["query"]["bool"]["must"].append({
                "range": {
                    "creation_timestamp": {
                        "gte": (now - timedelta(hours=2)).isoformat(),
                        "lt": now.isoformat()
                    }
                }
            })

        downloads = self.es_util.query(
            index=self.ES_INDEX_PATTERNS,
            body=body
        )
        self.logger.error(f"{len(downloads)=}")

        if processing_mode == "forward":
            downloads = list(filter(lambda d: (now - timedelta(hours=2)) <= dateutil.parser.parse(d["_source"]["creation_timestamp"]) < now, downloads))
            self.logger.info(f"forward mode. limiting unsubmitted granules by recent creation_timestamp. {len(downloads)=}")

        # Convert acquisition_ts to time object for convenience
        for download in downloads:
            download["_source"]["acquisition_ts"] = datetime.strptime(download["_source"]["acquisition_ts"], "%Y-%m-%dT%H:%M:%S")

        return self.process_query_result(downloads)