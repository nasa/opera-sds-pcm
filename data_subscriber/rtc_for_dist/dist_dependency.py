from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timedelta

import dateutil

from commons.logger import get_logger
from data_subscriber.cmr import CMR_TIME_FORMAT, DateTimeRange
from data_subscriber.dist_s1_utils import determine_previous_product_download_batch_id

from commons.es_connection import get_grq_es, get_mozart_es

# batch_id looks like this: 32UPD_4_302; download_batch_id looks like this: p32UPD_4_a302

GRQ_ES_DIST_S1_INDEX = "grq_v0.1_l3_dist_s1*"
CMR_RTC_CACHE_INDEX = "cmr_rtc_cache*"

class DistDependency:
    def __init__(self, logger, dist_products, bursts_to_products, product_to_bursts):
        self.logger = logger
        self.mozart_es = get_mozart_es(logger)
        self.grq_es = get_grq_es(logger)
        self.dist_products, self.bursts_to_products, self.product_to_bursts = (
            dist_products, bursts_to_products, product_to_bursts)

    def should_wait_previous_run(self, download_batch_id):
        """
        Check if the current run should wait for the previous run of this tile to complete. Here are the conditions:

         - The previous run for this tile output does not exist AND
         (
           - Download or SCIFLO job for previous run is in one of the following states: queued, running, offline --OR--
           - Download or SCIFLO job for previous run has failed but the retry count is less than 3.
         )

         return:
            - should_wait: True if we should wait for the previous run to complete, False otherwise
            - previous_tile_product_file_paths: list of file paths for the previous tile product, None if no previous tile product was found
            - previous_tile_job_id: job id for the previous tile job, None if no previous tile job was found
        """

        self.logger.info(f"Checking if we should wait for the previous run of {download_batch_id}")
        previous_tile_product, prev_product_download_batch_id = self.get_previous_tile_product(download_batch_id)
        if previous_tile_product is not None:
            # extract the file paths from the previous tile product
            #TODO: FileLocation is probably incorrect. When the PGE team defines this field, fix this.
            file_paths = [file["FileLocation"] for file in previous_tile_product["_source"]["metadata"]["Files"]]
            self.logger.info(f"Previous tile product found: {file_paths=}")
            return False, file_paths, None # Previous tile product exists so run with it.
        
        prev_tile_job = self.find_job_download_batch_id(prev_product_download_batch_id)
        if prev_tile_job is not None:
            self.logger.info(f"Previous tile job found in state {prev_tile_job['_source']['status']}")
            return True, None, prev_tile_job["_source"]["job_id"] # Wait for the job to complete.

        return False, None, None # Give up. Go ahead and run without previous tile product.

    def get_previous_tile_product(self, download_batch_id):
        """ Get the previous tile product record from GRQ ES."""

        tile_id, acquisition_group, acquisition_cycle = download_batch_id.split("_")
        tile_id = tile_id[1:] # Remove the "p" from the tile_id
        prev_product_download_batch_id = determine_previous_product_download_batch_id(self.dist_products, download_batch_id)

        self.logger.info(f"Searching for previous tile product: {prev_product_download_batch_id}")
        result = self.grq_es.search(
            index=GRQ_ES_DIST_S1_INDEX,
            body={
                "query": {
                    "bool": {
                        "must": [
                            {"match": {"metadata.accountability.L3_DIST_S1.trigger_dataset_id.keyword": prev_product_download_batch_id}}
                        ]
                    }
                }
            }
        )
        hits = result["hits"]["hits"]
        if len(hits) == 1:
            return hits[0], None
        elif len(hits) > 1:
            # Choose the one with the latest creation_timestamp
            self.logger.warning(f"Multiple previous tile products found in GRQ ES. Choosing the one with the latest creation_ts.")
            latest_hit = max(hits, key=lambda x: x["_source"]["creation_timestamp"])
            return latest_hit, None

        # If we didn't find the previous product from GRQ, we need to check GRQ cmr_rtc_cache for what the previous product should be
        self.logger.info(f"Previous tile product not found in GRQ ES. Searching GRQ cmr_rtc_cache for what it should be.")

        # Get all burst ids for this batch_id
        all_burst_ids = set()
        product_ids = self.dist_products[tile_id]
        for product_id in product_ids:
            burst_ids = self.product_to_bursts[product_id]
            all_burst_ids.update(burst_ids)
        all_burst_ids = list(all_burst_ids)
        print(f"All burst ids: {all_burst_ids}")

        should_query = []
        for burst_id in all_burst_ids:
            should_query.append({"match": {"burst_id.keyword": burst_id}})

        # Query the cmr_rtc_cache index for the previous product
        result = self.grq_es.search(
            index=CMR_RTC_CACHE_INDEX,
            body={
                "query": {
                    "bool": {
                        "should": should_query
                    }
                }
            }
        )

        hits = result["hits"]["hits"]
        hit_count = 1
        for hit in hits:
            print(f"Hit: {hit_count}: {hit['_id']}")
            hit_count += 1
        print(f"Hit count: {hit_count}")

        return None, prev_product_download_batch_id

    def find_job_download_batch_id(self, download_batch_id):
        """
        Get the previous tile run Mozart job.
        """
        job_id_prefix = "job-WF-SCIFLO_L3_DIST_S1-batch-" + download_batch_id
        self.logger.info(f"Searching for previous tile job with job_id_prefix: {job_id_prefix}")
        query = {"query": {"bool": {"must": [{"prefix": {"job_id": job_id_prefix}}]}}}
        result = self.mozart_es.search(
            index="job_status*",
            body=query
        )

        # The may seem overly verbose and redundant but we want the jobs in this order if there are multiple jobs with the same job_id_prefix
        # It's possible that one job had failed and so another was created, etc.
        hits = result["hits"]["hits"]
        for hit in hits:
            if hit["_source"]["status"] == "job-started":
                return hit
        for hit in hits:
            if hit["_source"]["status"] == "job-queued":
                return hit
        for hit in hits:
            if hit["_source"]["status"] == "job-offline":
                return hit
        for hit in hits:
            if hit["_source"]["status"] == "job-failed":
                if hit["_source"]["retry_count"] < 3:
                    return hit

        return None

