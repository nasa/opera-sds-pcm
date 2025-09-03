from collections import defaultdict
from copy import deepcopy
from datetime import datetime, timedelta

from opera_commons.logger import get_logger
from data_subscriber.cmr import CMR_TIME_FORMAT, DateTimeRange
from data_subscriber.dist_s1_utils import (previous_product_download_batch_id_from_rtc, basic_decorate_granule, decorate_granule)
from data_subscriber.es_conn_util import get_document_count, get_document_timestamp_min_max

from opera_commons.es_connection import get_grq_es, get_mozart_es

# batch_id looks like this: 32UPD_4_S1A_302; download_batch_id looks like this: p32UPD_4_S1A_a302

GRQ_ES_DIST_S1_INDEX = "grq_v0.1_l3_dist_s1*"
CMR_RTC_CACHE_INDEX = "cmr_rtc_cache" #TODO: We should use wildcard later after we add year and month to the index name

def file_paths_from_prev_product(previous_tile_product):
    """
    Extract the file paths from the previous tile product.
    The FileLocation field is from the local EC2 which has a lot of non-sense. We return just the immedimate folder and file name.
    """
    file_paths = []
    for file in previous_tile_product["_source"]["metadata"]["Files"]:
        #TODO: This will go away with gamma delivery of the SAS. SAS will filter the files itself.
        if file["FileName"].endswith(".tif"):  #   and not file["FileName"].endswith("ACQ.tif") and not file["FileName"].endswith("METRIC.tif"): # Get rid of the xml and png files and two other files
            file_paths.append(file["FileLocation"].split("/")[-1]+"/"+file["FileName"])
    return file_paths

class DistDependency:
    def __init__(self, logger, dist_products, bursts_to_products, product_to_bursts, settings):
        self.logger = logger
        self.mozart_es = get_mozart_es(logger)
        self.grq_es = get_grq_es(logger)
        self.dist_products, self.bursts_to_products, self.product_to_bursts = (
            dist_products, bursts_to_products, product_to_bursts)
        self.settings = settings

        self.min_cmr_rtc_cache_document_count = settings["DIST_S1_TRIGGERING"]["MIN_CMR_RTC_CACHE_DOCUMENT_COUNT"]
        self.warn_cmr_rtc_cache_document_count = settings["DIST_S1_TRIGGERING"]["WARN_CMR_RTC_CACHE_DOCUMENT_COUNT"]
        self.min_cmr_rtc_cache_document_date_range_days = settings["DIST_S1_TRIGGERING"]["MIN_CMR_RTC_CACHE_DOCUMENT_DATE_RANGE_DAYS"]
        self.warn_cmr_rtc_cache_document_date_range_days = settings["DIST_S1_TRIGGERING"]["WARN_CMR_RTC_CACHE_DOCUMENT_DATE_RANGE_DAYS"]
    
    def should_wait_previous_run(self, download_batch_id, acquisition_ts):
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

        
        self.logger.info(f"Checking if we should wait for the previous run for {download_batch_id=} with {acquisition_ts=}")
        previous_tile_product, prev_product_download_batch_id = self.get_previous_tile_product(download_batch_id, acquisition_ts)
        if previous_tile_product is not None:
            file_paths = file_paths_from_prev_product(previous_tile_product)
            self.logger.debug(f"Previous tile product found: {file_paths=}")
            return False, file_paths, None # Previous tile product exists so run with it.
        
        self.logger.info(f"No previous tile product was found and cannot determine what the previous product should be. \
Run without previous tile product.")
        if prev_product_download_batch_id is None:
            return False, None, None
        
        prev_tile_job = self.find_job_download_batch_id(prev_product_download_batch_id)
        if prev_tile_job is not None:
            self.logger.info(f"Previous tile job found in state {prev_tile_job['_source']['status']}")
            return True, None, prev_tile_job["_source"]["job_id"] # Wait for the job to complete.

        self.logger.info(f"No previous tile product and cannot find the previous tile job.  Run without previous tile product.")
        return False, None, None

    def get_previous_tile_product(self, download_batch_id, acquisition_ts):
        """ Get the previous tile product record from GRQ ES."""

        tile_id, acquisition_group, satellite, acquisition_cycle = download_batch_id.split("_")
        tile_id = tile_id[1:] # Remove the "p" from the tile_id
 
        # Consult GRQ cmr_rtc_cache for what the previous product should be
        self.logger.info(f"Searching GRQ cmr_rtc_cache for what the previous tile product should be for {download_batch_id=} {acquisition_ts=}.")

        # Get all burst ids for this batch_id
        all_burst_ids = set()
        product_ids = self.dist_products[tile_id]
        for product_id in product_ids:
            burst_ids = self.product_to_bursts[product_id]
            all_burst_ids.update(burst_ids)
        all_burst_ids = list(all_burst_ids)
        #print(f"All burst ids: {all_burst_ids}")

        should_query = []
        for burst_id in all_burst_ids:
            should_query.append({"match": {"burst_id.keyword": burst_id}})

        # Perform various sanity checks on the cmr_rtc_cache index to make sure it's been populated reasonably
        self.sanity_check_cmr_rtc_cache()

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

        # No previous tile product was found in GRQ ES and nothing in cmr_rtc_cache for this tile.
        if len(hits) == 0:
            return None, None

        # From the cmr_rtc_cache, we need to find the previous product download batch id
        granule_ids = []
        for hit in hits:
            rtc_granule = hit['_id']
            granule_ids.append(rtc_granule)
        
        prev_product_download_batch_id = \
            previous_product_download_batch_id_from_rtc(self.bursts_to_products, download_batch_id, acquisition_ts, granule_ids)
        
        # No previous product was determined from the cmr cache.
        if prev_product_download_batch_id is None:
            return None, None
        
        self.logger.info(f"Searching for previous tile product: {prev_product_download_batch_id} in GRQ products")
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

        return None, prev_product_download_batch_id

    def sanity_check_cmr_rtc_cache(self):
        """
        Perform sanity check on the cmr_rtc_cache index.
        """
        # Perform sanity check on the cache to make sure that there are reasonable number of records
        document_count = get_document_count(self.grq_es, CMR_RTC_CACHE_INDEX)
        assert document_count > self.min_cmr_rtc_cache_document_count, f"Expected at least {self.min_cmr_rtc_cache_document_count} records in cmr_rtc_cache but found {document_count}. You likely need to run tools/populate_cmr_rtc_cache.py script to populate cmr_rtc_cache in the GRQ ES."
        if document_count < self.warn_cmr_rtc_cache_document_count:
            self.logger.warning(f"Expected at least {self.warn_cmr_rtc_cache_document_count} records in cmr_rtc_cache but found {document_count}")

        # Get the earliest and latest timestamp for the cmr_rtc_cache index.
        earliest_timestamp, latest_timestamp = get_document_timestamp_min_max(self.grq_es, CMR_RTC_CACHE_INDEX, "acquisition_timestamp")
        earliest_timestamp = datetime.strptime(earliest_timestamp, "%Y-%m-%dT%H:%M:%S%z") #Timestamps are in string in this format: '2025-05-31T23:59:57+00:00'
        latest_timestamp = datetime.strptime(latest_timestamp, "%Y-%m-%dT%H:%M:%S%z")
        date_range_days = (latest_timestamp - earliest_timestamp).days
        assert date_range_days >= self.min_cmr_rtc_cache_document_date_range_days, f"Expected at least {self.min_cmr_rtc_cache_document_date_range_days} days of data in cmr_rtc_cache but found {date_range_days}. You likely need to run tools/populate_cmr_rtc_cache.py script to populate cmr_rtc_cache in the GRQ ES."
        if date_range_days < self.warn_cmr_rtc_cache_document_date_range_days:
            self.logger.warning(f"Expected at least {self.warn_cmr_rtc_cache_document_date_range_days} days of data in cmr_rtc_cache but found {date_range_days}")

    
    def find_job_download_batch_id(self, download_batch_id):
        """
        Get the previous tile run SCIFLO or download job.
        """
        sciflo_job_id_prefix = "job-WF-SCIFLO_L3_DIST_S1-batch-" + download_batch_id # e.g. job-WF-SCIFLO_L3_DIST_S1-batch-p11SLT_1_a348
        download_job_id_prefix = "job-WF-rtc_for_dist_download-" + download_batch_id # e.g. job-WF-rtc_for_dist_download-p11SLT_1_a348

        hits = []
        for job_id_prefix in [sciflo_job_id_prefix, download_job_id_prefix]:
            self.logger.info(f"Searching for previous tile job with job_id_prefix: {job_id_prefix}")
            query = {"query": {"bool": {"must": [{"prefix": {"job_id": job_id_prefix}}]}}}
            result = self.mozart_es.search(
                index="job_status*",
                body=query
            )
            hits.extend(result["hits"]["hits"])

        # The may seem overly verbose and redundant but we want the jobs in this order if there are multiple jobs with the same job_id_prefix
        # It's possible that one job had failed and so another was created, etc.
        
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

