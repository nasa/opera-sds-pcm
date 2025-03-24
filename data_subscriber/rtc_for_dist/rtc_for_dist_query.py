from collections import defaultdict
from datetime import datetime

from data_subscriber.url import determine_acquisition_cycle, rtc_for_dist_unique_id
from data_subscriber.query import CmrQuery, get_query_timerange
from data_subscriber.cslc_utils import parse_r2_product_file_name
from data_subscriber.dist_s1_utils import localize_dist_burst_db, process_dist_burst_db, compute_dist_s1_triggering, dist_s1_download_batch_id

class RtcForDistCmrQuery(CmrQuery):

    def __init__(self, args, token, es_conn, cmr, job_id, settings, dist_s1_burst_db_file = None):
        super().__init__(args, token, es_conn, cmr, job_id, settings)

        if dist_s1_burst_db_file:
            self.dist_products, self.bursts_to_products, self.product_to_bursts, self.all_tile_ids = process_dist_burst_db(dist_s1_burst_db_file)
        else:
            self.dist_products, self.bursts_to_products, self.product_to_bursts, self.all_tile_ids = localize_dist_burst_db()

        #TODO: Grace minutes? Read from settings.yaml

        #TODO: Set up es_conn and data structures for Baseline Set granules

    def validate_args(self):
        pass

    def query_cmr(self, timerange, now: datetime):
        granules = super().query_cmr(timerange, now)
        self.extend_additional_records(granules)
        return granules

    def extend_additional_records(self, granules):
        for granule in granules:
            rtc_granule_id = granule["granule_id"]
            burst_id, acquisition_dts = parse_r2_product_file_name()

            granule["burst_id"] = burst_id
            granule["tile_id"] = self.bursts_to_products[burst_id]
            granule["acquisition_cycle"] = determine_acquisition_cycle(burst_id, acquisition_dts, rtc_granule_id)
            granule["download_batch_id"] = dist_s1_download_batch_id(granule)
            granule["unique_id"] = rtc_for_dist_unique_id(granule["download_batch_id"], granule["burst_id"])

    def prepare_additional_fields(self, granule, args, granule_id):
        """This is used to determine download_batch_id and attaching it the granule.
        Function extend_additional_records must have been called before this function."""

        # Copy metadata fields to the additional_fields so that they are written to ES
        additional_fields = super().prepare_additional_fields(granule, args, granule_id)
        for f in ["burst_id", "tile_id", "acquisition_ts", "acquisition_cycle", "unique_id", "download_batch_id"]:
            additional_fields[f] = granule[f]

        return additional_fields

    def determine_download_granules(self, granules):
        if len(granules) == 0:
            return granules

        download_granules = []

        # Create a dict of granule_id to granule
        granules_dict = {granule["granule_id"]: granule for granule in granules}
        granule_ids = list(granules_dict.keys())
        products_triggered, tiles_untriggered, unused_rtc_granule_count = compute_dist_s1_triggering(
            self.bursts_to_products, granule_ids, self.all_tile_ids)

        by_download_batch_id = defaultdict(lambda: defaultdict(dict))

        for product_id, product in products_triggered.items():
            for rtc_granule in product.rtc_granules:
                by_download_batch_id[product_id][rtc_granule] = granules_dict[rtc_granule]
                download_granules.append(granules_dict[rtc_granule])

        self.logger.info("Received the following RTC granules from CMR: ")
        for batch_id, download_batch in by_download_batch_id.items():
            self.logger.info(f"batch_id=%s len(download_batch)=%d", batch_id, len(download_batch))

        return download_granules

    def get_download_chunks(self, batch_id_to_urls_map):
        '''For CSLC chunks we must group them by the batch_id that were determined at the time of triggering'''

        chunk_map = defaultdict(list)
        if len(list(batch_id_to_urls_map)) == 0:
            return chunk_map.values()

        frame_id, _ = split_download_batch_id(list(batch_id_to_urls_map)[0])

        for batch_chunk in batch_id_to_urls_map.items():

            # Chunking is done differently between historical and forward/reprocessing
            if self.proc_mode == "historical":
                chunk_map[frame_id].append(batch_chunk)
            else:
                chunk_map[batch_chunk[0]].append(
                    batch_chunk)  # We don't actually care about the URLs, we only care about the batch_id

        return chunk_map.values()


