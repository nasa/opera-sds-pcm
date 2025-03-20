from datetime import datetime

from data_subscriber.query import CmrQuery, get_query_timerange
from data_subscriber.dist_s1_utils import localize_dist_burst_db, process_dist_burst_db

class RtcForDistCmrQuery(CmrQuery):

    def __init__(self, args, token, es_conn, cmr, job_id, settings, dist_s1_burst_db_file = None):
        super().__init__(args, token, es_conn, cmr, job_id, settings)

        if dist_s1_burst_db_file:
            self.dist_products, self.bursts_to_products, self.product_to_bursts = self.process_dist_burst_db(dist_s1_burst_db_file)
        else:
            self.dist_products, self.bursts_to_products, self.product_to_bursts = self.localize_dist_burst_db()

        #TODO: Grace minutes? Read from settings.yaml

        #TODO: Set up es_conn and data structures for Baseline Set granules

    def validate_args(self):
        pass

    def determine_download_granules(self, granules):
        if self.proc_mode == "reprocessing":
            if len(granules) == 0:
                return granules