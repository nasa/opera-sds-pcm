
from data_subscriber.query import CmrQuery

class CslcStaticCmrQuery(CmrQuery):

    def __init__(self, args, token, es_conn, cmr, job_id, settings):
        super().__init__(args, token, es_conn, cmr, job_id, settings)
