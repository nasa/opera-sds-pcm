
from data_subscriber.query import BaseQuery

class CslcStaticCmrQuery(BaseQuery):

    def __init__(self, args, token, es_conn, cmr, job_id, settings):
        super().__init__(args, token, es_conn, cmr, job_id, settings)
