#!/usr/bin/env python3

import logging

from data_subscriber.query import CmrQuery

logger = logging.getLogger(__name__)

class NisarGcovCmrQuery(CmrQuery):

    def __init__(self, args, token, es_conn, cmr, job_id, settings):
        super().__init__(args, token, es_conn, cmr, job_id, settings)
