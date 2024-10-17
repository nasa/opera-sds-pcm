#!/usr/bin/env python3

'''Goes through the list of pending jobs and submits them to the job queue
after checking if they are ready to be submitted'''

import boto3
import logging
import sys

from commons.logger import NoJobUtilsFilter, NoBaseFilter, NoLogUtilsFilter
from util.conf_util import SettingsConf
from data_subscriber.cmr import get_cmr_token
from data_subscriber.parser import create_parser
from data_subscriber.query import submit_download_job
from data_subscriber import es_conn_util
from cslc_utils import (get_pending_download_jobs, localize_disp_frame_burst_hist, mark_pending_download_job_submitted)
from data_subscriber.cslc.cslc_dependency import CSLCDependency
from data_subscriber.cslc.cslc_blackout import DispS1BlackoutDates, localize_disp_blackout_dates
from data_subscriber.cslc.cslc_catalog import CSLCProductCatalog


from util.exec_util import exec_wrapper

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)


@exec_wrapper
def main():
    configure_logger()
    run(sys.argv)

def configure_logger():
    logger_hysds_commons = logging.getLogger("hysds_commons")
    logger_hysds_commons.addFilter(NoJobUtilsFilter())

    logger_elasticsearch = logging.getLogger("elasticsearch")
    logger_elasticsearch.addFilter(NoBaseFilter())

    boto3.set_stream_logger(name='botocore.credentials', level=logging.ERROR)

    logger.addFilter(NoLogUtilsFilter())


def run(argv: list[str]):
    logger.info(f"{argv=}")

    job_submission_tasks = []
    disp_burst_map, burst_to_frames, datetime_to_frames = localize_disp_frame_burst_hist()
    blackout_dates_obj = DispS1BlackoutDates(localize_disp_blackout_dates(), disp_burst_map, burst_to_frames)

    query_args = create_parser().parse_args(["query", "-c", "OPERA_L2_CSLC-S1_V1", "--processing-mode=forward"])

    es = es_conn_util.get_es_connection(logger)
    es_conn = CSLCProductCatalog(logging.getLogger(__name__))

    settings = SettingsConf().cfg
    cmr, token, username, password, edl = get_cmr_token(query_args.endpoint, settings)

    # Get unsubmitted jobs from Elasticsearch GRQ
    unsubmitted = get_pending_download_jobs(es)
    logger.info(f"Found {len(unsubmitted)=} Pending CSLC Download Jobs")

    # For each of the unsubmitted jobs, check if their compressed cslcs have been generated
    for job in unsubmitted:
        k = job['_source']['k']
        m = job['_source']['m']
        frame_id = job['_source']['frame_id']
        acq_index = job['_source']['acq_index']

        cslc_dependency = CSLCDependency(k, m, disp_burst_map, query_args, token, cmr, settings, blackout_dates_obj)

        # Check if the compressed cslc has been generated
        logger.info("Evaluating for frame_id: %s, acq_index: %s, k: %s, m: %s", frame_id, acq_index, k, m)
        if cslc_dependency.compressed_cslc_satisfied(frame_id, acq_index, es):
            logger.info("Compressed CSLC satisfied for frame_id: %s, acq_index: %s. Submitting CSLC download job",
                        frame_id, acq_index)

            download_job_id = submit_download_job(release_version=job['_source']['release_version'],
                    product_type=job['_source']['product_type'],
                    params=job['_source']['job_params'],
                    job_queue=job['_source']['job_queue'],
                    job_name = job['_source']['job_name'])

            # Record download job id in ES cslc_catalog
            for batch_id in job['_source']['batch_ids']:
                es_conn.mark_download_job_id(batch_id, download_job_id)

            # Also mark as submitted in ES pending downloads
            logger.info(mark_pending_download_job_submitted(es, job['_id'], download_job_id))

            job_submission_tasks.append(download_job_id)

        else:
            logger.info("Compressed CSLC NOT satisfied for frame_id: %s, acq_index: %s", frame_id, acq_index)

    logger.info(f"Submitted {len(job_submission_tasks)} CSLC Download Jobs {job_submission_tasks}")

if __name__ == "__main__":
    main()
