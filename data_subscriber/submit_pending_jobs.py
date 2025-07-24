#!/usr/bin/env python3

'''Goes through the list of pending jobs and submits them to the job queue
after checking if they are ready to be submitted'''

import logging
import sys
import json
from opera_commons.logger import configure_library_loggers
from data_subscriber import es_conn_util
from data_subscriber.cmr import get_cmr_token

from data_subscriber.cslc_utils import (get_pending_download_jobs,
                        localize_disp_frame_burst_hist,
                        mark_pending_download_job_submitted,
                        PENDING_TYPE_CSLC_DOWNLOAD)
from data_subscriber.cslc.cslc_blackout import DispS1BlackoutDates, localize_disp_blackout_dates
from data_subscriber.cslc.cslc_catalog import CSLCProductCatalog
from data_subscriber.cslc.cslc_dependency import CSLCDependency

from data_subscriber.dist_s1_utils import PENDING_TYPE_RTC_FOR_DIST_DOWNLOAD, localize_dist_burst_db
from data_subscriber.rtc_for_dist.dist_dependency import DistDependency, file_paths_from_prev_product
from data_subscriber.rtc_for_dist.rtc_for_dist_query import RtcForDistCmrQuery

from data_subscriber.parser import create_parser
from data_subscriber.query import submit_download_job
from util.conf_util import SettingsConf
from util.exec_util import exec_wrapper

logging.basicConfig(level="INFO")
logger = logging.getLogger(__name__)


@exec_wrapper
def main():
    configure_library_loggers()
    run(sys.argv)


def run(argv: list[str]):
    logger.info(f"{argv=}")

    es = es_conn_util.get_es_connection(logger)
    es_conn = CSLCProductCatalog(logging.getLogger(__name__))
    settings = SettingsConf().cfg

    job_submission_tasks = []

    disp_burst_map, burst_to_frames, datetime_to_frames = localize_disp_frame_burst_hist()
    blackout_dates_obj = DispS1BlackoutDates(localize_disp_blackout_dates(), disp_burst_map, burst_to_frames)
    cslc_query_args = create_parser().parse_args(["query", "-c", "OPERA_L2_CSLC-S1_V1", "--processing-mode=forward"])
    cmr, token, username, password, edl = get_cmr_token(cslc_query_args.endpoint, settings)

    # Get unsubmitted jobs from Elasticsearch GRQ
    unsubmitted = get_pending_download_jobs(es)
    logger.info(f"Found {len(unsubmitted)=} Pending Jobs")

    dist_products, bursts_to_products, product_to_bursts, _ = localize_dist_burst_db()
    rtc_for_dist_query_args = create_parser().parse_args(["query", "-c", "OPERA_L2_RTC-S1_V1", "--processing-mode=forward"])
    dist_dependency = DistDependency(logger, dist_products, bursts_to_products, product_to_bursts, settings)
    rtc_for_dist_query = RtcForDistCmrQuery(rtc_for_dist_query_args, token, es, cmr, None, settings)

    # For each of the unsubmitted jobs, check if their compressed cslcs have been generated
    for job in unsubmitted:
        job_source = job['_source']
        if job_source['job_type'] == PENDING_TYPE_CSLC_DOWNLOAD:
            logger.info(f"Found pending CSLC download job. batch ids: {job_source['batch_ids']}, ")
            k = job_source['k']
            m = job_source['m']
            frame_id = job_source['frame_id']
            acq_index = job_source['acq_index']

            cslc_dependency = CSLCDependency(k, m, disp_burst_map, cslc_query_args, token, cmr, settings, blackout_dates_obj)

            # Check if the compressed cslc has been generated
            logger.info("Evaluating for frame_id: %s, acq_index: %s, k: %s, m: %s", frame_id, acq_index, k, m)
            if cslc_dependency.compressed_cslc_satisfied(frame_id, acq_index, es):
                logger.info("Compressed CSLC satisfied for frame_id: %s, acq_index: %s. Submitting CSLC download job",
                            frame_id, acq_index)

                download_job_id = submit_download_job(release_version=job_source['release_version'],
                        product_type=job_source['product_type'],
                        params=job_source['job_params'],
                        job_queue=job_source['job_queue'],
                        job_name = job_source['job_name'])

                # Record download job id in ES cslc_catalog
                for batch_id in job_source['batch_ids']:
                    es_conn.mark_download_job_id(batch_id, download_job_id)

                # Also mark as submitted in ES pending downloads
                logger.info(mark_pending_download_job_submitted(es, job['_id'], download_job_id))

                job_submission_tasks.append(download_job_id)

            else:
                logger.info("Compressed CSLC NOT satisfied for frame_id: %s, acq_index: %s", frame_id, acq_index)
        
        elif job_source['job_type'] == PENDING_TYPE_RTC_FOR_DIST_DOWNLOAD:
            '''For rtc for dist download jobs, we have to make one of the 3 decisions: submit the job, continue to wait, or delete the job
                1. Submit the job if the previous tile product is found
                2. Continue to wait if the previous tile product is not found and the previous tile job is same as we've been waiting for
                3. Delete the job if the previous tile product is not found and the previous tile job is different or None'''
            
            logger.info(f"Found pending rtc for dist download job. Download batch id: {job_source['download_batch_id']}")
            should_wait, file_paths, previous_tile_job_id = dist_dependency.should_wait_previous_run(job_source['download_batch_id'])
            if should_wait:
                if previous_tile_job_id == job_source['previous_tile_job_id']:
                    logger.info(f"Previous tile product not found. Waiting for previous tile job to complete: {previous_tile_job_id}")
                    continue

            #If we shouldn't wait or we should wait but the previous tile job is different, submit the job with the information we have.

            if previous_tile_job_id != job_source['previous_tile_job_id']:
                logger.warning(f"Previous tile job is different from what we've been waiting for. We are in a bad state. Submitting download job: {file_paths}")
            else:
                logger.info(f"Previous tile product found. Submitting download job: {file_paths}")

            # Replace the previous_tile_product_file_paths with the file paths from the previous tile product
            product_metadata = None
            for param in job_source['job_params']:
                if param['name'] == 'product_metadata':
                    product_metadata = json.loads(param['value'])
                    logger.info(f"Product metadata: {product_metadata}")
                    logger.info(f"Populating product metadata with file paths: {file_paths}")
                    rtc_for_dist_query.populate_product_metadata(product_metadata, file_paths)
                    param['value'] = product_metadata
                    break

            download_job_id = submit_download_job(release_version=job_source['release_version'],
                product_type=job_source['product_type'],
                params=job_source['job_params'],
                job_queue=job_source['job_queue'],
                job_name = job_source['job_name'])

            # Record download job id in ES cslc_catalog
            for batch_id in job_source['download_batch_id']:
                es_conn.mark_download_job_id(batch_id, download_job_id)

            # Also mark as submitted in ES pending downloads
            logger.info(mark_pending_download_job_submitted(es, job['_id'], download_job_id))

            job_submission_tasks.append(download_job_id)

    logger.info(f"Submitted {len(job_submission_tasks)} Pending Jobs {job_submission_tasks}")

if __name__ == "__main__":
    main()
