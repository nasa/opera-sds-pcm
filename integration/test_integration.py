import logging
import time

import conftest
from int_test_util import \
    mock_cnm_r_success_sns, \
    mock_cnm_r_success_sqs, \
    wait_for_cnm_s_success, \
    wait_for_cnm_r_success, \
    wait_for_l3
from subscriber_util import \
    wait_for_query_job, \
    wait_for_download_jobs, \
    invoke_l30_subscriber_query_lambda, \
    update_env_vars_l30_subscriber_query_lambda, \
    reset_env_vars_l30_subscriber_query_lambda, \
    invoke_s30_subscriber_query_lambda, \
    update_env_vars_s30_subscriber_query_lambda, \
    reset_env_vars_s30_subscriber_query_lambda, \
    invoke_slc_subscriber_query_lambda, \
    update_env_vars_slc_subscriber_query_lambda, \
    reset_env_vars_slc_subscriber_query_lambda

config = conftest.config

    
def test_subscriber_l30():
    logging.info("TRIGGERING DATA SUBSCRIBE")

    update_env_vars_l30_subscriber_query_lambda()
    sleep_for(30)

    response = invoke_l30_subscriber_query_lambda()

    reset_env_vars_l30_subscriber_query_lambda()
    sleep_for(30)

    assert response["StatusCode"] == 200

    job_id = response["Payload"].read().decode().strip("\"")
    logging.info(f"{job_id=}")

    logging.info("Sleeping for query job execution...")
    sleep_for(300)

    wait_for_query_job(job_id)

    logging.info("Sleeping for download job execution...")
    sleep_for(300)
    wait_for_download_jobs(job_id)

    logging.info("CHECKING FOR L3 ENTRIES, INDICATING SUCCESSFUL PGE EXECUTION")

    logging.info("Sleeping for PGE execution...")
    sleep_for(300)

    response = wait_for_l3(_id="OPERA_L3_DSWx_HLS_T02LQK_20211228T211639Z_20211228T211639Z_L8_30_v2.0", index="grq_v2.0_l3_dswx_hls")
    assert response.hits[0]["id"] == "OPERA_L3_DSWx_HLS_T02LQK_20211228T211639Z_20211228T211639Z_L8_30_v2.0"

    logging.info("CHECKING FOR CNM-S SUCCESS")

    logging.info("Sleeping for CNM-S execution...")
    sleep_for(150)

    response = wait_for_cnm_s_success(_id="OPERA_L3_DSWx_HLS_T02LQK_20211228T211639Z_20211228T211639Z_L8_30_v2.0", index="grq_v2.0_l3_dswx_hls")
    assert_cnm_s_success(response)

    logging.info("TRIGGER AND CHECK FOR CNM-R SUCCESS")
    mock_cnm_r_success_sns(id="OPERA_L3_DSWx_HLS_T02LQK_20211228T211639Z_20211228T211639Z_L8_30_v2.0")

    logging.info("Sleeping for CNM-R execution...")
    sleep_for(150)

    response = wait_for_cnm_r_success(_id="OPERA_L3_DSWx_HLS_T02LQK_20211228T211639Z_20211228T211639Z_L8_30_v2.0", index="grq_v2.0_l3_dswx_hls")
    assert_cnm_r_success(response)


def test_subscriber_s30():
    logging.info("TRIGGERING DATA SUBSCRIBE")

    update_env_vars_s30_subscriber_query_lambda()
    sleep_for(30)

    response = invoke_s30_subscriber_query_lambda()

    reset_env_vars_s30_subscriber_query_lambda()
    sleep_for(30)

    assert response["StatusCode"] == 200

    job_id = response["Payload"].read().decode().strip("\"")
    logging.info(f"{job_id=}")

    logging.info("Sleeping for query job execution...")
    sleep_for(150)

    wait_for_query_job(job_id)

    logging.info("Sleeping for download job execution...")
    sleep_for(150)
    wait_for_download_jobs(job_id)

    logging.info("CHECKING FOR L3 ENTRIES, INDICATING SUCCESSFUL PGE EXECUTION")

    logging.info("Sleeping for PGE execution...")
    sleep_for(150)

    response = wait_for_l3(_id="OPERA_L3_DSWx_HLS_T15TUF_20200526T165849Z_20200526T165849Z_S2A_30_v2.0", index="grq_v2.0_l3_dswx_hls")
    assert response.hits[0]["id"] == "OPERA_L3_DSWx_HLS_T15TUF_20200526T165849Z_20200526T165849Z_S2A_30_v2.0"

    logging.info("CHECKING FOR CNM-S SUCCESS")

    logging.info("Sleeping for CNM-S execution...")
    sleep_for(150)

    response = wait_for_cnm_s_success(_id="OPERA_L3_DSWx_HLS_T15TUF_20200526T165849Z_20200526T165849Z_S2A_30_v2.0", index="grq_v2.0_l3_dswx_hls")
    assert_cnm_s_success(response)

    logging.info("TRIGGER AND CHECK FOR CNM-R SUCCESS")
    mock_cnm_r_success_sns(id="OPERA_L3_DSWx_HLS_T15TUF_20200526T165849Z_20200526T165849Z_S2A_30_v2.0")

    logging.info("Sleeping for CNM-R execution...")
    sleep_for(150)

    response = wait_for_cnm_r_success(_id="OPERA_L3_DSWx_HLS_T15TUF_20200526T165849Z_20200526T165849Z_S2A_30_v2.0", index="grq_v2.0_l3_dswx_hls")
    assert_cnm_r_success(response)


def test_subscriber_slc():
    logging.info("TRIGGERING DATA SUBSCRIBE")

    update_env_vars_slc_subscriber_query_lambda()
    sleep_for(30)

    response = invoke_slc_subscriber_query_lambda()

    reset_env_vars_slc_subscriber_query_lambda()
    sleep_for(30)

    assert response["StatusCode"] == 200

    job_id = response["Payload"].read().decode().strip("\"")
    logging.info(f"{job_id=}")

    logging.info("Sleeping for query job execution...")
    sleep_for(300)

    wait_for_query_job(job_id)

    logging.info("Sleeping for download job execution...")
    sleep_for(300)
    wait_for_download_jobs(job_id, index="slc_catalog")

    logging.info("CHECKING FOR L3 ENTRIES, INDICATING SUCCESSFUL PGE EXECUTION")

    logging.info("Sleeping for PGE execution...")
    sleep_for(300)

    response = wait_for_l3(_id="OPERA_L2_CSLC_S1A_IW_T64-135524-IW2_VV_20220131T193216Z_v0.1_20220131T193246Z", index="grq_1_l2_cslc_s1")
    assert response.hits[0]["id"] == "OPERA_L2_CSLC_S1A_IW_T64-135524-IW2_VV_20220131T193216Z_v0.1_20220131T193246Z"

    logging.info("CHECKING FOR CNM-S SUCCESS")

    logging.info("Sleeping for CNM-S execution...")
    sleep_for(300)

    response = wait_for_cnm_s_success(_id="OPERA_L2_CSLC_S1A_IW_T64-135524-IW2_VV_20220131T193216Z_v0.1_20220131T193246Z", index="grq_1_l2_cslc_s1")
    assert_cnm_s_success(response)

    logging.info("TRIGGER AND CHECK FOR CNM-R SUCCESS")
    mock_cnm_r_success_sqs(id="OPERA_L2_CSLC_S1A_IW_T64-135524-IW2_VV_20220131T193216Z_v0.1_20220131T193246Z")

    logging.info("Sleeping for CNM-R execution...")
    sleep_for(300)

    response = wait_for_cnm_r_success(_id="OPERA_L2_CSLC_S1A_IW_T64-135524-IW2_VV_20220131T193216Z_v0.1_20220131T193246Z", index="grq_1_l2_cslc_s1")
    assert_cnm_r_success(response)


def assert_cnm_s_success(response):
    assert response.hits.hits[0]["_source"]["daac_CNM_S_status"] == "SUCCESS"
    assert response.hits.hits[0]["_source"]["daac_CNM_S_timestamp"] is not None


def assert_cnm_r_success(response):
    assert response.hits.hits[0]["_source"]["daac_delivery_status"] == "SUCCESS"
    assert response.hits.hits[0]["_source"]["daac_identifier"] is not None

    assert response.hits.hits[0]["_source"]["daac_received_timestamp"] is not None
    assert response.hits.hits[0]["_source"]["daac_submission_timestamp"] is not None
    assert response.hits.hits[0]["_source"]["daac_collection"] is not None
    assert response.hits.hits[0]["_source"]["daac_process_complete_timestamp"] is not None

    # CNM-R ingestion metadata
    assert response.hits.hits[0]["_source"]["daac_catalog_id"] is not None
    assert response.hits.hits[0]["_source"]["daac_catalog_url"] is not None


def sleep_for(sec=None):
    logging.info(f"Sleeping for {sec} seconds...")
    time.sleep(sec)
    logging.info("Done sleeping.")
