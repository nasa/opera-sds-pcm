import logging
import re
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

    response = wait_for_l3(_id="OPERA_L3_DSWx-HLS_T02LQK_20211228T211639Z_", index="grq_v0.0_l3_dswx_hls", query_name="match_phrase")
    assert re.match(r"OPERA_L3_DSWx-HLS_T02LQK_20211228T211639Z_(\d+)T(\d+)Z_L8_30_v0.0", response.hits[0]["id"])

    logging.info("CHECKING FOR CNM-S SUCCESS")

    logging.info("Sleeping for CNM-S execution...")
    sleep_for(150)

    response = wait_for_cnm_s_success(_id=response.hits[0]["id"], index="grq_v0.0_l3_dswx_hls")
    assert_cnm_s_success(response)

    logging.info("TRIGGER AND CHECK FOR CNM-R SUCCESS")
    mock_cnm_r_success_sns(id=response.hits[0]["id"])

    logging.info("Sleeping for CNM-R execution...")
    sleep_for(150)

    response = wait_for_cnm_r_success(_id=response.hits[0]["id"], index="grq_v0.0_l3_dswx_hls")
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

    response = wait_for_l3(_id="OPERA_L3_DSWx-HLS_T15TUF_20200526T165849Z_", index="grq_v0.0_l3_dswx_hls", query_name="match_phrase")
    assert re.match(r"OPERA_L3_DSWx-HLS_T15TUF_20200526T165849Z_(\d+)T(\d+)Z_S2B_30_v0.0", response.hits[0]["id"])

    logging.info("CHECKING FOR CNM-S SUCCESS")

    logging.info("Sleeping for CNM-S execution...")
    sleep_for(150)

    response = wait_for_cnm_s_success(_id=response.hits[0]["id"], index="grq_v0.0_l3_dswx_hls")
    assert_cnm_s_success(response)

    logging.info("TRIGGER AND CHECK FOR CNM-R SUCCESS")
    mock_cnm_r_success_sns(id=response.hits[0]["id"])

    logging.info("Sleeping for CNM-R execution...")
    sleep_for(150)

    response = wait_for_cnm_r_success(_id=response.hits[0]["id"], index="grq_v0.0_l3_dswx_hls")
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

    # CSLC

    cslc_ids = []

    # 51-IW2, 51-IW3
    response = wait_for_l3(_id="OPERA_L2_CSLC-S1A_IW_T005-008851-IW2_VV_20221117T004741Z_v0.0_", index="grq_v0.0_l2_cslc_s1", query_name="match_phrase")
    assert re.match(r"OPERA_L2_CSLC-S1A_IW_T005-008851-IW2_VV_20221117T004741Z_v0.0_(\d+)T(\d+)Z", response.hits[0]["id"])
    cslc_ids.append(response.hits[0]["id"])
    response = wait_for_l3(_id="OPERA_L2_CSLC-S1A_IW_T005-008851-IW3_VV_20221117T004742Z_v0.0_", index="grq_v0.0_l2_cslc_s1", query_name="match_phrase")
    assert re.match(r"OPERA_L2_CSLC-S1A_IW_T005-008851-IW3_VV_20221117T004742Z_v0.0_(\d+)T(\d+)Z", response.hits[0]["id"])
    cslc_ids.append(response.hits[0]["id"])

    # 52-IW1, 52-IW2, 52-IW3
    response = wait_for_l3(_id="OPERA_L2_CSLC-S1A_IW_T005-008852-IW1_VV_20221117T004743Z_v0.0_", index="grq_v0.0_l2_cslc_s1", query_name="match_phrase")
    assert re.match(r"OPERA_L2_CSLC-S1A_IW_T005-008852-IW1_VV_20221117T004743Z_v0.0_(\d+)T(\d+)Z", response.hits[0]["id"])
    cslc_ids.append(response.hits[0]["id"])
    response = wait_for_l3(_id="OPERA_L2_CSLC-S1A_IW_T005-008852-IW2_VV_20221117T004744Z_v0.0_", index="grq_v0.0_l2_cslc_s1", query_name="match_phrase")
    assert re.match(r"OPERA_L2_CSLC-S1A_IW_T005-008852-IW2_VV_20221117T004744Z_v0.0_(\d+)T(\d+)Z", response.hits[0]["id"])
    cslc_ids.append(response.hits[0]["id"])
    response = wait_for_l3(_id="OPERA_L2_CSLC-S1A_IW_T005-008852-IW3_VV_20221117T004745Z_v0.0_", index="grq_v0.0_l2_cslc_s1", query_name="match_phrase")
    assert re.match(r"OPERA_L2_CSLC-S1A_IW_T005-008852-IW3_VV_20221117T004745Z_v0.0_(\d+)T(\d+)Z", response.hits[0]["id"])
    cslc_ids.append(response.hits[0]["id"])

    # 53-IW1, 53-IW2, 53-IW3
    response = wait_for_l3(_id="OPERA_L2_CSLC-S1A_IW_T005-008853-IW1_VV_20221117T004746Z_v0.0_", index="grq_v0.0_l2_cslc_s1", query_name="match_phrase")
    assert re.match(r"OPERA_L2_CSLC-S1A_IW_T005-008853-IW1_VV_20221117T004746Z_v0.0_(\d+)T(\d+)Z", response.hits[0]["id"])
    cslc_ids.append(response.hits[0]["id"])
    response = wait_for_l3(_id="OPERA_L2_CSLC-S1A_IW_T005-008853-IW2_VV_20221117T004747Z_v0.0_", index="grq_v0.0_l2_cslc_s1", query_name="match_phrase")
    assert re.match(r"OPERA_L2_CSLC-S1A_IW_T005-008853-IW2_VV_20221117T004747Z_v0.0_(\d+)T(\d+)Z", response.hits[0]["id"])
    cslc_ids.append(response.hits[0]["id"])
    response = wait_for_l3(_id="OPERA_L2_CSLC-S1A_IW_T005-008853-IW3_VV_20221117T004748Z_v0.0_", index="grq_v0.0_l2_cslc_s1", query_name="match_phrase")
    assert re.match(r"OPERA_L2_CSLC-S1A_IW_T005-008853-IW3_VV_20221117T004748Z_v0.0_(\d+)T(\d+)Z", response.hits[0]["id"])
    cslc_ids.append(response.hits[0]["id"])

    # 54-IW1, 54-IW2, 54-IW3
    response = wait_for_l3(_id="OPERA_L2_CSLC-S1A_IW_T005-008854-IW1_VV_20221117T004749Z_v0.0_", index="grq_v0.0_l2_cslc_s1", query_name="match_phrase")
    assert re.match(r"OPERA_L2_CSLC-S1A_IW_T005-008854-IW1_VV_20221117T004749Z_v0.0_(\d+)T(\d+)Z", response.hits[0]["id"])
    cslc_ids.append(response.hits[0]["id"])
    response = wait_for_l3(_id="OPERA_L2_CSLC-S1A_IW_T005-008854-IW2_VV_20221117T004749Z_v0.0_", index="grq_v0.0_l2_cslc_s1", query_name="match_phrase")
    assert re.match(r"OPERA_L2_CSLC-S1A_IW_T005-008854-IW2_VV_20221117T004749Z_v0.0_(\d+)T(\d+)Z", response.hits[0]["id"])
    cslc_ids.append(response.hits[0]["id"])
    response = wait_for_l3(_id="OPERA_L2_CSLC-S1A_IW_T005-008854-IW3_VV_20221117T004750Z_v0.0_", index="grq_v0.0_l2_cslc_s1", query_name="match_phrase")
    assert re.match(r"OPERA_L2_CSLC-S1A_IW_T005-008854-IW3_VV_20221117T004750Z_v0.0_(\d+)T(\d+)Z", response.hits[0]["id"])
    cslc_ids.append(response.hits[0]["id"])

    # 55-IW1, 55-IW2, 55-IW3
    response = wait_for_l3(_id="OPERA_L2_CSLC-S1A_IW_T005-008855-IW1_VV_20221117T004751Z_v0.0_", index="grq_v0.0_l2_cslc_s1", query_name="match_phrase")
    assert re.match(r"OPERA_L2_CSLC-S1A_IW_T005-008855-IW1_VV_20221117T004751Z_v0.0_(\d+)T(\d+)Z", response.hits[0]["id"])
    cslc_ids.append(response.hits[0]["id"])
    response = wait_for_l3(_id="OPERA_L2_CSLC-S1A_IW_T005-008855-IW2_VV_20221117T004752Z_v0.0_", index="grq_v0.0_l2_cslc_s1", query_name="match_phrase")
    assert re.match(r"OPERA_L2_CSLC-S1A_IW_T005-008855-IW2_VV_20221117T004752Z_v0.0_(\d+)T(\d+)Z", response.hits[0]["id"])
    cslc_ids.append(response.hits[0]["id"])
    response = wait_for_l3(_id="OPERA_L2_CSLC-S1A_IW_T005-008855-IW3_VV_20221117T004753Z_v0.0_", index="grq_v0.0_l2_cslc_s1", query_name="match_phrase")
    assert re.match(r"OPERA_L2_CSLC-S1A_IW_T005-008855-IW3_VV_20221117T004753Z_v0.0_(\d+)T(\d+)Z", response.hits[0]["id"])
    cslc_ids.append(response.hits[0]["id"])

    # RTC

    rtc_ids = []

    # 51-IW2, 51-IW3
    response = wait_for_l3(_id="OPERA_L2_RTC-S1_T005-008851-IW2_20221117T004741Z_", index="grq_v0.0_l2_rtc_s1", query_name="match_phrase")
    assert re.match(r"OPERA_L2_RTC-S1_T005-008851-IW2_20221117T004741Z_(\d+)T(\d+)Z_S1A_30_v0.0", response.hits[0]["id"])
    rtc_ids.append(response.hits[0]["id"])
    response = wait_for_l3(_id="OPERA_L2_RTC-S1_T005-008851-IW3_20221117T004742Z_", index="grq_v0.0_l2_rtc_s1", query_name="match_phrase")
    assert re.match(r"OPERA_L2_RTC-S1_T005-008851-IW3_20221117T004742Z_(\d+)T(\d+)Z_S1A_30_v0.0", response.hits[0]["id"])
    rtc_ids.append(response.hits[0]["id"])

    # 52-IW1, 52-IW2, 52-IW3
    response = wait_for_l3(_id="OPERA_L2_RTC-S1_T005-008852-IW1_20221117T004743Z_", index="grq_v0.0_l2_rtc_s1", query_name="match_phrase")
    assert re.match(r"OPERA_L2_RTC-S1_T005-008852-IW1_20221117T004743Z_(\d+)T(\d+)Z_S1A_30_v0.0", response.hits[0]["id"])
    rtc_ids.append(response.hits[0]["id"])
    response = wait_for_l3(_id="OPERA_L2_RTC-S1_T005-008852-IW2_20221117T004744Z_", index="grq_v0.0_l2_rtc_s1", query_name="match_phrase")
    assert re.match(r"OPERA_L2_RTC-S1_T005-008852-IW2_20221117T004744Z_(\d+)T(\d+)Z_S1A_30_v0.0", response.hits[0]["id"])
    rtc_ids.append(response.hits[0]["id"])
    response = wait_for_l3(_id="OPERA_L2_RTC-S1_T005-008852-IW3_20221117T004745Z_", index="grq_v0.0_l2_rtc_s1", query_name="match_phrase")
    assert re.match(r"OPERA_L2_RTC-S1_T005-008852-IW3_20221117T004745Z_(\d+)T(\d+)Z_S1A_30_v0.0", response.hits[0]["id"])
    rtc_ids.append(response.hits[0]["id"])

    # 53-IW1, 53-IW2, 53-IW3
    response = wait_for_l3(_id="OPERA_L2_RTC-S1_T005-008853-IW1_20221117T004746Z_", index="grq_v0.0_l2_rtc_s1", query_name="match_phrase")
    assert re.match(r"OPERA_L2_RTC-S1_T005-008853-IW1_20221117T004746Z_(\d+)T(\d+)Z_S1A_30_v0.0", response.hits[0]["id"])
    rtc_ids.append(response.hits[0]["id"])
    response = wait_for_l3(_id="OPERA_L2_RTC-S1_T005-008853-IW2_20221117T004747Z_", index="grq_v0.0_l2_rtc_s1", query_name="match_phrase")
    assert re.match(r"OPERA_L2_RTC-S1_T005-008853-IW2_20221117T004747Z_(\d+)T(\d+)Z_S1A_30_v0.0", response.hits[0]["id"])
    rtc_ids.append(response.hits[0]["id"])
    response = wait_for_l3(_id="OPERA_L2_RTC-S1_T005-008853-IW3_20221117T004748Z_", index="grq_v0.0_l2_rtc_s1", query_name="match_phrase")
    assert re.match(r"OPERA_L2_RTC-S1_T005-008853-IW3_20221117T004748Z_(\d+)T(\d+)Z_S1A_30_v0.0", response.hits[0]["id"])
    rtc_ids.append(response.hits[0]["id"])

    # 54-IW1, 54-IW2, 54-IW3
    response = wait_for_l3(_id="OPERA_L2_RTC-S1_T005-008854-IW1_20221117T004749Z_", index="grq_v0.0_l2_rtc_s1", query_name="match_phrase")
    assert re.match(r"OPERA_L2_RTC-S1_T005-008854-IW1_20221117T004749Z_(\d+)T(\d+)Z_S1A_30_v0.0", response.hits[0]["id"])
    rtc_ids.append(response.hits[0]["id"])
    response = wait_for_l3(_id="OPERA_L2_RTC-S1_T005-008854-IW2_20221117T004749Z_", index="grq_v0.0_l2_rtc_s1", query_name="match_phrase")
    assert re.match(r"OPERA_L2_RTC-S1_T005-008854-IW2_20221117T004749Z_(\d+)T(\d+)Z_S1A_30_v0.0", response.hits[0]["id"])
    rtc_ids.append(response.hits[0]["id"])
    response = wait_for_l3(_id="OPERA_L2_RTC-S1_T005-008854-IW3_20221117T004750Z_", index="grq_v0.0_l2_rtc_s1", query_name="match_phrase")
    assert re.match(r"OPERA_L2_RTC-S1_T005-008854-IW3_20221117T004750Z_(\d+)T(\d+)Z_S1A_30_v0.0", response.hits[0]["id"])
    rtc_ids.append(response.hits[0]["id"])

    # 55-IW1, 55-IW2, 55-IW3
    response = wait_for_l3(_id="OPERA_L2_RTC-S1_T005-008855-IW1_20221117T004751Z_", index="grq_v0.0_l2_rtc_s1", query_name="match_phrase")
    assert re.match(r"OPERA_L2_RTC-S1_T005-008855-IW1_20221117T004751Z_(\d+)T(\d+)Z_S1A_30_v0.0", response.hits[0]["id"])
    rtc_ids.append(response.hits[0]["id"])
    response = wait_for_l3(_id="OPERA_L2_RTC-S1_T005-008855-IW2_20221117T004752Z_", index="grq_v0.0_l2_rtc_s1", query_name="match_phrase")
    assert re.match(r"OPERA_L2_RTC-S1_T005-008855-IW2_20221117T004752Z_(\d+)T(\d+)Z_S1A_30_v0.0", response.hits[0]["id"])
    rtc_ids.append(response.hits[0]["id"])
    response = wait_for_l3(_id="OPERA_L2_RTC-S1_T005-008855-IW3_20221117T004753Z_", index="grq_v0.0_l2_rtc_s1", query_name="match_phrase")
    assert re.match(r"OPERA_L2_RTC-S1_T005-008855-IW3_20221117T004753Z_(\d+)T(\d+)Z_S1A_30_v0.0", response.hits[0]["id"])
    rtc_ids.append(response.hits[0]["id"])

    logging.info("CHECKING FOR CNM-S SUCCESS")

    logging.info("Sleeping for CNM-S execution...")
    sleep_for(300)

    for cslc_id in cslc_ids:
        response = wait_for_cnm_s_success(_id=cslc_id, index="grq_v0.0_l2_cslc_s1")
        assert_cnm_s_success(response)

    for rtc_id in rtc_ids:
        response = wait_for_cnm_s_success(_id=rtc_id, index="grq_v0.0_l2_rtc_s1")
        assert_cnm_s_success(response)

    logging.info("TRIGGER AND CHECK FOR CNM-R SUCCESS")

    for cslc_id in cslc_ids:
        mock_cnm_r_success_sqs(id=cslc_id)

    for rtc_id in rtc_ids:
        mock_cnm_r_success_sqs(id=rtc_id)

    logging.info("Sleeping for CNM-R execution...")
    sleep_for(300)

    for cslc_id in cslc_ids:
        response = wait_for_cnm_r_success(_id=cslc_id, index="grq_v0.0_l2_cslc_s1")
        assert_cnm_r_success(response)

    for rtc_id in rtc_ids:
        response = wait_for_cnm_r_success(_id=rtc_id, index="grq_v0.0_l2_rtc_s1")
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
