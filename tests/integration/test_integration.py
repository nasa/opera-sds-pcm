import logging
import re
import time
from pathlib import Path

import pytest

from . import conftest
from .int_test_util import \
    mock_cnm_r_success_sns, \
    mock_cnm_r_success_sqs, \
    wait_for_cnm_s_success, \
    wait_for_cnm_r_success, \
    wait_for_l3
from .subscriber_util import \
    wait_for_query_job, \
    wait_for_download_job, \
    invoke_l30_subscriber_query_lambda, \
    update_env_vars_l30_subscriber_query_lambda, \
    reset_env_vars_l30_subscriber_query_lambda, \
    invoke_s30_subscriber_query_lambda, \
    update_env_vars_s30_subscriber_query_lambda, \
    reset_env_vars_s30_subscriber_query_lambda, \
    invoke_slc_subscriber_query_lambda, \
    update_env_vars_slc_subscriber_query_lambda, \
    reset_env_vars_slc_subscriber_query_lambda, invoke_slc_subscriber_ionosphere_download_lambda, \
    update_env_vars_subscriber_slc_ionosphere_download_lambda, wait_for_job, \
    update_env_vars_rtc_subscriber_query_lambda, invoke_rtc_subscriber_query_lambda, \
    reset_env_vars_rtc_subscriber_query_lambda

logger = logging.getLogger(__name__)

config = conftest.config


def test_subscriber_l30():
    logger.info("TRIGGERING DATA SUBSCRIBE")

    update_env_vars_l30_subscriber_query_lambda()
    sleep_for(30)

    response = invoke_l30_subscriber_query_lambda()

    reset_env_vars_l30_subscriber_query_lambda()
    sleep_for(30)

    assert response["StatusCode"] == 200

    job_id = response["Payload"].read().decode().strip("\"")
    logger.info(f"{job_id=}")

    logger.info("Sleeping for query job execution...")
    sleep_for(300)  # max queue dwell time
    sleep_for(180)  # observed job-started bootstrapping time

    wait_for_query_job(job_id)

    logger.info("Sleeping for download job execution...")
    sleep_for(300)  # max queue dwell time
    sleep_for(180)  # observed job-started bootstrapping time
    wait_for_download_job(job_id, index="hls_catalog-*")

    logger.info("CHECKING FOR L3 ENTRIES, INDICATING SUCCESSFUL PGE EXECUTION")

    logger.info("Sleeping for PGE execution...")
    sleep_for(300)  # max queue dwell time
    sleep_for(180)  # observed job-started bootstrapping time

    response = wait_for_l3(_id="OPERA_L3_DSWx-HLS_T54PVQ_20220101T005855Z_", index="grq_v2.0_l3_dswx_hls-*", query_name="match_phrase")
    assert re.match(r"OPERA_L3_DSWx-HLS_T54PVQ_20220101T005855Z_(\d+)T(\d+)Z_L8_30_v2.0", response.hits[0]["id"])

    logger.info("CHECKING FOR CNM-S SUCCESS")

    logger.info("Sleeping for CNM-S execution...")
    sleep_for(300)  # max queue dwell time
    sleep_for(180)  # observed job-started bootstrapping time

    response = wait_for_cnm_s_success(_id=response.hits[0]["id"], index="grq_v2.0_l3_dswx_hls-*")
    assert_cnm_s_success(response)

    logger.info("TRIGGER AND CHECK FOR CNM-R SUCCESS")
    mock_cnm_r_success_sns(id=response.hits[0]["id"])

    logger.info("Sleeping for CNM-R execution...")
    sleep_for(300)  # max queue dwell time
    sleep_for(180)  # observed job-started bootstrapping time

    response = wait_for_cnm_r_success(_id=response.hits[0]["id"], index="grq_v2.0_l3_dswx_hls-*")
    assert_cnm_r_success(response)


def test_subscriber_s30():
    logger.info("TRIGGERING DATA SUBSCRIBE")

    update_env_vars_s30_subscriber_query_lambda()
    sleep_for(30)

    response = invoke_s30_subscriber_query_lambda()

    reset_env_vars_s30_subscriber_query_lambda()
    sleep_for(30)

    assert response["StatusCode"] == 200

    job_id = response["Payload"].read().decode().strip("\"")
    logger.info(f"{job_id=}")

    logger.info("Sleeping for query job execution...")
    sleep_for(300)  # max queue dwell time
    sleep_for(180)  # observed job-started bootstrapping time

    wait_for_query_job(job_id)

    logger.info("Sleeping for download job execution...")
    sleep_for(300)  # max queue dwell time
    sleep_for(180)  # observed job-started bootstrapping time
    wait_for_download_job(job_id, index="hls_catalog-*")

    logger.info("CHECKING FOR L3 ENTRIES, INDICATING SUCCESSFUL PGE EXECUTION")

    logger.info("Sleeping for PGE execution...")
    sleep_for(300)  # max queue dwell time
    sleep_for(180)  # observed job-started bootstrapping time

    response = wait_for_l3(_id="OPERA_L3_DSWx-HLS_T53HQV_20220101T003711Z_", index="grq_v2.0_l3_dswx_hls-*", query_name="match_phrase")
    assert re.match(r"OPERA_L3_DSWx-HLS_T53HQV_20220101T003711Z_(\d+)T(\d+)Z_S2A_30_v2.0", response.hits[0]["id"])

    logger.info("CHECKING FOR CNM-S SUCCESS")

    logger.info("Sleeping for CNM-S execution...")
    sleep_for(300)  # max queue dwell time
    sleep_for(180)  # observed job-started bootstrapping time

    response = wait_for_cnm_s_success(_id=response.hits[0]["id"], index="grq_v2.0_l3_dswx_hls-*")
    assert_cnm_s_success(response)

    logger.info("TRIGGER AND CHECK FOR CNM-R SUCCESS")
    mock_cnm_r_success_sns(id=response.hits[0]["id"])

    logger.info("Sleeping for CNM-R execution...")
    sleep_for(300)  # max queue dwell time
    sleep_for(180)  # observed job-started bootstrapping time

    response = wait_for_cnm_r_success(_id=response.hits[0]["id"], index="grq_v2.0_l3_dswx_hls-*")
    assert_cnm_r_success(response)


def test_subscriber_slc():
    logger.info("TRIGGERING DATA SUBSCRIBE")

    update_env_vars_slc_subscriber_query_lambda()
    sleep_for(30)

    response = invoke_slc_subscriber_query_lambda()

    reset_env_vars_slc_subscriber_query_lambda()
    sleep_for(30)

    assert response["StatusCode"] == 200

    job_id = response["Payload"].read().decode().strip("\"")
    logger.info(f"{job_id=}")

    logger.info("Sleeping for query job execution...")
    sleep_for(300)  # max queue dwell time
    sleep_for(180)  # observed job-started bootstrapping time

    wait_for_query_job(job_id)

    logger.info("Sleeping for download job execution...")
    sleep_for(300)  # max queue dwell time
    sleep_for(180)  # observed job-started bootstrapping time
    wait_for_download_job(job_id, index="slc_catalog-*")

    logger.info("TRIGGERING SLC IONOSPHERE DOWNLOAD")
    update_env_vars_subscriber_slc_ionosphere_download_lambda()
    sleep_for(30)
    response = invoke_slc_subscriber_ionosphere_download_lambda()
    assert response["StatusCode"] == 200
    job_id = response["Payload"].read().decode().strip("\"")
    logger.info(f"{job_id=}")

    logger.info("Sleeping for SLC ionosphere download job execution...")
    sleep_for(300)  # max queue dwell time
    sleep_for(180)  # observed job-started bootstrapping time

    wait_for_job(job_id)

    logger.info("CHECKING FOR L3 ENTRIES, INDICATING SUCCESSFUL PGE EXECUTION")

    logger.info("Sleeping for PGE execution...")
    sleep_for(300)  # max queue dwell time
    sleep_for(180)  # observed job-started bootstrapping time

    # CSLC

    # 18-IW1, 18-IW2, 18-IW3
    response_cslc_1 = wait_for_l3(_id="OPERA_L2_CSLC-S1_T064-135518-IW1_", index="grq_v0.1_l2_cslc_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L2_CSLC-S1_T064-135518-IW1_(\d+T\d+)Z_(\d+T\d+)Z_S1A_VV_v0.1", response_cslc_1.hits[0]["id"])
    response_cslc_2 = wait_for_l3(_id="OPERA_L2_CSLC-S1_T064-135518-IW2_", index="grq_v0.1_l2_cslc_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L2_CSLC-S1_T064-135518-IW2_(\d+T\d+)Z_(\d+T\d+)Z_S1A_VV_v0.1", response_cslc_2.hits[0]["id"])
    response_cslc_3 = wait_for_l3(_id="OPERA_L2_CSLC-S1_T064-135518-IW3_", index="grq_v0.1_l2_cslc_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L2_CSLC-S1_T064-135518-IW3_(\d+T\d+)Z_(\d+T\d+)Z_S1A_VV_v0.1", response_cslc_3.hits[0]["id"])

    # 19-IW1, 19-IW2, 19-IW3
    response_cslc_4 = wait_for_l3(_id="OPERA_L2_CSLC-S1_T064-135519-IW1_", index="grq_v0.1_l2_cslc_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L2_CSLC-S1_T064-135519-IW1_(\d+T\d+)Z_(\d+T\d+)Z_S1A_VV_v0.1", response_cslc_4.hits[0]["id"])
    response_cslc_5 = wait_for_l3(_id="OPERA_L2_CSLC-S1_T064-135519-IW2_", index="grq_v0.1_l2_cslc_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L2_CSLC-S1_T064-135519-IW2_(\d+T\d+)Z_(\d+T\d+)Z_S1A_VV_v0.1", response_cslc_5.hits[0]["id"])
    response_cslc_6 = wait_for_l3(_id="OPERA_L2_CSLC-S1_T064-135519-IW3_", index="grq_v0.1_l2_cslc_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L2_CSLC-S1_T064-135519-IW3_(\d+T\d+)Z_(\d+T\d+)Z_S1A_VV_v0.1", response_cslc_6.hits[0]["id"])

    # 20-IW1, 20-IW2, 20-IW3
    response_cslc_7 = wait_for_l3(_id="OPERA_L2_CSLC-S1_T064-135520-IW1_", index="grq_v0.1_l2_cslc_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L2_CSLC-S1_T064-135520-IW1_(\d+T\d+)Z_(\d+T\d+)Z_S1A_VV_v0.1", response_cslc_7.hits[0]["id"])
    response_cslc_8 = wait_for_l3(_id="OPERA_L2_CSLC-S1_T064-135520-IW2_", index="grq_v0.1_l2_cslc_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L2_CSLC-S1_T064-135520-IW2_(\d+T\d+)Z_(\d+T\d+)Z_S1A_VV_v0.1", response_cslc_8.hits[0]["id"])
    response_cslc_9 = wait_for_l3(_id="OPERA_L2_CSLC-S1_T064-135520-IW3_", index="grq_v0.1_l2_cslc_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L2_CSLC-S1_T064-135520-IW3_(\d+T\d+)Z_(\d+T\d+)Z_S1A_VV_v0.1", response_cslc_9.hits[0]["id"])

    # RTC

    # 70-IW1, 70-IW3
    response_rtc_1 = wait_for_l3(_id="OPERA_L2_RTC-S1_T069-147170-IW1_", index="grq_v0.1_l2_rtc_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L2_RTC-S1_T069-147170-IW1_(\d+T\d+)Z_(\d+T\d+)Z_S1A_30_v0.1", response_rtc_1.hits[0]["id"])
    response_rtc_2 = wait_for_l3(_id="OPERA_L2_RTC-S1_T069-147170-IW3_", index="grq_v0.1_l2_rtc_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L2_RTC-S1_T069-147170-IW3_(\d+T\d+)Z_(\d+T\d+)Z_S1A_30_v0.1", response_rtc_2.hits[0]["id"])

    # 71-IW1, 71-IW2, 71-IW3
    response_rtc_3 = wait_for_l3(_id="OPERA_L2_RTC-S1_T069-147171-IW1_", index="grq_v0.1_l2_rtc_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L2_RTC-S1_T069-147171-IW1_(\d+T\d+)Z_(\d+T\d+)Z_S1A_30_v0.1", response_rtc_3.hits[0]["id"])
    response_rtc_4 = wait_for_l3(_id="OPERA_L2_RTC-S1_T069-147171-IW2_", index="grq_v0.1_l2_rtc_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L2_RTC-S1_T069-147171-IW2_(\d+T\d+)Z_(\d+T\d+)Z_S1A_30_v0.1", response_rtc_4.hits[0]["id"])
    response_rtc_5 = wait_for_l3(_id="OPERA_L2_RTC-S1_T069-147171-IW3_", index="grq_v0.1_l2_rtc_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L2_RTC-S1_T069-147171-IW3_(\d+T\d+)Z_(\d+T\d+)Z_S1A_30_v0.1", response_rtc_5.hits[0]["id"])

    # 72-IW1, 72-IW2, 72-IW3
    response_rtc_6 = wait_for_l3(_id="OPERA_L2_RTC-S1_T069-147172-IW1_", index="grq_v0.1_l2_rtc_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L2_RTC-S1_T069-147172-IW1_(\d+T\d+)Z_(\d+T\d+)Z_S1A_30_v0.1", response_rtc_6.hits[0]["id"])
    response_rtc_7 = wait_for_l3(_id="OPERA_L2_RTC-S1_T069-147172-IW2_", index="grq_v0.1_l2_rtc_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L2_RTC-S1_T069-147172-IW2_(\d+T\d+)Z_(\d+T\d+)Z_S1A_30_v0.1", response_rtc_7.hits[0]["id"])
    response_rtc_8 = wait_for_l3(_id="OPERA_L2_RTC-S1_T069-147172-IW3_", index="grq_v0.1_l2_rtc_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L2_RTC-S1_T069-147172-IW3_(\d+T\d+)Z_(\d+T\d+)Z_S1A_30_v0.1", response_rtc_8.hits[0]["id"])

    # 73-IW1
    response_rtc_9 = wait_for_l3(_id="OPERA_L2_RTC-S1_T069-147173-IW1_", index="grq_v0.1_l2_rtc_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L2_RTC-S1_T069-147173-IW1_(\d+T\d+)Z_(\d+T\d+)Z_S1A_30_v0.1", response_rtc_9.hits[0]["id"])

    logger.info("CHECKING FOR CNM-S SUCCESS")

    logger.info("Sleeping for CNM-S execution...")
    sleep_for(300)  # max queue dwell time

    response = wait_for_cnm_s_success(_id=response_cslc_1.hits[0]["id"], index="grq_v0.1_l2_cslc_s1-*")
    assert_cnm_s_success(response)
    response = wait_for_cnm_s_success(_id=response_cslc_2.hits[0]["id"], index="grq_v0.1_l2_cslc_s1-*")
    assert_cnm_s_success(response)
    response = wait_for_cnm_s_success(_id=response_cslc_3.hits[0]["id"], index="grq_v0.1_l2_cslc_s1-*")
    assert_cnm_s_success(response)
    response = wait_for_cnm_s_success(_id=response_cslc_4.hits[0]["id"], index="grq_v0.1_l2_cslc_s1-*")
    assert_cnm_s_success(response)
    response = wait_for_cnm_s_success(_id=response_cslc_5.hits[0]["id"], index="grq_v0.1_l2_cslc_s1-*")
    assert_cnm_s_success(response)
    response = wait_for_cnm_s_success(_id=response_cslc_6.hits[0]["id"], index="grq_v0.1_l2_cslc_s1-*")
    assert_cnm_s_success(response)
    response = wait_for_cnm_s_success(_id=response_cslc_7.hits[0]["id"], index="grq_v0.1_l2_cslc_s1-*")
    assert_cnm_s_success(response)
    response = wait_for_cnm_s_success(_id=response_cslc_8.hits[0]["id"], index="grq_v0.1_l2_cslc_s1-*")
    assert_cnm_s_success(response)
    response = wait_for_cnm_s_success(_id=response_cslc_9.hits[0]["id"], index="grq_v0.1_l2_cslc_s1-*")
    assert_cnm_s_success(response)

    # 70-IW1, 70-IW3
    response = wait_for_cnm_s_success(_id=response_rtc_1.hits[0]["id"], index="grq_v0.1_l2_rtc_s1-*")
    assert_cnm_s_success(response)
    response = wait_for_cnm_s_success(_id=response_rtc_2.hits[0]["id"], index="grq_v0.1_l2_rtc_s1-*")
    assert_cnm_s_success(response)

    # 71-IW1, 71-IW2, 71-IW3
    response = wait_for_cnm_s_success(_id=response_rtc_3.hits[0]["id"], index="grq_v0.1_l2_rtc_s1-*")
    assert_cnm_s_success(response)
    response = wait_for_cnm_s_success(_id=response_rtc_4.hits[0]["id"], index="grq_v0.1_l2_rtc_s1-*")
    assert_cnm_s_success(response)
    response = wait_for_cnm_s_success(_id=response_rtc_5.hits[0]["id"], index="grq_v0.1_l2_rtc_s1-*")
    assert_cnm_s_success(response)

    # 72-IW1, 72-IW2, 72-IW3
    response = wait_for_cnm_s_success(_id=response_rtc_6.hits[0]["id"], index="grq_v0.1_l2_rtc_s1-*")
    assert_cnm_s_success(response)
    response = wait_for_cnm_s_success(_id=response_rtc_7.hits[0]["id"], index="grq_v0.1_l2_rtc_s1-*")
    assert_cnm_s_success(response)
    response = wait_for_cnm_s_success(_id=response_rtc_8.hits[0]["id"], index="grq_v0.1_l2_rtc_s1-*")
    assert_cnm_s_success(response)

    # 73-IW1
    response = wait_for_cnm_s_success(_id=response_rtc_9.hits[0]["id"], index="grq_v0.1_l2_rtc_s1-*")
    assert_cnm_s_success(response)

    logger.info("TRIGGER AND CHECK FOR CNM-R SUCCESS")
    # Note these file names use a dummy timestamp for production time
    mock_cnm_r_success_sqs(id=response_cslc_1.hits[0]["id"])
    mock_cnm_r_success_sqs(id=response_cslc_2.hits[0]["id"])
    mock_cnm_r_success_sqs(id=response_cslc_3.hits[0]["id"])
    mock_cnm_r_success_sqs(id=response_cslc_4.hits[0]["id"])
    mock_cnm_r_success_sqs(id=response_cslc_5.hits[0]["id"])
    mock_cnm_r_success_sqs(id=response_cslc_6.hits[0]["id"])
    mock_cnm_r_success_sqs(id=response_cslc_7.hits[0]["id"])
    mock_cnm_r_success_sqs(id=response_cslc_8.hits[0]["id"])
    mock_cnm_r_success_sqs(id=response_cslc_9.hits[0]["id"])

    # 70-IW1, 70-IW3
    mock_cnm_r_success_sqs(id=response_rtc_1.hits[0]["id"])
    mock_cnm_r_success_sqs(id=response_rtc_2.hits[0]["id"])

    # 71-IW1, 71-IW2, 71-IW3
    mock_cnm_r_success_sqs(id=response_rtc_3.hits[0]["id"])
    mock_cnm_r_success_sqs(id=response_rtc_4.hits[0]["id"])
    mock_cnm_r_success_sqs(id=response_rtc_5.hits[0]["id"])

    # 72-IW1, 72-IW2, 72-IW3
    mock_cnm_r_success_sqs(id=response_rtc_6.hits[0]["id"])
    mock_cnm_r_success_sqs(id=response_rtc_7.hits[0]["id"])
    mock_cnm_r_success_sqs(id=response_rtc_8.hits[0]["id"])

    # 73-IW1
    mock_cnm_r_success_sqs(id=response_rtc_9.hits[0]["id"])

    logger.info("Sleeping for CNM-R execution...")
    sleep_for(300)  # max queue dwell time

    response = wait_for_cnm_r_success(_id=response_cslc_1.hits[0]["id"], index="grq_v0.1_l2_cslc_s1-*")
    assert_cnm_r_success(response)
    response = wait_for_cnm_r_success(_id=response_cslc_2.hits[0]["id"], index="grq_v0.1_l2_cslc_s1-*")
    assert_cnm_r_success(response)
    response = wait_for_cnm_r_success(_id=response_cslc_3.hits[0]["id"], index="grq_v0.1_l2_cslc_s1-*")
    assert_cnm_r_success(response)
    response = wait_for_cnm_r_success(_id=response_cslc_4.hits[0]["id"], index="grq_v0.1_l2_cslc_s1-*")
    assert_cnm_r_success(response)
    response = wait_for_cnm_r_success(_id=response_cslc_5.hits[0]["id"], index="grq_v0.1_l2_cslc_s1-*")
    assert_cnm_r_success(response)
    response = wait_for_cnm_r_success(_id=response_cslc_6.hits[0]["id"], index="grq_v0.1_l2_cslc_s1-*")
    assert_cnm_r_success(response)
    response = wait_for_cnm_r_success(_id=response_cslc_7.hits[0]["id"], index="grq_v0.1_l2_cslc_s1-*")
    assert_cnm_r_success(response)
    response = wait_for_cnm_r_success(_id=response_cslc_8.hits[0]["id"], index="grq_v0.1_l2_cslc_s1-*")
    assert_cnm_r_success(response)
    response = wait_for_cnm_r_success(_id=response_cslc_9.hits[0]["id"], index="grq_v0.1_l2_cslc_s1-*")
    assert_cnm_r_success(response)

    # 70-IW1, 70-IW3
    response = wait_for_cnm_r_success(_id=response_rtc_1.hits[0]["id"], index="grq_v0.1_l2_rtc_s1-*")
    assert_cnm_r_success(response)
    response = wait_for_cnm_r_success(_id=response_rtc_2.hits[0]["id"], index="grq_v0.1_l2_rtc_s1-*")
    assert_cnm_r_success(response)

    # 71-IW1, 71-IW2, 71-IW3
    response = wait_for_cnm_r_success(_id=response_rtc_3.hits[0]["id"], index="grq_v0.1_l2_rtc_s1-*")
    assert_cnm_r_success(response)
    response = wait_for_cnm_r_success(_id=response_rtc_4.hits[0]["id"], index="grq_v0.1_l2_rtc_s1-*")
    assert_cnm_r_success(response)
    response = wait_for_cnm_r_success(_id=response_rtc_5.hits[0]["id"], index="grq_v0.1_l2_rtc_s1-*")
    assert_cnm_r_success(response)

    # 72-IW1, 72-IW2, 72-IW3
    response = wait_for_cnm_r_success(_id=response_rtc_6.hits[0]["id"], index="grq_v0.1_l2_rtc_s1-*")
    assert_cnm_r_success(response)
    response = wait_for_cnm_r_success(_id=response_rtc_7.hits[0]["id"], index="grq_v0.1_l2_rtc_s1-*")
    assert_cnm_r_success(response)
    response = wait_for_cnm_r_success(_id=response_rtc_8.hits[0]["id"], index="grq_v0.1_l2_rtc_s1-*")
    assert_cnm_r_success(response)

    # 73-IW1
    response = wait_for_cnm_r_success(_id=response_rtc_9.hits[0]["id"], index="grq_v0.1_l2_rtc_s1-*")
    assert_cnm_r_success(response)


def test_subscriber_rtc():
    logger.info("TRIGGERING DATA SUBSCRIBE")

    update_env_vars_rtc_subscriber_query_lambda()
    sleep_for(30)

    response = invoke_rtc_subscriber_query_lambda()

    reset_env_vars_rtc_subscriber_query_lambda()
    sleep_for(30)

    assert response["StatusCode"] == 200

    job_id = response["Payload"].read().decode().strip("\"")
    logger.info(f"{job_id=}")

    logger.info("Sleeping for query job execution...")
    sleep_for(300)  # max queue dwell time
    sleep_for(180)  # observed job-started bootstrapping time

    wait_for_query_job(job_id)

    logger.info("Sleeping for download job execution...")
    sleep_for(300)  # max queue dwell time
    sleep_for(180)  # observed job-started bootstrapping time
    wait_for_download_job(job_id, index="rtc_catalog-*")

    logger.info("CHECKING FOR L3 ENTRIES, INDICATING SUCCESSFUL PGE EXECUTION")

    logger.info("Sleeping for PGE execution...")
    sleep_for(300)  # max queue dwell time
    sleep_for(180)  # observed job-started bootstrapping time

    response_dswx_s1_1 = wait_for_l3(_id="OPERA_L3_DSWx-S1_T18MWA_", index="grq_v0.1_l3_dswx_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L3_DSWx-S1_T18MWA_(\d+T\d+)Z_(\d+T\d+)Z_S1A_30_v0.1", response_dswx_s1_1.hits[0]["id"])
    response_dswx_s1_2 = wait_for_l3(_id="OPERA_L3_DSWx-S1_T18MVA_", index="grq_v0.1_l3_dswx_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L3_DSWx-S1_T18MVA_(\d+T\d+)Z_(\d+T\d+)Z_S1A_30_v0.1", response_dswx_s1_2.hits[0]["id"])
    response_dswx_s1_3 = wait_for_l3(_id="OPERA_L3_DSWx-S1_T18MWV_", index="grq_v0.1_l3_dswx_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L3_DSWx-S1_T18MWV_(\d+T\d+)Z_(\d+T\d+)Z_S1A_30_v0.1", response_dswx_s1_3.hits[0]["id"])
    response_dswx_s1_4 = wait_for_l3(_id="OPERA_L3_DSWx-S1_T18MWU_", index="grq_v0.1_l3_dswx_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L3_DSWx-S1_T18MWU_(\d+T\d+)Z_(\d+T\d+)Z_S1A_30_v0.1", response_dswx_s1_4.hits[0]["id"])
    response_dswx_s1_5 = wait_for_l3(_id="OPERA_L3_DSWx-S1_T18MXV_", index="grq_v0.1_l3_dswx_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L3_DSWx-S1_T18MXV_(\d+T\d+)Z_(\d+T\d+)Z_S1A_30_v0.1", response_dswx_s1_5.hits[0]["id"])
    response_dswx_s1_6 = wait_for_l3(_id="OPERA_L3_DSWx-S1_T18MVT_", index="grq_v0.1_l3_dswx_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L3_DSWx-S1_T18MVT_(\d+T\d+)Z_(\d+T\d+)Z_S1A_30_v0.1", response_dswx_s1_6.hits[0]["id"])
    response_dswx_s1_7 = wait_for_l3(_id="OPERA_L3_DSWx-S1_T18MVV_", index="grq_v0.1_l3_dswx_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L3_DSWx-S1_T18MVV_(\d+T\d+)Z_(\d+T\d+)Z_S1A_30_v0.1", response_dswx_s1_7.hits[0]["id"])
    response_dswx_s1_8 = wait_for_l3(_id="OPERA_L3_DSWx-S1_T18MWT_", index="grq_v0.1_l3_dswx_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L3_DSWx-S1_T18MWT_(\d+T\d+)Z_(\d+T\d+)Z_S1A_30_v0.1", response_dswx_s1_8.hits[0]["id"])
    response_dswx_s1_9 = wait_for_l3(_id="OPERA_L3_DSWx-S1_T18MXA_", index="grq_v0.1_l3_dswx_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L3_DSWx-S1_T18MXA_(\d+T\d+)Z_(\d+T\d+)Z_S1A_30_v0.1", response_dswx_s1_9.hits[0]["id"])
    response_dswx_s1_10 = wait_for_l3(_id="OPERA_L3_DSWx-S1_T18MXT_", index="grq_v0.1_l3_dswx_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L3_DSWx-S1_T18MXT_(\d+T\d+)Z_(\d+T\d+)Z_S1A_30_v0.1", response_dswx_s1_10.hits[0]["id"])
    response_dswx_s1_11= wait_for_l3(_id="OPERA_L3_DSWx-S1_T18MXU_", index="grq_v0.1_l3_dswx_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L3_DSWx-S1_T18MXU_(\d+T\d+)Z_(\d+T\d+)Z_S1A_30_v0.1", response_dswx_s1_11.hits[0]["id"])
    response_dswx_s1_12 = wait_for_l3(_id="OPERA_L3_DSWx-S1_T18MVU_", index="grq_v0.1_l3_dswx_s1-*", query_name="match_phrase")
    assert re.match(r"OPERA_L3_DSWx-S1_T18MVU_(\d+T\d+)Z_(\d+T\d+)Z_S1A_30_v0.1", response_dswx_s1_12.hits[0]["id"])

    logger.info("CHECKING FOR CNM-S SUCCESS")

    logger.info("Sleeping for CNM-S execution...")
    sleep_for(300)  # max queue dwell time

    response_dswx_s1_1 = wait_for_cnm_s_success(_id=response_dswx_s1_1.hits[0]["id"], index="grq_v0.1_l3_dswx_s1-*")
    assert_cnm_s_success(response_dswx_s1_1)
    response_dswx_s1_2 = wait_for_cnm_s_success(_id=response_dswx_s1_2.hits[0]["id"], index="grq_v0.1_l3_dswx_s1-*")
    assert_cnm_s_success(response_dswx_s1_2)
    response_dswx_s1_3 = wait_for_cnm_s_success(_id=response_dswx_s1_3.hits[0]["id"], index="grq_v0.1_l3_dswx_s1-*")
    assert_cnm_s_success(response_dswx_s1_3)
    response_dswx_s1_4 = wait_for_cnm_s_success(_id=response_dswx_s1_4.hits[0]["id"], index="grq_v0.1_l3_dswx_s1-*")
    assert_cnm_s_success(response_dswx_s1_4)
    response_dswx_s1_5 = wait_for_cnm_s_success(_id=response_dswx_s1_5.hits[0]["id"], index="grq_v0.1_l3_dswx_s1-*")
    assert_cnm_s_success(response_dswx_s1_5)
    response_dswx_s1_6 = wait_for_cnm_s_success(_id=response_dswx_s1_6.hits[0]["id"], index="grq_v0.1_l3_dswx_s1-*")
    assert_cnm_s_success(response_dswx_s1_6)
    response_dswx_s1_7 = wait_for_cnm_s_success(_id=response_dswx_s1_7.hits[0]["id"], index="grq_v0.1_l3_dswx_s1-*")
    assert_cnm_s_success(response_dswx_s1_7)
    response_dswx_s1_8 = wait_for_cnm_s_success(_id=response_dswx_s1_8.hits[0]["id"], index="grq_v0.1_l3_dswx_s1-*")
    assert_cnm_s_success(response_dswx_s1_8)
    response_dswx_s1_9 = wait_for_cnm_s_success(_id=response_dswx_s1_9.hits[0]["id"], index="grq_v0.1_l3_dswx_s1-*")
    assert_cnm_s_success(response_dswx_s1_9)
    response_dswx_s1_10 = wait_for_cnm_s_success(_id=response_dswx_s1_10.hits[0]["id"], index="grq_v0.1_l3_dswx_s1-*")
    assert_cnm_s_success(response_dswx_s1_10)
    response_dswx_s1_11 = wait_for_cnm_s_success(_id=response_dswx_s1_11.hits[0]["id"], index="grq_v0.1_l3_dswx_s1-*")
    assert_cnm_s_success(response_dswx_s1_11)
    response_dswx_s1_12 = wait_for_cnm_s_success(_id=response_dswx_s1_12.hits[0]["id"], index="grq_v0.1_l3_dswx_s1-*")
    assert_cnm_s_success(response_dswx_s1_12)

    logger.info("TRIGGER AND CHECK FOR CNM-R SUCCESS")
    # Note these file names use a dummy timestamp for production time
    mock_cnm_r_success_sqs(id=response_dswx_s1_1.hits[0]["id"])
    mock_cnm_r_success_sqs(id=response_dswx_s1_2.hits[0]["id"])
    mock_cnm_r_success_sqs(id=response_dswx_s1_3.hits[0]["id"])
    mock_cnm_r_success_sqs(id=response_dswx_s1_4.hits[0]["id"])
    mock_cnm_r_success_sqs(id=response_dswx_s1_5.hits[0]["id"])
    mock_cnm_r_success_sqs(id=response_dswx_s1_6.hits[0]["id"])
    mock_cnm_r_success_sqs(id=response_dswx_s1_7.hits[0]["id"])
    mock_cnm_r_success_sqs(id=response_dswx_s1_8.hits[0]["id"])
    mock_cnm_r_success_sqs(id=response_dswx_s1_9.hits[0]["id"])
    mock_cnm_r_success_sqs(id=response_dswx_s1_10.hits[0]["id"])
    mock_cnm_r_success_sqs(id=response_dswx_s1_11.hits[0]["id"])
    mock_cnm_r_success_sqs(id=response_dswx_s1_12.hits[0]["id"])

    logger.info("Sleeping for CNM-R execution...")
    sleep_for(300)  # max queue dwell time

    response_dswx_s1_1 = wait_for_cnm_r_success(_id=response_dswx_s1_1.hits[0]["id"], index="grq_v0.1_l3_dswx_s1-*")
    assert_cnm_r_success(response_dswx_s1_1)
    response_dswx_s1_2 = wait_for_cnm_r_success(_id=response_dswx_s1_2.hits[0]["id"], index="grq_v0.1_l3_dswx_s1-*")
    assert_cnm_r_success(response_dswx_s1_2)
    response_dswx_s1_3 = wait_for_cnm_r_success(_id=response_dswx_s1_3.hits[0]["id"], index="grq_v0.1_l3_dswx_s1-*")
    assert_cnm_r_success(response_dswx_s1_3)
    response_dswx_s1_4 = wait_for_cnm_r_success(_id=response_dswx_s1_4.hits[0]["id"], index="grq_v0.1_l3_dswx_s1-*")
    assert_cnm_r_success(response_dswx_s1_4)
    response_dswx_s1_5 = wait_for_cnm_r_success(_id=response_dswx_s1_5.hits[0]["id"], index="grq_v0.1_l3_dswx_s1-*")
    assert_cnm_r_success(response_dswx_s1_5)
    response_dswx_s1_6 = wait_for_cnm_r_success(_id=response_dswx_s1_6.hits[0]["id"], index="grq_v0.1_l3_dswx_s1-*")
    assert_cnm_r_success(response_dswx_s1_6)
    response_dswx_s1_7 = wait_for_cnm_r_success(_id=response_dswx_s1_7.hits[0]["id"], index="grq_v0.1_l3_dswx_s1-*")
    assert_cnm_r_success(response_dswx_s1_7)
    response_dswx_s1_8 = wait_for_cnm_r_success(_id=response_dswx_s1_8.hits[0]["id"], index="grq_v0.1_l3_dswx_s1-*")
    assert_cnm_r_success(response_dswx_s1_8)
    response_dswx_s1_9 = wait_for_cnm_r_success(_id=response_dswx_s1_9.hits[0]["id"], index="grq_v0.1_l3_dswx_s1-*")
    assert_cnm_r_success(response_dswx_s1_9)
    response_dswx_s1_10 = wait_for_cnm_r_success(_id=response_dswx_s1_10.hits[0]["id"], index="grq_v0.1_l3_dswx_s1-*")
    assert_cnm_r_success(response_dswx_s1_10)
    response_dswx_s1_11 = wait_for_cnm_r_success(_id=response_dswx_s1_11.hits[0]["id"], index="grq_v0.1_l3_dswx_s1-*")
    assert_cnm_r_success(response_dswx_s1_11)
    response_dswx_s1_12 = wait_for_cnm_r_success(_id=response_dswx_s1_12.hits[0]["id"], index="grq_v0.1_l3_dswx_s1-*")
    assert_cnm_r_success(response_dswx_s1_12)


@pytest.mark.asyncio
async def test_subscriber_rtc_trigger_logic():
    regression_test_results = Path(__file__).parent.parent.parent.joinpath("target", "results_test_subscriber_rtc_trigger_logic")
    assert regression_test_results.exists()
    with regression_test_results.open() as fp:
        assert "PASS" in fp.read().strip()

    regression_test_results = Path(__file__).parent.parent.parent.joinpath("target", "results_test_subscriber_rtc_trigger_logic_b")
    assert regression_test_results.exists()
    with regression_test_results.open() as fp:
        assert "PASS" in fp.read().strip()



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
    logger.info(f"Sleeping for {sec} seconds...")
    time.sleep(sec)
    logger.info("Done sleeping.")
