import logging
import time
from pathlib import Path

import conftest
from int_test_util import \
    mock_cnm_r_success_sns, \
    upload_file, \
    wait_for_cnm_s_success, \
    wait_for_cnm_r_success, \
    wait_for_l2, \
    wait_for_l3, \
    wait_for_state_config
from subscriber_util import \
    wait_for_query_job, \
    wait_for_download_jobs, \
    invoke_l30_subscriber_query_lambda, \
    invoke_s30_subscriber_query_lambda, \
    update_env_vars_l30_subscriber_query_lambda, \
    reset_env_vars_l30_subscriber_query_lambda, \
    reset_env_vars_s30_subscriber_query_lambda, \
    update_env_vars_s30_subscriber_query_lambda

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
    sleep_for(150)

    wait_for_query_job(job_id)

    logging.info("Sleeping for download job execution...")
    sleep_for(150)
    wait_for_download_jobs(job_id)


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


def test_l30():
    logging.info("UPLOADING INPUT FILES")

    download_dir: Path = Path(config["L30_INPUT_DIR"]).expanduser()
    input_filepaths = [
        download_dir / "HLS.L30.T22VEQ.2021248T143156.v2.0.B02.tif",
        download_dir / "HLS.L30.T22VEQ.2021248T143156.v2.0.B03.tif",
        download_dir / "HLS.L30.T22VEQ.2021248T143156.v2.0.B04.tif",
        download_dir / "HLS.L30.T22VEQ.2021248T143156.v2.0.B05.tif",
        download_dir / "HLS.L30.T22VEQ.2021248T143156.v2.0.B06.tif",
        download_dir / "HLS.L30.T22VEQ.2021248T143156.v2.0.B07.tif",
        download_dir / "HLS.L30.T22VEQ.2021248T143156.v2.0.Fmask.tif"
    ]
    for i, input_filepath in enumerate(input_filepaths):
        logging.info(f"Uploading file {i+1} of {len(input_filepaths)}")
        upload_file(filepath=input_filepath)

    logging.info("CHECKING FOR L2 ENTRIES, INDICATING SUCCESSFUL DATA INGEST")

    logging.info("Sleeping for L2 ingest...")
    sleep_for(150)

    response = wait_for_l2(_id="HLS.L30.T22VEQ.2021248T143156.v2.0.B02", index="grq_v2.0_l2_hls_l30")
    assert response.hits[0]["id"] == "HLS.L30.T22VEQ.2021248T143156.v2.0.B02"

    response = wait_for_l2(_id="HLS.L30.T22VEQ.2021248T143156.v2.0.B03", index="grq_v2.0_l2_hls_l30")
    assert response.hits[0]["id"] == "HLS.L30.T22VEQ.2021248T143156.v2.0.B03"

    response = wait_for_l2(_id="HLS.L30.T22VEQ.2021248T143156.v2.0.B04", index="grq_v2.0_l2_hls_l30")
    assert response.hits[0]["id"] == "HLS.L30.T22VEQ.2021248T143156.v2.0.B04"

    response = wait_for_l2(_id="HLS.L30.T22VEQ.2021248T143156.v2.0.B05", index="grq_v2.0_l2_hls_l30")
    assert response.hits[0]["id"] == "HLS.L30.T22VEQ.2021248T143156.v2.0.B05"

    response = wait_for_l2(_id="HLS.L30.T22VEQ.2021248T143156.v2.0.B06", index="grq_v2.0_l2_hls_l30")
    assert response.hits[0]["id"] == "HLS.L30.T22VEQ.2021248T143156.v2.0.B06"

    response = wait_for_l2(_id="HLS.L30.T22VEQ.2021248T143156.v2.0.B07", index="grq_v2.0_l2_hls_l30")
    assert response.hits[0]["id"] == "HLS.L30.T22VEQ.2021248T143156.v2.0.B07"

    response = wait_for_l2(_id="HLS.L30.T22VEQ.2021248T143156.v2.0.Fmask", index="grq_v2.0_l2_hls_l30")
    assert response.hits[0]["id"] == "HLS.L30.T22VEQ.2021248T143156.v2.0.Fmask"

    logging.info("Sleeping for state-config execution...")
    sleep_for(150)
    response = wait_for_state_config(_id="HLS.L30.T22VEQ.2021248T143156.v2.0_state_config", index="grq_1_l2_hls_l30-state-config")
    assert response.hits[0]["id"] == "HLS.L30.T22VEQ.2021248T143156.v2.0_state_config"

    logging.info("CHECKING FOR L3 ENTRIES, INDICATING SUCCESSFUL PGE EXECUTION")

    logging.info("Sleeping for PGE execution...")
    sleep_for(150)

    response = wait_for_l3(_id="OPERA_L3_DSWx_HLS_T22VEQ_20210905T143156Z_20210905T143156Z_L8_30_v2.0", index="grq_v2.0_l3_dswx_hls")
    assert response.hits[0]["id"] == "OPERA_L3_DSWx_HLS_T22VEQ_20210905T143156Z_20210905T143156Z_L8_30_v2.0"

    logging.info("CHECKING FOR CNM-S SUCCESS")

    logging.info("Sleeping for CNM-S execution...")
    sleep_for(150)

    response = wait_for_cnm_s_success(_id="OPERA_L3_DSWx_HLS_T22VEQ_20210905T143156Z_20210905T143156Z_L8_30_v2.0", index="grq_v2.0_l3_dswx_hls")
    assert_cnm_s_success(response)

    logging.info("TRIGGER AND CHECK FOR CNM-R SUCCESS")
    mock_cnm_r_success_sns(id="OPERA_L3_DSWx_HLS_T22VEQ_20210905T143156Z_20210905T143156Z_L8_30_v2.0")

    logging.info("Sleeping for CNM-R execution...")
    sleep_for(150)

    response = wait_for_cnm_r_success(_id="OPERA_L3_DSWx_HLS_T22VEQ_20210905T143156Z_20210905T143156Z_L8_30_v2.0", index="grq_v2.0_l3_dswx_hls")
    assert_cnm_r_success(response)


def test_s30():
    logging.info("UPLOADING INPUT FILES")

    download_dir: Path = Path(config["S30_INPUT_DIR"]).expanduser()
    input_filepaths = [
        download_dir / "HLS.S30.T15SXR.2021250T163901.v2.0.B02.tif",
        download_dir / "HLS.S30.T15SXR.2021250T163901.v2.0.B03.tif",
        download_dir / "HLS.S30.T15SXR.2021250T163901.v2.0.B04.tif",
        download_dir / "HLS.S30.T15SXR.2021250T163901.v2.0.B11.tif",
        download_dir / "HLS.S30.T15SXR.2021250T163901.v2.0.B12.tif",
        download_dir / "HLS.S30.T15SXR.2021250T163901.v2.0.B8A.tif",
        download_dir / "HLS.S30.T15SXR.2021250T163901.v2.0.Fmask.tif"
    ]
    for i, input_filepath in enumerate(input_filepaths):
        logging.info(f"Uploading file {i+1} of {len(input_filepaths)}")
        upload_file(filepath=input_filepath)

    logging.info("CHECKING FOR L2 ENTRIES, INDICATING SUCCESSFUL DATA INGEST")

    logging.info("Sleeping for L2 ingest...")
    sleep_for(150)

    response = wait_for_l2(_id="HLS.S30.T15SXR.2021250T163901.v2.0.B02", index="grq_v2.0_l2_hls_s30")
    assert response.hits[0]["id"] == "HLS.S30.T15SXR.2021250T163901.v2.0.B02"

    response = wait_for_l2(_id="HLS.S30.T15SXR.2021250T163901.v2.0.B03", index="grq_v2.0_l2_hls_s30")
    assert response.hits[0]["id"] == "HLS.S30.T15SXR.2021250T163901.v2.0.B03"

    response = wait_for_l2(_id="HLS.S30.T15SXR.2021250T163901.v2.0.B04", index="grq_v2.0_l2_hls_s30")
    assert response.hits[0]["id"] == "HLS.S30.T15SXR.2021250T163901.v2.0.B04"

    response = wait_for_l2(_id="HLS.S30.T15SXR.2021250T163901.v2.0.B11", index="grq_v2.0_l2_hls_s30")
    assert response.hits[0]["id"] == "HLS.S30.T15SXR.2021250T163901.v2.0.B11"

    response = wait_for_l2(_id="HLS.S30.T15SXR.2021250T163901.v2.0.B12", index="grq_v2.0_l2_hls_s30")
    assert response.hits[0]["id"] == "HLS.S30.T15SXR.2021250T163901.v2.0.B12"

    response = wait_for_l2(_id="HLS.S30.T15SXR.2021250T163901.v2.0.B8A", index="grq_v2.0_l2_hls_s30")
    assert response.hits[0]["id"] == "HLS.S30.T15SXR.2021250T163901.v2.0.B8A"

    response = wait_for_l2(_id="HLS.S30.T15SXR.2021250T163901.v2.0.Fmask", index="grq_v2.0_l2_hls_s30")
    assert response.hits[0]["id"] == "HLS.S30.T15SXR.2021250T163901.v2.0.Fmask"

    logging.info("Sleeping for state-config execution...")
    sleep_for(150)
    response = wait_for_state_config(_id="HLS.S30.T15SXR.2021250T163901.v2.0_state_config", index="grq_1_l2_hls_s30-state-config")
    assert response.hits[0]["id"] == "HLS.S30.T15SXR.2021250T163901.v2.0_state_config"

    logging.info("CHECKING FOR L3 ENTRIES, INDICATING SUCCESSFUL PGE EXECUTION")

    logging.info("Sleeping for PGE execution...")
    sleep_for(150)

    response = wait_for_l3(_id="OPERA_L3_DSWx_HLS_T15SXR_20210907T163901Z_20210907T163901Z_S2A_30_v2.0", index="grq_v2.0_l3_dswx_hls")
    assert response.hits[0]["id"] == "OPERA_L3_DSWx_HLS_T15SXR_20210907T163901Z_20210907T163901Z_S2A_30_v2.0"

    logging.info("CHECKING FOR CNM-S SUCCESS")

    logging.info("Sleeping for CNM-S execution...")
    sleep_for(150)

    response = wait_for_cnm_s_success(_id="OPERA_L3_DSWx_HLS_T15SXR_20210907T163901Z_20210907T163901Z_S2A_30_v2.0", index="grq_v2.0_l3_dswx_hls")
    assert_cnm_s_success(response)

    logging.info("TRIGGER AND CHECK FOR CNM-R SUCCESS")
    mock_cnm_r_success_sns(id="OPERA_L3_DSWx_HLS_T15SXR_20210907T163901Z_20210907T163901Z_S2A_30_v2.0")

    logging.info("Sleeping for CNM-R execution...")
    sleep_for(150)

    mock_cnm_r_success_sns(id="OPERA_L3_DSWx_HLS_T15SXR_20210907T163901Z_20210907T163901Z_S2A_30_v2.0")
    response = wait_for_cnm_r_success(_id="OPERA_L3_DSWx_HLS_T15SXR_20210907T163901Z_20210907T163901Z_S2A_30_v2.0", index="grq_v2.0_l3_dswx_hls")

    assert_cnm_r_success(response)


def test_slc():
    logging.info("UPLOADING INPUT FILES")

    download_dir: Path = Path(config["SLC_INPUT_DIR"]).expanduser()
    input_filepaths = [
        download_dir / "S1A_IW_SLC__1SDV_20220501T015035_20220501T015102_043011_0522A4_42CC.zip"
    ]
    for i, input_filepath in enumerate(input_filepaths):
        logging.info(f"Uploading file {i+1} of {len(input_filepaths)}")
        upload_file(filepath=input_filepath)

    logging.info("CHECKING FOR L2 ENTRIES, INDICATING SUCCESSFUL DATA INGEST")

    logging.info("Sleeping for L2 ingest...")
    sleep_for(150)

    response = wait_for_l2(_id="S1A_IW_SLC__1SDV_20220501T015035_20220501T015104_043011_0522A4_42CC", index="grq_1_l1_s1_slc")
    assert response.hits[0]["id"] == "S1A_IW_SLC__1SDV_20220501T015035_20220501T015104_043011_0522A4_42CC"

    logging.info("CHECKING FOR L3 ENTRIES, INDICATING SUCCESSFUL PGE EXECUTION")

    logging.info("Sleeping for PGE execution...")
    sleep_for(150)

    response = wait_for_l3(_id="S1A_IW_SLC__1SDV_20220501T015035_20220501T015104_043011_0522A4_42CC", index="grq_1_l1_s1_slc")
    assert response.hits[0]["id"] == "S1A_IW_SLC__1SDV_20220501T015035_20220501T015104_043011_0522A4_42CC"

    logging.info("CHECKING FOR CNM-S SUCCESS")

    logging.info("Sleeping for CNM-S execution...")
    sleep_for(150)

    response = wait_for_cnm_s_success(_id="S1A_IW_SLC__1SDV_20220501T015035_20220501T015104_043011_0522A4_42CC", index="grq_1_l1_s1_slc")
    assert_cnm_s_success(response)

    logging.info("TRIGGER AND CHECK FOR CNM-R SUCCESS")
    mock_cnm_r_success_sns(id="S1A_IW_SLC__1SDV_20220501T015035_20220501T015104_043011_0522A4_42CC")

    logging.info("Sleeping for CNM-R execution...")
    sleep_for(150)

    response = wait_for_cnm_r_success(_id="S1A_IW_SLC__1SDV_20220501T015035_20220501T015104_043011_0522A4_42CC", index="grq_1_l1_s1_slc")
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
