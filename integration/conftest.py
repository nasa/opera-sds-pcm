import json
import logging
import os

import pytest
from dotenv import dotenv_values
from filelock import FileLock

config = {
    **dotenv_values(".env"),
    **os.environ
}

logging.getLogger("elasticsearch").setLevel("WARN")
logging.getLogger("botocore").setLevel("WARN")


def pytest_configure(config: pytest.Config):
    worker_id = os.environ.get("PYTEST_XDIST_WORKER")
    if worker_id is not None:
        logging.basicConfig(
            format=config.getini("log_file_format"),
            filename=f"target/reports/tests_{worker_id}.log",
            level=config.getini("log_file_level"),
        )


@pytest.fixture(scope="session", autouse=True)
def setup_session(tmp_path_factory, worker_id):
    logging.info("*****SETUP SESSION*****")

    if worker_id == "master":
        logging.debug("Single worker mode detected.")
        logging.info("Executing session setup.")
        # not executing in with multiple workers, just produce the data and let
        # pytest's fixture caching do its job
        clear_pcm_test_state()
        return

    logging.debug("Multiple worker mode detected.")

        # get the temp directory shared by all workers
    root_tmp_dir = tmp_path_factory.getbasetemp().parent

    fn = root_tmp_dir / "data.json"
    with FileLock(str(fn) + ".lock"):
        if fn.is_file():
            logging.info("Session setup already executed by a different worker.")
            return
        else:
            logging.info("Executing session setup.")
            clear_pcm_test_state()
            fn.write_text(json.dumps({}))
    return


def clear_pcm_test_state():
    from int_test_util import \
        es_index_delete, \
        delete_output_files, \
        mozart_es_index_delete

    if not str2bool(config.get("CLEAR_DATA")):
        logging.info(f'Skipping data reset. {config.get("CLEAR_DATA")=}')
        return

    # clear job index to prevent job duplicates
    #  job duplicates are likely to happen
    #  when executing subscriber query jobs frequently
    mozart_es_index_delete("job_status-current")

    # clear data subscriber indexes
    es_index_delete("hls_catalog")
    es_index_delete("hls_spatial_catalog")
    es_index_delete("slc_catalog")

    # clear ingest data indexes
    es_index_delete("grq_1_l1_s1_slc")

    es_index_delete("grq_v2.0_l2_hls_l30")
    es_index_delete("grq_v2.0_l2_hls_s30")

    # clear PGE indexes
    es_index_delete("grq_1_l2_cslc_s1")
    es_index_delete("grq_v2.0_l3_dswx_hls")

    es_index_delete("jobs_accountability_catalog")

    # empty out S3 to make it easier to inspect test outputs and side effects
    delete_output_files(bucket=config["RS_BUCKET"], prefix="products/")


def str2bool(s):
    """Convert a string to a bool. None returns False."""
    if s is None:
        return False

    if s.lower() == "true":
        return True
    if s.lower() == "false":
        return False
