import random
import asyncio
import sys
import json
from time import sleep
from datetime import datetime, timedelta
import logging
from pathlib import Path
import netrc
from util.conf_util import SettingsConf
from data_subscriber.cslc.cslc_catalog import CSLCProductCatalog
from data_subscriber import daac_data_subscriber, query, cslc_utils
from data_subscriber.cslc import cslc_query
from data_subscriber.aws_token import supply_token
from data_subscriber.cmr import CMR_TIME_FORMAT

DT_FORMAT = CMR_TIME_FORMAT

"""This test runs cslc query several times in succession and verifies download jobs submitted
This test is run as a regular python script as opposed to pytest
This test requires a GRQ ES instance. Best to run this on a Mozart box in a functional cluster.
Set ASG Max of cslc_download worker to 0 to prevent it from running if that's desired."""

# k comes from the input json file. We could parameterize other argments too if desired.
query_arguments = ["query", "-c", "OPERA_L2_CSLC-S1_V1", "--chunk-size=1"]#, "--no-schedule-download"]
base_args = daac_data_subscriber.create_parser().parse_args(query_arguments)
settings = SettingsConf().cfg
cmr = settings["DAAC_ENVIRONMENTS"][base_args.endpoint]["BASE_URL"]
edl = settings["DAAC_ENVIRONMENTS"][base_args.endpoint]["EARTHDATA_LOGIN"]
username, _, password = netrc.netrc().authenticators(edl)
token = supply_token(edl, username, password)
es_conn = CSLCProductCatalog(logging.getLogger(__name__))

# Clear the elasticsearch index if clear argument is passed
if len(sys.argv) > 2 and sys.argv[2] == "clear":
    for index in es_conn.es.es.indices.get_alias(index="*").keys():
        if "cslc_catalog" in index:
            logging.info("Deleting index: " + index)
            es_conn.es.es.indices.delete(index=index, ignore=[400, 404])

disp_burst_map, burst_to_frame, metadata, version = cslc_utils.localize_disp_frame_burst_json()

def group_by_download_batch_id(granules):
    """Group granules by download batch id"""
    batch_id_to_granules = {}
    for granule in granules:
        download_batch_id = granule["download_batch_id"]
        if download_batch_id not in batch_id_to_granules:
            batch_id_to_granules[download_batch_id] = []
        batch_id_to_granules[download_batch_id].append(granule)
    return batch_id_to_granules

async def run_query(validation_json):
    """Run query several times over a date range specifying the start and stop dates"""

    # Open the scenario file and parse it. Get k from it and add as parameter.
    # Start and end dates are the min and max dates in the file.
    j = json.load(open(validation_json))
    cslc_k = j["k"]
    proc_mode = j["processing_mode"]
    validation_data = j["validation_data"]

    # Sleep map is optional
    sleep_map = {}
    if "sleep_seconds" in j:
        sleep_map = j["sleep_seconds"]

    query_arguments.extend([f"--k={cslc_k}", f"--processing-mode={proc_mode}"])

    if (proc_mode == "forward"):
        datetimes = sorted(validation_data.keys())
        start_date = datetime.strptime(datetimes[0], DT_FORMAT)
        end_date = datetime.strptime(datetimes[-1], DT_FORMAT) + timedelta(hours=1)

        # Run in 1 hour increments from start date to end date
        while start_date < end_date:

            # Sleep if this start_date is in the sleep map
            if start_date.strftime(DT_FORMAT) in sleep_map:
                sleep_seconds = sleep_map[start_date.strftime(DT_FORMAT)]
                logging.info(f"Sleeping for {sleep_seconds} seconds")
                sleep(sleep_seconds)

            new_end_date = start_date + timedelta(hours=1)
            current_args = query_arguments + [f"--grace-mins={j['grace_mins']}", f"--start-date={start_date.isoformat()}Z", f"--end-date={new_end_date.isoformat()}Z"]
            await query_and_validate(current_args, start_date.strftime(DT_FORMAT), validation_data)

            start_date = new_end_date # To the next query time range
    elif (proc_mode == "reprocessing"):
        # Run one native id at a time
        for native_id in validation_data.keys():
            current_args = query_arguments + [f"--native-id={native_id}", "--job-queue=opera-job_worker-cslc_data_download"]
            await query_and_validate(current_args, native_id, validation_data)
    elif (proc_mode == "historical"):
        # Run one frame range at a time over the data date range
        data_start_date = j["data_start_date"]
        data_end_date = (datetime.strptime(data_start_date, DT_FORMAT) + timedelta(days=cslc_k * 12)).isoformat() + "Z"
        for frame_range in validation_data.keys():
            current_args = query_arguments + [f"--frame-range={frame_range}", "--job-queue=opera-job_worker-cslc_data_download_hist",
                                              f"--start-date={data_start_date}", f"--end-date={data_end_date}",
                                              "--use-temporal"]
            await query_and_validate(current_args, frame_range, validation_data)

async def query_and_validate(current_args, test_range, validation_data):
    print("Querying with args: " + " ".join(current_args))
    args = daac_data_subscriber.create_parser().parse_args(current_args)
    c_query = cslc_query.CslcCmrQuery(args, token, es_conn, cmr, "job_id", settings,
                                      cslc_utils.DISP_FRAME_BURST_MAP_JSON)
    q_result = await c_query.run_query(args, token, es_conn, cmr, "job_id", settings)
    q_result = q_result["download_granules"]
    print("+++++++++++++++++++++++++++++++++++++++++++++++")
    # for r in q_result:
    #    print(r["granule_id"])
    q_result_dict = group_by_download_batch_id(q_result)
    print(f'"{test_range}" :' + ' { ')
    for k, v in q_result_dict.items():
        print(f'"{k}": {len(v)},')
    print('}')
    print(len(q_result))
    '''Looks like this  {"f26694_a149": 27,
                         "f26694_a148": 27}'''
    print("+++++++++++++++++++++++++++++++++++++++++++++++")

    # Validation
    validate_hour(q_result_dict, test_range, validation_data)

def validate_hour(q_result_dict, test_range, validation_data):
    """Validate the number of files to be downloaded for a given hour"""
    expected_files = validation_data[test_range]
    expected_count = sum(expected_files.values())
    q_result_count = sum([len(l) for l in q_result_dict.values()])
    logging.info(f"For {test_range}, we should have {expected_count} files ready to download")
    assert q_result_count == expected_count

    for batch_id, count in expected_files.items():
        logging.info(f"Batch id {batch_id} should have {count} files ready to download")
        assert len(q_result_dict[batch_id]) == count

test_start_time = datetime.now()
validation_json = sys.argv[1]
asyncio.run(run_query(validation_json))

logging.info("If no assertion errors were raised, then the test passed.")
logging.info(f"Test took {datetime.now() - test_start_time} seconds to run")
