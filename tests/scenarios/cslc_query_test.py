#!/usr/bin/env python3

import asyncio
import json
import logging
import netrc
import requests
from time import sleep
from datetime import datetime, timedelta
import argparse
from data_subscriber import cslc_utils
from data_subscriber.aws_token import supply_token
from data_subscriber.cmr import CMR_TIME_FORMAT
from data_subscriber.cslc import cslc_query
from data_subscriber.cslc.cslc_catalog import CSLCProductCatalog
from data_subscriber.parser import create_parser
from util.conf_util import SettingsConf

DT_FORMAT = CMR_TIME_FORMAT

_rabbitmq_url = "https://localhost:15673/api/queues/%2F/"
_jobs_processed_queue = "jobs_processed"

"""This test runs cslc query several times in succession and verifies download jobs submitted
This test is run as a regular python script as opposed to pytest
This test requires a GRQ ES instance. Best to run this on a Mozart box in a functional cluster.
Set ASG Max of cslc_download worker to 0 to prevent it from running if that's desired."""

# k comes from the input json file. We could parameterize other argments too if desired.
# NOTE: We really can't use the --no-schedule-download in forward processing test because the download job
# marks the granule as downloaded in the ES. And that info is used to determine what to download in every interation.
# This can be used in historical and reprocessing but don't think there's enough value to make it inconsistent.
query_arguments = ["query", "-c", "OPERA_L2_CSLC-S1_V1", "--chunk-size=1"]#, "--no-schedule-download"]
base_args = create_parser().parse_args(query_arguments)
settings = SettingsConf().cfg
cmr = settings["DAAC_ENVIRONMENTS"][base_args.endpoint]["BASE_URL"]
edl = settings["DAAC_ENVIRONMENTS"][base_args.endpoint]["EARTHDATA_LOGIN"]
username, _, password = netrc.netrc().authenticators(edl)
token = supply_token(edl, username, password)
es_conn = CSLCProductCatalog(logging.getLogger(__name__))

# Create dict of proc_mode to job_queue
job_queue = {
    "forward":      "opera-job_worker-cslc_data_download",
    "reprocessing": "opera-job_worker-cslc_data_download",
    "historical":   "opera-job_worker-cslc_data_download_hist"
}

def group_by_download_batch_id(granules):
    """Group granules by download batch id"""
    batch_id_to_granules = {}
    for granule in granules:
        download_batch_id = granule["download_batch_id"]
        if download_batch_id not in batch_id_to_granules:
            batch_id_to_granules[download_batch_id] = []
        batch_id_to_granules[download_batch_id].append(granule)
    return batch_id_to_granules

def do_delete_queue(args, authorization, job_queue):
    '''Delete the job queue from rabbitmq. First check to make sure that the job_processed queue is empty.
    The jobs seem to move from job_processed queue to the actual queue'''
    if args.no_delete_jobs:
        logging.info("NOT deleting jobs. These may continue to run as verdi workers when this test is over.")
    else:
        # Delete the download job from rabbitmq
        logging.info(f"Purging {job_queue} queue. We don't need them to execute for this test.")
        sleep(10)
        response = requests.get(_rabbitmq_url+_jobs_processed_queue, auth=authorization, verify=False)
        num_jobs_processed = response.json()['messages']
        while num_jobs_processed > 0:
            print(f"sleeping for 10 seconds until {_jobs_processed_queue} is empty...")
            sleep(10)
            response = requests.delete(_rabbitmq_url + job_queue, auth=('hysdsops', password), verify=False)
            print(response.status_code)
            response = requests.get(_rabbitmq_url+_jobs_processed_queue, auth=authorization, verify=False)
            num_jobs_processed = response.json()['messages']
        sleep(15)
        response = requests.delete(_rabbitmq_url+job_queue, auth=('hysdsops', password), verify=False)
        print(response.status_code)

async def run_query(args, authorization):
    """Run query several times over a date range specifying the start and stop dates"""

    # Open the scenario file and parse it. Get k from it and add as parameter.
    # Start and end dates are the min and max dates in the file.

    j = json.load(open(args.validation_json))
    cslc_k = j["k"]
    cslc_m = j["m"]
    proc_mode = j["processing_mode"]
    validation_data = j["validation_data"]

    # Sleep map is optional
    sleep_map = {}
    if "sleep_seconds" in j:
        sleep_map = j["sleep_seconds"]

    query_arguments.extend([f"--k={cslc_k}", f"--m={cslc_m}", f"--processing-mode={proc_mode}"])

    if (proc_mode == "forward"):
        if validation_data == "load_test":
            start_date = datetime.strptime(j["load_test_start"], DT_FORMAT)
            end_date = datetime.strptime(j["load_test_end"], DT_FORMAT)

            while start_date < end_date:
                new_end_date = start_date + timedelta(hours=1)
                current_args = query_arguments + [f"--grace-mins={j['grace_mins']}",
                                                  f"--job-queue={job_queue[proc_mode]}", \
                                                  f"--start-date={start_date.isoformat()}Z",
                                                  f"--end-date={new_end_date.isoformat()}Z"]

                await query_and_validate(current_args, start_date.strftime(DT_FORMAT), None)

                start_date = new_end_date
        else:
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
                current_args = query_arguments + [f"--grace-mins={j['grace_mins']}", f"--job-queue={job_queue[proc_mode]}", \
                                                  f"--start-date={start_date.isoformat()}Z", f"--end-date={new_end_date.isoformat()}Z"]

                await query_and_validate(current_args, start_date.strftime(DT_FORMAT), validation_data)

                start_date = new_end_date # To the next query time range

    elif (proc_mode == "reprocessing"):
        if j["param_type"] == "native_id":
            # Run one native id at a time
            for native_id in validation_data.keys():
                current_args = query_arguments + [f"--native-id={native_id}", f"--job-queue={job_queue[proc_mode]}"]
                await query_and_validate(current_args, native_id, validation_data)
        elif j["param_type"] == "date_range":
            # Run one date range at a time
            for date_range in validation_data.keys():
                start_date = date_range.split(",")[0].strip()
                end_date = date_range.split(",")[1].strip()
                current_args = query_arguments + [f"--start-date={start_date}", f"--end-date={end_date}",  f"--job-queue={job_queue[proc_mode]}"]
                await query_and_validate(current_args, date_range, validation_data)

    elif (proc_mode == "historical"):
        # Run one frame range at a time over the data date range
        data_start_date = j["data_start_date"]
        data_end_date = (datetime.strptime(data_start_date, DT_FORMAT) + timedelta(days=cslc_k * 12)).isoformat() + "Z"
        for frame_range in validation_data.keys():
            current_args = query_arguments + [f"--frame-range={frame_range}", f"--job-queue={job_queue[proc_mode]}",
                                              f"--start-date={data_start_date}", f"--end-date={data_end_date}",
                                              "--use-temporal"]
            await query_and_validate(current_args, frame_range, validation_data)

    do_delete_queue(args, authorization, job_queue[proc_mode])

async def query_and_validate(current_args, test_range, validation_data=None):
    print("Querying with args: " + " ".join(current_args))
    args = create_parser().parse_args(current_args)
    c_query = cslc_query.CslcCmrQuery(args, token, es_conn, cmr, "job_id", settings,
                                      cslc_utils.DISP_FRAME_BURST_MAP_HIST)
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
    if validation_data:
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

parser = argparse.ArgumentParser()
parser.add_argument("validation_json", help="Input validation json file used to drive and validate this test")
parser.add_argument("--clear", dest="clear", help="Clear GRQ ES cslc_catalog before running this test. You usually want this", required=False)
parser.add_argument("--no-delete-jobs", dest="no_delete_jobs", help="By default the submitted downloaded jobs are deleted from the system.\
 Set this flag to let the jobs stay in the system", required=False)
args = parser.parse_args()

# Clear the elasticsearch index if clear argument is passed
if args.clear:
    for index in es_conn.es.es.indices.get_alias(index="*").keys():
        if "cslc_catalog" in index:
            logging.info("Deleting index: " + index)
            es_conn.es.es.indices.delete(index=index, ignore=[400, 404])

test_start_time = datetime.now()

''' Get password for the hysdsops user from the file ~/.creds and set up rabbit_mq channel
Looks like this:
rabbitmq-admin hysdsops xxxxxxxxx
redis single-password xxxxxxxxxxxxxxxxxxxxx
'''
with open('/export/home/hysdsops/.creds') as f:
    lines = f.readlines()
    for line in lines:
        if "rabbitmq-admin" in line:
            password = line.split()[2]
            break

asyncio.run(run_query(args, ('hysdsops', password)))

logging.info("If no assertion errors were raised, then the test passed.")
logging.info(f"Test took {datetime.now() - test_start_time} seconds to run")
