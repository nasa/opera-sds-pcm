import random
import asyncio
import sys
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

DT_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

"""This test runs cslc query several times in succession and verifies download jobs submitted
This test is run as a regular python script as opposed to pytest
This test requires a GRQ ES instance. Best to run this on a Mozart box in a functional cluster.
Set ASG Max of cslc_download worker to 0 to prevent it from running if that's desired."""

arguments = ["query", "-c", "OPERA_L2_CSLC-S1_V1", "--job-queue=opera-job_worker-cslc_data_download", "--processing-mode=forward", "--k=2", "--chunk-size=1"]#, "--no-schedule-download"]
base_args = daac_data_subscriber.create_parser().parse_args(arguments)
settings = SettingsConf().cfg
cmr = settings["DAAC_ENVIRONMENTS"][base_args.endpoint]["BASE_URL"]
edl = settings["DAAC_ENVIRONMENTS"][base_args.endpoint]["EARTHDATA_LOGIN"]
username, _, password = netrc.netrc().authenticators(edl)
token = supply_token(edl, username, password)
es_conn = CSLCProductCatalog(logging.getLogger(__name__))

# Clear the elasticsearch index if clear argument is passed
# TODO: Improve the way it's being cleared so that we don't have to specify the year and month in index name
if len(sys.argv) > 1 and sys.argv[1] == "clear":
    logging.info("Clearing CSLC index")
    es_conn.es.es.indices.delete(index="cslc_catalog-2024.01", ignore=[400, 404])

disp_burst_map, burst_to_frame, metadata, version = cslc_utils.process_disp_frame_burst_json(cslc_utils.DISP_FRAME_BURST_MAP_JSON)

def group_by_download_batch_id(granules):
    """Group granules by download batch id"""
    batch_id_to_granules = {}
    for granule in granules:
        download_batch_id = granule["download_batch_id"]
        if download_batch_id not in batch_id_to_granules:
            batch_id_to_granules[download_batch_id] = []
        batch_id_to_granules[download_batch_id].append(granule)
    return batch_id_to_granules

async def run_query():
    """Run query several times over a date range specifying the start and stop dates"""

    # Build validation dictionary
    date_time_expected_files_dict = {}
    date_time_expected_files_dict["2023-12-01T06:00:00Z"] = 0
    date_time_expected_files_dict["2023-12-01T07:00:00Z"] = 0
    date_time_expected_files_dict["2023-12-01T08:00:00Z"] = 1080
    '''f26694_a149: 27
        f26694_a148: 27
        f26693_a149: 27
        f26693_a148: 27
        f26692_a149: 27
        f26692_a148: 27
        f26691_a149: 27
        f26691_a148: 27
        f26690_a149: 27
        f26690_a148: 27
        f26689_a149: 27
        f26689_a148: 27
        f26688_a149: 27
        f26688_a148: 27
        f26687_a149: 27
        f26687_a148: 27
        f26684_a149: 27
        f26684_a148: 27
        f26683_a149: 27
        f26683_a148: 27
        f26682_a149: 27
        f26682_a148: 27
        f26681_a149: 27
        f26681_a148: 27
        f26680_a149: 27
        f26680_a148: 27
        f26679_a149: 27
        f26679_a148: 27
        f26678_a149: 27
        f26678_a148: 27
        f26435_a149: 27
        f26435_a148: 27
        f26434_a149: 27
        f26434_a148: 27
        f24733_a149: 27
        f24733_a148: 27
        f24725_a149: 27
        f24725_a148: 27
        f24724_a149: 27
        f24724_a148: 27'''
    date_time_expected_files_dict["2023-12-01T09:00:00Z"] = 162
    '''f26686_a149: 27
        f26686_a148: 27
        f26685_a149: 27
        f26685_a148: 27
        f26677_a149: 27
        f26677_a148: 27'''
    date_time_expected_files_dict["2023-12-01T10:00:00Z"] = 540
    '''f12908_a149: 27
        f12908_a148: 27
        f12907_a149: 27
        f12907_a148: 27
        f12906_a149: 27
        f12906_a148: 27
        f12905_a149: 27
        f12905_a148: 27
        f12904_a149: 27
        f12904_a148: 27
        f12903_a149: 27
        f12903_a148: 27
        f12902_a149: 27
        f12902_a148: 27
        f12901_a149: 27
        f12901_a148: 27
        f10859_a149: 27
        f10859_a148: 27
        f10855_a149: 27
        f10855_a148: 27
        540'''
    date_time_expected_files_dict["2023-12-01T11:00:00Z"] = 594
    '''f11116_a149: 27
        f11116_a148: 27
        f11115_a149: 27
        f11115_a148: 27
        f11114_a149: 27
        f11114_a148: 27
        f11113_a149: 27
        f11113_a148: 27
        f11112_a149: 27
        f11112_a148: 27
        f11111_a149: 27
        f11111_a148: 27
        f11110_a149: 27
        f11110_a148: 27
        f10860_a149: 27
        f10860_a148: 27
        f10858_a149: 27
        f10858_a148: 27
        f10857_a149: 27
        f10857_a148: 27
        f10856_a149: 27
        f10856_a148: 27'''
    date_time_expected_files_dict["2023-12-01T12:00:00Z"] = 0
    date_time_expected_files_dict["2023-12-01T13:00:00Z"] = 486
    '''f11639_a149: 27
            f11639_a148: 27
            f11634_a149: 27
            f11634_a148: 27
            f11633_a149: 27
            f11633_a148: 27
            f11632_a149: 27
            f11632_a148: 27
            f11631_a149: 27
            f11631_a148: 27
            f11630_a149: 27
            f11630_a148: 27
            f11629_a149: 27
            f11629_a148: 27
            f11628_a149: 27
            f11628_a148: 27
            f11627_a149: 27
            f11627_a148: 27'''
    date_time_expected_files_dict["2023-12-01T14:00:00Z"] = 0
    date_time_expected_files_dict["2023-12-01T15:00:00Z"] = 0
    date_time_expected_files_dict["2023-12-01T16:00:00Z"] = 108
    '''f12917_a149: 27
        f12917_a148: 27
        f12916_a149: 27
        f12916_a148: 27'''

    # TODO: These can be derived from the dict above
    start_date = datetime(2023, 12, 1, 6, 0, 0)
    end_date = datetime(2023, 12, 1, 17, 0, 0)

    # Run in 1 hour increments from start date to end date
    while start_date < end_date:
        new_end_date = start_date + timedelta(hours=1)
        current_args = arguments + [f"--start-date={start_date.isoformat()}Z", f"--end-date={new_end_date.isoformat()}Z"]
        args = daac_data_subscriber.create_parser().parse_args(current_args)
        c_query = cslc_query.CslcCmrQuery(args, token, es_conn, cmr, "job_id", settings, cslc_utils.DISP_FRAME_BURST_MAP_JSON)
        q_result = await c_query.run_query(args, token, es_conn, cmr, "job_id", settings)
        q_result = q_result["download_granules"]
        print("+++++++++++++++++++++++++++++++++++++++++++++++")
        #print(q_result)
        #for r in q_result:
        #    print(r["granule_id"])
        d = group_by_download_batch_id(q_result)
        for k, v in d.items():
            print(f"{k}: {len(v)}")
        print(len(q_result))
        '''Looks like this 
                f26694_a149: 27
                f26694_a148: 27
                f26693_a149: 27
                f26693_a148: 27'''
        print("+++++++++++++++++++++++++++++++++++++++++++++++")

        # Validation
        # TODO: We might want to validate more than just the new files to be downloaded
        validate_hour(q_result, start_date, date_time_expected_files_dict)

        start_date = new_end_date # To the next query time range

        #sleep(1)

def validate_hour(q_result, start_date, date_time_expected_files_dict):
    """Validate the number of files to be downloaded for a given hour"""
    expected_files = date_time_expected_files_dict[start_date.strftime(DT_FORMAT)]
    logging.info(f"On datetime {start_date}, we should have {expected_files} files ready to download")
    assert len(q_result) == expected_files

now = datetime.now()
asyncio.run(run_query())

logging.info("If no assertion errors were raised, then the test passed.")
logging.info(f"Test took {datetime.now() - now} seconds to run")