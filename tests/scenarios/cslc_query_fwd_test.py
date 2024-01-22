import random
import asyncio
import sys
from datetime import datetime, timedelta
import logging
from pathlib import Path
import netrc
from util.conf_util import SettingsConf
from data_subscriber.cslc.cslc_catalog import CSLCProductCatalog
from data_subscriber import daac_data_subscriber, query, cslc_utils
from data_subscriber.cslc import cslc_query
from data_subscriber.aws_token import supply_token

"""This test runs cslc query several times in succession and verifies download jobs submitted
This test is run as a regular python script as opposed to pytest
This test requires a GRQ ES instance. Best to run this on a Mozart box in a functional cluster

TODO: Need to find a way to mark files as downloaded in the catalog for this test to be completely valid"""

arguments = ["query", "-c", "OPERA_L2_CSLC-S1_V1", "--job-queue=opera-job_worker-cslc_data_download" "--processing-mode=forward", "--use-temporal", "--k=2", "--chunk-size=1", "--no-schedule-download"]
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

    start_date = datetime(2023, 11, 1, 1, 0, 0)
    end_date = datetime(2023, 11, 1, 6, 0, 0) #TODO: Change 6 to 18

    # Run in 1 hour increments from start date to end date
    while start_date < end_date:
        new_end_date = start_date + timedelta(hours=1)
        current_args = arguments + [f"--start-date={start_date.isoformat()}Z", f"--end-date={new_end_date.isoformat()}Z"]
        args = daac_data_subscriber.create_parser().parse_args(current_args)
        c_query = cslc_query.CslcCmrQuery(args, token, es_conn, cmr, "job_id", settings, cslc_utils.DISP_FRAME_BURST_MAP_JSON)
        q_result = await c_query.run_query(args, token, es_conn, cmr, "job_id", settings)
        print("+++++++++++++++++++++++++++++++++++++++++++++++")
        #print(q_result)
        for r in q_result:
            print(r["granule_id"])
        d = group_by_download_batch_id(q_result)
        for k, v in d.items():
            print(f"{k}: {len(v)}")
        print(len(q_result))
        print("+++++++++++++++++++++++++++++++++++++++++++++++")

        # Validation
        # TODO: 1. We might want to validate more than just the new files to be downloaded
        # TODO: 2. Should restructure this to be less repetitive and easier to update once we figure out #1 above
        if (start_date == datetime(2023, 11, 1, 1, 0, 0)):
            expected_files = 108
            logging.info(f"On datetime {start_date}, we should have {expected_files} files ready to download")
            assert len(q_result) == expected_files
        if (start_date == datetime(2023, 11, 1, 2, 0, 0)):
            expected_files = 216
            logging.info(f"On datetime {start_date}, we should have {expected_files} files ready to download")
            assert len(q_result) == expected_files
        if (start_date == datetime(2023, 11, 1, 3, 0, 0)):
            expected_files = 216
            logging.info(f"On datetime {start_date}, we should have {expected_files} files ready to download")
            assert len(q_result) == expected_files
        if (start_date == datetime(2023, 11, 1, 4, 0, 0)):
            expected_files = 324
            logging.info(f"On datetime {start_date}, we should have {expected_files} files ready to download")
            assert len(q_result) == expected_files
        if (start_date == datetime(2023, 11, 1, 5, 0, 0)):
            expected_files = 432
            logging.info(f"On datetime {start_date}, we should have {expected_files} files ready to download")
            assert len(q_result) == expected_files
        '''if (start_date == datetime(2023, 11, 1, 6, 0, 0)):
            expected_files = 2
            logging.info(f"On datetime {start_date}, we should have {expected_files} files ready to download")
            assert len(q_result) == expected_files
        if (start_date == datetime(2023, 11, 1, 7, 0, 0)):
            expected_files = 2
            logging.info(f"On datetime {start_date}, we should have {expected_files} files ready to download")
            assert len(q_result) == expected_files
        if (start_date == datetime(2023, 11, 1, 8, 0, 0)):
            expected_files = 2
            logging.info(f"On datetime {start_date}, we should have {expected_files} files ready to download")
            assert len(q_result) == expected_files
        if (start_date == datetime(2023, 11, 1, 9, 0, 0)):
            expected_files = 2
            logging.info(f"On datetime {start_date}, we should have {expected_files} files ready to download")
            assert len(q_result) == expected_files
        if (start_date == datetime(2023, 11, 1, 10, 0, 0)):
            expected_files = 2
            logging.info(f"On datetime {start_date}, we should have {expected_files} files ready to download")
            assert len(q_result) == expected_files
        if (start_date == datetime(2023, 11, 1, 11, 0, 0)):
            expected_files = 2
            logging.info(f"On datetime {start_date}, we should have {expected_files} files ready to download")
            assert len(q_result) == expected_files
        if (start_date == datetime(2023, 11, 1, 13, 0, 0)):
            expected_files = 2
            logging.info(f"On datetime {start_date}, we should have {expected_files} files ready to download")
            assert len(q_result) == expected_files
        if (start_date == datetime(2023, 11, 1, 14, 0, 0)):
            expected_files = 2
            logging.info(f"On datetime {start_date}, we should have {expected_files} files ready to download")
            assert len(q_result) == expected_files
        if (start_date == datetime(2023, 11, 1, 15, 0, 0)):
            expected_files = 2
            logging.info(f"On datetime {start_date}, we should have {expected_files} files ready to download")
            assert len(q_result) == expected_files
        if (start_date == datetime(2023, 11, 1, 16, 0, 0)):
            expected_files = 2
            logging.info(f"On datetime {start_date}, we should have {expected_files} files ready to download")
            assert len(q_result) == expected_files
        if (start_date == datetime(2023, 11, 1, 17, 0, 0)):
            expected_files = 2
            logging.info(f"On datetime {start_date}, we should have {expected_files} files ready to download")
            assert len(q_result) == expected_files'''

        start_date = new_end_date

now = datetime.now()
asyncio.run(run_query())

logging.info("If no assertion errors were raised, then the test passed.")
logging.info(f"Test took {datetime.now() - now} seconds to run")