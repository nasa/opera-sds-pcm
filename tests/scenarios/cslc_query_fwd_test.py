import random
import asyncio
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
This test requires a GRQ ES instance. Best to run this on a Mozart box in a functional cluster"""

arguments = ["query", "-c", "OPERA_L2_CSLC-S1_V1", "--job-queue=opera-job_worker-cslc_data_download" "--processing-mode=forward", "--use-temporal", "--k=2", "--chunk-size=1", "--no-schedule-download"]
base_args = daac_data_subscriber.create_parser().parse_args(arguments)
settings = SettingsConf().cfg
cmr = settings["DAAC_ENVIRONMENTS"][base_args.endpoint]["BASE_URL"]
edl = settings["DAAC_ENVIRONMENTS"][base_args.endpoint]["EARTHDATA_LOGIN"]
username, _, password = netrc.netrc().authenticators(edl)
token = supply_token(edl, username, password)
es_conn = CSLCProductCatalog(logging.getLogger(__name__))

disp_burst_map, burst_to_frame, metadata, version = cslc_utils.process_disp_frame_burst_json(cslc_utils.DISP_FRAME_BURST_MAP_JSON)

async def run_query():
    """Run query several times over a date range specifying the start and stop dates"""

    start_date = datetime(2023, 12, 1, 1, 0, 0)
    end_date = datetime(2023, 12, 1, 18, 0, 0)

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
        print(len(q_result))
        print("+++++++++++++++++++++++++++++++++++++++++++++++")

        # Validation
        if (start_date == datetime(2023, 12, 1, 1, 0, 0)):
            assert len(q_result) == 0
        if (start_date == datetime(2023, 12, 1, 2, 0, 0)):
            assert len(q_result) == 592
        if (start_date == datetime(2023, 12, 1, 3, 0, 0)):
            assert len(q_result) == 1188
        if (start_date == datetime(2023, 12, 1, 4, 0, 0)):
            assert len(q_result) == 2
        if (start_date == datetime(2023, 12, 1, 5, 0, 0)):
            assert len(q_result) == 2
        if (start_date == datetime(2023, 12, 1, 6, 0, 0)):
            assert len(q_result) == 2
        if (start_date == datetime(2023, 12, 1, 7, 0, 0)):
            assert len(q_result) == 2
        if (start_date == datetime(2023, 12, 1, 8, 0, 0)):
            assert len(q_result) == 2
        if (start_date == datetime(2023, 12, 1, 9, 0, 0)):
            assert len(q_result) == 2
        if (start_date == datetime(2023, 12, 1, 10, 0, 0)):
            assert len(q_result) == 2
        if (start_date == datetime(2023, 12, 1, 11, 0, 0)):
            assert len(q_result) == 2
        if (start_date == datetime(2023, 12, 1, 12, 0, 0)):
            assert len(q_result) == 2
        if (start_date == datetime(2023, 12, 1, 13, 0, 0)):
            assert len(q_result) == 2
        if (start_date == datetime(2023, 12, 1, 14, 0, 0)):
            assert len(q_result) == 2
        if (start_date == datetime(2023, 12, 1, 15, 0, 0)):
            assert len(q_result) == 2
        if (start_date == datetime(2023, 12, 1, 16, 0, 0)):
            assert len(q_result) == 2
        if (start_date == datetime(2023, 12, 1, 17, 0, 0)):
            assert len(q_result) == 2

        start_date = new_end_date

asyncio.run(run_query())