import logging
import logging.handlers
import os
import sys
import argparse
from collections import defaultdict

from dotenv import dotenv_values
from tabulate import tabulate
from typing import Literal
import pandas as pd

from data_subscriber.cmr import CMR_TIME_FORMAT
# NOTE! Only import this if this code is being run locally instead of a deployed environment.
#import tests.data_subscriber.conftest

from data_subscriber.cslc_utils import parse_cslc_file_name, localize_disp_frame_burst_hist
from cmr_audit_slc import get_out_filename
from report.opera_validator.opv_disp_s1 import validate_disp_s1

OPERA_VALIDATOR_TIME_FORMAT = "%Y%m%dT%H%M%SZ"

def init_logging(level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"] = logging.INFO):
    log_file_format = "%(asctime)s %(levelname)7s %(name)13s:%(filename)19s:%(funcName)22s:%(lineno)3s - %(message)s"
    log_format = "%(levelname)s: %(relativeCreated)7d %(process)d %(processName)s %(thread)d %(threadName)s %(name)s:%(filename)s:%(funcName)s:%(lineno)s - %(message)s"
    logging.basicConfig(level=level, format=log_format, datefmt="%Y-%m-%d %H:%M:%S", force=True)

    rfh1 = logging.handlers.RotatingFileHandler('cmr_audit_disp_s1.log', mode='a', maxBytes=100 * 2 ** 20, backupCount=10)
    rfh1.setLevel(logging.INFO)
    rfh1.setFormatter(logging.Formatter(fmt=log_file_format))
    logging.getLogger().addHandler(rfh1)

    rfh2 = logging.handlers.RotatingFileHandler('cmr_audit_disp_s1-error.log', mode='a', maxBytes=100 * 2 ** 20, backupCount=10)
    rfh2.setLevel(logging.ERROR)
    rfh2.setFormatter(logging.Formatter(fmt=log_file_format))
    logging.getLogger().addHandler(rfh2)


def create_parser():
    argparser = argparse.ArgumentParser(add_help=True)
    argparser.add_argument(
        "--start-datetime",
        required=True,
        help=f'ISO formatted datetime string. Must be compatible with CMR. ex) 2023-08-02T04:00:00'
    )
    argparser.add_argument(
        "--end-datetime",
        required=True,
        help=f'ISO formatted datetime string. Must be compatible with CMR. ex) 2023-08-02T04:00:00'
    )
    argparser.add_argument(
        "--output", "-o",
        help=f'Output filepath.'
    )
    argparser.add_argument(
        "--format",
        default="txt",
        choices=["txt", "json"],
        help=f'Output file format. Defaults to "%(default)s".'
    )
    argparser.add_argument('--log-level', default='INFO', choices=('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'))

    return argparser


class CMRAudit:
    def __init__(self):
        logging.getLogger("compact_json.formatter").setLevel(level=logging.INFO)
        logging.basicConfig(
            format="%(levelname)7s: %(relativeCreated)7d %(name)s:%(filename)s:%(funcName)s:%(lineno)s - %(message)s",
            # alternative format which displays time elapsed.
            # format="%(asctime)s %(levelname)7s %(name)4s:%(filename)8s:%(funcName)22s:%(lineno)3s - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
            level=logging.INFO)
        self.logger = logging.getLogger(__name__)

        config = {
            **dotenv_values("../../.env"),
            **os.environ
        }

        self.argparser = create_parser()
        self.add_more_args()

        self.disp_burst_map, self.burst_to_frames, self.day_indices_to_frames = localize_disp_frame_burst_hist()

    def add_more_args(self):
        self.argparser.add_argument("--frames-only", required=False, help="Restrict validation to these frame numbers only. Comma-separated list of frames")
        self.argparser.add_argument("--validate-with-grq", action='store_true', help="Instead of retrieving DISP-S1 products from CMR, retrieve from GRQ database. ")
        self.argparser.add_argument("--processing-mode", required=True, choices=['forward', 'reprocessing', 'historical'], help="DISP-S1 only. Processing mode to use for DISP-S1 validation")
        self.argparser.add_argument("--k", required=False, default=15, help="It should almost always be 15 but that could be changed in some edge cases. ")
        self.argparser.add_argument("--use-pickle-file", required=False, dest="pickle_file", help="Use a picked file for input instead of querying CMR. Used in testing.")

    def perform_audit(self, args):

        # Perform all validation work in this function
        if args.processing_mode is None:
            logging.error("Processing mode must be specified for DISP-S1 validation.")
            sys.exit(1)
        else:
            processing_mode = args.processing_mode
        passing, should_df, result_df = validate_disp_s1(args.start_datetime, args.end_datetime, "TEMPORAL", "OPS",
                                                         "OPS", args.frames_only,
                                                         args.validate_with_grq,
                                                         processing_mode, args.k)

        return passing, should_df, result_df

    def run(self):
        args = self.argparser.parse_args(sys.argv[1:])
        self.logger.info(f'{args=}')
        init_logging(args.log_level)

        cmr_start_dt_str = args.start_datetime
        cmr_end_dt_str = args.end_datetime

        if args.pickle_file:
            self.logger.info("Reading in existing result_df from {args.pickle_file}")
            result_df = pd.read_pickle(args.pickle_file)
        else:
            self.logger.info("Performing DISP-S1 audit")
            passing, should_df, result_df = self.perform_audit(args)

            # Pickle out result_df
            #result_df.to_pickle("cmr_audit_disp_s1.pickle")

        # From the result_df, count the number of products that have product ID not "UNPROCESSED"
        disp_s1_products = []
        disp_s1_products_miss = []
        for index, d in result_df.iterrows():
            if d["Product ID"] != "UNPROCESSED":
                disp_s1_products.append(d)
            else:
                disp_s1_products_miss.append(d)

        self.logger.info(f"Fully published (granules) (DISP-S1): {len(disp_s1_products)=:,}")
        self.logger.info(f"Missing (granules) (DISP-S1): {len(disp_s1_products_miss)=:,}")

        '''print(tabulate(result_df[
                           ['Product ID', 'Frame ID', 'Last Acq Day Index', 'All Bursts Count', 'Matching Bursts Count',
                            'Unmatching Bursts Count']], headers='keys', tablefmt='plain', showindex=False))'''

        # Generate the output filename
        out_filename = get_out_filename(cmr_start_dt_str, cmr_end_dt_str, "DISP-S1", "CSLC")
        output_file_missing_cmr_frames = args.output if args.output else f"{out_filename}.txt"

        # If processing mode is historical, group by frame_id and k_cycle
        if args.processing_mode == "historical":

            class TwoDates:
                def __init__(self):
                    self.first_date = None
                    self.last_date = None

            start_end_date_map = defaultdict(TwoDates)

            for d in disp_s1_products_miss:
                _, acq_date = parse_cslc_file_name(list(d["All Bursts"])[0])
                day_index = d["Last Acq Day Index"]
                frame_id = d["Frame ID"]
                frame = self.disp_burst_map[frame_id]
                index_number = frame.sensing_datetime_days_index.index(day_index)  # note "index" is overloaded term here
                k_order = index_number % args.k
                k_cycle = index_number // args.k

                # acq_date looks like this: 20160810T140608Z
                acq_date = pd.to_datetime(acq_date, format=OPERA_VALIDATOR_TIME_FORMAT, utc=True)
                if k_order == 0:
                    # First date should be 30 minutes before acq_date. Format the output to be like 2021-01-14T00:00:00Z
                    start_date = (acq_date + pd.Timedelta(minutes=-30)).strftime(CMR_TIME_FORMAT)
                    start_end_date_map[(frame_id, k_cycle)].first_date = start_date
                if k_order == args.k - 1:
                    # Last date should be 30 mins after. This way we cover the small variations in time
                    end_date = (acq_date + pd.Timedelta(minutes=30)).strftime(CMR_TIME_FORMAT)
                    start_end_date_map[(frame_id, k_cycle)].last_date = end_date

        # Write out all bursts from the missing products
        with open(output_file_missing_cmr_frames, "w") as out_file:

            out_file.write("Frame ID, Start Date, End Date, K-Cycle\n")

            if args.processing_mode == "historical":
                for (frame_id, k_cycle), dates in start_end_date_map.items():
                    out_file.write(f"{frame_id}, {dates.first_date}, {dates.last_date}, {k_cycle}\n")
            else:
                for d in disp_s1_products_miss:
                    _, acq_date = parse_cslc_file_name(list(d["All Bursts"])[0])
                    start_date = (pd.to_datetime(acq_date, format=OPERA_VALIDATOR_TIME_FORMAT, utc=True) + pd.Timedelta(minutes=-30)).strftime(CMR_TIME_FORMAT)
                    end_date = (pd.to_datetime(acq_date, format=OPERA_VALIDATOR_TIME_FORMAT, utc=True) + pd.Timedelta(minutes=30)).strftime(CMR_TIME_FORMAT)
                    out_file.write(f"{d['Frame ID']}, {start_date}, {end_date}\n")

if __name__ == "__main__":
    cmr_audit = CMRAudit()
    cmr_audit.run()
