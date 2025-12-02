import json
import logging
import logging.handlers
import os
import sys
import argparse
from collections import defaultdict

from dotenv import dotenv_values
from tabulate import tabulate
import pandas as pd

from data_subscriber.cmr import CMR_TIME_FORMAT
# NOTE! Only import this if this code is being run locally instead of a deployed environment.
#import tests.data_subscriber.conftest

from data_subscriber.cslc_utils import parse_cslc_file_name, localize_disp_frame_burst_hist
from cmr_audit_slc import get_out_filename
from tools.ops.cmr_audit.cmr_audit_utils import init_logging, create_parser
from report.opera_validator.opv_disp_s1 import validate_disp_s1

OPERA_VALIDATOR_TIME_FORMAT = "%Y%m%dT%H%M%SZ"

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
        self.argparser.add_argument("--k", required=False, default=15, type=int, help="It should almost always be 15 but that could be changed in some edge cases. ")
        self.argparser.add_argument("--use-pickle-file", required=False, dest="pickle_file", help="Use a picked file for input instead of querying CMR. Used in testing.")
        self.argparser.add_argument("--output-frame-states", required=False, dest="output_frame_states",
                                    help="Output file path for frame states JSON. When specified, calculates the expected frame states based on what DISP-S1 products exist in CMR/GRQ and outputs a JSON file that can be used to create/update a batch proc.")

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

    def calculate_expected_frame_states(self, result_df, k):
        """
        Calculate the expected frame states based on DISP-S1 products that exist.

        For historical processing, frame_state represents the position in the sensing_datetimes list
        that has been processed. This is calculated as (highest_complete_k_cycle + 1) * k.

        Args:
            result_df: DataFrame containing the audit results with 'Frame ID', 'Last Acq Day Index', 'Product ID'
            k: The k parameter (number of acquisitions per cycle)

        Returns:
            dict: A dictionary mapping frame_id (str) -> frame_state (int)
        """
        frame_states = {}

        # Group by Frame ID and find the highest processed acquisition day index for each frame
        # Only consider products that were actually processed (not UNPROCESSED)
        processed_df = result_df[result_df['Product ID'] != 'UNPROCESSED']

        if processed_df.empty:
            self.logger.warning("No processed DISP-S1 products found. All frame states will be 0.")
            return frame_states

        for frame_id in processed_df['Frame ID'].unique():
            frame_data = processed_df[processed_df['Frame ID'] == frame_id]

            # Get the highest Last Acq Day Index for this frame
            max_acq_day_index = frame_data['Last Acq Day Index'].max()

            # Find the position in the sensing_datetime_days_index list
            frame = self.disp_burst_map[frame_id]
            try:
                # Find the index position of this day index in the frame's sensing time list
                index_position = frame.sensing_datetime_days_index.index(max_acq_day_index)

                # The frame_state should be index_position + 1 (since it's the count of processed items)
                # But we need to align to k boundaries for historical processing
                # The state represents how many sensing times have been submitted/processed
                # If we're at index_position, and this is the last of a k-cycle, then state = index_position + 1
                frame_state = index_position + 1

                self.logger.info(f"Frame {frame_id}: max_acq_day_index={max_acq_day_index}, "
                               f"index_position={index_position}, frame_state={frame_state}")

            except ValueError:
                self.logger.warning(f"Frame {frame_id}: acq_day_index {max_acq_day_index} not found in "
                                  f"sensing_datetime_days_index. This may be a forward processing product.")
                # For forward processing products outside historical database, we can't determine position
                continue

            frame_states[str(frame_id)] = frame_state

        return frame_states

    def run(self):
        args = self.argparser.parse_args(sys.argv[1:])
        self.logger.info(f'{args=}')
        init_logging('cmr_audit_disp_s1.log', 'cmr_audit_disp_s1-error.log', args.log_level)

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

        # Output frame states JSON if requested
        if args.output_frame_states:
            self.logger.info("Calculating expected frame states from audit results...")
            frame_states = self.calculate_expected_frame_states(result_df, args.k)

            # Also include frames that had no products (frame_state = 0)
            if args.frames_only:
                requested_frames = set([int(f) for f in args.frames_only.split(',')])
                for frame_id in requested_frames:
                    if str(frame_id) not in frame_states:
                        frame_states[str(frame_id)] = 0

            # Sort by frame_id for readability
            frame_states = dict(sorted(frame_states.items(), key=lambda x: int(x[0])))

            output_data = {
                "frame_states": frame_states,
                "k": args.k,
                "audit_start_date": cmr_start_dt_str,
                "audit_end_date": cmr_end_dt_str,
                "processing_mode": args.processing_mode,
                "total_frames": len(frame_states),
                "frames_with_products": len([v for v in frame_states.values() if v > 0]),
                "frames_without_products": len([v for v in frame_states.values() if v == 0])
            }

            with open(args.output_frame_states, 'w') as f:
                json.dump(output_data, f, indent=4)

            self.logger.info(f"Frame states written to {args.output_frame_states}")
            self.logger.info(f"Total frames: {output_data['total_frames']}, "
                           f"With products: {output_data['frames_with_products']}, "
                           f"Without products: {output_data['frames_without_products']}")

if __name__ == "__main__":
    cmr_audit = CMRAudit()
    cmr_audit.run()
