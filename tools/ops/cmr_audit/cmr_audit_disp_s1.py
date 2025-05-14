import logging
import logging.handlers
import os
import sys
from dotenv import dotenv_values
from tabulate import tabulate

# NOTE! Only import this if this code is being run locally instead of a deployed environment.
#import tests.data_subscriber.conftest

from cmr_audit_hls import create_parser, init_logging
from cmr_audit_slc import get_out_filename
from report.opera_validator.opv_disp_s1 import validate_disp_s1

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

    def add_more_args(self):
        self.argparser.add_argument("--frames-only", required=False, help="DISP-S1 only. Restrict validation to these frame numbers only. Comma-separated list of frames")
        self.argparser.add_argument("--validate-with-grq", action='store_true', help="DISP-S1 only. Instead of retrieving DISP-S1 products from CMR, retrieve from GRQ database. ")
        self.argparser.add_argument("--processing-mode", required=True, choices=['forward', 'reprocessing', 'historical'], help="DISP-S1 only. Processing mode to use for DISP-S1 validation")
        self.argparser.add_argument("--view-all-cslc-groupings", action='store_true', help="DISP-S1 only. View all CSLC input grouping data in addition to the normal output. ")
        self.argparser.add_argument("--k", required=False, default=15, help="DISP-S1 only. It should almost always be 15 but that could be changed in some edge cases. ")

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
        init_logging("cmr_audit_disp_s1.log", "cmr_audit_disp_s1-error.log", args.log_level)

        cmr_start_dt_str = args.start_datetime
        cmr_end_dt_str = args.end_datetime

        passing, should_df, result_df = self.perform_audit(args)

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

        # if processing mode is historical, deduplicate All Bursts by the entire groupings per frame
        # and write out only the unique groupings

        if args.processing_mode == "historical":
            unique_burst_sets = {}
            for d in disp_s1_products_miss:
                burst_set = frozenset(d["All Bursts"])
                if burst_set not in unique_burst_sets:
                    unique_burst_sets[burst_set] = d

        # Write out all bursts from the missing products
        with open(out_filename, "w") as out_file:
            out_file.write("Frame ID, Burst ID, Product ID\n")
            for d in unique_burst_sets:
                out_file.write(f"{d['Frame ID']}, {d['Burst ID']}, {d['Product ID']}\n")


if __name__ == "__main__":
    cmr_audit = CMRAudit()
    cmr_audit.run()
