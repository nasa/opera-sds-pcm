import argparse
import asyncio
import json
import logging.handlers
import sys
from pathlib import Path, PurePath
from typing import Literal

import boto3
import pandas as pd
from dateutil.parser import isoparse

from data_subscriber.cmr import async_query_cmr_v2

logging.getLogger("elasticsearch").setLevel(level=logging.WARNING)


def init_logging(level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]):
    log_file_format = "%(asctime)s %(levelname)7s %(name)13s:%(filename)19s:%(funcName)22s:%(lineno)3s - %(message)s"
    log_format = "%(levelname)s: %(relativeCreated)7d %(process)d %(processName)s %(thread)d %(threadName)s %(name)s:%(filename)s:%(funcName)s:%(lineno)s - %(message)s"
    logging.basicConfig(level=level, format=log_format, force=True)

    rfh1 = logging.handlers.RotatingFileHandler("cmr_audit_disp_s1_static.log", mode="a", maxBytes=100 * 2 ** 20, backupCount=10)
    rfh1.setLevel(logging.INFO)
    rfh1.setFormatter(logging.Formatter(fmt=log_file_format))
    logging.getLogger().addHandler(rfh1)

    rfh2 = logging.handlers.RotatingFileHandler("cmr_audit_disp_s1_static-error.log", mode="a", maxBytes=100 * 2 ** 20, backupCount=10)
    rfh2.setLevel(logging.ERROR)
    rfh2.setFormatter(logging.Formatter(fmt=log_file_format))
    logging.getLogger().addHandler(rfh2)

def create_parser():
    argparser = argparse.ArgumentParser(add_help=True)
    argparser.add_argument("--filter-is-north-america", action=argparse.BooleanOptionalAction, default=True, required=False, help="Toggle for filtering frames in North America as defined in the frame-to-burst JSON.")
    argparser.add_argument("--frame-to-burst-db", type=argparse_path, required=True, help="Custom opera-s1-disp-frame-to-burst.json filepath.")
    argparser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"), help="(default: %(default)s)")
    argparser.add_argument(
        "--output", "-o",
        required=True,
        help=f"Output filepath."
    )
    argparser.add_argument(
        "--format",
        default="txt",
        choices=["txt", "json"],
        help=f'Output file format. Defaults to "%(default)s".'
    )

    return argparser

def argparse_path(path_str):
    path = Path(path_str).resolve()
    if not path.exists():
        raise ValueError()
    return path

def main(
    filter_is_north_america=True,
    frame_to_burst_db=None,
    output=None,
     **kwargs
):
    # READ BURST DB
    with frame_to_burst_db.open() as fp:
        df = pd.DataFrame.from_dict(json.load(fp)["data"], orient="index")
        df = df[df["is_north_america"] == filter_is_north_america]
        source_frames = set(df.index)

    cmr_products = asyncio.run(
        async_query_cmr_v2(timerange=None, provider="ASF", collection="OPERA_L3_DISP-S1-STATIC_PROVISIONAL_V0",
                           cmr_hostname="cmr.uat.earthdata.nasa.gov")
    )

    # e.g. native-id: 'OPERA_L3_DISP-S1-STATIC_F16938_20140403_S1A_v1.0'
    cmr_frames = {p["meta"]["native-id"].split("_")[3][1:] for p in cmr_products}  # skip "F" prefix before frame number
    coverage = source_frames - cmr_frames

    logging.info(f"Number of frames in frames-to-burst DB JSON: {len(source_frames)}")
    logging.info(f"Number of unique frames referenced in OPERA_L3_DISP-S1-STATIC in CMR: {len(cmr_frames)}")
    logging.info(f"Number of missing frames: {len(coverage)}")
    logging.info(f"OPERA_L3_DISP-S1-STATIC Coverage:")
    logging.info(f"{len(coverage)/len(source_frames):.2f}%")

    if args.format == "txt":
        logger.info(f"Writing granule list to file {output!r}")
        with open(output, mode='w') as fp:
            fp.write('\n'.join(sorted(coverage)))
    elif args.format == "json":
        with open(output, mode='w') as fp:
            from compact_json import Formatter
            formatter = Formatter(indent_spaces=2, max_inline_length=300, max_compact_list_complexity=0)
            json_str = formatter.serialize(sorted(coverage))
            fp.write(json_str)
    else:
        raise Exception()


def download_burst_db(s3_url, downloads_dir: Path):
    # download burst DB
    AWS_REGION = "us-west-2"
    s3 = boto3.Session(region_name=AWS_REGION).client("s3")
    filename = PurePath(s3_url).name

    source = s3_url[len("s3://"):].partition("/")
    source_bucket = source[0]
    source_key = source[2]

    s3.download_file(source_bucket, source_key, f"{downloads_dir}/{filename}")
    return Path(f"{downloads_dir}/{filename}")

if __name__ == "__main__":
    args = create_parser().parse_args(sys.argv[1:])
    init_logging(level=args.log_level)
    logger = logging.getLogger(__name__)

    logger.debug(f"{__file__} invoked with {sys.argv=}")

    main(**args.__dict__)
