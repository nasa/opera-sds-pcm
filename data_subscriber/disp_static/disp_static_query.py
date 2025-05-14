import argparse
import concurrent.futures
import logging
import logging.handlers
import os
import sys
import threading
import uuid
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import partial
from pathlib import Path, PurePath
from typing import Literal

import boto3
import requests

import json
import pandas as pd

from data_subscriber.cmr import Endpoint
from util.conf_util import SettingsConf

from util.job_util import is_running_outside_verdi_worker_context, multithread_gather

from data_subscriber.cslc_utils import get_bounding_box_for_frame, localize_frame_geo_json
from util.job_submitter import try_submit_mozart_job

is_dev_mode = None
settings = None


def init_logging(level: Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]):
    log_file_format = "%(asctime)s %(levelname)7s %(name)13s:%(filename)19s:%(funcName)22s:%(lineno)3s - %(message)s"
    log_format = "%(levelname)s: %(relativeCreated)7d %(process)d %(processName)s %(thread)d %(threadName)s %(name)s:%(filename)s:%(funcName)s:%(lineno)s - %(message)s"
    logging.basicConfig(level=level, format=log_format, force=True)

    rfh1 = logging.handlers.RotatingFileHandler("app.log", mode="a", maxBytes=100 * 2 ** 20, backupCount=10)
    rfh1.setLevel(logging.INFO)
    rfh1.setFormatter(logging.Formatter(fmt=log_file_format))
    logging.getLogger().addHandler(rfh1)

    rfh2 = logging.handlers.RotatingFileHandler("app-error.log", mode="a", maxBytes=100 * 2 ** 20, backupCount=10)
    rfh2.setLevel(logging.ERROR)
    rfh2.setFormatter(logging.Formatter(fmt=log_file_format))
    logging.getLogger().addHandler(rfh2)


def create_parser():
    argparser = argparse.ArgumentParser(add_help=True)
    argparser.add_argument("--filter-frames", nargs="*", dest="filter_frame_numbers", required=True, help="List of frame numbers to process. If unset, this tool will process all frames in the frame-to-burst JSON.")
    argparser.add_argument("--filter-is-north-america", action=argparse.BooleanOptionalAction, default=True, required=False, help="Toggle for filtering frames in North America as defined in the frame-to-burst JSON.")

    argparser.add_argument("--settings", type=Path, required=False, help="Custom settings.yaml filepath. Refer to the implementation of this workflow for the specification required.")
    argparser.add_argument("--frame-to-burst-db", required=False, help="Required outside of PCM. S3 URL pointing to the frame-to-burst JSON. If not provided, the URL will be read from PCM settings when running in PCM.")
    argparser.add_argument("--endpoint", choices=[endpoint.value for endpoint in Endpoint], default=Endpoint.OPS.value, help="Specify the DAAC endpoint to use. (default: %(default)s)")

    argparser.add_argument("--max-concurrent-frames", default=1, type=int, choices=(1, 2, 3), help="Maximum number of frames to concurrently query products for in CMR. (default: %(default)s)")

    argparser.add_argument("--smoke-run", action="store_true")
    argparser.add_argument("--dry-run", action="store_true")
    argparser.add_argument("--dev", dest="is_dev_mode", action="store_true", default=False)
    argparser.add_argument("--log-level", default="INFO", choices=("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"), help="(default: %(default)s)")

    return argparser


def main(
        filter_frame_numbers=None,
        filter_is_north_america=True,
        frame_to_burst_db=None,
        endpoint=None,
        max_concurrent_frames=None,
        smoke_run=None,
        **kwargs
):
    # LOCALIZE BURST DB

    downloads_dir = Path("downloads")
    downloads_dir.mkdir(exist_ok=True)

    path_burst_db = downloads_dir / "opera-s1-disp-0.9.0-frame-to-burst.json"

    if not path_burst_db.exists():
        if not frame_to_burst_db:
            frame_to_burst_db = settings["DISP_S1"]["FRAME_TO_BURST_JSON"]
        path_burst_db = download_burst_db(frame_to_burst_db, downloads_dir=downloads_dir)

    # READ BURST DB
    with path_burst_db.open() as fp:
        df = pd.DataFrame.from_dict(json.load(fp)["data"], orient="index")

    # apply filters
    if filter_is_north_america:
        df = df[df["is_north_america"] == True]
    if filter_frame_numbers:
        if smoke_run:
            filter_frame_numbers = filter_frame_numbers[:1]
        df = df[df.index.isin(filter_frame_numbers)]

    job_data = {}  # hold processing data
    for frame, row in df.iterrows():
        job_data[frame] = {}

    # PARSE BURST ID SETS AS REDUCED CMR QUERY NATIVE ID PATTERNS
    for frame, row in df.iterrows():
        burst_ids = [b.replace("_", "-").upper() for b in row.burst_ids]

        cslc_static_native_id_pattern_batch = []
        rtc_static_native_id_pattern_batch = []
        for burst_id in burst_ids:
            cslc_static_native_id_pattern_batch.append(f"OPERA_L2_CSLC-S1-STATIC_{burst_id}*")
            rtc_static_native_id_pattern_batch.append(f"OPERA_L2_RTC-S1-STATIC_{burst_id}*")
        cslc_static_native_id_pattern_batch = reduce_cslc_bursts_to_cmr_patterns(cslc_static_native_id_pattern_batch)
        rtc_static_native_id_pattern_batch = reduce_rtc_bursts_to_cmr_patterns(rtc_static_native_id_pattern_batch)

        job_data[frame]["frame"] = frame
        job_data[frame]["burst_ids"] = burst_ids
        job_data[frame]["L2_CSLC-S1-STATIC"] = {}
        job_data[frame]["L2_CSLC-S1-STATIC"]["native-id-pattern-batch"] = cslc_static_native_id_pattern_batch
        job_data[frame]["L2_RTC-S1-STATIC"] = {}
        job_data[frame]["L2_RTC-S1-STATIC"]["native-id-pattern-batch"] = rtc_static_native_id_pattern_batch

    # FORMAT CMR QUERIES
    cmr = settings["DAAC_ENVIRONMENTS"][endpoint]["BASE_URL"]
    for frame in job_data:
        for type_ in ("CSLC", "RTC"):
            static_native_id_pattern_batch = job_data[frame][f"L2_{type_}-S1-STATIC"]["native-id-pattern-batch"]

            native_id_patterns_query_params = "&native_id[]=" + "&native_id[]=".join(static_native_id_pattern_batch)
            request_url = (
                f"https://{cmr}/search/granules.umm_json"
                "?provider=ASF"
                f"&ShortName=OPERA_L2_{type_}-S1-STATIC_V1"
                "&sort_key[]=start_date"
                "&options[native-id][pattern]=true"
                f"{native_id_patterns_query_params}"
                "&page_size=27"  # assumes no duplicate records
            )
            job_data[frame][f"L2_{type_}-S1-STATIC"]["request_url"] = request_url

            del job_data[frame][f"L2_{type_}-S1-STATIC"]["native-id-pattern-batch"]

    # ISSUE CMR QUERIES. COLLECT RESULTS
    #  for OPERA-CMR performance reasons, the queries will be executed sequentially.
    #  this COULD be bumped up to 2-5 concurrent requests MAXIMUM, per CMR recommendations.

    frame_to_type_to_results_map = defaultdict(partial(defaultdict, partial(defaultdict, dict)))
    futures = []
    with SemaphoreThreadPoolExecutor(max_concurrent_frames) as executor:
        for frame in job_data:
            future = executor.submit(get_products_for_frame, frame, job_data)
            futures.append(future)
        for future in concurrent.futures.as_completed(futures):
            frame_to_type_to_results_map.update(future.result())

    for frame in frame_to_type_to_results_map:
        try:
            for type_ in frame_to_type_to_results_map[frame]:
                if not frame_to_type_to_results_map[frame][type_]["products"]:
                    raise CmrProductsNotFoundError(frame=frame, type_=type_)
                job_data[frame][type_]["products"] = frame_to_type_to_results_map[frame][type_]["products"]
        except CmrProductsNotFoundError as err:
            logger.warning(f"CMR {err.type_=} products not found for {err.frame=}. Removing frame from further processing.")
            del job_data[frame]
            continue

    job_products = []
    logger.info("SUBMITTING MOZART JOBS")
    for frame in job_data:
        disp_s1_job_product = create_job_submission_product(job_data, frame)
        job_products.append(disp_s1_job_product)

    job_submission_tasks = []
    for job_product in job_products:
        job_submission_tasks.append(partial(submit_disp_s1_job, product=job_product))

    results = multithread_gather(job_submission_tasks, max_workers=min(8, os.cpu_count() + 4), return_exceptions=True)

    suceeded_frames = [job_id for job_id in results if isinstance(job_id, str)]
    failed_frames = [e for e in results if isinstance(e, Exception)]

    succeeded = suceeded_frames
    failed = failed_frames

    logger.debug(f"{results=}")
    logger.info(f"{len(succeeded)} DISP-S1 jobs {succeeded=}")
    logger.info(f"{len(failed)} DISP-S1 jobs {failed=}")
    logger.debug(f"{succeeded=}")

    results = {
        "success": succeeded,
        "fail": failed,
    }

    # logger.info(f"{len(results)=}")
    logger.debug(f"{results=}")
    logger.info("END")


def get_products_for_frame(frame, job_data):
    result = defaultdict(partial(defaultdict, partial(defaultdict, list)))
    for type_ in ("CSLC", "RTC"):
        request_url = job_data[frame][f"L2_{type_}-S1-STATIC"]["request_url"]
        logger.info(f"{request_url=}")
        rsp = requests.get(
            request_url,
            headers={"Client-Id": f'nasa.jpl.opera.sds.pcm.data_subscriber.dist_static.{os.environ["USER"]}'}
        ).json()

        products = cmr_response_to_cmr_product(rsp)
        result[frame][f"L2_{type_}-S1-STATIC"]["products"] = products
    return result


def cmr_response_to_cmr_product(rsp):
    products = []
    # COLLECT S3 URLS PER SET
    for item in rsp.get("items", []):
        meta = item["meta"]
        umm = item["umm"]

        product = {
            "native_id": meta["native-id"],
            "s3_urls": [
                d["URL"]
                for d in umm["RelatedUrls"]
                if (
                        d["Type"] == "GET DATA VIA DIRECT ACCESS"
                        and d.get("URL").startswith("s3")
                        and (
                                d.get("URL").endswith(".h5") or d.get("URL").endswith("_mask.tif")
                        )
                )
            ]
        }
        products.append(product)
    return products


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


def create_job_submission_product(job_data, frame):
    product_paths = {}
    for type_ in ("CSLC", "RTC"):
        s3_urls = [
            s3_url
            for p in job_data[frame][f"L2_{type_}-S1-STATIC"]["products"]
            for s3_url in p["s3_urls"]
        ]
        product_paths[f"L2_{type_}_S1_STATIC"] = s3_urls
    product_type = "DISP-S1-JOB-SUBMISSION"
    product_id = f"{frame}"
    if is_dev_mode:
        logger.info(f"{ is_dev_mode=}. Using global bounding box.")
        bounding_box = [-180., -90., 180., 90.]
    else:
        bounding_box = get_bounding_box_for_frame(int(frame), localize_frame_geo_json())
    disp_s1_job_product = {
        "_id": f"{product_id}",
        "_source": {
            "dataset": f"{product_type}-{product_id}",
            "metadata": {
                "frame_id": f"{frame}",
                "ProductReceivedTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                "product_paths": product_paths,
                "FileName": f"{product_id}",
                "id": f"{product_id}",
                "bounding_box": bounding_box,
                "Files": [
                    {
                        "FileName": PurePath(s3path).name,
                        "FileSize": 1,
                        "FileLocation": os.path.dirname(s3path),
                        "id": PurePath(s3path).name,
                        "product_paths": "$.product_paths"
                    }
                    for type_ in product_paths
                    for s3path in product_paths[type_]
                ]
            }
        }
    }
    return disp_s1_job_product


def submit_disp_s1_job(product):
    if is_dev_mode:
        logger.info(f"{ is_dev_mode=}. Skipping job submission.")
        return str(uuid.uuid4())
    else:
        logger.info(f'SUBMITTING MOZART JOB. frame={product["_source"]["metadata"]["frame_id"]}')

        frame_id = product["_source"]["metadata"]["frame_id"]
        return try_submit_mozart_job(
            product=product,
            job_queue="opera-job_worker-sciflo-l3_disp_s1_static",
            rule_name="trigger-SCIFLO_L3_DISP_S1_static",
            params=create_job_params(product),
            job_spec=f'job-SCIFLO_L3_DISP_S1_STATIC:{settings["RELEASE_VERSION"]}',
            job_type=f'hysds-io-SCIFLO_L3_DISP_S1_STATIC:{settings["RELEASE_VERSION"]}',
            job_name=f'job-WF-SCIFLO_L3_DISP_S1_STATIC-frame-{frame_id}'
        )


def create_job_params(product):
    return [
        {
            "name": "input_dataset_id",
            "type": "text",
            "from": "value",
            "value": product["_source"]["metadata"]["frame_id"]
        },
        {
           "name": "product_metadata",
           "from": "value",
           "type": "object",
           "value": product["_source"]
        }
    ]


def reduce_cslc_bursts_to_cmr_patterns(cslc_native_id_patterns_burst_sets):
    native_id_pattern_tree = tree()
    for pattern in cslc_native_id_patterns_burst_sets:
        native_id_pattern_tree[pattern[:-7]][pattern[:-6]][pattern[:-5]][pattern]
    native_id_pattern_tree = dicts(native_id_pattern_tree)

    cslc_native_id_patterns = set()
    for k1, v1 in native_id_pattern_tree.items():
        if len(v1.keys()) == 10:
            cslc_native_id_patterns.add(k1)
        else:
            for k2, v2 in v1.items():
                if len(v2.keys()) == 10:
                    cslc_native_id_patterns.add(k2)
                else:
                    for k3, v3 in v2.items():
                        if len(v3.keys()) == 3:  # got to the list of full native-ids
                            cslc_native_id_patterns.add(k3)
                        else:
                            cslc_native_id_patterns.update(set(v3.keys()))  # all the individual beams (1/3 or 2/3)
    cslc_native_id_patterns = {p + "*" for p in cslc_native_id_patterns}
    return cslc_native_id_patterns


def reduce_rtc_bursts_to_cmr_patterns(rtc_native_id_patterns_burst_sets):
    native_id_pattern_tree = tree()
    for pattern in rtc_native_id_patterns_burst_sets:
        native_id_pattern_tree[pattern[:-7]][pattern[:-6]][pattern[:-5]][pattern[:-4]][pattern]
    native_id_pattern_tree = dicts(native_id_pattern_tree)

    rtc_native_id_patterns = set()
    for k1, v1 in native_id_pattern_tree.items():
        if len(v1.keys()) == 10:
            rtc_native_id_patterns.add(k1)
        else:
            for k2, v2 in v1.items():
                if len(v2.keys()) == 10:
                    rtc_native_id_patterns.add(k2)
                else:
                    for k3, v3 in v2.items():
                        if len(v3.keys()) == 10:
                            rtc_native_id_patterns.add(k3)
                        else:
                            for k4, v4 in v3.items():
                                if len(v4.keys()) == 3:  # got to the list of full native-ids
                                    rtc_native_id_patterns.add(k4)
                                else:
                                    rtc_native_id_patterns.update(set(v4.keys()))  # all the individual beams (1/3 or 2/3)
    rtc_native_id_patterns = {p + "*" for p in rtc_native_id_patterns}

    return rtc_native_id_patterns


def tree():
    """
    Simple implementation of a tree data structure in python. Essentially a defaultdict of default dicts.

    Usage: foo = tree() ; foo["a"]["b"]["c"]... = bar
    """
    return defaultdict(tree)


def dicts(t):
    """Utility function for casting a tree to a complex dict"""
    return {k: dicts(t[k]) for k in t}


class SemaphoreThreadPoolExecutor(concurrent.futures.ThreadPoolExecutor):
    def __init__(self, max_workers=None, *args, **kwargs):
        super().__init__(*args, max_workers=max_workers, **kwargs)
        self.sem = threading.Semaphore(self._max_workers)

    def submit(self, __fn, *args, **kwargs):
        self.sem.acquire()
        future = super().submit(__fn, *args, **kwargs)
        future.add_done_callback(lambda _: self.sem.release())
        return future


class CmrProductsNotFoundError(Exception):
    def __init__(self, frame=None, type_=None):
        self.frame = frame
        self.type_ = type_


if __name__ == "__main__":
    args = create_parser().parse_args(sys.argv[1:])
    init_logging(level=args.log_level)
    logger = logging.getLogger(__name__)

    logger.info(f"{__file__} invoked with {sys.argv=}")

    is_dev_mode = args.is_dev_mode

    if is_running_outside_verdi_worker_context():
        settings_filepath = str(args.settings.absolute()) if args.settings else None
        settings = SettingsConf(file=settings_filepath).cfg
    else:
        settings = SettingsConf().cfg

    main(**args.__dict__)
