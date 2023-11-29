import asyncio
import logging
import math
import netrc
import re
import uuid
from collections import namedtuple, defaultdict
from datetime import datetime, timedelta
from functools import partial
from pathlib import Path
from typing import Literal
from urllib.parse import urlparse

import dateutil.parser
from hysds_commons.job_utils import submit_mozart_job
from more_itertools import chunked, first

import data_subscriber.download
import extractor.extract
from data_subscriber.aws_token import supply_token
from data_subscriber.cmr import COLLECTION_TO_PRODUCT_TYPE_MAP, async_query_cmr
from data_subscriber.hls.hls_catalog import HLSProductCatalog
from data_subscriber.hls_spatial.hls_spatial_catalog_connection import get_hls_spatial_catalog_connection
from data_subscriber.rtc import evaluator, mgrs_bursts_collection_db_client as mbc_client
from data_subscriber.slc_spatial.slc_spatial_catalog_connection import get_slc_spatial_catalog_connection
from data_subscriber.url import form_batch_id, _slc_url_to_chunk_id
from geo.geo_util import does_bbox_intersect_north_america, does_bbox_intersect_region, _NORTH_AMERICA
from util.conf_util import SettingsConf
from util.pge_util import download_object_from_s3

logger = logging.getLogger(__name__)

DateTimeRange = namedtuple("DateTimeRange", ["start_date", "end_date"])


async def run_query(args, token, es_conn: HLSProductCatalog, cmr, job_id, settings):
    query_dt = datetime.now()
    now = datetime.utcnow()
    query_timerange: DateTimeRange = get_query_timerange(args, now)

    logger.info("CMR query STARTED")
    granules = await async_query_cmr(args, token, cmr, settings, query_timerange, now)
    logger.info("CMR query FINISHED")

    if args.smoke_run:
        logger.info(f"{args.smoke_run=}. Restricting to 1 granule(s).")
        granules = granules[:1]

    # If we are processing ASF collection, we're gonna need the north america geojson
    if COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "SLC":
        localize_geojsons([_NORTH_AMERICA])

    # If processing mode is historical, apply include/exclude-region filtering
    if args.proc_mode == "historical":
        logging.info(f"Processing mode is historical so applying include and exclude regions...")

        # Fetch all necessary geojson files from S3
        localize_include_exclude(args)
        granules[:] = filter_granules_by_regions(granules, args.include_regions, args.exclude_regions)

    logger.info("catalogue-ing STARTED")

    if COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "RTC":
        affected_mgrs_set_id_acquisition_ts_cycle_indexes = set()
        granules[:] = filter_granules_rtc(granules, args)

    for granule in granules:
        granule_id = granule.get("granule_id")
        revision_id = granule.get("revision_id")

        additional_fields = {}
        additional_fields["revision_id"] = revision_id
        additional_fields["processing_mode"] = args.proc_mode

        if COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "RTC":
            additional_fields["instrument"] = "S1A" if "S1A" in granule_id else "S1B"

            match_product_id = re.match(r"OPERA_L2_RTC-S1_(?P<burst_id>[^_]+)_(?P<acquisition_dts>[^_]+)_*", granule_id)
            acquisition_dts = match_product_id.group("acquisition_dts")
            burst_id = match_product_id.group("burst_id")

            mgrs = mbc_client.cached_load_mgrs_burst_db(filter_land=True)
            mgrs_burst_set_ids = mbc_client.burst_id_to_mgrs_set_ids(mgrs, mbc_client.product_burst_id_to_mapping_burst_id(burst_id))
            additional_fields["mgrs_set_ids"] = mgrs_burst_set_ids


            # RTC: Calculating the Collection Cycle Index (Part 1):
            #  required constants
            MISSION_EPOCH_S1A = dateutil.parser.isoparse("20190101T000000Z")  # set approximate mission start date
            MISSION_EPOCH_S1B = MISSION_EPOCH_S1A + timedelta(days=6)  # S1B is offset by 6 days
            MAX_BURST_IDENTIFICATION_NUMBER = 375887  # gleamed from MGRS burst collection database
            ACQUISITION_CYCLE_DURATION_SECS = timedelta(days=12).total_seconds()

            # RTC: Calculating the Collection Cycle Index (Part 2):
            #  RTC products can be indexed into their respective elapsed collection cycle since mission start/epoch.
            #  The cycle restarts periodically with some miniscule drift over time and the life of the mission.
            burst_identification_number = int(burst_id.split(sep="-")[1])
            instrument_epoch = MISSION_EPOCH_S1A if "S1A" in granule_id else MISSION_EPOCH_S1B
            seconds_after_mission_epoch = (dateutil.parser.isoparse(acquisition_dts) - instrument_epoch).total_seconds()
            acquisition_index = (
                 seconds_after_mission_epoch - (ACQUISITION_CYCLE_DURATION_SECS * (burst_identification_number / MAX_BURST_IDENTIFICATION_NUMBER))
            ) / ACQUISITION_CYCLE_DURATION_SECS
            acquisition_cycle = round(acquisition_index)
            additional_fields["acquisition_cycle"] = acquisition_cycle

            update_additional_fields_mgrs_set_id_acquisition_ts_cycle_indexes(acquisition_cycle, acquisition_index, additional_fields, mgrs_burst_set_ids)
            update_affected_mgrs_set_ids(acquisition_cycle, acquisition_index, affected_mgrs_set_id_acquisition_ts_cycle_indexes, mgrs_burst_set_ids)

        if COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "SLC":
            if does_bbox_intersect_north_america(granule["bounding_box"]):
                additional_fields["intersects_north_america"] = True
        elif COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "RTC":
            pass
        elif COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "CSLC":
            raise NotImplementedError()
        else:
            pass

        update_url_index(
            es_conn,
            granule.get("filtered_urls"),
            granule_id,
            job_id,
            query_dt,
            temporal_extent_beginning_dt=dateutil.parser.isoparse(granule["temporal_extent_beginning_datetime"]),
            revision_date_dt=dateutil.parser.isoparse(granule["revision_date"]),
            **additional_fields
        )

        if COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "HLS":
            spatial_catalog_conn = get_hls_spatial_catalog_connection(logger)
            update_granule_index(spatial_catalog_conn, granule)
        elif COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "SLC":
            spatial_catalog_conn = get_slc_spatial_catalog_connection(logger)
            update_granule_index(spatial_catalog_conn, granule)
        elif COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "RTC":
            pass
        elif COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "CSLC":
            raise NotImplementedError()
        else:
            pass

    logger.info("catalogue-ing FINISHED")

    succeeded = []
    failed = []
    if COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "RTC":
        logger.info("performing index refresh")
        es_conn.refresh()
        logger.info("performed index refresh")

        logger.info("evaluating available burst sets")
        logger.info(f"{affected_mgrs_set_id_acquisition_ts_cycle_indexes=}")
        mgrs_sets, incomplete_mgrs_sets = await evaluator.main(
            mgrs_set_id_acquisition_ts_cycle_indexes=affected_mgrs_set_id_acquisition_ts_cycle_indexes,
            coverage_target=settings["DSWX_S1_COVERAGE_TARGET"]
        )

        # convert to "batch_id" mapping
        batch_id_to_products_map = defaultdict(set)
        for mgrs_set_id, product_burst_sets in mgrs_sets.items():
            for product_burstset in product_burst_sets:
                rtc_granule_id_to_product_docs_map = first(product_burstset)
                first_product_doc_list = first(rtc_granule_id_to_product_docs_map.values())
                first_product_doc = first(first_product_doc_list)
                acquisition_cycle = first_product_doc["acquisition_cycle"]
                batch_id = "{}${}".format(mgrs_set_id, acquisition_cycle)
                batch_id_to_products_map[batch_id] = product_burstset

        edl = settings["DAAC_ENVIRONMENTS"][args.endpoint]["EARTHDATA_LOGIN"]
        username, _, password = netrc.netrc().authenticators(edl)
        token = supply_token(edl, username, password)
        netloc = urlparse(f"https://{edl}").netloc

        # create args for downloading products which is handled by download mode for other product types
        Namespace = namedtuple(
            "Namespace",
            ["provider", "transfer_protocol", "batch_ids", "dry_run", "smoke_run"],
            defaults=["ASF-RTC", args.transfer_protocol, None, args.dry_run, args.smoke_run]
        )

        uploaded_batch_id_to_products_map = {}
        uploaded_batch_id_to_s3paths_map = {}
        for batch_id, product_burstset in batch_id_to_products_map.items():
            args_for_downloader = Namespace(provider="ASF-RTC", batch_ids=[batch_id])
            downloader = data_subscriber.download.DaacDownload.get_download_object(args=args_for_downloader)

            run_download_kwargs = {
                "token": token,
                "es_conn": es_conn,
                "netloc": netloc,
                "username": username,
                "password": password,
                "job_id": job_id
            }

            product_to_product_filepaths_map: dict[str, set[Path]] = downloader.run_download(args=args_for_downloader, **run_download_kwargs, rm_downloads_dir=False)

            # TODO chrisjrd: use or remove metadata extraction
            logger.info("Extracting metadata from RTC products")
            product_to_products_metadata_map = defaultdict(list[dict])
            for product, filepaths in product_to_product_filepaths_map.items():
                for filepath in filepaths:
                    dataset_id, product_met, dataset_met = extractor.extract.extract_in_mem(
                        product_filepath=filepath,
                        product_types=settings["PRODUCT_TYPES"],
                        workspace_dirpath=Path.cwd()
                    )
                    product_to_products_metadata_map[product].append(product_met)

            logger.info(f"Uploading MGRS burst set files to S3")
            files_to_upload = [fp for fp_set in product_to_product_filepaths_map.values() for fp in fp_set]
            s3paths: list[str] = concurrent_s3_client_try_upload_file(bucket=settings["DATASET_BUCKET"], key_prefix=f"tmp/dswx_s1/{batch_id}/", files=files_to_upload)
            uploaded_batch_id_to_products_map[batch_id] = product_burstset
            uploaded_batch_id_to_s3paths_map[batch_id] = s3paths

            logger.info(f"Submitting MGRS burst set download job {batch_id=}, num_bursts={len(product_burstset)}")
            # create args for job-submission which is handled by download mode for other product types
            args_for_job_submitter = namedtuple(
                "Namespace",
                ["chunk_size", "job_queue", "release_version"],
                defaults=[1, args.job_queue, args.release_version]
            )()
            job_submission_tasks = submit_dswx_s1_job_submissions_tasks(uploaded_batch_id_to_s3paths_map, args_for_job_submitter)
            results = await asyncio.gather(*job_submission_tasks, return_exceptions=True)
            results = [str(uuid.uuid4())]
            suceeded_batch = [job_id for job_id in results if isinstance(job_id, str)]
            failed_batch = [e for e in results if isinstance(e, Exception)]
            if suceeded_batch:
                for products_map in uploaded_batch_id_to_products_map[batch_id]:
                    for products in products_map.values():
                        for product in products:
                            if not product.get("mgrs_set_id_jobs_dict"):
                                product["mgrs_set_id_jobs_dict"] = {}
                            if not product.get("mgrs_set_id_jobs_submitted_for"):
                                product["mgrs_set_id_jobs_submitted_for"] = []

                            if not product.get("ati_jobs_dict"):
                                product["ati_jobs_dict"] = {}
                            if not product.get("ati_jobs_submitted_for"):
                                product["ati_jobs_submitted_for"] = []

                            if not product.get("dswx_s1_jobs_ids"):
                                product["dswx_s1_jobs_ids"] = []

                            # use doc obj to pass params to elasticsearch client
                            product["mgrs_set_id_jobs_dict"][batch_id.split("$")[0]] = first(suceeded_batch)
                            product["mgrs_set_id_jobs_submitted_for"].append(batch_id.split("$")[0])

                            product["ati_jobs_dict"][batch_id] = first(suceeded_batch)
                            product["ati_jobs_submitted_for"].append(batch_id)

                            product["dswx_s1_jobs_ids"].append(first(suceeded_batch))

                from data_subscriber.rtc.rtc_catalog import RTCProductCatalog
                es_conn: RTCProductCatalog
                es_conn.mark_products_as_job_submitted({batch_id: uploaded_batch_id_to_products_map[batch_id]})

                succeeded.extend(suceeded_batch)
                failed.extend(failed_batch)
    else:
        if args.subparser_name == "full":
            logger.info(f"{args.subparser_name=}. Skipping download job submission. Download will be performed directly.")
            return
        if args.no_schedule_download:
            logger.info(f"{args.no_schedule_download=}. Forcefully skipping download job submission.")
            return
        if not args.chunk_size:
            logger.info(f"{args.chunk_size=}. Insufficient chunk size. Skipping download job submission.")
            return

        job_submission_tasks = download_job_submission_handler(args, granules, query_timerange)

    if COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "RTC":
        succeeded = succeeded
        failed = failed
    else:
        results = await asyncio.gather(*job_submission_tasks, return_exceptions=True)
        logger.info(f"{len(results)=}")
        logger.info(f"{results=}")

        succeeded = [job_id for job_id in results if isinstance(job_id, str)]
        failed = [e for e in results if isinstance(e, Exception)]

    logger.info(f"{succeeded=}")
    logger.info(f"{failed=}")

    return {
        "success": succeeded,
        "fail": failed
    }


def update_affected_mgrs_set_ids(acquisition_cycle, acquisition_index, affected_mgrs_set_id_acquisition_ts_cycle_indexes, mgrs_burst_set_ids):
    acquisition_index_floor = math.floor(acquisition_index)
    acquisition_index_ceil = math.ceil(acquisition_index)
    # construct filters for evaluation
    if len(mgrs_burst_set_ids) == 1:
        # ati = Acquisition Time Index
        if acquisition_cycle == acquisition_index_floor:  # rounded down, closer to start of cycle
            current_ati = "{}${}".format(sorted(mgrs_burst_set_ids)[0], acquisition_cycle)
            future_ati = "{}${}".format(sorted(mgrs_burst_set_ids)[0], acquisition_cycle + 1)

            affected_mgrs_set_id_acquisition_ts_cycle_indexes.add(current_ati)
            affected_mgrs_set_id_acquisition_ts_cycle_indexes.add(future_ati)

        if acquisition_cycle == acquisition_index_ceil:  # rounded up, closer to end of cycle
            past_ati = "{}${}".format(sorted(mgrs_burst_set_ids)[0], acquisition_cycle - 1)
            current_ati = "{}${}".format(sorted(mgrs_burst_set_ids)[0], acquisition_cycle)

            affected_mgrs_set_id_acquisition_ts_cycle_indexes.add(past_ati)
            affected_mgrs_set_id_acquisition_ts_cycle_indexes.add(current_ati)
    elif len(mgrs_burst_set_ids) == 2:
        if acquisition_cycle == acquisition_index_floor:  # rounded down, closer to start of cycle
            current_ati_a = "{}${}".format(sorted(mgrs_burst_set_ids)[0], acquisition_cycle)
            current_ati_b = "{}${}".format(sorted(mgrs_burst_set_ids)[1], acquisition_cycle)
            future_ati = "{}${}".format(sorted(mgrs_burst_set_ids)[1], acquisition_cycle + 1)

            affected_mgrs_set_id_acquisition_ts_cycle_indexes.add(current_ati_a)
            affected_mgrs_set_id_acquisition_ts_cycle_indexes.add(current_ati_b)
            affected_mgrs_set_id_acquisition_ts_cycle_indexes.add(future_ati)
        if acquisition_cycle == acquisition_index_ceil:  # rounded up, closer to end of cycle
            past_ati = "{}${}".format(sorted(mgrs_burst_set_ids)[0], acquisition_cycle - 1)
            current_ati_a = "{}${}".format(sorted(mgrs_burst_set_ids)[0], acquisition_cycle)
            current_ati_b = "{}${}".format(sorted(mgrs_burst_set_ids)[1], acquisition_cycle)

            affected_mgrs_set_id_acquisition_ts_cycle_indexes.add(past_ati)
            affected_mgrs_set_id_acquisition_ts_cycle_indexes.add(current_ati_a)
            affected_mgrs_set_id_acquisition_ts_cycle_indexes.add(current_ati_b)
    else:
        raise AssertionError("Unexpected burst overlap")


def update_additional_fields_mgrs_set_id_acquisition_ts_cycle_indexes(acquisition_cycle, acquisition_index, additional_fields, mgrs_burst_set_ids):
    acquisition_index_floor = math.floor(acquisition_index)
    acquisition_index_ceil = math.ceil(acquisition_index)
    # construct filters for evaluation
    if len(mgrs_burst_set_ids) == 1:
        # ati = Acquisition Time Index
        if acquisition_cycle == acquisition_index_floor:  # rounded down, closer to start of cycle
            current_ati = "{}${}".format(sorted(mgrs_burst_set_ids)[0], acquisition_cycle)
            future_ati = "{}${}".format(sorted(mgrs_burst_set_ids)[0], acquisition_cycle + 1)

            additional_fields["mgrs_set_id_acquisition_ts_cycle_indexes"] = [current_ati]

        if acquisition_cycle == acquisition_index_ceil:  # rounded up, closer to end of cycle
            past_ati = "{}${}".format(sorted(mgrs_burst_set_ids)[0], acquisition_cycle - 1)
            current_ati = "{}${}".format(sorted(mgrs_burst_set_ids)[0], acquisition_cycle)

            additional_fields["mgrs_set_id_acquisition_ts_cycle_indexes"] = [current_ati]
    elif len(mgrs_burst_set_ids) == 2:
        if acquisition_cycle == acquisition_index_floor:  # rounded down, closer to start of cycle
            current_ati_a = "{}${}".format(sorted(mgrs_burst_set_ids)[0], acquisition_cycle)
            current_ati_b = "{}${}".format(sorted(mgrs_burst_set_ids)[1], acquisition_cycle)
            future_ati = "{}${}".format(sorted(mgrs_burst_set_ids)[1], acquisition_cycle + 1)

            additional_fields["mgrs_set_id_acquisition_ts_cycle_indexes"] = [current_ati_a, current_ati_b]
        if acquisition_cycle == acquisition_index_ceil:  # rounded up, closer to end of cycle
            past_ati = "{}${}".format(sorted(mgrs_burst_set_ids)[0], acquisition_cycle - 1)
            current_ati_a = "{}${}".format(sorted(mgrs_burst_set_ids)[0], acquisition_cycle)
            current_ati_b = "{}${}".format(sorted(mgrs_burst_set_ids)[1], acquisition_cycle)

            additional_fields["mgrs_set_id_acquisition_ts_cycle_indexes"] = [current_ati_a, current_ati_b]
    else:
        raise AssertionError("Unexpected burst overlap")


def download_job_submission_handler(args, granules, query_timerange):
    batch_id_to_urls_map = defaultdict(set)
    for granule in granules:
        granule_id = granule.get("granule_id")
        revision_id = granule.get("revision_id")

        if granule.get("filtered_urls"):
            # group URLs by this mapping func. E.g. group URLs by granule_id
            if COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] in ("HLS", "CSLC"):
                url_grouping_func = form_batch_id
            elif COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "SLC":
                url_grouping_func = _slc_url_to_chunk_id
            elif COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "RTC":
                pass
            else:
                raise AssertionError(f"Can't use {args.collection=} to select grouping function.")

            for filter_url in granule.get("filtered_urls"):
                batch_id_to_urls_map[url_grouping_func(granule_id, revision_id)].add(filter_url)
    logger.info(f"{batch_id_to_urls_map=}")
    if COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "RTC":
        raise NotImplementedError()
    else:
        job_submission_tasks = submit_download_job_submissions_tasks(batch_id_to_urls_map, query_timerange, args)
    return job_submission_tasks


def get_query_timerange(args, now: datetime, silent=False):
    now_minus_minutes_dt = (now - timedelta(minutes=args.minutes)) if not args.native_id else dateutil.parser.isoparse("1900-01-01T00:00:00Z")

    start_date = args.start_date if args.start_date else now_minus_minutes_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
    end_date = args.end_date if args.end_date else now.strftime("%Y-%m-%dT%H:%M:%SZ")

    query_timerange = DateTimeRange(start_date, end_date)
    if not silent:
        logger.info(f"{query_timerange=}")
    return query_timerange


def submit_download_job_submissions_tasks(batch_id_to_urls_map, query_timerange, args):
    job_submission_tasks = []
    logger.info(f"{args.chunk_size=}")
    for batch_chunk in chunked(batch_id_to_urls_map.items(), n=args.chunk_size):
        chunk_id = str(uuid.uuid4())
        logger.info(f"{chunk_id=}")

        chunk_batch_ids = []
        chunk_urls = []
        for batch_id, urls in batch_chunk:
            chunk_batch_ids.append(batch_id)
            chunk_urls.extend(urls)

        logger.info(f"{chunk_batch_ids=}")
        logger.info(f"{chunk_urls=}")

        job_submission_tasks.append(
            asyncio.get_event_loop().run_in_executor(
                executor=None,
                func=partial(
                    submit_download_job,
                    release_version=args.release_version,
                    product_type=COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection],
                    params=create_download_job_params(args, query_timerange, chunk_batch_ids),
                    job_queue=args.job_queue
                )
            )
        )
    return job_submission_tasks


def create_download_job_params(args, query_timerange, chunk_batch_ids):
    return [
        {
            "name": "batch_ids",
            "value": "--batch-ids " + " ".join(chunk_batch_ids) if chunk_batch_ids else "",
            "from": "value"
        },
        {
            "name": "smoke_run",
            "value": "--smoke-run" if args.smoke_run else "",
            "from": "value"
        },
        {
            "name": "dry_run",
            "value": "--dry-run" if args.dry_run else "",
            "from": "value"
        },
        {
            "name": "endpoint",
            "value": f"--endpoint={args.endpoint}",
            "from": "value"
        },
        {
            "name": "start_datetime",
            "value": f"--start-date={query_timerange.start_date}",
            "from": "value"
        },
        {
            "name": "end_datetime",
            "value": f"--end-date={query_timerange.end_date}",
            "from": "value"
        },
        {
            "name": "use_temporal",
            "value": "--use-temporal" if args.use_temporal else "",
            "from": "value"
        },
        {
            "name": "transfer_protocol",
            "value": f"--transfer-protocol={args.transfer_protocol}",
            "from": "value"
        },
        {
            "name": "proc_mode",
            "value": f"--processing-mode={args.proc_mode}",
            "from": "value"
        }
    ]


def submit_download_job(*, release_version=None, product_type: Literal["HLS", "SLC", "RTC", "CSLC"], params: list[dict[str, str]], job_queue: str) -> str:
    job_spec_str = f"job-{product_type.lower()}_download:{release_version}"

    return _submit_mozart_job_minimal(
        hysdsio={
            "id": str(uuid.uuid4()),
            "params": params,
            "job-specification": job_spec_str
        },
        job_queue=job_queue,
        provider_str=product_type.lower()
    )


def _submit_mozart_job_minimal(*, hysdsio: dict, job_queue: str, provider_str: str) -> str:
    return submit_mozart_job(
        hysdsio=hysdsio,
        product={},
        rule={
            "rule_name": f"trigger-{provider_str}_download",
            "queue": job_queue,
            "priority": "0",
            "kwargs": "{}",
            "enable_dedup": True
        },
        queue=None,
        job_name=f"job-WF-{provider_str}_download",
        payload_hash=None,
        enable_dedup=None,
        soft_time_limit=None,
        time_limit=None,
        component=None
    )


def update_url_index(
        es_conn,
        urls: list[str],
        granule_id: str,
        job_id: str,
        query_dt: datetime,
        temporal_extent_beginning_dt: datetime,
        revision_date_dt: datetime,
        *args,
        **kwargs
):
    # group pairs of URLs (http and s3) by filename
    filename_to_urls_map = defaultdict(list)
    for url in urls:
        filename = Path(url).name
        filename_to_urls_map[filename].append(url)

    for filename, filename_urls in filename_to_urls_map.items():
        es_conn.process_url(filename_urls, granule_id, job_id, query_dt, temporal_extent_beginning_dt, revision_date_dt, *args, **kwargs)


def update_granule_index(es_spatial_conn, granule, *args, **kwargs):
    es_spatial_conn.process_granule(granule, *args, **kwargs)


def localize_include_exclude(args):

    geojsons = []

    if args.include_regions is not None:
        geojsons.extend(args.include_regions.split(","))

    if args.exclude_regions is not None:
        geojsons.extend(args.exclude_regions.split(","))

    localize_geojsons(geojsons)


def localize_geojsons(geojsons):
    settings = SettingsConf().cfg
    bucket = settings["GEOJSON_BUCKET"]

    try:
        for geojson in geojsons:
            key = geojson.strip() + ".geojson"
            # output_filepath = os.path.join(working_dir, key)
            download_object_from_s3(bucket, key, key, filetype="geojson")
    except Exception as e:
        raise Exception("Exception while fetching geojson file: %s. " % key + str(e))


def does_granule_intersect_regions(granule, intersect_regions):
    regions = intersect_regions.split(',')
    for region in regions:
        region = region.strip()
        if does_bbox_intersect_region(granule["bounding_box"], region):
            return True, region

    return False, None


def filter_granules_by_regions(granules, include_regions, exclude_regions):
    '''Filters granules based on include and exclude regions lists'''
    filtered = []

    for granule in granules:

        # Skip this granule if it's not in the include list
        if include_regions is not None:
            (result, region) = does_granule_intersect_regions(granule, include_regions)
            if result is False:
                logging.info(
                    f"The following granule does not intersect with any include regions. Skipping processing %s"
                    % granule.get("granule_id"))
                continue

        # Skip this granule if it's in the exclude list
        if exclude_regions is not None:
            (result, region) = does_granule_intersect_regions(granule, exclude_regions)
            if result is True:
                logging.info(f"The following granule intersects with the exclude region %s. Skipping processing %s"
                             % (region, granule.get("granule_id")))
                continue

        # If both filters don't apply, add this granule to the list
        filtered.append(granule)

    return filtered


def filter_granules_rtc(granules, args):
    filtered_granules = []
    for granule in granules:
        granule_id = granule.get("granule_id")

        if COLLECTION_TO_PRODUCT_TYPE_MAP[args.collection] == "RTC":
            match_product_id = re.match(r"OPERA_L2_RTC-S1_(?P<burst_id>[^_]+)_(?P<acquisition_dts>[^_]+)_*", granule_id)
            burst_id = match_product_id.group("burst_id")

            mgrs = mbc_client.cached_load_mgrs_burst_db(filter_land=True)
            mgrs_sets = mbc_client.burst_id_to_mgrs_set_ids(mgrs,
                                                            mbc_client.product_burst_id_to_mapping_burst_id(burst_id))
            if not mgrs_sets:
                logging.debug(f"{burst_id=} not associated with land or land/water data. skipping.")
                continue

        filtered_granules.append(granule)
    return filtered_granules


