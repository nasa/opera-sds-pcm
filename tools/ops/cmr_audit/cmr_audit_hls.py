import argparse
import asyncio
from datetime import datetime, timedelta, date, timezone
import functools
import logging
import logging.handlers
import os
import re
import sys
import urllib.parse
from collections import defaultdict
from typing import Union, Iterable

import aiohttp
import dateutil.parser
from dotenv import dotenv_values
from more_itertools import always_iterable, chunked, partition


from tools.ops.cmr_audit.cmr_audit_utils import async_get_cmr_granules, get_cmr_audit_granules

logging.getLogger("compact_json.formatter").setLevel(level=logging.INFO)
logging.basicConfig(
    format="%(levelname)7s: %(relativeCreated)7d %(name)s:%(filename)s:%(funcName)s:%(lineno)s - %(message)s",  # alternative format which displays time elapsed.
    # format="%(asctime)s %(levelname)7s %(name)4s:%(filename)8s:%(funcName)22s:%(lineno)3s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO)
logger = logging.getLogger(__name__)

config = {
    **dotenv_values("../../.env"),
    **os.environ
}


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
        choices=["txt", "json", "db"],
        help=f'Output file format. Defaults to "%(default)s".'
    )
    argparser.add_argument('--log-level', default='INFO', choices=('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL'))

    return argparser


def init_logging(log_level=logging.INFO):
    log_file_format = "%(asctime)s %(levelname)7s %(name)13s:%(filename)19s:%(funcName)22s:%(lineno)3s - %(message)s"
    log_format = "%(levelname)s: %(relativeCreated)7d %(process)d %(processName)s %(thread)d %(threadName)s %(name)s:%(filename)s:%(funcName)s:%(lineno)s - %(message)s"
    logging.basicConfig(level=log_level, format=log_format, datefmt="%Y-%m-%d %H:%M:%S", force=True)

    rfh1 = logging.handlers.RotatingFileHandler('cmr_audit_hls.log', mode='a', maxBytes=100 * 2 ** 20, backupCount=10)
    rfh1.setLevel(logging.INFO)
    rfh1.setFormatter(logging.Formatter(fmt=log_file_format))
    logging.getLogger().addHandler(rfh1)

    rfh2 = logging.handlers.RotatingFileHandler('cmr_audit_hls-error.log', mode='a', maxBytes=100 * 2 ** 20, backupCount=10)
    rfh2.setLevel(logging.ERROR)
    rfh2.setFormatter(logging.Formatter(fmt=log_file_format))
    logging.getLogger().addHandler(rfh2)

def print_mem():
    import psutil
    print('memory % used:', psutil.virtual_memory()[2])


#######################################################################
# CMR AUDIT FUNCTIONS
#######################################################################

async def async_get_cmr_granules_hls_l30(temporal_date_start: str, temporal_date_end: str):
    return await async_get_cmr_granules(collection_short_name="HLSL30",
                                        temporal_date_start=temporal_date_start, temporal_date_end=temporal_date_end,
                                        platform_short_name="LANDSAT-8")


async def async_get_cmr_granules_hls_s30(temporal_date_start: str, temporal_date_end: str):
    return await async_get_cmr_granules(collection_short_name="HLSS30",
                                        temporal_date_start=temporal_date_start, temporal_date_end=temporal_date_end,
                                        platform_short_name=["Sentinel-2A", "Sentinel-2B"])


async def async_get_cmr_dswx(rtc_native_id_patterns: set, temporal_date_start: str, temporal_date_end: str):
    return await async_get_cmr(rtc_native_id_patterns, collection_short_name=["OPERA_L3_DSWX-HLS_PROVISIONAL_V0", "OPERA_L3_DSWX-HLS_PROVISIONAL_V1", "OPERA_L3_DSWX-HLS_V1"],
                               temporal_date_start=temporal_date_start, temporal_date_end=temporal_date_end)


async def async_get_cmr(
        native_id_patterns: set,
        collection_short_name: Union[str, Iterable[str]],
        temporal_date_start: str, temporal_date_end: str,
        chunk_size=1000
):
    logger.debug(f"entry({len(native_id_patterns)=:,})")

    # batch granules-requests due to CMR limitation. 1000 native-id clauses seems to be near the limit.
    native_id_patterns = always_iterable(native_id_patterns)
    native_id_pattern_batches = list(chunked(native_id_patterns, chunk_size))  # 1000 == 55,100 length

    request_url = "https://cmr.earthdata.nasa.gov/search/granules.umm_json"

    sem = asyncio.Semaphore(15)
    async with aiohttp.ClientSession() as session:
        post_cmr_tasks = []
        for i, native_id_pattern_batch in enumerate(native_id_pattern_batches, start=1):
            # native_id_patterns_query_params = "&native_id[]=" + "&native_id[]=".join(native_id_pattern_batch)

            request_body = (
                "provider=POCLOUD"
                f'{"&short_name[]=" + "&short_name[]=".join(always_iterable(collection_short_name))}'
                # "&options[native-id][pattern]=true"
                # f"{native_id_patterns_query_params}"
                f"&temporal[]={urllib.parse.quote(temporal_date_start, safe='/:')},{urllib.parse.quote(temporal_date_end, safe='/:')}"
            )
            logger.debug(f"Creating request task {i} of {len(native_id_pattern_batches)}")
            post_cmr_tasks.append(get_cmr_audit_granules(request_url, request_body, session, sem))
            break
        logger.debug(f"Number of query requests to make: {len(post_cmr_tasks)=}")

        logger.debug("Batching tasks")
        cmr_granules = list()
        cmr_granules_details = {}
        task_chunks = list(chunked(post_cmr_tasks, len(post_cmr_tasks)))  # CMR recommends 2-5 threads.
        for i, task_chunk in enumerate(task_chunks, start=1):
            logger.debug(f"Processing batch {i} of {len(task_chunks)}")
            post_cmr_tasks_results, post_cmr_tasks_failures = partition(
                lambda it: isinstance(it, Exception),
                await asyncio.gather(*task_chunk, return_exceptions=False)
            )
            for post_cmr_tasks_result in post_cmr_tasks_results:
                cmr_granules.extend(post_cmr_tasks_result[0])
            # DEV: uncomment as needed
                cmr_granules_details.update(post_cmr_tasks_result[1])
        logger.info(f"{collection_short_name} {len(cmr_granules)=:,}")
        return cmr_granules, cmr_granules_details


hls_regex = (
    r'(?P<product_shortname>HLS[.]([LS])30)[.]'
    r'(?P<tile_id>T[^\W_]{5})[.]'
    r'(?P<acquisition_ts>(?P<year>\d{4})(?P<day_of_year>\d{3})T(?P<hour>\d{2})(?P<minute>\d{2})(?P<second>\d{2}))[.]'
    r'(?P<collection_version>v\d+[.]\d+)$'
)


def hls_granule_ids_to_dswx_native_id_patterns(cmr_granules: set[str], input_to_outputs_map: defaultdict, output_to_inputs_map: defaultdict):
    dswx_native_id_patterns = set()
    for granule in cmr_granules:
        m = re.match(hls_regex, granule)
        tile = m.group("tile_id")
        year = m.group("year")
        doy = m.group("day_of_year")
        time_of_day = m.group("acquisition_ts").split("T")[1]
        date = datetime(int(year), 1, 1) + timedelta(int(doy) - 1)
        dswx_acquisition_dt_str = f"{date.strftime('%Y%m%d')}T{time_of_day}"

        dswx_native_id_pattern = f'OPERA_L3_DSWx-HLS_{tile}_{dswx_acquisition_dt_str}Z_'
        dswx_native_id_patterns.add(dswx_native_id_pattern + "*")

        # bi-directional mapping of HLS-DSWx inputs and outputs
        if hasattr(input_to_outputs_map[granule], "add"):
            input_to_outputs_map[granule].add(dswx_native_id_pattern)  # strip wildcard char
        else:  # hasattr(input_to_outputs_map[granule], "append"):
            input_to_outputs_map[granule].append(dswx_native_id_pattern)  # strip wildcard char

        output_to_inputs_map[dswx_native_id_pattern].add(granule)

    return dswx_native_id_patterns


def dswx_native_ids_to_prefixes(cmr_dswx_native_ids):
    return {dswx_native_id_to_prefix(cmr_dswx_native_id) for cmr_dswx_native_id in cmr_dswx_native_ids}

def dswx_native_id_to_prefix(cmr_dswx_native_id):
    dswx_regex_pattern = (
        r'(?P<project>OPERA)_'
        r'(?P<level>L3)_'
        r'(?P<product_type>DSWx)-(?P<source>HLS)_'
        r'(?P<tile_id>T[^\W_]{5})_'
        r'(?P<acquisition_ts>(?P<acq_year>\d{4})(?P<acq_month>\d{2})(?P<acq_day>\d{2})T(?P<acq_hour>\d{2})(?P<acq_minute>\d{2})(?P<acq_second>\d{2})Z)_'
    )
    return re.match(dswx_regex_pattern, cmr_dswx_native_id).group(0)



def to_dsxw_metadata_small(missing_cmr_granules, cmr_granules_details, input_hls_to_outputs_dswx_map):
    missing_cmr_granules_details_short = [
        {
            "id": i,
            "expected-dswx-id-prefix": next(iter(input_hls_to_outputs_dswx_map[i])),
            "revision-date": cmr_granules_details[i]["meta"]["revision-date"],
            # TODO chrisjrd: commented out for ad-hoc request. 5/10/2023
            # "provider-date": next(iter(
            #     cmr_granules_details[i]["umm"]["ProviderDates"]
            # ))["Date"],
            # "temporal-date": cmr_granules_details[i]["umm"]["TemporalExtent"]["RangeDateTime"]["BeginningDateTime"],
            # "hls-processing-time": next(iter(
            #     next(iter(
            #         list(filter(lambda a: a["Name"] == "HLS_PROCESSING_TIME",
            #                     cmr_granules_details[i]["umm"]["AdditionalAttributes"]))
            #     ))["Values"]
            # )),
            "sensing-time": next(iter(
                next(iter(
                    list(filter(lambda a: a["Name"] == "SENSING_TIME",
                                cmr_granules_details[i]["umm"]["AdditionalAttributes"]))
                ))["Values"]
            ))
        }
        for i in missing_cmr_granules
    ]

    return missing_cmr_granules_details_short

#######################################################################
# CMR AUDIT
#######################################################################

async def run(argv: list[str]):
    logger.info(f'{argv=}')
    args = create_parser().parse_args(argv[1:])

    logger.info("Querying CMR for list of expected L30 and S30 granules (HLS)")
    cmr_start_dt_str = args.start_datetime
    cmr_start_dt = dateutil.parser.isoparse(cmr_start_dt_str)
    cmr_end_dt_str = args.end_datetime
    cmr_end_dt = dateutil.parser.isoparse(cmr_end_dt_str)

    cmr_granules_l30, cmr_granules_l30_details = await async_get_cmr_granules_hls_l30(temporal_date_start=cmr_start_dt_str, temporal_date_end=cmr_end_dt_str)
    cmr_granules_s30, cmr_granules_s30_details = await async_get_cmr_granules_hls_s30(temporal_date_start=cmr_start_dt_str, temporal_date_end=cmr_end_dt_str)

    cmr_granules_hls = cmr_granules_l30.union(cmr_granules_s30)
    cmr_granules_details = {}; cmr_granules_details.update(cmr_granules_l30_details); cmr_granules_details.update(cmr_granules_s30_details)
    logger.info(f"Expected input (granules): {len(cmr_granules_hls)=:,}")

    dswx_native_id_patterns = hls_granule_ids_to_dswx_native_id_patterns(
        cmr_granules_hls,
        input_hls_to_outputs_dswx_map := defaultdict(list),
        output_dswx_to_inputs_hls_map := defaultdict(set)
    )
    dswx_native_id_prefixes = {prefix[:-1] for prefix in dswx_native_id_patterns}

    logger.info("Querying CMR for list of expected DSWx granules")
    cmr_dswx_products, cmr_dswx_products_details = await async_get_cmr_dswx(dswx_native_id_patterns, temporal_date_start=cmr_start_dt_str, temporal_date_end=cmr_end_dt_str)

    cmr_dswx_prefix_expected = dswx_native_id_prefixes
    cmr_dswx_prefix_actual = dswx_native_ids_to_prefixes(cmr_dswx_products)
    missing_cmr_dswx_granules_prefixes = cmr_dswx_prefix_expected - cmr_dswx_prefix_actual

    #######################################################################
    # CMR_AUDIT SUMMARY
    #######################################################################
    # logger.debug(f"{pstr(missing_cmr_dswx_granules_prefixes)=!s}")

    missing_cmr_granules_hls = [output_dswx_to_inputs_hls_map[prefix] for prefix in missing_cmr_dswx_granules_prefixes]
    missing_cmr_granules_hls = set(functools.reduce(set.union, missing_cmr_granules_hls)) if missing_cmr_granules_hls else set()

    # logger.debug(f"{pstr(missing_cmr_granules)=!s}")
    logger.info(f"Expected input (granules): {len(cmr_granules_hls)=:,}")
    logger.info(f"Fully published (granules): {len(cmr_dswx_products)=:,}")
    logger.info(f"Missing processed (granules): {len(missing_cmr_granules_hls)=:,}")

    start_dt_str = cmr_start_dt.strftime("%Y%m%d-%H%M%S")
    end_dt_str = cmr_start_dt.strftime("%Y%m%d-%H%M%S")
    current_dt_str = datetime.now().strftime("%Y%m%d-%H%M%S")
    outfilename = f"missing_granules_HLS-DSWx_{start_dt_str}Z_{end_dt_str}Z_{current_dt_str}Z"

    if args.format == "db":
        def date_from_hls(granule_id):
            return date.fromtimestamp(
                datetime.strptime(granule_id.split(".")[3].split("T")[0], "%Y%j").timestamp())
        def dt_from_hls(granule_id):
            return datetime.fromtimestamp(
                datetime.strptime(granule_id.split(".")[3], "%Y%jT%H%M%S").timestamp())
        def dt_from_dswx_hls(granule_id):
            return datetime.fromtimestamp(
                dateutil.parser.isoparse(granule_id.split("_")[4].replace("Z", "")).timestamp())

        # group input products by acquisition dt
        hls_products_by_acq_date = defaultdict(set)
        for hls in input_hls_to_outputs_dswx_map:
            date_hls = date_from_hls(hls)
            hls_products_by_acq_date[date_hls].add(hls)

        # TODO use this instead for parity with SLC scripts
        hls_products_by_acq_dt = defaultdict(set)
        hls_l30_products_by_acq_dt = defaultdict(set)
        hls_s30_products_by_acq_dt = defaultdict(set)
        for hls in input_hls_to_outputs_dswx_map:
            dt_hls = dt_from_hls(hls)
            tile_id = hls.split(".")[2]
            hls_products_by_acq_dt[(tile_id, dt_hls)].add(hls)
            if "L30" in hls:
                hls_l30_products_by_acq_dt[(tile_id, dt_hls)].add(hls)
            elif "S30" in hls:
                hls_s30_products_by_acq_dt[(tile_id, dt_hls)].add(hls)

        # group output products by acquisition dt
        dswx_hls_prefix_by_acq_date = defaultdict(set)
        for hls in input_hls_to_outputs_dswx_map:
            date_hls = date_from_hls(hls)
            dswx_hls_prefix_by_acq_date[date_hls].update(input_hls_to_outputs_dswx_map[hls])

        # TODO use this instead for parity with SLC scripts
        dswx_hls_products_by_acq_dt = defaultdict(set)
        dswx_hls_l30_products_by_acq_dt = defaultdict(set)
        dswx_hls_s30_products_by_acq_dt = defaultdict(set)
        for dswx in cmr_dswx_products:
            dt_dswx = dt_from_dswx_hls(dswx)
            tile_id = dswx.split("_")[3]
            dswx_hls_products_by_acq_dt[(tile_id, dt_dswx)].update([dswx])
            if "_L8_30" in dswx:
                dswx_hls_l30_products_by_acq_dt[(tile_id, dt_dswx)].update([dswx])
            elif "_S2A_30" in dswx or "_S2B_30" in dswx:
                dswx_hls_s30_products_by_acq_dt[(tile_id, dt_dswx)].update([dswx])


        # duplicate detection
        dswx_hls_prefix_to_products_map = defaultdict(set)
        for cmr_dswx_product in cmr_dswx_products:
            prefix = dswx_native_id_to_prefix(cmr_dswx_product)
            dswx_hls_prefix_to_products_map[prefix].add(cmr_dswx_product)

        duplicates_dswx_hls_prefix_to_products_map = {
            prefix: products
            for prefix, products in dswx_hls_prefix_to_products_map.items()
            if len(products) >= 2
        }

        hls_to_dswx_map = defaultdict(set)
        for hls, dswx_prefix_list in input_hls_to_outputs_dswx_map.items():
            for dswx_prefix in dswx_prefix_list:
                if dswx_prefix in duplicates_dswx_hls_prefix_to_products_map:
                    hls_to_dswx_map[hls].update(duplicates_dswx_hls_prefix_to_products_map[dswx_prefix])
        duplicates_report = hls_to_dswx_map

        # TODO use this for parity with SLC script
        dswx_hls_duplicates_by_acq_dt = {
            dt: products
            for dt, products in dswx_hls_products_by_acq_dt.items()
            if len(products) >= 2
        }
        dswx_hls_l30_duplicates_by_acq_dt = {
            dt: products
            for dt, products in dswx_hls_l30_products_by_acq_dt.items()
            if len(products) >= 2
        }
        dswx_hls_s30_duplicates_by_acq_dt = {
            dt: products
            for dt, products in dswx_hls_s30_products_by_acq_dt.items()
            if len(products) >= 2
        }

        missing_dswx_hls_l30_dts = hls_l30_products_by_acq_dt.keys() - dswx_hls_l30_products_by_acq_dt.keys()
        print(f"{missing_dswx_hls_l30_dts=}")
        missing_dswx_hls_s30_dts = hls_s30_products_by_acq_dt.keys() - dswx_hls_s30_products_by_acq_dt.keys()
        print(f"{missing_dswx_hls_s30_dts=}")

        print_mem()

        # group metrics by output type and acquisition dt
        product_accountability_map = {}

        dswx_hls_accountability_map = {"DSWx-HLS": {}}
        for acquisition_date in hls_products_by_acq_date:
            dswx_hls_accountability_map["DSWx-HLS"][acquisition_date] = {}

            num_inputs = len(hls_products_by_acq_date[acquisition_date])
            dswx_hls_accountability_map["DSWx-HLS"][acquisition_date]["expected_inputs"] = num_inputs

            num_outputs = len(dswx_hls_prefix_by_acq_date[acquisition_date])
            dswx_hls_accountability_map["DSWx-HLS"][acquisition_date]["produced_outputs"] = num_outputs
        # print(dswx_hls_accountability_map)

        product_accountability_map.update(dswx_hls_accountability_map)

        # group missing inputs by output type and acquisition dt
        output_product_types_to_products_map = {
            # "DSWx-HLS": {
            #     "2024-01-01": {"HLS-A", "HLS-B", ...}
            # }
        }
        dswx_hls_output_date_to_missing_input_products_map = {"DSWx-HLS": {}}
        output_product_types_to_products_map.update(dswx_hls_output_date_to_missing_input_products_map)
        for hls in missing_cmr_granules_hls:
            date_hls = date_from_hls(hls)
            if not output_product_types_to_products_map["DSWx-HLS"].get(date_hls):
                output_product_types_to_products_map["DSWx-HLS"][date_hls] = set()
            output_product_types_to_products_map["DSWx-HLS"][date_hls].add(hls)
        # print(dswx_hls_output_date_to_missing_input_products_map)

        # create DB model (accountability)
        docs = []
        for product_type in product_accountability_map:
            acquisition_date: date
            for acquisition_date in product_accountability_map[product_type]:
                doc = {
                    "acquisition_date": acquisition_date.isoformat(),
                    "product_type": product_type,
                    "num_inputs": product_accountability_map[product_type][acquisition_date]["expected_inputs"],
                    "num_outputs": product_accountability_map[product_type][acquisition_date]["produced_outputs"],
                    "num_missing": len(output_product_types_to_products_map[product_type].get(acquisition_date, []))
                }
                docs.append(doc)


        from pymongo import MongoClient
        def db_init():
            client = MongoClient(host="localhost")
            # client = MongoClient(host="mongo")  # TODO chrisjrd: needed for docker-compose (name of service)
            db = client["new_db"]  # switch db

            # NOTE: In EC2 against a Mongo DB Docker container, this doesn't create the DB.
            #  This works locally on macOS against a Mongo DB Docker container, however.
            try:
                assert "new_db" in client.list_database_names()
            except:
                pass
            return db

        def create_collection(db, collection_name):
            # try to create the collection upfront in the new db in Mongo DB
            #  may not work in some version-client combinations, so ignore errors:
            #  Mongo DB will likely create the collection on document insert instead
            try:
                db.create_collection(collection_name)
                assert collection_name in db.list_collection_names()
            except Exception:
                logger.warning("Failed to create collection. It may already exist. This warning may be safely ignored.")
            jobs_collection = db[collection_name]
            return jobs_collection

        # write out to DB
        db = db_init()

        create_collection(db, "accountability")
        jobs_collection = db["accountability"]
        for doc in docs:
            doc.update({"last_update": datetime.now(tz=timezone.utc)})
            jobs_collection.update_one(
                filter={
                    "acquisition_date": doc["acquisition_date"],
                    "product_type": doc["product_type"]
                },
                update={"$set": doc},
                upsert=True
            )

        # create DB model (missing products)
        docs = []
        for product_type in output_product_types_to_products_map:
            acquisition_date: date
            for acquisition_date in product_accountability_map[product_type]:
                missing_products = output_product_types_to_products_map[product_type].get(acquisition_date, [])
                for missing_product in missing_products:
                    doc = {
                        "acquisition_date": acquisition_date.isoformat(),
                        "product_type": product_type,
                        "product": missing_product
                    }
                    docs.append(doc)

        create_collection(db, "missing_products")
        missing_collection = db["missing_products"]
        for doc in docs:
            doc.update({"last_update": datetime.now(tz=timezone.utc)})
            missing_collection.update_one(
                filter={
                    "acquisition_date": doc["acquisition_date"],
                    "product_type": doc["product_type"],
                    "product": doc["product"]
                },
                update={"$set": doc},
                upsert=True
            )

        # create db model (duplicates)
        docs = []
        for _, duplicates in dswx_hls_duplicates_by_acq_dt.items():
            for duplicate in duplicates:
                docs.append({"product": duplicate})

        create_collection(db, "duplicate_products")
        duplicates_collection = db["duplicate_products"]
        for doc in docs:
            doc.update({"last_update": datetime.now(tz=timezone.utc)})
            duplicates_collection.update_one(
                filter={"product": doc["product"]},
                update={"$set": doc},
                upsert=True
            )


    elif args.format == "txt":
        output_file_missing_cmr_granules = args.output if args.output else f"{outfilename}.txt"
        logger.info(f"Writing granule list to file {output_file_missing_cmr_granules!r}")
        with open(output_file_missing_cmr_granules, mode='w') as fp:
            fp.write('\n'.join(missing_cmr_granules_hls))

        logger.info(f"Finished writing to file {output_file_missing_cmr_granules!r}")
    elif args.format == "json":
        output_file_missing_cmr_granules = args.output if args.output else f"{outfilename}.json"
        with open(output_file_missing_cmr_granules, mode='w') as fp:
            from compact_json import Formatter
            formatter = Formatter(indent_spaces=2, max_inline_length=300, max_compact_list_complexity=0)
            json_str = formatter.serialize(list(missing_cmr_granules_hls))
            fp.write(json_str)

        logger.info(f"Finished writing to file {output_file_missing_cmr_granules!r}")
    else:
        raise Exception()

    # DEV: uncomment to export granules and metadata
    # missing_cmr_granules_details_short = to_dsxw_metadata_small(missing_cmr_granules, cmr_granules_details, input_hls_to_outputs_dswx_map)
    # with open(output_file_missing_cmr_granules.replace(".json", " - details.json"), mode='w') as fp:
    #     from compact_json import Formatter
    #     formatter = Formatter(indent_spaces=2, max_inline_length=300)
    #     json_str = formatter.serialize(missing_cmr_granules_details_short)
    #     fp.write(json_str)


if __name__ == "__main__":
    print_mem()

    args = create_parser().parse_args(sys.argv[1:])
    log_level = args.log_level
    init_logging()

    asyncio.run(run(sys.argv))
