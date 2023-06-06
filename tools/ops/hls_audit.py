import argparse
import logging
import os
import sys
from getpass import getpass
from io import StringIO
from pathlib import PurePath
from pprint import pprint

import elasticsearch
import more_itertools
from dotenv import dotenv_values
from elasticsearch import RequestsHttpConnection, helpers

logging.getLogger("elasticsearch").setLevel(level=logging.WARNING)
logging.basicConfig(
    # format="%(levelname)s: %(relativeCreated)7d %(name)s:%(filename)s:%(funcName)s:%(lineno)s - %(message)s",  # alternative format which displays time elapsed.
    format="%(asctime)s %(levelname)7s %(name)4s:%(filename)8s:%(funcName)22s:%(lineno)3s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO)
logger = logging.getLogger()

config = {
    **dotenv_values("../.env"),
    **os.environ
}

try:
    logger.warning("Setting password via dotenv is not recommended. Leave empty to be prompted to enter password.")
    password = config["ES_PASSWORD"]
except:
    password = getpass()

kwargs = {
    "http_auth": (config["ES_USER"], password),
    "connection_class": RequestsHttpConnection,
    "use_ssl": True,
    "verify_certs": False,
    "ssl_show_warn": False,
}
es = elasticsearch.Elasticsearch(hosts=[config["ES_BASE_URL"]], **kwargs)
del password


def pstr(o):
    sio = StringIO()
    pprint(o, stream=sio)
    sio.seek(0)
    return sio.read()


def get_body() -> dict:
    return {
        "query": {
            "bool": {
                "must": [{"match_all": {}}],
                "must_not": [],
                "should": []
            }
        },
        "from": 0,
        "size": 10_000,
        "sort": [],
        "aggs": {},
        "_source": {"includes": [], "excludes": []}
    }

argparser = argparse.ArgumentParser(add_help=True)
argparser.add_argument(
    "--start-datetime",
    default="1970-01-01T00:00:00.000000",
    help=f'ISO formatted datetime string. Must be compatible with Python Elasticsearch Client. Defaults to "%(default)s".'
)
argparser.add_argument(
    "--end-datetime",
    default="9999-01-01T00:00:00.000000",
    help=f'ISO formatted datetime string. Must be compatible with Python Elasticsearch Client. Defaults to "%(default)s".'
)

logger.info(f'{sys.argv=}')
args = argparser.parse_args(sys.argv[1:])


def get_range(
        datetime_fieldname="creation_timestamp",
        start_dt_iso=args.start_datetime,
        end_dt_iso=args.end_datetime
) -> dict:
    return {
        "range": {
            datetime_fieldname: {
                "gte": start_dt_iso,
                "lt": end_dt_iso
            }
        }
    }


#######################################################################
# GET MASTER LIST. THIS IS THE LIST OF QUERIED/DOWNLOADED FILES
#######################################################################

body = get_body()
body["_source"]["includes"] = "false"
body["query"]["bool"]["must"].append(get_range("query_datetime"))
search_results = list(helpers.scan(es, body, index="hls_catalog", scroll="5m", size=10_000))
queried_or_downloaded_files = {hit["_id"].removesuffix(".tif") for hit in search_results}
logger.debug(f'{pstr(queried_or_downloaded_files)=!s}')

logger.info(f'Data queried or downloaded (files): {len(search_results)=:,}')
logger.debug(f'{pstr(queried_or_downloaded_files)=!s}')

queried_or_downloaded_granules = more_itertools.map_reduce(
    queried_or_downloaded_files,
    lambda k: PurePath(k).with_suffix("").name
)
queried_or_downloaded_granules = set(queried_or_downloaded_granules.keys())
logger.info(f'Data queried or downloaded (granules): {len(queried_or_downloaded_granules)=:,}')
logger.debug(f'{pstr(queried_or_downloaded_granules)=!s}')

# missing_queried_or_downloaded_granules = cmr_granules - queried_or_downloaded_granules
# logger.info(f'Missing queried (granules): {len(missing_queried_or_downloaded_granules)=:,}')
# logger.debug(f'{pstr(missing_queried_or_downloaded_granules)=!s}')

body = get_body()
body["_source"]["includes"] = "false"
body["query"]["bool"]["must"].append(get_range("query_datetime"))
body["query"]["bool"]["must"].append({"term": {"downloaded": "true"}})
search_results = list(helpers.scan(es, body, index="hls_catalog", scroll="5m", size=10_000))
downloaded_files = {hit["_id"].removesuffix(".tif") for hit in search_results}

logger.info(f'Data downloaded (files): {len(search_results)=:,}')
logger.debug(f'{pstr(downloaded_files)=!s}')

downloaded_granules = more_itertools.map_reduce(
    downloaded_files,
    lambda k: PurePath(k).with_suffix("").name
)
downloaded_granules = set(downloaded_granules.keys())
logger.info(f'Data downloaded (granules): {len(downloaded_granules)=:,}')
logger.debug(f'{pstr(downloaded_granules)=!s}')

missing_download_files = queried_or_downloaded_files - downloaded_files
logger.info(f'Missing download (files): {len(missing_download_files)=:,}')
logger.debug(f'{pstr(missing_download_files)=!s}')

missing_download_granules = more_itertools.map_reduce(
    missing_download_files,
    lambda k: PurePath(k).with_suffix("").name
)
missing_download_granules = set(missing_download_granules.keys())
logger.info(f'Missing download (granules): {len(missing_download_granules)=:,}')
logger.debug(f'{pstr(missing_download_granules)=!s}')

#######################################################################
# GET L2 products (data ingested)
#######################################################################

body = get_body()
body["_source"]["includes"] = ["metadata.Files"]
body["query"]["bool"]["must"].append(get_range("creation_timestamp"))
search_results = list(helpers.scan(es, body, index="grq_*_l2_hls_l30", scroll="5m", size=10_000))
l30_ingested_files = {input["FileName"].removesuffix(".tif")
                      for hit in search_results
                      for input in hit["_source"]["metadata"]["Files"]}

logger.info(f'Data ingested (L30): {len(search_results)=:,}')

body = get_body()
body["query"]["bool"]["must"].append(get_range("creation_timestamp"))
search_results = list(helpers.scan(es, body, index="grq_*_l2_hls_s30", scroll="5m", size=10_000))
s30_ingested_files = {input["FileName"].removesuffix(".tif")
                      for hit in search_results
                      for input in hit["_source"]["metadata"]["Files"]}

logger.info(f'Data ingested (S30): {len(search_results)=:,}')

all_ingested_files = l30_ingested_files.union(s30_ingested_files)
logger.info(f'Data ingested (total) (files): {len(all_ingested_files)=:,}')

all_ingested_granules = more_itertools.map_reduce(
    all_ingested_files,
    lambda k: PurePath(k).with_suffix("").name
)
all_ingested_granules = set(all_ingested_granules.keys())
logger.info(f'Data ingested (total) (granules): {len(all_ingested_granules)=:,}')

missing_data_ingest_files = downloaded_files - all_ingested_files
logger.info(f'Missing data ingest (files): {len(missing_data_ingest_files)=:,}')
logger.debug(f'{pstr(missing_data_ingest_files)=!s}')

missing_data_ingest_granules = more_itertools.map_reduce(
    missing_data_ingest_files,
    lambda k: PurePath(k).with_suffix("").name
)
missing_data_ingest_granules = set(missing_data_ingest_granules.keys())
logger.info(f'Missing data ingest (granules): {len(missing_data_ingest_granules)=:,}')
logger.debug(f'{pstr(missing_data_ingest_granules)=!s}')

#######################################################################
# GET L3 products (products used by PGE runs)
# Note: It is possible for L3 products to exist without CNM-S / accountability information briefly after PGE but before CNM-S processing,
#  especially for present or future timeranges.
#  Similarly, it is possible for a L3 data product record to not have CNM-R information if PO.DAAC has not responded yet.
#######################################################################

body = get_body()
body["_source"]["includes"] = ["metadata.runconfig.localize", "metadata.accountability", "daac_CNM_S_status", "daac_delivery_status"]
body["query"]["bool"]["must"].append(get_range("creation_timestamp"))
# body["query"]["bool"]["must"].append({"wildcard": {"daac_CNM_S_status": "*"}})
search_results = list(helpers.scan(es, body, index="grq_*_l3_dswx_hls", scroll="5m", size=10_000))
pge_input_files = {PurePath(input).name.removesuffix(".tif")
                   for hit in search_results
                   for input in hit["_source"]["metadata"]["runconfig"]["localize"]}

pge_output_granules = {hit["_id"] for hit in search_results}
logger.info(f'Data produced by PGE(s) (DSWx): {len(pge_output_granules)}')

logger.info(f'Data processed through PGE(s): {len(pge_input_files)}')

missing_pge_files = all_ingested_files - pge_input_files
logger.info(f'Inputs Missing PGE: {len(missing_pge_files)=:,}')
logger.debug(f'{pstr(missing_pge_files)=!s}')

missing_pge_granules = more_itertools.map_reduce(
    missing_pge_files,
    lambda k: PurePath(k).with_suffix("").name
)
missing_pge_granules = set(missing_pge_granules.keys())
logger.info(f'Inputs Missing PGE: {len(missing_pge_granules)=:,}')

pge_input_granules = more_itertools.map_reduce(
    pge_input_files,
    lambda k: PurePath(k).with_suffix("").name
)
pge_input_granules = set(pge_input_granules.keys())


#######################################################################
# CNM-S & CNM-R
#######################################################################
cnm_s_input_files = {PurePath(input).name.removesuffix(".tif")
                        for hit in search_results
                        for input in hit["_source"]["metadata"]["accountability"]["L3_DSWx_HLS"]["metadata"]["filenames"] if hit["_source"].get("daac_CNM_S_status") == "SUCCESS"}
cnm_s_input_granules = {PurePath(input).name.removesuffix(".tif")
                        for hit in search_results
                        for input in hit["_source"]["metadata"]["accountability"]["L3_DSWx_HLS"]["inputs"] if hit["_source"].get("daac_CNM_S_status") == "SUCCESS"}

missing_cnm_s_files = pge_input_files - cnm_s_input_files
logger.info(f'Inputs Missing successful CNM-S (files): {len(missing_cnm_s_files)=:,}')
missing_cnm_s_granules = pge_input_granules - cnm_s_input_granules
logger.info(f'Inputs Missing successful CNM-S (granules): {len(missing_cnm_s_granules)=:,}')
logger.debug(f'{pstr(missing_cnm_s_granules)=!s}')

cnm_r_input_files = {PurePath(input).name.removesuffix(".tif")
                        for hit in search_results
                        for input in hit["_source"]["metadata"]["accountability"]["L3_DSWx_HLS"]["metadata"]["filenames"] if hit["_source"].get("daac_delivery_status") == "SUCCESS"}
cnm_r_input_granules = {PurePath(input).name.removesuffix(".tif")
                        for hit in search_results
                        for input in hit["_source"]["metadata"]["accountability"]["L3_DSWx_HLS"]["inputs"] if hit["_source"].get("daac_delivery_status") == "SUCCESS"}

missing_cnm_r_files = cnm_s_input_files - cnm_r_input_files
logger.info(f'Inputs Missing successful CNM-R (files): {len(missing_cnm_r_files)=:,}')
missing_cnm_r_granules = cnm_s_input_granules - cnm_r_input_granules
logger.info(f'Inputs Missing successful CNM-R (granules): {len(missing_cnm_r_granules)=:,}')
logger.debug(f'{pstr(missing_cnm_r_granules)=!s}')

#######################################################################
# Summary
#######################################################################
all_unpublished_files = missing_download_files \
    .union(missing_data_ingest_files) \
    .union(missing_pge_files) \
    .union(missing_cnm_s_files) \
    .union(missing_cnm_r_files)
logger.debug(f'{pstr(all_unpublished_files)=!s}')
logger.info(f'ALL Unpublished (files): {len(all_unpublished_files)=:,}')

all_unpublished_granules = missing_download_granules \
    .union(missing_data_ingest_granules) \
    .union(missing_pge_granules) \
    .union(missing_cnm_s_granules) \
    .union(missing_cnm_r_granules)
logger.info(f'{pstr(all_unpublished_granules)=!s}')
logger.info(f'ALL Unpublished (granules): {len(all_unpublished_granules)=:,}')
