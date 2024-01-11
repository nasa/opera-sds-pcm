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

from util.grq_client import get_body

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
    password = config["ES_PASSWORD"]
    logger.warning("Setting password via dotenv is not recommended. Leave empty to be prompted to enter password.")
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

def files_to_granules(files):
    # from HLS.L30.T27XVF.2023202T212144.v2.0.B03-r1 to
    #      HLS.L30.T27XVF.2023202T212144.v2.0-r1
    queried_or_downloaded_granules = more_itertools.map_reduce(
        files,
        lambda k: '.'.join(k.split('.')[:-1]) + '-' + k.split('.')[-1].split('-')[1])

    return set(queried_or_downloaded_granules.keys())

def get_ingested_files(index):
    '''from _source.metadata.Files.Filename, get rid of .tif and then append the revision string'''
    ingested_files = set()
    body = get_body()
    body["sort"] = []
    body["_source"]["includes"] = ["metadata.Files"]
    body["query"]["bool"]["must"].append(get_range("creation_timestamp"))
    search_results = list(helpers.scan(es, body, index=index, scroll="5m", size=10_000))
    for hit in search_results:
        for file in hit["_source"]["metadata"]["Files"]:
            filename = file["FileName"].removesuffix(".tif")
            file_id = filename + '-' + hit['_id'].split('-')[1]
            ingested_files.add(file_id)

    return ingested_files

#######################################################################
# GET MASTER LIST. THIS IS THE LIST OF QUERIED/DOWNLOADED FILES
#######################################################################

body = get_body()
body["sort"] = []
body["_source"]["includes"] = "false"
body["query"]["bool"]["must"].append(get_range("query_datetime"))
search_results = list(helpers.scan(es, body, index=",".join(["hls_catalog", "hls_catalog-*"]), scroll="5m", size=10_000))
# from HLS.L30.T27XVF.2023202T212144.v2.0.B03.tif-r1 to
#      HLS.L30.T27XVF.2023202T212144.v2.0.B03-r1
queried_or_downloaded_files = {''.join(hit["_id"].split(".tif")) for hit in search_results}
logger.debug(f'{pstr(queried_or_downloaded_files)=!s}')

logger.info(f'Data queried or downloaded (files): {len(search_results)=:,}')
logger.debug(f'{pstr(queried_or_downloaded_files)=!s}')

queried_or_downloaded_granules = files_to_granules(queried_or_downloaded_files)
logger.info(f'Data queried or downloaded (granules): {len(queried_or_downloaded_granules)=:,}')
logger.debug(f'{pstr(queried_or_downloaded_granules)=!s}')

# missing_queried_or_downloaded_granules = cmr_granules - queried_or_downloaded_granules
# logger.info(f'Missing queried (granules): {len(missing_queried_or_downloaded_granules)=:,}')
# logger.debug(f'{pstr(missing_queried_or_downloaded_granules)=!s}')

body = get_body()
body["sort"] = []
body["_source"]["includes"] = "false"
body["query"]["bool"]["must"].append(get_range("query_datetime"))
body["query"]["bool"]["must"].append({"term": {"downloaded": "true"}})
search_results = list(helpers.scan(es, body, index=",".join(["hls_catalog", "hls_catalog-*"]), scroll="5m", size=10_000))
downloaded_files = {''.join(hit["_id"].split(".tif")) for hit in search_results}

logger.info(f'Data downloaded (files): {len(search_results)=:,}')
logger.debug(f'{pstr(downloaded_files)=!s}')

downloaded_granules = files_to_granules(downloaded_files)
logger.info(f'Data downloaded (granules): {len(downloaded_granules)=:,}')
logger.debug(f'{pstr(downloaded_granules)=!s}')

missing_download_files = queried_or_downloaded_files - downloaded_files
logger.info(f'Missing download (files): {len(missing_download_files)=:,}')
logger.debug(f'{pstr(missing_download_files)=!s}')

missing_download_granules = files_to_granules(missing_download_files)
logger.info(f'Missing download (granules): {len(missing_download_granules)=:,}')
logger.debug(f'{pstr(missing_download_granules)=!s}')

#######################################################################
# GET L2 products (data ingested)
#######################################################################

l30_ingested_files = get_ingested_files(",".join(["grq_*_l2_hls_l30", "grq_*_l2_hls_l30-*"]))
logger.info(f'Data ingested (L30): {len(l30_ingested_files)=:,}')

s30_ingested_files = get_ingested_files(",".join(["grq_*_l2_hls_s30", "grq_*_l2_hls_s30-*"]))
logger.info(f'Data ingested (S30): {len(s30_ingested_files)=:,}')

all_ingested_files = l30_ingested_files.union(s30_ingested_files)
logger.info(f'Data ingested (total) (files): {len(all_ingested_files)=:,}')

all_ingested_granules = files_to_granules(all_ingested_files)
logger.info(f'Data ingested (total) (granules): {len(all_ingested_granules)=:,}')

missing_data_ingest_files = downloaded_files - all_ingested_files
logger.info(f'Missing data ingest (files): {len(missing_data_ingest_files)=:,}')
logger.debug(f'{pstr(missing_data_ingest_files)=!s}')

missing_data_ingest_granules = files_to_granules(missing_data_ingest_files)
logger.info(f'Missing data ingest (granules): {len(missing_data_ingest_granules)=:,}')
logger.debug(f'{pstr(missing_data_ingest_granules)=!s}')

#######################################################################
# GET L3 products (products used by PGE runs)
# Note: It is possible for L3 products to exist without CNM-S / accountability information briefly after PGE but before CNM-S processing,
#  especially for present or future timeranges.
#  Similarly, it is possible for a L3 data product record to not have CNM-R information if PO.DAAC has not responded yet.
#######################################################################

body = get_body()
body["sort"] = []
body["_source"]["includes"] = ["metadata.runconfig.localize", "metadata.accountability", "daac_CNM_S_status", "daac_delivery_status"]
body["query"]["bool"]["must"].append(get_range("creation_timestamp"))
# body["query"]["bool"]["must"].append({"wildcard": {"daac_CNM_S_status": "*"}})

search_results = list(helpers.scan(es, body, index=",".join(["grq_*_l3_dswx_hls", "grq_*_l3_dswx_hls-*"]), scroll="5m", size=10_000))
pge_input_files = set()
for hit in search_results:
    for input in hit["_source"]["metadata"]["runconfig"]["localize"]:
        # input looks like "s3://s3-us-west-2.amazonaws.com:80/opera-dev-rs-fwd-pyoon/inputs/HLS_S30/HLS.S30.T43VEL.2023208T064629.v2.0-r2/HLS.S30.T43VEL.2023208T064629.v2.0.B02.tif"
        revision = input.split('/')[-2].split('-')[1]
        file_id = PurePath(input).name.removesuffix(".tif") + '-' + revision
        pge_input_files.add(file_id)

pge_output_granules = {hit["_id"] for hit in search_results}
logger.info(f'Data produced by PGE(s) (DSWx): {len(pge_output_granules)}')

logger.info(f'Data processed through PGE(s): {len(pge_input_files)}')

missing_pge_files = all_ingested_files - pge_input_files
logger.info(f'Inputs Missing PGE: {len(missing_pge_files)=:,}')
logger.debug(f'{pstr(missing_pge_files)=!s}')

missing_pge_granules = files_to_granules(missing_pge_files)
logger.info(f'Inputs Missing PGE: {len(missing_pge_granules)=:,}')

pge_input_granules = files_to_granules(pge_input_files)

#######################################################################
# CNM-S & CNM-R
#######################################################################

def get_file_granule(hit, files, granules):
    if hit["_source"].get("daac_CNM_S_status") == "SUCCESS":
        granule = hit["_source"]["metadata"]["accountability"]["L3_DSWx_HLS"]["trigger_dataset_id"]
        revision = granule.split('-')[1]
        granules.add(granule)

        for input in hit["_source"]["metadata"]["accountability"]["L3_DSWx_HLS"]["metadata"]["filenames"]:
            file_id = PurePath(input).name.removesuffix(".tif") + '-' + revision
            files.add(file_id)

cnm_s_input_files = set()
cnm_s_input_granules = set()
cnm_r_input_files = set()
cnm_r_input_granules = set()
for hit in search_results:
    if hit["_source"].get("daac_CNM_S_status") == "SUCCESS":
        get_file_granule(hit, cnm_s_input_files, cnm_s_input_granules)

    if hit["_source"].get("daac_delivery_status") == "SUCCESS":
        get_file_granule(hit, cnm_r_input_files, cnm_r_input_granules)

missing_cnm_s_files = pge_input_files - cnm_s_input_files
logger.info(f'Inputs Missing successful CNM-S (files): {len(missing_cnm_s_files)=:,}')
missing_cnm_s_granules = pge_input_granules - cnm_s_input_granules
logger.info(f'Inputs Missing successful CNM-S (granules): {len(missing_cnm_s_granules)=:,}')
logger.debug(f'{pstr(missing_cnm_s_granules)=!s}')

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
