import os
from io import StringIO
from pathlib import PurePath
from pprint import pprint

import elasticsearch
from dotenv import dotenv_values
from elasticsearch import RequestsHttpConnection, helpers

import logging

logging.getLogger("elasticsearch").setLevel(level=logging.WARNING)
logging.basicConfig(
    # format="%(levelname)s: %(relativeCreated)7d %(name)s:%(filename)s:%(funcName)s:%(lineno)s - %(message)s",
    format="%(asctime)s %(levelname)7s %(name)s:%(filename)s:%(funcName)s:%(lineno)3s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO)
logging.getLogger()

config = {
    **dotenv_values("../.env"),
    **os.environ
}

kwargs = {
    "http_auth": (config["ES_USER"], config["ES_PASSWORD"]),
    "connection_class": RequestsHttpConnection,
    "use_ssl": True,
    "verify_certs": False,
    "ssl_show_warn": False,
}
es = elasticsearch.Elasticsearch(hosts=[config["ES_BASE_URL"]], **kwargs)


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
        "size": 10000,
        "sort": [],
        "aggs": {}
    }


def get_range(
        datetime_fieldname="creation_timestamp",
        start_dt_iso="1970-01-01T00:00:00.000000",
        end_dt_iso="2023-01-01T00:00:00.000000"
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
body["query"]["bool"]["must"].append(get_range("query_datetime"))
search_results = list(helpers.scan(es, body, index="hls_catalog", scroll="5m", size=10000))
queried_or_downloaded_files = {hit["_id"].removesuffix(".tif") for hit in search_results}
logging.debug(pstr(queried_or_downloaded_files))

logging.debug(f'Data queried or downloaded: {len(search_results)=}')
logging.debug(pstr(queried_or_downloaded_files))

body = get_body()
body["query"]["bool"]["must"].append(get_range("query_datetime"))
body["query"]["bool"]["must"].append({"term": {"downloaded": "true"}})
search_results = list(helpers.scan(es, body, index="hls_catalog", scroll="5m", size=10000))
downloaded_files = {hit["_id"].removesuffix(".tif") for hit in search_results}

logging.debug(f'Data downloaded: {len(search_results)=}')
logging.debug(pstr(downloaded_files))

missing_download = queried_or_downloaded_files - downloaded_files
logging.info(f'Missing download: {len(missing_download)=}')
logging.debug(pstr(missing_download))

#######################################################################
# GET L2 products (data ingested)
#######################################################################

body = get_body()
body["query"]["bool"]["must"].append(get_range("creation_timestamp"))
search_results = list(helpers.scan(es, body, index="grq_*_l2_hls_l30", scroll="5m", size=10000))
l30_ingested_files = {hit["_id"] for hit in search_results}

logging.info(f'Data ingested (L30): {len(search_results)=}')

body = get_body()
body["query"]["bool"]["must"].append(get_range("creation_timestamp"))
search_results = list(helpers.scan(es, body, index="grq_*_l2_hls_s30", scroll="5m", size=10000))
s30_ingested_files = {hit["_id"] for hit in search_results}

logging.info(f'Data ingested (S30): {len(search_results)=}')

all_ingested_files = l30_ingested_files.union(s30_ingested_files)

logging.info(f'Data ingested (total): {len(all_ingested_files)=}')

missing_data_ingest = downloaded_files - all_ingested_files
logging.info(f'Missing data ingest: {len(missing_data_ingest)=}')
logging.debug(pstr(missing_data_ingest))

#######################################################################
# GET state-config products
#######################################################################

body = get_body()
body["query"]["bool"]["must"].append(get_range("creation_time"))
search_results = list(helpers.scan(es, body, index="grq_*_l2_hls_l30-state-config", scroll="5m", size=10000))
l30_state_config = {PurePath(v['product_path']).name.removesuffix(".tif")
                    for hit in search_results
                    for k, v in hit["_source"]["metadata"].items() if k != "@timestamp"}

body = get_body()
body["query"]["bool"]["must"].append(get_range("creation_time"))
search_results = list(helpers.scan(es, body, index="grq_*_l2_hls_s30-state-config", scroll="5m", size=10000))
s30_state_config = {PurePath(v['product_path']).name.removesuffix(".tif")
                    for hit in search_results
                    for k, v in hit["_source"]["metadata"].items() if k != "@timestamp"}

all_state_config = l30_state_config.union(s30_state_config)

missing_state_config = all_ingested_files - all_state_config
logging.info(f'Missing state-config: {len(missing_state_config)=}')
logging.debug(pstr(missing_state_config))

#######################################################################
# GET L3 products (products used by PGE runs)
# Note: It is possible for L3 products to exist without CNM-S / accountability information briefly after PGE but before CNM-S processing,
#  especially for present or future timeranges.
#  Similarly, it is possible for a L3 data product record to not have CNM-R information if PO.DAAC has not responded yet.
#######################################################################

body = get_body()
body["query"]["bool"]["must"].append(get_range("creation_timestamp"))
# body["query"]["bool"]["must"].append({"wildcard": {"daac_CNM_S_status": "*"}})
search_results = list(helpers.scan(es, body, index="grq_*_l3_dswx_hls", scroll="5m", size=10000))
pge_input_products = {PurePath(input).name.removesuffix(".tif")
                      for hit in search_results
                      for input in hit["_source"]["metadata"]["accountability"]["L3_DSWx_HLS"]["inputs"]}

missing_pge = all_state_config - pge_input_products
logging.info(f'Inputs Missing PGE: {len(missing_pge)=}')
logging.debug(pstr(missing_pge))


#######################################################################
# GET L3 state-config products
#######################################################################

body = get_body()
body["query"]["bool"]["must"].append(get_range("@timestamp"))
search_results = list(helpers.scan(es, body, index="grq_*_opera_state_config", scroll="5m", size=10000))
opera_state_config = {hit["_id"] for hit in search_results}
logging.info(f'All opera_state_config: {len(opera_state_config)=}')

body = get_body()
body["query"]["bool"]["must"].append(get_range("production_datetime"))
search_results = list(helpers.scan(es, body, index="hls_spatial_catalog", scroll="5m", size=10000))
spatial_granules = {hit["_id"] for hit in search_results}
logging.debug(pstr(spatial_granules))
logging.info(f'Spatial Granules: {len(spatial_granules)=}')

missing_opera_state_config = spatial_granules - opera_state_config
logging.info(f'Missing opera_state_config: {len(missing_opera_state_config)=}')

body = get_body()
body["query"]["bool"]["must"].append(get_range("creation_timestamp"))
search_results = list(helpers.scan(es, body, index="grq_*_l3_dswx_hls", scroll="5m", size=10000))

pge_input_granules = {hit["_source"]["metadata"]["accountability"]["L3_DSWx_HLS"]["trigger_dataset_id"].removesuffix("_state_config") for hit in search_results}

logging.info(f'Granules PGE Executed: {len(pge_input_granules)=}')
logging.debug(pstr(pge_input_granules))

missing_pge_granules = opera_state_config - pge_input_granules
logging.info(f'Granules Missing PGE: {len(missing_pge_granules)=}\n')
logging.debug(pstr(missing_pge_granules))


#######################################################################
# CNM-S & CNM-R
#######################################################################

cnm_s_input_products = {PurePath(input).name.removesuffix(".tif")
                        for hit in search_results
                        for input in hit["_source"]["metadata"]["accountability"]["L3_DSWx_HLS"]["inputs"] if hit["_source"].get("daac_CNM_S_status") == "SUCCESS"}


missing_cnm_s = pge_input_products - cnm_s_input_products
logging.info(f'Inputs Missing successful CNM-S: {len(missing_cnm_s)=}')
logging.debug(pstr(missing_cnm_s))

cnm_r_input_products = {PurePath(input).name.removesuffix(".tif")
                        for hit in search_results
                        for input in hit["_source"]["metadata"]["accountability"]["L3_DSWx_HLS"]["inputs"] if hit["_source"].get("daac_delivery_status") == "SUCCESS"}

missing_cnm_r = cnm_s_input_products - cnm_r_input_products
logging.info(f'Inputs Missing successful CNM-R: {len(missing_cnm_r)=}')
logging.debug(pstr(missing_cnm_r))

#######################################################################
# Summary
#######################################################################

all_unpublished = missing_download \
    .union(missing_data_ingest) \
    .union(missing_state_config) \
    .union(missing_pge) \
    .union(missing_cnm_s) \
    .union(missing_cnm_r)
logging.info(f'ALL Unpublished: {len(all_unpublished)=}')
logging.info(pstr(all_unpublished))
logging.info(f'ALL Unpublished: {len(all_unpublished)=}')
