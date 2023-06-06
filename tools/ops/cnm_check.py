import argparse
import datetime
import functools
import json
import logging
import os
import re
import sys
from collections import defaultdict
from getpass import getpass
from io import StringIO
from pprint import pprint

import elasticsearch
from compact_json import Formatter
from dotenv import dotenv_values
from elasticsearch import RequestsHttpConnection, helpers
from more_itertools import chunked

logging.getLogger("compact_json.formatter").setLevel(level=logging.INFO)
logging.getLogger("elasticsearch").setLevel(level=logging.WARNING)
logging.basicConfig(
    # format="%(levelname)s: %(relativeCreated)7d %(name)s:%(filename)s:%(funcName)s:%(lineno)s - %(message)s",  # alternative format which displays time elapsed.
    format="%(asctime)s %(levelname)7s %(name)4s:%(filename)8s:%(funcName)22s:%(lineno)3s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO)
logging.getLogger()

config = {
    **dotenv_values("../.env"),
    **os.environ
}

try:
    logging.warning("Setting password via dotenv is not recommended. Leave empty to be prompted to enter password.")
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


argparser = argparse.ArgumentParser(add_help=True)
argparser.add_argument(
    nargs="?",
    default=sys.stdin,
    type=argparse.FileType('r'),
    help=f'Input filepath.',
    dest="input"
)
argparser.add_argument(
    "--output", "-o",
    default=f'{__file__}.json',
    type=argparse.FileType('w'),
    help=f'Output filepath.'
)


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

logging.info(f'{sys.argv=}')
args = argparser.parse_args(sys.argv[1:])


#######################################################################
# CNM CHECK
#######################################################################


def hls_granule_ids_to_dswx_native_id_patterns(cmr_granules: set[str], input_to_outputs_map: defaultdict, output_to_inputs_map: defaultdict):
    dswx_native_id_patterns = set()
    for granule in cmr_granules:
        m = re.match(
            r'(?P<product_shortname>HLS[.]([LS])30)[.]'
            r'(?P<tile_id>T[^\W_]{5})[.]'
            r'(?P<acquisition_ts>(?P<year>\d{4})(?P<day_of_year>\d{3})T(?P<hour>\d{2})(?P<minute>\d{2})(?P<second>\d{2}))[.]'
            r'(?P<collection_version>v\d+[.]\d+)$',
            granule
        )
        tile = m.group("tile_id")
        year = m.group("year")
        doy = m.group("day_of_year")
        time_of_day = m.group("acquisition_ts").split("T")[1]
        date = datetime.datetime(int(year), 1, 1) + datetime.timedelta(int(doy) - 1)
        dswx_acquisition_dt_str = f"{date.strftime('%Y%m%d')}T{time_of_day}"

        dswx_native_id_pattern = f'OPERA_L3_DSWx-HLS_{tile}_{dswx_acquisition_dt_str}Z_*'
        dswx_native_id_patterns.add(dswx_native_id_pattern)

        # bi-directional mapping of HLS-DSWx inputs and outputs
        input_to_outputs_map[granule].add(dswx_native_id_pattern[:-1])  # strip wildcard char
        output_to_inputs_map[dswx_native_id_pattern[:-1]].add(granule)

    return dswx_native_id_patterns


def simplify_output(results):
    for result in results:
        result.update(result["_source"])
        del result["_index"], result["_type"], result["_score"], result["sort"], result["_source"]


with args.input as fp:
    hls_granules = set(json.load(fp))
logging.info(f'input file {len(hls_granules)=}')

input_to_outputs_map = defaultdict(set)
_ = hls_granule_ids_to_dswx_native_id_patterns(hls_granules, input_to_outputs_map=input_to_outputs_map, output_to_inputs_map=defaultdict(set))
dswx_granules_patterns = list(functools.reduce(set.union, list(input_to_outputs_map.values())))

results = []
dswx_granules_patterns_batches = chunked(dswx_granules_patterns, 1024)
for dswx_granules_patterns_batch in dswx_granules_patterns_batches:
    body = get_body()
    del body["query"]["bool"]["must"]  # removing match_all behavior
    body["_source"] = { "includes": [], "excludes": []}
    body["_source"]["includes"] = ["daac_CNM_S_status", "daac_delivery_status",
                                   "daac_delivery_error_message",
                                   "daac_CNM_S_timestamp", "daac_submission_timestamp", "daac_received_timestamp", "daac_process_complete_timestamp"]
    # body["_source"]["includes"] = "false"  # NOTE: uncomment to return barebones response
    for dswx_granules_pattern in dswx_granules_patterns_batch:
        body["query"]["bool"]["should"].append({"match_phrase_prefix": {"metadata.id": dswx_granules_pattern}})
    search_results = list(helpers.scan(es, body, index="grq_*_l3_dswx_hls", scroll="5m", size=10_000))
    results.extend(search_results)
logging.info(f'results {len(results)=}')

#######################################################################
# CNM CHECK SUMMARY
#######################################################################

simplify_output(results)

with args.output as fp:
    formatter = Formatter(indent_spaces=2, max_inline_length=300)
    json_str = formatter.serialize(results)
    fp.write(json_str)

logging.info(f"Finished writing to file {args.output!r}")
