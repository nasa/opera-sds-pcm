import sys
import logging
import os
import json
import shutil
import backoff
import re

from commons.constants import product_metadata
from hysds.es_util import get_grq_es

# This creates an LDF state config file Current limitations is that it only accepts 1 TLM file set logger
log_format = "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

TLM_REGEX = "(?P<Mission>NISAR)_S(?P<SCID>\\d{3})_(?P<Station>\\w{2,3})_(?P<Antenna>\\w{3,4})_M(?P<Mode>\\d{2})_P(?P<Pass>\\d{5})_R(?P<Receiver>\\d{2})_C(?P<Channel>\\d{2})_G(?P<Group>\\d{2})_(?P<FileCreationDateTime>(?P<year>\\d{4})_(?P<doy>\\d{3})_(?P<hour>\\d{2})_(?P<minute>\\d{2})_(?P<second>\\d{2})_\\d{6})\\d{3}.(?P<VCID>vc\\d{2})"
TLM_PATTERN = re.compile(TLM_REGEX)


class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, "id"):
            record.id = "--"
        return True


logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())

grq_es = get_grq_es()  # get connection to GRQ's Elasticsearch


@backoff.on_exception(backoff.expo, Exception, max_value=64, max_tries=50, max_time=360)
def search_for_tlm(idx, query_body):
    tlm_result = grq_es.search(index=idx, body=query_body)

    count = tlm_result["hits"]["total"]["value"]
    if count == 1:
        return tlm_result
    raise RuntimeError(
        "ERROR: No datasets found with following query: {}.\n".format(
            json.dumps(query, indent=2)
        )
    )


if __name__ == "__main__":
    tlm_file = sys.argv[1]
    output_dir = os.path.abspath(sys.argv[2])

    query = {"query": {"bool": {}}}

    condition = []

    match = {"match": {"metadata.FileName": tlm_file}}
    condition.append(match)

    query["query"]["bool"]["must"] = condition
    index = "grq_*_nen_l_rrst"

    logger.info("index: {}".format(index))
    logger.info("query: {}".format(json.dumps(query, indent=2)))

    result = search_for_tlm(index, query)
    if result["hits"]["total"]["value"] == 0:
        raise RuntimeError("Could not find {} in ElasticSearch".format(tlm_file))

    tlm_urls = result["hits"]["hits"][0]["_source"]["urls"]
    tlm_met = result["hits"]["hits"][0]["_source"]["metadata"]
    s3_url = None
    for url in tlm_urls:
        if url.startswith("s3://"):
            s3_url = url
            break
    if s3_url is None:
        raise RuntimeError("Could not find an S3 url: {}".format(tlm_urls))

    # Create state config dataset
    tlm_name = tlm_met["id"]
    match = TLM_PATTERN.search(tlm_name)
    vcid = ""
    dataset_id = ""
    if match:
        if "Mission" in list(match.groupdict().keys()):
            mission = match.groupdict()["Mission"]
            station = match.groupdict()["Station"]
            year = match.groupdict()["year"]
            doy = match.groupdict()["doy"]
            hour = match.groupdict()["hour"]
            minute = match.groupdict()["minute"]
            second = match.groupdict()["second"]
            vcid = match.groupdict()["VCID"]
            dataset_id = "{}_{}_{}_{}_{}_{}_{}".format(
                station, mission, year, doy, hour, minute, second
            )
        else:
            print("Couldn't find mission")
    if dataset_id:
        dataset_name = "{}_{}_state-config".format(dataset_id, vcid)
        metadata = {
            "ldf_name": "{}.ldf".format(dataset_id),
            "missing_rrsts": [],
            "found_rrsts": [tlm_file],
            "rrst_product_paths": [s3_url],
            "state": "job-completed",
        }
        dataset_dir = os.path.join(output_dir, dataset_name)

    if os.path.exists(dataset_dir):
        shutil.rmtree(dataset_dir)
    os.makedirs(dataset_dir)

    with open("{}/{}.met.json".format(dataset_dir, dataset_name), "w") as out:
        json.dump(metadata, out)

    dataset_met = {
        "version": "1",
        "starttime": tlm_met.get(product_metadata.FILE_CREATION_DATE_TIME),
    }
    with open("{}/{}.dataset.json".format(dataset_dir, dataset_name), "w") as out:
        json.dump(dataset_met, out)

    print("Successfully created state-config dataset: {}".format(dataset_dir))
