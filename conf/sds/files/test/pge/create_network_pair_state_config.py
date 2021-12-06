import sys
import logging
import os
import json
import shutil
import backoff

from hysds.es_util import get_grq_es

# This creates an LDF state config file Current limitations is that it only accepts 1 TLM file set logger
log_format = "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)


class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, "id"):
            record.id = "--"
        return True


logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())

grq_es = get_grq_es()  # get connection to GRQ's Elasticsearch


@backoff.on_exception(backoff.expo, Exception, max_value=64, max_tries=50, max_time=600)
def search_for_rslc(idx, query_body):
    rslc_result = grq_es.search(index=idx, body=query_body)

    count = rslc_result["hits"]["total"]["value"]
    if count == 1:
        return rslc_result
    raise RuntimeError(
        "ERROR: No datasets found with following query: {}.\n".format(
            json.dumps(query_body, indent=2)
        )
    )


def get_s3_url(urls):
    for url in urls:
        if url.startswith("s3://"):
            return url
    raise RuntimeError("Could not find an S3 url: {}".format(urls))


if __name__ == "__main__":
    ref_rslc_id = sys.argv[1]
    sec_rslc_id = sys.argv[2]
    dataset_dir = os.path.abspath(sys.argv[3])
    index = "grq_*_l1_l_rslc"

    # query for reference RSLC
    ref_res = search_for_rslc(index, {"query": {"term": {"_id": ref_rslc_id}}})
    if ref_res["hits"]["total"]["value"] == 0:
        raise RuntimeError(
            "Could not find {} in ElasticSearch".format(ref_rslc_id))
    logger.info("reference RSLC: {}".format(json.dumps(ref_res)))

    # query for secondary RSLC
    sec_res = search_for_rslc(index, {"query": {"term": {"_id": sec_rslc_id}}})
    if sec_res["hits"]["total"]["value"] == 0:
        raise RuntimeError(
            "Could not find {} in ElasticSearch".format(sec_rslc_id))
    logger.info("secondary RSLC: {}".format(json.dumps(sec_res)))

    # build network pair state-config metadata
    ref_met = ref_res["hits"]["hits"][0]["_source"]["metadata"]
    sec_met = sec_res["hits"]["hits"][0]["_source"]["metadata"]
    ref_bounding_polygon = None
    sec_bounding_polygon = None
    if "Bounding_Polygon" in ref_met:
        ref_bounding_polygon = ref_met["Bounding_Polygon"]

    if "Bounding_Polygon" in sec_met:
        sec_bounding_polygon = sec_met["Bounding_Polygon"]

    metadata = {
        "reference": ref_met,
        "secondary": sec_met,
        "is_complete": True,
        "l1_rslc_product_paths": [
            get_s3_url(ref_res["hits"]["hits"][0]["_source"]["urls"]),
            get_s3_url(sec_res["hits"]["hits"][0]["_source"]["urls"]),
        ],
        "network_pair_rslcs": [ref_met["FileName"], sec_met["FileName"]],
        "ReferenceTrackFramePolygon": ref_bounding_polygon,
        "SecondaryTrackFramePolygon": sec_bounding_polygon,
        "RelativeOrbitNumber": ref_met["RelativeOrbitNumber"],
        "ReferenceCycleNumber": ref_met["CycleNumber"],
        "SecondaryCycleNumber": sec_met["CycleNumber"],
        "ProductAccuracy": sec_met["Fidelity"],
        "FrameNumber": sec_met["TrackFrame"],
        "CoverageIndicator": sec_met["CoverageIndicator"],
        "OrbitDirectionLetter": sec_met["Direction"],
        "ProcessingType": "PR"
    }
    logger.info("metadata: {}".format(
        json.dumps(metadata, indent=2, sort_keys=True)))

    # create dataset directory
    if os.path.exists(dataset_dir):
        shutil.rmtree(dataset_dir)
    os.makedirs(dataset_dir)

    # create dataset
    dataset_name = os.path.basename(dataset_dir)
    with open("{}/{}.met.json".format(dataset_dir, dataset_name), "w") as out:
        json.dump(metadata, out, indent=2, sort_keys=True)
    dataset_met = {
        "version": "1",
        "starttime": ref_res["hits"]["hits"][0]["_source"]["starttime"],
        "endtime": sec_res["hits"]["hits"][0]["_source"]["endtime"],
    }
    with open("{}/{}.dataset.json".format(dataset_dir, dataset_name), "w") as out:
        json.dump(dataset_met, out, indent=2, sort_keys=True)

    print("Successfully created state-config dataset: {}".format(dataset_dir))
