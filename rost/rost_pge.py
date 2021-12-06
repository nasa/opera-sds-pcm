# !/usr/bin/env python
import json
import os
import argparse
import logging
import xmltodict
from datetime import datetime, timedelta
import boto3

from rost.es_connection import get_rost_connection
from rost.sclk2scet import ingestLRCLKToSCETFile, radarTimeToUTC
from util.common_util import lower_keys
from util.common_util import convert_datetime
from util.exec_util import exec_wrapper
from opera_chimera.constants.opera_chimera_const import (
    NisarChimeraConstants as nc_const
)

from commons.es_connection import get_grq_es


log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger("create_ifg")

ISO_DATETIME_PATTERN = "%Y-%m-%dT%H:%M:%S.%f"

dir_path = os.path.dirname(os.path.realpath(__file__))


def str_to_datetime(time):
    return datetime.strptime(time, '%Y-%m-%dT%H:%M:%S.%f')


def create_dataset_json(rost_id, version, ds_file):
    # build dataset
    ds = {
        "creation_timestamp": convert_datetime(datetime.utcnow()),
        "version": version,
        "label": rost_id,
    }

    # write out dataset json
    with open(ds_file, "w") as f:
        json.dump(ds, f, indent=2)


def parse(xml_file):
    with open(xml_file) as in_file:
        xml = in_file.read()
        rost_dict = xmltodict.parse(xml)
        d = rost_dict["NISAR_TABLE_TYPE"]["NISAR_TABLE_KIND"]["NISAR_TABLE_VARIANT"]
    return lower_keys(d)


def convert_opera_xml_to_json(opera_xml, typ):
    logging.info("-------------Processing XML file: {}".format(opera_xml))
    opera_xml = opera_xml[:-4] if opera_xml.endswith(".xml") else opera_xml
    if typ == "orost":
        file_name = os.path.join(os.path.abspath(opera_xml), "{}.xml".format(opera_xml))
    else:
        file_name = os.path.abspath("{}.xml".format(opera_xml))
    p = parse(file_name)
    return p


def get_pri(rc_json, rc_id):
    """
    Given radar config file and the rc_id, this function looks up the PRI value
    :param rc_json:
    :param rc_id:
    :return:
    """
    for rec in rc_json["table_record"]:
        rec = {k.lower(): v for k, v in rec.items()}
        if str(rec["@index"]) == str(rc_id):
            return int(rec["base_pri"])


def get_time_offset(ofs_file_name):
    """
    Reads in the OFS file and returns the time offset
    :param ofs_file_name:
    :return:
    """
    # extract TIME_OFFSET from ofs file
    ofs_content = open(ofs_file_name, "r").read()
    ssto = float(ofs_content.split("\t")[2])
    return ssto


def get_cycle_time_zero(srost_json):
    """
    Read in the first table record of the SROST file
    and return the value of CYCLE_TIME_ZERO
    :param srost_json:
    :return:
    """
    for rec in srost_json["table_record"]:
        rec = {k.lower(): v for k, v in rec.items()}
        ctz = rec["cycle_time_zero"]
        return float(ctz)


def convert_orost_records(orost_id, orost_json, ctz, ssto, rc_json):
    """
    Creating OROST records
    Performing time conversion to convert radar time to UTC
    :param orost_id:
    :param orost_json:
    :param ctz:
    :param ssto:
    :param rc_json:
    :return:
    """
    # Must call ingestLRCLKToSCETFile() before calling this function
    records = []
    table_id = orost_id[3:13]
    prev_st = None
    prev_et = None
    for rec in orost_json["table_record"]:
        rec = {k.lower(): v for k, v in rec.items()}
        # read the start time and see if record is first of the datatake
        rec_st = float(rec["start_time"])
        first_rec_in_dt = True if prev_st is None or prev_st != rec_st else False

        rec_id = "%s-%s" % (table_id, str(rec["@index"]))
        rec["refrec_id"] = rec_id

        # For first record in datatake, calculate UTC time else, use end time of previous record
        if first_rec_in_dt:
            record_start_time = radarTimeToUTC(ctz + ssto + float(rec["start_time"]))
            logging.info("First record of datatake")
        else:
            record_start_time = prev_et

        # Calculate observation duration and add end_time_iso
        num_pulses = int(rec["number_of_pulses"])
        rc_id = rec["rc_id"]
        pri = int(get_pri(rc_json, rc_id))
        obs_duration = num_pulses * pri
        if type(record_start_time) is str:
            utc_start = str_to_datetime(record_start_time[:-1])
        else:
            utc_start = record_start_time
        record_end_time = utc_start + timedelta(seconds=obs_duration * eval("10e-08"))
        rec["start_time_iso"] = utc_start.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        rec["end_time_iso"] = record_end_time.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        rec["rost_id"] = orost_id
        rec["table_id"] = table_id
        rec["record_update_time"] = convert_datetime(datetime.utcnow())
        records.append(rec)

        # setting up values for next iteration
        prev_st = rec_st
        prev_et = record_end_time

    return records


def convert_rost_file(orost_xml_filename: str, sclk_scet_filename: str, srost_filename: str, ofs_filename: str,
                      radar_cfg_filename: str):
    """
    Reads ROST xml file and converts the ROST records
    """
    logging.info("Ingesting SCLK-SCET (Spacecraft Clock/Spacecraft Event Time) correlation file %s",
                 sclk_scet_filename)
    ingestLRCLKToSCETFile(sclk_scet_filename)
    # get time offset
    ssto = get_time_offset(ofs_filename)
    # get orost name
    orost_id = os.path.splitext(os.path.basename(orost_xml_filename))[0]
    orost_json = convert_opera_xml_to_json(orost_xml_filename, "orost")
    # get cycle time zero
    srost_json = convert_opera_xml_to_json(srost_filename, "srost")
    ctz = get_cycle_time_zero(srost_json)
    # get radar config file
    rc_json = convert_opera_xml_to_json(radar_cfg_filename, "radar_config")

    records = convert_orost_records(orost_id, orost_json, ctz, ssto, rc_json)
    return records


def get_latest_file_by_type(product_type):
    """
    Finds and returns the filename of the most recent SCLC-SCET file by creation date.
    Example of returned filename:
      s3://s3-us-west-2.amazonaws.com:80/opera-dev-lts-fwd-davidbw
        /products/SCLKSCET/LRCLK
        /NISAR_198900_SCLKSCET_LRCLK.00002/NISAR_198900_SCLKSCET_LRCLK.00002'
    """
    ancillary_es = get_grq_es(logger)

    query = {
        "query": {
            "match_all": {}
        }
    }

    filename = ancillary_es.get_latest_product_by_version(
        index="grq_*_{}".format(product_type.lower()),
        es_query=query
    )

    return filename


def get_related_files(coorelation_id, typ):
    """
    Finds the related SROST and OFS files by matching the
    FileCorrelationId field.
    :return:
    """
    ancillary_es = get_grq_es(logger)

    query = {"query": {"bool": {"must": [{"match": {"metadata.FileCorrelationId.keyword": coorelation_id}}]}}}

    product_type = typ

    filename = ancillary_es.get_latest_product_by_version(
        index="grq_*_{}".format(product_type.lower()),
        es_query=query
    )

    return filename


def s3_path_to_region_bucket_key(s3_path: str) -> (str, str, str):
    """
    Given an S3 path similar to the following:
      s3://s3-us-west-2.amazonaws.com:80/opera-dev-lts-fwd-davidbw
        /products/SCLKSCET/LRCLK
        /NISAR_198900_SCLKSCET_LRCLK.00002/NISAR_198900_SCLKSCET_LRCLK.00002'
    Returns the region name, S3 bucket name, and key (path)
    """
    if not s3_path.startswith("s3://"):
        raise ValueError("Remote SCLK-SCET filename '{}' does not start with 's3//'".format(
            s3_path))

    path_parts = s3_path.split('/')

    region_name = path_parts[2].split('.')[0]
    if region_name.startswith('s3-'):
        region_name = region_name[3:]

    bucket = path_parts[3]
    key = '/'.join(path_parts[4:])
    return region_name, bucket, key


def download_dependency_file(filename: str):
    """
    Gets the filename of the most recent SCLK-SCET file and, if the filename
    is an S3 path, downloads it.
    Returns the name of the local file.
    """
    logging.info("Downloading %s", filename)
    region_name, bucket, key = s3_path_to_region_bucket_key(filename)
    local_filename = key.split('/')[-1]
    client = boto3.client("s3", region_name=region_name)
    client.download_file(Bucket=bucket, Key=key, Filename=local_filename)
    return local_filename


def rost_pge(rost_xml_filename: str, sclk_scet_filename: str = None):
    """
    The rost PGE function
    Arguments:
    rost_xml_filename   ROST XML filename
    sclk_scet_filename  (optional) SCLK-SCET (Spacecraft Clock/Spacecraft Event Time)
                        correlation filename. By default, the latest SCLK-SCET
                        file is used; see get_latest_sclk_scet_filename()
    """
    download_sclk_scet = not sclk_scet_filename
    if download_sclk_scet:
        sclk_scet_file = get_latest_file_by_type(product_type=nc_const.SCLKSCET)
        sclk_scet_filename = download_dependency_file(sclk_scet_file)

    # download the radar config file
    radar_cfg_file = get_latest_file_by_type(product_type=nc_const.RADAR_CFG)
    radar_cfg_filename = download_dependency_file(radar_cfg_file)

    """
    extract the identifier to co-relate the input OROST
    to the SROST and OFS
    it is the {DOY}-{cycle number}-{day of cycle} section of the filename
    """
    pos1 = rost_xml_filename.find("rost-")
    pos2 = rost_xml_filename.rfind("-v")
    coorelation_id = rost_xml_filename[pos1+5: pos2]

    # download the related SROST and OFS files
    srost_file = get_related_files(coorelation_id, typ="SROST")
    srost_file_name = download_dependency_file(srost_file)

    ofs_file = get_related_files(coorelation_id, typ="OFS")
    ofs_file_name = download_dependency_file(ofs_file)

    records = convert_rost_file(rost_xml_filename, sclk_scet_filename,
                                srost_filename=srost_file_name, ofs_filename=ofs_file_name,
                                radar_cfg_filename=radar_cfg_filename)

    # Delete the downloaded dependency files: SCLKSCET, SROST, OFS
    # so they don't get picked up by verdi as datasets after job run
    if download_sclk_scet:
        os.remove(sclk_scet_filename)
    os.remove(srost_file_name)
    os.remove(ofs_file_name)
    os.remove(radar_cfg_filename)

    header = []

    rost_catalog = get_rost_connection(logger)
    logging.info("Ingesting ROST into ElasticSearch")

    rost_catalog.post(records, header)
    logging.info("Successfully ingested observation data from the following ROST file: %s",
                 rost_xml_filename)


@exec_wrapper
def main():
    """
    Main entry point when run from the command line
    """
    parser = argparse.ArgumentParser(description="generate rost product")
    parser.add_argument("rost_xml", action="store", help="path to rost xml file")
    parser.add_argument(
        "--sclkscet",
        action="store",
        required=False,
        help="filename of SCLK-SCET (Spacecraft Clock/Spacecraft Event Time) correlation file"
    )
    args = parser.parse_args()
    rost_pge(args.rost_xml, args.sclkscet)


if __name__ == "__main__":
    main()
