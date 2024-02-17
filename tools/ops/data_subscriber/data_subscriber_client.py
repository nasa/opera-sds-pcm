import argparse
import asyncio
import json
import logging
import re
import sys
from collections import defaultdict
from pprint import pprint

from data_subscriber import daac_data_subscriber
from data_subscriber.rtc.mgrs_bursts_collection_db_client import cached_load_mgrs_burst_db, \
    product_burst_id_to_mapping_burst_id, burst_id_to_mgrs_set_ids

logger = logging.getLogger(__name__)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--rtc-native-ids", nargs="+", default=[], help="list of RTC native IDs to reduce to distinct native IDs per RTC burst set.")
    parser.add_argument("--rtc-native-ids-file", "-f", type=argparse.FileType(), help="file housing a list of RTC native IDs. See `--rtc-native-ids`. Supports JSON and plain text formats (1 ID per line).")
    parser.add_argument("--submit-job", action="store_true", default=False, help="toggle submitting a query job. Only works when executed on mozart. Defaults to False.")
    parser.add_argument('--output', "-o", nargs='?', type=argparse.FileType('w'), default=sys.stdout, help="The output file to write the reduced list of RTC native IDs. Defaults to stdout.")
    args = parser.parse_args(sys.argv[1:])

    if args.rtc_native_ids:
        rtc_ids = args.rtc_native_ids
    elif args.rtc_native_ids_file:
        with args.rtc_native_ids_file:
            try:
                rtc_ids = json.load(args.rtc_native_ids_file)
            except Exception:
                args.rtc_native_ids_file.seek(0)
                rtc_ids = [line.strip() for line in args.rtc_native_ids_file.readlines() if line.strip()]
    else:
        raise AssertionError()

    rtc_pattern = "(?P<id>(?P<project>OPERA)_(?P<level>L2)_(?P<product_type>RTC)-(?P<source>S1)_(?P<burst_id>\\w{4}-\\w{6}-\\w{3})_(?P<acquisition_ts>(?P<acq_year>\\d{4})(?P<acq_month>\\d{2})(?P<acq_day>\\d{2})T(?P<acq_hour>\\d{2})(?P<acq_minute>\\d{2})(?P<acq_second>\\d{2})Z)_(?P<creation_ts>(?P<cre_year>\\d{4})(?P<cre_month>\\d{2})(?P<cre_day>\\d{2})T(?P<cre_hour>\\d{2})(?P<cre_minute>\\d{2})(?P<cre_second>\\d{2})Z)_(?P<sensor>S1A|S1B)_(?P<spacing>30)_(?P<product_version>v\\d+[.]\\d+))(_(?P<pol>VV|VH|HH|HV|VV\\+VH|HH\\+HV)|_BROWSE|_mask)?$"
    mgrs_burst_collections_gdf = cached_load_mgrs_burst_db(filter_land=False)

    set_to_rtcs = defaultdict(list)
    for rtc_id in rtc_ids:
        m = re.match(rtc_pattern, rtc_id).group("burst_id")
        burst_id = product_burst_id_to_mapping_burst_id(m)
        burst_id = product_burst_id_to_mapping_burst_id(burst_id)
        mapped_mgrs_set_ids = burst_id_to_mgrs_set_ids(mgrs_burst_collections_gdf, burst_id)
        for set_ in mapped_mgrs_set_ids:
            set_to_rtcs[set_].append(rtc_id)

    set_to_rtcs = dict(set_to_rtcs)
    pprint(set_to_rtcs)
    for k, v in set_to_rtcs.items():
        set_to_rtcs[k] = v[0]
    set_to_rtcs = dict(set_to_rtcs)
    pprint(set_to_rtcs)

    args.output.write("\n".join(set_to_rtcs.values()))

    if args.submit_job:
        for rtc_id in rtc_ids:
            asyncio.run(run_data_subscriber(rtc_id))


async def run_data_subscriber(rtc_id):
    args = "dummy.py query " \
           "--endpoint=OPS " \
           "--collection-shortname=OPERA_L2_RTC-S1_V1 " \
           f"--native-id={rtc_id} " \
           "--transfer-protocol=https " \
           "--chunk-size=1 " \
           "--use-temporal " \
           "".split()
    await daac_data_subscriber.run(args)


if __name__ == "__main__":
    main()
