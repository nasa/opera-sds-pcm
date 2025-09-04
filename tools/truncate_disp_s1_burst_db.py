#!/usr/bin/env python3

import logging
import json
from datetime import datetime, timezone
from opera_commons.datetime_utils import parse_iso_datetime
import argparse

''' Tool to truncate the DISP S1 burst database sensing_time_list to a specific datetime. 
    Writes out the new file with .mod added to the end of the file name'''

TRUNCATION_DATETIME="2016-07-01T00:00:00"

parser = argparse.ArgumentParser()
parser.add_argument("file", help="The DISP S1 burst database file to truncate")

truncation_datetime = parse_iso_datetime(TRUNCATION_DATETIME)

args = parser.parse_args()
j = json.load(open(args.file))
for frame in j:
    frame_truncation_list = []
    for sensing_time in j[frame]["sensing_time_list"]:
        if parse_iso_datetime(sensing_time) < truncation_datetime:
            print(f"Truncating {frame} {sensing_time}")
            frame_truncation_list.append(sensing_time)

    for sensing_time in frame_truncation_list:
        j[frame]["sensing_time_list"].remove(sensing_time)

new_file = args.file + ".mod"
with open(new_file, "w") as f:
    json.dump(j, f, indent=4)
    print(f"Truncated file written to {new_file}")
