#!/usr/bin/env python3

from collections import defaultdict
import sys
import datetime
import argparse
import boto3

from report.opera_validator.opv_util import retrieve_r3_products

_DISP_S1_PRODUCT_TYPE = "OPERA_L3_DISP-S1_V1"

'''
Queries for DISP-S1 products from DAAC and downloads them to a specified S3 bucket path. Only works for DISP-S1 for now but can 
easily be generalized for other products as desired.
'''

parser = argparse.ArgumentParser()
parser.add_argument("--verbose", action="store_true", help="If set, print out verbose information.", required=False, default=False)
parser.add_argument("--dry-run", action="store_true", help="If set, do not actually copy any files.", required=False, default=False)
parser.add_argument("--daac-endpoint", required=False, choices=['UAT', 'OPS'], default='OPS', help='CMR endpoint venue')
parser.add_argument("--s3-destination", dest="s3_destination", help="S3 bucket name and path to write files to", required=True)
parser.add_argument("--frame-list-file", dest="frame_list_file", help="DISP-S1 frames to ", required=True)
args = parser.parse_args()

smallest_date = datetime.datetime.strptime("1999-12-31T23:59:59.999999Z", "%Y-%m-%dT%H:%M:%S.%fZ")
greatest_date = datetime.datetime.strptime("2099-01-01T00:00:00.000000Z", "%Y-%m-%dT%H:%M:%S.%fZ")

# Open up the text file frame_list_file and parse out all the frame numbers in there. They can be separated by commas or newlines.
frames_to_download = []
with open(args.frame_list_file, "r") as f:
    for line in f:
        frames_to_download.extend([int(frame) for frame in line.strip().split(",")])

filtered_disp_s1 = {}
frame_to_count = defaultdict(int)

for frame in frames_to_download:

    native_id_pattern = "OPERA_L3_DISP-S1_IW_F%05d*" % frame
    if args.verbose:
        print(f"Searching for DISP-S1 products with native-id pattern: {native_id_pattern}")
    extra_params = {"native-id[]": native_id_pattern, "options[native-id][pattern]": "true"}

    # Retrieve all DISP-S1 products from CMR within the acquisition time range as a list of granuleIDs
    all_disp_s1 = retrieve_r3_products(smallest_date, greatest_date, "UAT", _DISP_S1_PRODUCT_TYPE, extra_params=extra_params)
    for disp_s1 in all_disp_s1:

        # Getting to the frame_id is a bit of a pain
        for attrib in disp_s1.get("umm").get("AdditionalAttributes"):
            # Need to perform secondary filter. Not sure if we always need to do this or temporarily so.
            actual_temporal_time = datetime.datetime.strptime(
                disp_s1.get("umm").get("TemporalExtent")['RangeDateTime']['EndingDateTime'], "%Y-%m-%dT%H:%M:%SZ")
            if actual_temporal_time >= smallest_date and actual_temporal_time <= greatest_date:
                # If the path umm.RelatedUrls contains "URL" that starts with "s3" and "Format" field value "netCDF-4" then store that value
                for related_url in disp_s1.get("umm").get("RelatedUrls"):
                    if related_url.get("URL").startswith("s3") and related_url.get("Format") == "netCDF-4":
                        filtered_disp_s1[disp_s1.get("umm").get("GranuleUR")] = related_url.get("URL")
                        frame_to_count[frame] += 1
                        break

print(f"Found {len(filtered_disp_s1.keys())} DISP-S1 products:")
for frame, count in frame_to_count.items():
    print(f"Frame {frame}: {count} products")
if args.verbose:
    for granule_id, url in filtered_disp_s1.items():
        print(f"{granule_id}: {url}")
    print(f"Found {len(filtered_disp_s1.keys())} DISP-S1 products:")

if args.dry_run:
    print("Dry run. Not copying any files.")
    sys.exit(0)

# Copy down all the S3 files to here
s3 = boto3.client('s3')
for granule_id, url in filtered_disp_s1.items():
    s3.download_file(url.split("/")[2], "/".join(url.split("/")[3:]), url.split("/")[-1])