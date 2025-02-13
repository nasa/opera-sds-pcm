#!/usr/bin/env python3

'''
Given a S3 bucket and a prefix, fix any DISP-S1 product iso xmls if they do not validate.

Usage: python fix_disp_s1_iso_xml_from_s3.py opera-int-rs-pop1 products/DISP_S1
File path looks like this: s3://opera-pst-rs-pop1/products/DISP_S1/OPERA_L3_DISP-S1_IW_F12640_VV_20220815T232903Z_20220827T232903Z_v1.0_20250211T022048Z/OPERA_L3_DISP-S1_IW_F12640_VV_20220815T232903Z_20220827T232903Z_v1.0_20250211T022048Z.iso.xml
'''

import sys
import boto3
from collections import defaultdict
from lxml import etree

def fix_iso_xmls(bucket: str, prefix: str, dry_run: bool = False):

    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    files = defaultdict(list)
    for response in response_iterator:
        for obj in response['Contents']:
            key = obj['Key']
            file_name = key.split('/')[-1]
            if file_name[-4:] != '.xml':
                continue

            # Test to see if the xml file is valid by downloading the file and then trying to parse it
            try:
                s3.download_file(bucket, key, file_name)
                etree.parse(file_name)
            except Exception as e:
                print(f'Fixing {key}')
                fixed_name = file_name + "fixed.xml"
                with open(file_name, 'r') as f:
                    xml = f.read()
                    xml = xml.replace('&', '&amp;')
                # Write out that file
                with open(fixed_name, 'w') as f:
                    f.write(xml)
                if not dry_run:
                    s3.upload_file(fixed_name, bucket, key)
                    print(f'Uploaded {key}')
                else:
                    print(f'Dry run: Would have uploaded {key}')

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Usage: fix_disp_s1_iso_xml_from_s3.py <bucket> <prefix> [<dry-run>]')
        print("Example: python fix_disp_s1_iso_xml_from_s3.py opera-pst-rs-pop1 products/DISP_S1/")
        sys.exit(1)

    dry_run = False
    if len(sys.argv) > 3:
        if sys.argv[3] == 'dryrun':
            print('Dry run mode')
            dry_run = True
        else:
            print("Did you mean dryrun?")
            sys.exit(1)

    fix_iso_xmls(sys.argv[1], sys.argv[2], dry_run)