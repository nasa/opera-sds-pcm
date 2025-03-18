#!/usr/bin/env python3

'''
Given a S3 bucket and a prefix, fix any DISP-S1 product iso xmls if they do not validate and also update the md5 file.

Usage: python fix_disp_s1_iso_xml_from_s3.py opera-int-rs-pop1 products/DISP_S1
File path looks like this: s3://opera-pst-rs-pop1/products/DISP_S1/OPERA_L3_DISP-S1_IW_F12640_VV_20220815T232903Z_20220827T232903Z_v1.0_20250211T022048Z/OPERA_L3_DISP-S1_IW_F12640_VV_20220815T232903Z_20220827T232903Z_v1.0_20250211T022048Z.iso.xml
'''

import sys
import boto3
from collections import defaultdict
from lxml import etree
import hashlib
import re

def fix_iso_xmls(bucket: str, prefix: str, dry_run: bool = False):

    count = 0

    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)

    for response in response_iterator:
        for obj in response['Contents']:
            key = obj['Key']

            if "v1.0" not in key:
                continue

            file_name = key.split('/')[-1]
            if file_name[-4:] != '.xml':
                continue

            # Skip ahead to half way so that we can run multiple of these scripts to speed up
            count += 1
            #if count < 5000:
            #    print(f'Skipping {key}')
            #    continue

            # Test to see if the xml file is valid by downloading the file and then trying to parse it
            try:
                s3.download_file(bucket, key, file_name)
                etree.parse(file_name)
                print(f'Valid {key}')
            except Exception as e:

                print(f'Fixing {key}')
                fixed_name = file_name + "fixed.xml"
                with open(file_name, 'r') as f:
                    xml = f.read()
                    xml = xml.replace('&', '&amp;')
                    xml = xml.replace('&amp;amp;amp;amp;amp;', '&amp;')
                    xml = xml.replace('&amp;amp;amp;amp;', '&amp;')
                    xml = xml.replace('&amp;amp;amp;', '&amp;')
                    xml = xml.replace('&amp;amp;', '&amp;')

                    # Remove string that starts with <gco:CharacterString>{"algorithm_theoretical_basis_document_id and then ends with </gco:CharacterString>
                    xml = re.sub(
                        r'<gco:CharacterString>{"algorithm_theoretical_basis_document_id.*?</gco:CharacterString>',
                        '<gco:CharacterString>876000.0</gco:CharacterString>', xml, flags=re.DOTALL)

                # Write out that file
                with open(fixed_name, 'w') as f:
                    f.write(xml)

                # Validate this new file
                etree.parse(fixed_name)

                if not dry_run:
                    s3.upload_file(fixed_name, bucket, key)
                    print(f'Replaced {key}')
                else:
                    print(f'Dry run: Would have replaced {key}')

                # Update the md5 file
                md5_file = file_name + ".md5"
                md5_key = key + ".md5"
                md5_str = hashlib.md5(open(fixed_name).read().encode('utf-8')).hexdigest()
                with open(md5_file, 'w') as f:
                    f.write(md5_str)
                if not dry_run:
                    s3.upload_file(md5_file, bucket, md5_key)
                    print(f'Replaced {md5_key}')
                else:
                    print(f'Dry run: Would have replaced {md5_key}')

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