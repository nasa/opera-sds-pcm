#!/usr/bin/env python3

'''
Given a S3 bucket and a prefix, detect duplicate files in the bucket.

Usage: python detect_duplicate_products_s3.py opera-int-rs-pop1 products/DISP_S1
'''

import sys
import boto3
from collections import defaultdict

def detect_duplicate_products(bucket: str, prefix: str):
    '''Detect duplicate files in the bucket.'''
    s3 = boto3.client('s3')
    paginator = s3.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(Bucket=bucket, Prefix=prefix)
    files = defaultdict(list)
    for response in response_iterator:
        for obj in response['Contents']:
            key = obj['Key']
            file_name = key.split('/')[-1]
            if file_name[-3:] != '.nc':
                continue

            # get rid of the last string after the last underscore
            disp_s1 = file_name.rsplit('_', 1)[0]

            files[disp_s1].append(file_name)
    for disp_s1, file_name in files.items():
        if len(file_name) > 1:
            print(f'{disp_s1} has {len(file_name)} duplicates:')
            for f in file_name:
                print(f)

    print(f'Found total of {len(files)} files in {bucket}/{prefix}')

if __name__ == '__main__':
    if len(sys.argv) != 3:
        print('Usage: detect_duplicate_products_s3.py <bucket> <prefix>')
        sys.exit(1)
    detect_duplicate_products(sys.argv[1], sys.argv[2])