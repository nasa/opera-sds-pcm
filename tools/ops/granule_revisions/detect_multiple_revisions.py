import subprocess
import boto3
import os
import shutil
import time
import sys
from botocore import UNSIGNED
from botocore.client import Config
from collections import defaultdict

if __name__ == '__main__':

    granules = defaultdict(list)

    s3_prefix = 'inputs/HLS_S30/'
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket='opera-dev-rs-fwd-pyoon', Prefix=s3_prefix, Delimiter='/')
    keyList = []
    for page in pages:
        #pprint(page)
        for obj in page['CommonPrefixes']:
            path = obj.get('Prefix')
            fname = path.split('/')[-2]
            try:
                granule = fname.split('-')[0]
                revision = fname.split('-')[1]
                granules[granule].append(revision)
            except Exception:
                print(f"{fname} doesn't make sense.")
    #print(granules)
    print(f'{len(granules)} Unique granules found')
    print("Following are granules with more than one revision:")

    total_granules_multiple_revisions = 0
    for granule, revisions in granules.items():
        if len(revisions) > 1:
            print(granule, revisions)
            total_granules_multiple_revisions += 1

    print(f'{total_granules_multiple_revisions} Granules have more than one revisions')