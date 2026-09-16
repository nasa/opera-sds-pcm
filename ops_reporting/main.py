import json
import logging
import shutil
from contextlib import ExitStack
from datetime import datetime
from opera_commons.logger import logger
from ops_reporting.sources import Duplicates, get_accountability_cls_for_product
from ops_reporting.sources.source import Attachment, Source
from ops_reporting.sources.accountability.slc_accountability import SLCAccountability
from os.path import isdir, join
import os


results = {}

sources: list[Source] = [
    # Duplicates('DSWX_HLS', 'PROD', window=(datetime(2026, 5, 1), datetime(2026, 5, 2))),
    # Duplicates('CSLC_S1', 'PROD', window=(datetime(2026, 5, 1), datetime(2026, 5, 2))),
    # Duplicates('RTC_S1', 'PROD', window=(datetime(2026, 5, 1), datetime(2026, 5, 2))),
    # Duplicates('DSWX_S1', 'PROD', window=(datetime(2026, 5, 1), datetime(2026, 5, 2))),
    # Duplicates('TROPO', 'PROD', window=(datetime(2026, 5, 1), datetime(2026, 5, 2))),
    # get_accountability_cls_for_product('DSWX_HLS', 'PROD', window=(datetime(2026, 5, 1), datetime(2026, 5, 2))),
    # get_accountability_cls_for_product('DSWX_S1', 'PROD', window=(datetime(2026, 5, 1), datetime(2026, 5, 2))),
]

slc_sources: list[Source] = [
    get_accountability_cls_for_product(
        'RTC_S1',
        'PROD',
        window=(datetime(2026, 5, 1), datetime(2026, 5, 1, 2, 0))
    ),
    get_accountability_cls_for_product(
        'CSLC_S1',
        'PROD',
        window=(datetime(2026, 5, 1), datetime(2026, 5, 1, 2, 0))
    ),
]


for source in slc_sources:
    with source:
        source.run()
        results[source.source_id] = source.results()

with ExitStack() as stack:
    for source in sources:
        stack.enter_context(source)

        source.run()

    for source in sources:
        results[source.source_id] = source.results()

now = datetime.now()

report_dir = f'OPERA_OPS_REPORT_DATA_{now.strftime("%Y%m%d%H%M%SZ")}'

if isdir(report_dir):
    shutil.rmtree(report_dir)

os.makedirs(report_dir)

print(results)

for source in results:
    attachments = [a.serialize(report_dir) for a in results[source]['attachments']]

    results[source] = {
        'data': results[source]['data'],
        'attachments': attachments,
        'errors': results[source]['errors'],
    }

slc_combined = SLCAccountability.combine_slc_lists(*slc_sources)

results['ACCOUNTABILITY-s1-slc'] = {
    'attachments': [a.serialize(report_dir) for a in slc_combined],
}


with open(join(report_dir, f'{report_dir}.met.json'), 'w') as f:
    json.dump(results, f, indent=4)

with open(join(report_dir, f'{report_dir}.dataset.json'), 'w') as f:
    json.dump({
        "version": "1",
        "creation_time": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
        "index": {
            "suffix": "1_opera_ops_report_data-{}".format(
                now.strftime("%Y.%m")
            )
        },
    }, f, indent=4)
