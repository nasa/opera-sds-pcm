import json
import os
import shutil
from contextlib import ExitStack
from datetime import datetime, timezone, timedelta
from os.path import isdir, join

from ops_reporting.sources import Duplicates, GithubSource, DAACDeliverySource, get_accountability_cls_for_product
from ops_reporting.sources.accountability.slc_accountability import SLCAccountability
from ops_reporting.sources.source import Source
from util.ctx_util import JobContext
from util.exec_util import exec_wrapper
from util.job_util import supply_job_id

DUPLICATES_PRODUCTS = [
    'DSWX_HLS',
    'CSLC_S1',
    'RTC_S1',
    'DSWX_S1',
    'TROPO',
]

SIMPLE_ACCOUNTABILITY_PRODUCTS = [
    'DSWX_HLS',
    'DSWX_S1',
    # 'TROPO',
]


def _strptime(s) -> datetime:
    return datetime.strptime(s, "%Y-%m-%dT%H:%M:%SZ")


def _get_window(
        override: dict, default_start: datetime, default_end: datetime, now: datetime
) -> tuple[datetime, datetime]:
    if 'window' in override:
        end = now.replace(minute=0, second=0, microsecond=0)
        start = end - timedelta(days=override['window'])
    else:
        start = override.get('start', default_start)
        end = override.get('end', default_end)

        if isinstance(start, str):
            start = _strptime(start)
        if isinstance(end, str):
            end = _strptime(end)

    return start, end


@exec_wrapper
def run():
    """HySDS job entry point."""

    jc = JobContext("_context.json")
    job_context = jc.ctx

    # Window: Number of days back from today, can be null, will override start and end
    # Start/end: Explicit start/end dates for window

    data_gather_start_time = datetime.now(timezone.utc).replace(tzinfo=None)

    report_tag = job_context['report_tag']

    default_duplicates_window = job_context.get("default_duplicates_window")
    if default_duplicates_window:
        default_duplicates_end = data_gather_start_time.replace(minute=0, second=0, microsecond=0)
        default_duplicates_start = default_duplicates_end - timedelta(days=default_duplicates_window)
    else:
        default_duplicates_start = _strptime(job_context.get("default_duplicates_start"))
        default_duplicates_end = _strptime(job_context.get("default_duplicates_end"))
    default_duplicates_venue = job_context.get("default_duplicates_venue")

    default_accountability_window = job_context.get("default_accountability_window")
    if default_accountability_window:
        default_accountability_end = data_gather_start_time.replace(minute=0, second=0, microsecond=0)
        default_accountability_start = default_accountability_end - timedelta(days=default_accountability_window)
    else:
        default_accountability_start = _strptime(job_context.get("default_accountability_start"))
        default_accountability_end = _strptime(job_context.get("default_accountability_end"))
    default_accountability_venue = job_context.get("default_accountability_venue")

    override_config = job_context.get("override_config", {})

    report_data = {}

    duplicate_sources: list[Source] = [
        Duplicates(
            product,
            override_config.get('products', {}).get(product, {}).get('duplicates', {}).get('venue', default_duplicates_venue),
            window=_get_window(
                override_config.get('products', {}).get(product, {}).get('duplicates', {}),
                default_duplicates_start,
                default_duplicates_end,
                data_gather_start_time
            )
        )
        for product in DUPLICATES_PRODUCTS
    ]

    simple_accountability_sources: list[Source] = [
        get_accountability_cls_for_product(
            product,
            override_config.get('products', {}).get(product, {}).get('accountability', {}).get('venue', default_accountability_venue),
            window=_get_window(
                override_config.get('products', {}).get(product, {}).get('accountability', {}),
                default_accountability_start,
                default_accountability_end,
                data_gather_start_time
            )
        )
        for product in SIMPLE_ACCOUNTABILITY_PRODUCTS
    ]

    burst_accountability_sources: list[Source] = [
        get_accountability_cls_for_product(
            'RTC_S1',
            override_config.get('products', {}).get('RTC_S1', {}).get('accountability', {}).get('venue', default_accountability_venue),
            window=_get_window(
                override_config.get('products', {}).get('RTC_S1', {}).get('accountability', {}),
                default_accountability_start,
                default_accountability_end,
                data_gather_start_time
            )
        ),
        get_accountability_cls_for_product(
            'CSLC_S1',
            override_config.get('products', {}).get('CSLC_S1', {}).get('accountability', {}).get('venue', default_accountability_venue),
            window=_get_window(
                override_config.get('products', {}).get('CSLC_S1', {}).get('accountability', {}),
                default_accountability_start,
                default_accountability_end,
                data_gather_start_time
            )
        ),
    ]

    extra_sources: list[Source] = [
        DAACDeliverySource(
            window=_get_window(
                override_config.get('daac_accountability', {}),
                default_accountability_start,
                default_accountability_end,
                data_gather_start_time
            )
        )
    ]

    if GithubSource.can_run(check_token=True):
        extra_sources.append(GithubSource())

    with ExitStack() as stack:
        parallel_sources = duplicate_sources + simple_accountability_sources + extra_sources

        # Start the dupes + simpla acc + extras in parallel
        for source in parallel_sources:
            stack.enter_context(source)
            source.run()

        # Run the burst sources sequentially to utilize caching between runs
        for source in burst_accountability_sources:
            with source:
                source.run()
                report_data[source.source_id] = source.results()

        for source in parallel_sources:
            report_data[source.source_id] = source.results()

    data_gather_end_time = datetime.now(timezone.utc).replace(tzinfo=None)

    report_dir = f'OPERA_OPS_REPORT_DATA_{data_gather_start_time.strftime("%Y%m%d%H%M%SZ")}'

    if isdir(report_dir):
        shutil.rmtree(report_dir)

    os.makedirs(report_dir)

    for source in report_data:
        attachments = [a.serialize(report_dir) for a in report_data[source]['attachments']]

        report_data[source] = {
            'data': report_data[source]['data'],
            'attachments': attachments,
            'errors': report_data[source]['errors'],
        }

    if burst_accountability_sources:
        slc_combined = SLCAccountability.combine_slc_lists(*burst_accountability_sources)

        report_data['ACCOUNTABILITY-s1-slc'] = {
            'attachments': [a.serialize(report_dir) for a in slc_combined],
        }

    report_data = {k: report_data[k] for k in sorted(report_data.keys())}

    metadata = {
        'data_gather_start_time': data_gather_start_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'data_gather_end_time': data_gather_end_time.strftime('%Y-%m-%dT%H:%M:%SZ'),
        'data_gather_job_id': supply_job_id(),
        'tag': report_tag,
        'report_data': report_data,
    }

    with open(join(report_dir, f'{report_dir}.met.json'), 'w') as f:
        json.dump(metadata, f, indent=4)

    with open(join(report_dir, f'{report_dir}.dataset.json'), 'w') as f:
        json.dump({
            "version": "1",
            "creation_time": datetime.now().strftime("%Y-%m-%dT%H:%M:%S.%fZ"),
            "index": {
                "suffix": "1_opera_ops_report_data-{}".format(
                    data_gather_end_time.strftime("%Y.%m")
                )
            },
        }, f, indent=4)


if __name__ == '__main__':
    run()
