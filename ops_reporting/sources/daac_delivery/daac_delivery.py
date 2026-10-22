import json
import logging
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from io import StringIO
from typing import Tuple

import opensearchpy
from opensearchpy.helpers import scan
from tabulate import tabulate

from opera_commons.es_connection import get_grq_es
from ops_reporting.sources import Source, Attachment
from ops_reporting.sources.daac_delivery import queries as q

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] [%(name)s::%(lineno)d] %(message)s'
)

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


PRODUCT_INDEX_MAP = {
    'L3_DSWX_HLS': 'grq_*_l3_dswx_hls-*',
    'L2_RTC_S1': 'grq_*_l2_rtc_s1-*',
    'L2_CSLC_S1': 'grq_*_l2_cslc_s1-*',
    'L3_DSWX_S1': 'grq_*_l3_dswx_s1-*',
    'L3_DISP_S1': 'grq_*_l3_disp_s1-*',
    'L4_TROPO': 'grq_*_l4_tropo-*',
    # 'L3_DSWX_NI': 'grq_*_l3_dswx_ni-*',
    'L3_DIST_S1': 'grq_*_l3_dist_s1-*',
    # 'L3_DISP_NI': 'grq_*_l3_disp_ni-*',
    # 'L4_CAL_DISP': 'grq_*_l4_cal_disp-*',
    'L2_RTC_S1_STATIC': 'grq_*_l2_rtc_s1_static-*',
    'L2_CSLC_S1_STATIC': 'grq_*_l2_cslc_s1_static-*',
    'L3_DISP_S1_STATIC': 'grq_*_l3_disp_s1_static-*',
}

TOTAL_PRODUCTS = 'TOTAL_PRODUCTS'
CNM_UNSENT = 'CNM_UNSENT'
CNM_SEND_FAILED = 'CNM_SEND_FAILED'
CNM_NO_RESPONSE = 'CNM_NO_RESPONSE'
CNM_INGEST_FAILED = 'CNM_INGEST_FAILED'

COL_NAME_MAP = {
    TOTAL_PRODUCTS: 'Total Products',
    CNM_UNSENT: 'CNM-S Unsent',
    CNM_SEND_FAILED: 'CNM-S Failed',
    CNM_NO_RESPONSE: 'No DAAC Response',
    CNM_INGEST_FAILED: 'Ingest Failed',
}

TIME_FMT = '%Y-%m-%dT%H:%M:%SZ'

DELETE_SCROLLS = False
"""Delete ElasticSearch scroll contexts when finished, or just let them expire"""


def get_time_range(start_days_back=0, range_size=1):
    end_date = (datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0) -
                timedelta(days=start_days_back))
    return end_date - timedelta(days=range_size), end_date


def get_grq_client(host=None) -> opensearchpy.OpenSearch:
    if host:
        client = opensearchpy.OpenSearch(
            [host] if isinstance(host, str) else host,
            timeout=30,
            max_retries=10,
            retry_on_timeoout=True,
            use_ssl=True,
            verify_certs=False,
            ssl_assert_hostname=False,
            ssl_show_warn=False,
        )
    else:
        client = get_grq_es().es

    return client


def query_for_ids(client, index_pattern, query, transform=None):
    def id_from_doc(doc):
        return doc['id']

    if transform is None:
        transform = id_from_doc

    return [transform(doc['_source']) for doc in scan(client, query, index=index_pattern,
                                                      size=10_000, clear_scroll=DELETE_SCROLLS)]


def get_cnm_accountability_for_product(client: opensearchpy.OpenSearch, index_pattern: str,
                                       start: datetime, end: datetime):
    return {
        TOTAL_PRODUCTS: client.count(
            index=index_pattern, body=q.add_time_range(q.BLANK_QUERY, start, end)
        )['count'],
        CNM_UNSENT: query_for_ids(
            client, index_pattern, q.add_time_range(q.CNM_NOT_SENT, start, end)
        ),
        CNM_SEND_FAILED: query_for_ids(
            client, index_pattern, q.add_time_range(q.CNM_SEND_FAILED, start, end)
        ),
        CNM_NO_RESPONSE: query_for_ids(
            client, index_pattern, q.add_time_range(q.CNM_NO_RESPONSE, start, end)
        ),
        CNM_INGEST_FAILED: query_for_ids(
            client, index_pattern, q.add_time_range(q.CNM_INGEST_FAILED, start, end), transform=lambda x: {
                'id': x['id'],
                'error_code': x['daac_delivery_status'],
                'error_message': x['daac_delivery_error_message']
            }
        ),
    }


def report(accountability, start: datetime, end: datetime):
    # Map product type -> {count type -> count}
    counts = {
        product_type: {
            count_type: len(accountability[product_type][count_type]) if
            isinstance(accountability[product_type][count_type], list) else
            accountability[product_type][count_type]
            for count_type in accountability[product_type]
        } for product_type in accountability
    }

    # Remove products not generated at all from the report
    for product_type in counts:
        if accountability[product_type][TOTAL_PRODUCTS] == 0:
            del accountability[product_type]

    # Remove empty counts from the report
    for product_type in accountability:
        for count_type in list(accountability[product_type].keys()):
            if isinstance(accountability[product_type][count_type], list):
                if len(accountability[product_type][count_type]) == 0:
                    del accountability[product_type][count_type]

    def _insert_products_header(table_str, header_row=0, sep_chars=' |', insert_str='Product'):
        lines = table_str.split('\n')
        row = lines[header_row]
        idx = row.index(sep_chars)
        row_after = row[idx:]
        row_before = row[:idx]
        keep_chars = len(row_before) - len(insert_str)
        new_row = row_before[:keep_chars] + insert_str + row_after
        lines[header_row] = new_row
        return '\n'.join(lines)

    txt_report = StringIO()

    report_title = (f'OPERA DAAC Delivery Accountability Report | '
                    f'{start.strftime(TIME_FMT)} to {end.strftime(TIME_FMT)}')

    txt_report.write(
        f'{report_title}\n\n'
    )

    txt_report.write(_insert_products_header(tabulate(
        counts.values(),
        showindex=counts.keys(),
        headers=COL_NAME_MAP,
        tablefmt='grid',
        intfmt=','
    ), header_row=1))

    header = True
    for product_type in accountability:
        if CNM_INGEST_FAILED in accountability[product_type]:
            if header:
                txt_report.write('\n\n-------------Ingestion Failures-------------')
                header = False
            txt_report.write(
                f'\n\nProduct type: {product_type} ({len(accountability[product_type][CNM_INGEST_FAILED]):,})')
            for i, failure in enumerate(accountability[product_type][CNM_INGEST_FAILED]):
                if i >= 5:
                    txt_report.write(f'\n  (skipping {len(accountability[product_type][CNM_INGEST_FAILED]) - i} '
                                     f'additional products for brevity - see attached JSON report for full list)')
                    break

                txt_report.write(f'\n  - ID: {failure["id"]}')
                txt_report.write(f'\n    - Error Code:    {failure["error_code"]}')
                txt_report.write(f'\n    - Error Message: {failure["error_message"]}')

    if header:
        txt_report.write('\n\nNo ingestion failures found!')

    logger.info('CNM accountability:\n' + txt_report.getvalue())

    json_report_data = json.dumps(accountability or {}, indent=2).encode('utf-8')

    return txt_report.getvalue(), json_report_data


class DAACDeliverySource(Source):
    """Source class for CNM status accountability"""

    def __init__(self, window: Tuple[datetime, datetime] | None = None, dev=False):
        if window is None or (window[0] is None or window[1] is None):
            raise ValueError(f'CNM accountabiltiy requires a window to be specified')

        super().__init__('DAAC_delivery_status', window)

        self._executor = None
        self._future = None
        self._client = None

        self._dev = dev
        """If true, get user password to authenticate to an external cluster"""

    def __enter__(self):
        self._executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix='cnm-accountability-')

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._executor.shutdown(wait=True)
        self._executor = None

    def run(self):
        if self._executor is None:
            raise RuntimeError('Script executor has not been created, please run in with block')

        if self._dev:
            from getpass import getpass, getuser
            from urllib.parse import urlparse, urlunparse

            mozart_host = input('Please enter mozart host or IP address: ')
            username = getuser()
            password = getpass('JPL Password: ')
            grq_url = f'https://{mozart_host.rstrip("/")}/grq_es'

            p_url = list(urlparse(grq_url))
            p_url[1] = f'{username}:{password}@' + p_url[1]
            orig_grq_url = grq_url
            grq_url = urlunparse(p_url)

            grq = get_grq_client(grq_url)
            assert grq.ping(), f'Cannot reach GRQ cluster at {orig_grq_url}'

            del grq_url, password, p_url
            self._client = grq
        else:
            self._client = get_grq_client()

        self._future = self._executor.submit(self._run_accountability)

    def _run_accountability(self):
        try:
            cnn_accountability = {
                product_type: get_cnm_accountability_for_product(self._client, pattern, self._window_start, self._window_end)
                for product_type, pattern in PRODUCT_INDEX_MAP.items()
            }

            return True, cnn_accountability, None
        except Exception as e:
            return False, None, str(e)

    def _join(self):
        success, accountability, error = self._future.result()

        if not success:
            self._data = {}
            self._errors.append(
                f'CNM accountability failed: {error}'
            )
            return

        txt_report, accountability_bytes = report(accountability, self._window_start, self._window_end)

        self._data = accountability
        self._attachments.extend([
            Attachment(
                accountability_bytes,
                'cnm_accountability.json',
                content_type='application/json',
            ),
            Attachment(
                txt_report.encode('utf-8'),
                'cnm_accountability.txt',
                content_type='text/plain',
            )
        ])
