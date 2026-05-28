import argparse
import boto3
import hashlib
import json
import os
import re
from os.path import basename, dirname, join
from urllib.parse import urlparse

import backoff
import requests

from opera_commons.logger import logger
from util.backoff_util import fatal_code, backoff_logger
from util.conf_util import SettingsConf

try:
    from util.job_submitter import try_submit_mozart_job
except ImportError:
    logger.critical('Could not import util.job_submitter.try_submit_mozart_job. USING A MOCK INSTEAD')


    def try_submit_mozart_job(*args, **kwargs):
        from uuid import uuid4
        return str(uuid4()) + ' (MOCK)'

CMR_SEARCH_URLS = {
    'PROD': 'https://cmr.earthdata.nasa.gov/search/granules.umm_json_v1_4',
    'UAT': 'https://cmr.uat.earthdata.nasa.gov/search/granules.umm_json'
}

DISP_S1_CCID_MAP = {
    'PROD': 'C3294057315-ASF',
    'UAT': 'C1271830354-ASF'
}

FRAME_ID = re.compile(r'F?\d{5}')
settings = SettingsConf().cfg

S3_DIRECT_ACCESS = {}
S3_CLIENT = None


def get_parser():
    parser = argparse.ArgumentParser()

    inputs = parser.add_mutually_exclusive_group(required=True)

    inputs.add_argument(
        '--frames',
        nargs='+',
        help='List of frames to update'
    )

    inputs.add_argument(
        '--file',
        required=False,
        help='Path to JSON file containing a list of frame IDs (List[int])'
    )

    parser.add_argument(
        '-c', '--cmr',
        required=False,
        choices=list(CMR_SEARCH_URLS.keys()),
        default='PROD',
        help='CMR endpoint to query. One of PROD (default) or UAT'
    )

    parser.add_argument(
        '-s', '--start-date',
        default=None,
        help="The ISO date time after which data should be retrieved. For Example, --start-date 2021-01-14T00:00:00Z"
    )

    parser.add_argument(
        '-e', '--end-date',
        default=None,
        help="The ISO date time before which data should be retrieved. For Example, --end-date 2021-01-14T00:00:00Z"
    )

    parser.add_argument(
        '--use-temporal',
        action='store_true',
        help='Toggle for using temporal range rather than revision date (range) in the query.'
    )

    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Dry run HySDS job submission'
    )

    parser.add_argument(
        '-q', '--quiet-dry-run',
        action='store_true',
        dest='quiet',
        help=argparse.SUPPRESS
    )

    def _posint(s):
        v = int(s)

        if v <= 0:
            raise ValueError(f'Value {s} must be positive')

        return v

    parser.add_argument(
        '--limit',
        type=_posint,
        help='Limit the number of frames to update'
    )

    def _s3_url(s):
        parsed = urlparse(s)

        if parsed.scheme != 's3':
            raise ValueError(f'Invalid URL: {s}')

        if parsed.netloc == '':
            raise ValueError(f'Invalid URL: {s}')

        return s

    parser.add_argument(
        '--browse-download-location',
        default=None,
        type=_s3_url,
        help='Optional S3 URL to stage browse images to. Useful is the Verdi workers do not have direct S3 access as '
             'they cannot download via HTTPS',
        dest='browse_dst'
    )

    job_params = parser.add_argument_group('Job parameters', description='Parameters for the HySDS job')

    job_params.add_argument(
        '--subsample',
        type=_posint,
        help='Subsampling factor for baseline computation, default 50.',
        default=None
    )

    job_params.add_argument(
        '--new-version',
        help='If set, sets a new version for updated products in the ISO XML and product metadata',
        default=None
    )

    job_params.add_argument(
        '--update-processed-time',
        action='store_true',
        help='If set, sets the processing datetime of the output product to now in the ISO XML and product metadata',
    )

    job_params.add_argument(
        '--update-product-id',
        action='store_true',
        help='If set, updates the product ID in the ISO XML and in the filename',
    )

    return parser


def _validate_frames(frame_list):
    valid = []

    for frame in frame_list:
        if isinstance(frame, int):
            frame = f'{frame:05d}'

        if FRAME_ID.fullmatch(frame) is None:
            raise ValueError(f'Invalid frame ID {frame}')

        valid.append(frame.lstrip('F'))

    return valid


@backoff.on_exception(backoff.constant,
                      requests.exceptions.RequestException,
                      max_time=300,
                      giveup=fatal_code,
                      on_backoff=backoff_logger,
                      interval=15)
@backoff.on_exception(backoff.expo, requests.exceptions.Timeout, max_tries=2)
def _do_cmr_query(url, params, headers=None):
    if headers is None:
        headers = {}
    logger.info(f'Querying {url} with params {params} and headers {headers}')
    response = requests.get(url, params=params, headers=headers)
    response.raise_for_status()
    response_json = response.json()
    return response_json['items'], response.headers.get('CMR-Search-After', None)


def _get_product_s3_url_from_cmr_item(cmr_item):
    urls = cmr_item['umm']['RelatedUrls']

    filtered_urls = [
        url['URL'] for url in urls
        if url['Type'] in {'GET DATA', 'GET DATA VIA DIRECT ACCESS'} and
        url['URL'].startswith('s3://') and url['URL'].endswith('.nc')
    ]

    assert len(filtered_urls) == 1

    filtered_browse_urls_https = [
        url['URL'] for url in urls if
        url['Type'] == 'GET RELATED VISUALIZATION' and
        url['URL'].startswith('https://') and
        url['URL'].endswith('_BROWSE.png')
    ]

    assert len(filtered_browse_urls_https) == 1

    filtered_browse_urls_s3 = [
        url['URL'] for url in urls if
        url['Type'] == 'GET RELATED VISUALIZATION' and
        url['URL'].startswith('s3://') and
        url['URL'].endswith('_BROWSE.png')
    ]

    assert len(filtered_browse_urls_s3) == 1

    return filtered_urls[0], (filtered_browse_urls_s3[0], filtered_browse_urls_https[0])


def _try_localize_browse_image(urls, dst):
    global S3_CLIENT
    s3_url, https_url = urls

    local_browse_path = join('/tmp', basename(https_url))

    logger.info(f'Attempting to localize browse image {basename(https_url)} to OPERA S3 bucket')

    s3_url_parsed = urlparse(s3_url)
    s3_bucket = s3_url_parsed.netloc
    s3_key = s3_url_parsed.path.lstrip('/')

    try:
        if S3_DIRECT_ACCESS.get(s3_bucket, True):
            if S3_CLIENT is None:
                S3_CLIENT = boto3.client('s3')

            logger.info(f'Attempting direct S3 download from {s3_url} to {local_browse_path}')

            S3_CLIENT.download_file(
                s3_bucket,
                s3_key,
                local_browse_path
            )
    except:
        logger.error(f'S3 download from bucket {s3_bucket} failed, all future downloads in this bucket will fall back '
                     f'to using HTTPS')
        S3_DIRECT_ACCESS[s3_bucket] = False

    if not S3_DIRECT_ACCESS.get(s3_bucket, True):
        # This has permissions issues; switch hosts; should be equivalent per MP
        if urlparse(https_url).netloc == 'cumulus-test.asf.earthdatacloud.nasa.gov':
            https_url = https_url.replace('cumulus-test.asf.earthdatacloud.nasa.gov', 'cumulus-test.asf.alaska.edu')

        logger.info(f'Attempting HTTPS download from {https_url} to {local_browse_path}')
        response = requests.get(https_url, stream=True)
        logger.info(f'HTTPS GET {https_url}: {response.status_code}')
        response.raise_for_status()

        with open(local_browse_path, 'wb') as f:
            for chunk in response.iter_content(chunk_size=1024 ** 2):
                if chunk:
                    f.write(chunk)

    parsed_dst = urlparse(dst)

    dst_bucket = parsed_dst.netloc
    dst_prefix = parsed_dst.path.strip('/')

    dst_key = f'{dst_prefix}/{basename(local_browse_path)}'

    logger.info(f'Successfully downloaded browse image {basename(local_browse_path)}. Now attempting to push to OPERA '
                f'S3 at s3://{s3_bucket}/{dst_key}')

    S3_CLIENT.upload_file(
        local_browse_path,
        dst_bucket,
        dst_key
    )

    try:
        os.unlink(local_browse_path)
    except:
        ...

    return f's3://{dst_bucket}/{dst_key}'


def main(args):
    if args.frames is not None:
        frame_list = _validate_frames(args.frames)
    else:
        with open(args.file) as f:
            frame_list = _validate_frames(json.load(f))

    granules = []
    cmr_url = CMR_SEARCH_URLS[args.cmr]
    ccid = DISP_S1_CCID_MAP[args.cmr]

    for frame in frame_list:
        params = {
            'collection_concept_id': ccid,
            'page_size': 2000,
            'attribute[]': f'int,FRAME_NUMBER,{frame}'
        }

        start_q_str = args.start_date.strftime('%Y-%m-%dT%H:%M:%SZ') if args.start_date is not None else ''
        end_q_str = args.end_date.strftime('%Y-%m-%dT%H:%M:%SZ') if args.end_date is not None else ''

        if args.start_date is not None or args.end_date is not None:
            if args.use_temporal:
                params['temporal[]'] = f'{start_q_str},{end_q_str}'
            else:
                params['revision_date[]'] = f'{start_q_str},{end_q_str}'

        query_result, search_after = _do_cmr_query(cmr_url, params)
        granules.extend(query_result)
        while search_after is not None:
            headers = {'CMR-Search-After': search_after}
            query_result, search_after = _do_cmr_query(cmr_url, params, headers)
            granules.extend(query_result)

    logger.info(f'CMR query found {len(granules):,} products across {len(frame_list):,} frames')

    if args.limit is not None and len(granules) > args.limit:
        logger.info(f'Limiting {len(granules):,} products to {args.limit}')
        granules = granules[:args.limit]

    granules = {g['meta']['native-id']: _get_product_s3_url_from_cmr_item(g) for g in granules}

    submitted_jobs = []

    for granule in granules:
        url = dirname(granules[granule][0]) + '/'
        browse_urls = granules[granule][1]

        if args.browse_dst is not None:
            browse_url = _try_localize_browse_image(browse_urls, args.browse_dst)
        else:
            browse_url = browse_urls[0]

        update_params = {
            "update_processed_time": args.update_processed_time,
            "update_product_id": args.update_product_id,
        }

        if args.subsample is not None:
            update_params['subsample'] = args.subsample

        if args.new_version is not None:
            update_params['new_version'] = args.new_version

        mozart_kwargs = dict(
            product={},
            params=[
                {
                    "name": "dataset_type",
                    "from": "value",
                    "type": "text",
                    "value": "L3_DISP_S1"
                },
                {
                    "name": "product_update_image",
                    "type": "text",
                    "from": "value",
                    "value": "disp-s1-bperp-update:latest"
                },
                {
                    "name": "product_metadata",
                    "from": "value",
                    "type": "object",
                    "value": {
                        "dataset": "L3_DISP_S1",
                        "metadata": {
                            "ProductId": granule,
                            "ProductRootPath": url,
                            "Id": granule,
                            "AncillaryFiles": {
                                "browse_image": browse_url
                            },
                            "UpdateParams": update_params
                        }
                    }
                },
                {
                    "name": "input_dataset_id",
                    "type": "text",
                    "from": "value",
                    "value": granule
                }
            ],
            job_queue="opera-job_worker-sciflo-product_update",
            rule_name='trigger-SCIFLO_Product_Update',
            job_spec=f'job-SCIFLO_Product_Update:{settings["RELEASE_VERSION"]}',
            job_type=f'hysds-io-SCIFLO_Product_Update:{settings["RELEASE_VERSION"]}',
            job_name=f'job-WF-SCIFLO_L3_Product_Update-{granule}',
            payload_hash=hashlib.md5(url.encode()).hexdigest()
        )

        if args.dry_run:
            if not args.quiet:
                logger.info(f'DRY RUN Mozart job submission with arguments:\n'
                            f'{json.dumps(mozart_kwargs, indent=2, default=repr)}')
        else:
            job_id = try_submit_mozart_job(**mozart_kwargs)
            logger.info(f'Submitted job to Mozart for url {url} with update params: {update_params}. ID: {job_id}')
            submitted_jobs.append(job_id)

    if not args.dry_run:
        msg = f'Submitted {len(submitted_jobs):,} jobs'
        if not args.quiet:
            msg += f': {submitted_jobs}'
        logger.info(msg)


if __name__ == '__main__':
    main(get_parser().parse_args())
