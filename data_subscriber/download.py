import itertools
import json
import logging
import shutil
from collections import defaultdict, namedtuple
from datetime import datetime
from pathlib import PurePath, Path
from typing import Any, Iterable

import boto3
import dateutil.parser
import requests
import requests.utils
import validators
from cachetools.func import ttl_cache
from smart_open import open

import extractor.extract
import product2dataset.product2dataset
from data_subscriber import ionosphere_download
from data_subscriber.url import _to_granule_id, _to_orbit_number, _has_url, _to_url, _to_https_url
from product2dataset import product2dataset
from tools import stage_orbit_file
from tools.stage_ionosphere_file import IonosphereFileNotFoundException
from tools.stage_orbit_file import NoQueryResultsException
from util.conf_util import SettingsConf

logger = logging.getLogger(__name__)

DateTimeRange = namedtuple("DateTimeRange", ["start_date", "end_date"])
PRODUCT_PROVIDER_MAP = {"HLSL30": "LPCLOUD",
                        "HLSS30": "LPCLOUD",
                        "SENTINEL-1A_SLC": "ASF",
                        "SENTINEL-1B_SLC": "ASF"}


class SessionWithHeaderRedirection(requests.Session):
    """
    Borrowed from https://wiki.earthdata.nasa.gov/display/EL/How+To+Access+Data+With+Python
    """

    def __init__(self, username, password, auth_host):
        super().__init__()
        self.auth = (username, password)
        self.auth_host = auth_host

    # Overrides from the library to keep headers when redirected to or from
    # the NASA auth host.
    def rebuild_auth(self, prepared_request, response):
        headers = prepared_request.headers
        url = prepared_request.url

        if "Authorization" in headers:
            original_parsed = requests.utils.urlparse(response.request.url)
            redirect_parsed = requests.utils.urlparse(url)
            if (original_parsed.hostname != redirect_parsed.hostname) and \
                    redirect_parsed.hostname != self.auth_host and \
                    original_parsed.hostname != self.auth_host:
                del headers["Authorization"]


def run_download(args, token, es_conn, netloc, username, password, job_id):
    provider = PRODUCT_PROVIDER_MAP[args.collection] if hasattr(args, "collection") else args.provider
    download_timerange = get_download_timerange(args)
    all_pending_downloads: Iterable[dict] = es_conn.get_all_between(
        dateutil.parser.isoparse(download_timerange.start_date),
        dateutil.parser.isoparse(download_timerange.end_date),
        args.use_temporal
    )
    logger.info(f"{len(list(all_pending_downloads))=}")

    downloads = all_pending_downloads
    if args.batch_ids:
        logger.info(f"Filtering pending downloads by {args.batch_ids=}")
        id_func = _to_granule_id if provider == "LPCLOUD" else _to_orbit_number
        downloads = list(filter(lambda d: id_func(d) in args.batch_ids, all_pending_downloads))
        logger.info(f"{len(downloads)=}")
        logger.debug(f"{downloads=}")

    if not downloads:
        logger.info(f"No undownloaded files found in index.")
        return

    if args.smoke_run:
        logger.info(f"{args.smoke_run=}. Restricting to 1 tile(s).")
        args.batch_ids = args.batch_ids[:1]

    session = SessionWithHeaderRedirection(username, password, netloc)

    if provider == "ASF":
        download_from_asf(session=session, es_conn=es_conn, downloads=downloads, args=args, token=token, job_id=job_id)
    else:
        download_urls = [_to_url(download) for download in downloads if _has_url(download)]
        logger.debug(f"{download_urls=}")

        granule_id_to_download_urls_map = group_download_urls_by_granule_id(download_urls)

        download_granules(session, es_conn, granule_id_to_download_urls_map, args, token, job_id)


def get_download_timerange(args):
    start_date = args.start_date if args.start_date else "1900-01-01T00:00:00Z"
    end_date = args.end_date if args.end_date else datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%SZ")

    download_timerange = DateTimeRange(start_date, end_date)
    logger.info(f"{download_timerange=}")
    return download_timerange


def download_from_asf(
        session: requests.Session,
        es_conn,
        downloads: list[dict],
        args,
        token,
        job_id
):
    settings_cfg = SettingsConf().cfg  # has metadata extractor config
    logger.info("Creating directories to process products")
    provider = PRODUCT_PROVIDER_MAP[args.collection] if hasattr(args, "collection") else args.provider

    # house all file downloads
    downloads_dir = Path("downloads")
    downloads_dir.mkdir(exist_ok=True)

    if args.dry_run:
        logger.info(f"{args.dry_run=}. Skipping downloads.")

    for download in downloads:
        if not _has_url(download):
            continue

        if args.transfer_protocol == "https":
            product_url = _to_https_url(download)
        else:
            product_url = _to_url(download)

        logger.info(f"Processing {product_url=}")
        product_id = PurePath(product_url).name

        product_download_dir = downloads_dir / product_id
        product_download_dir.mkdir(exist_ok=True)

        # download product
        if args.dry_run:
            logger.debug(f"{args.dry_run=}. Skipping download.")
            continue

        if product_url.startswith("s3"):
            product = product_filepath = download_product_using_s3(
                product_url,
                token,
                target_dirpath=product_download_dir.resolve(),
                args=args
            )
        else:
            product = product_filepath = download_asf_product(
                product_url, token, product_download_dir
            )

        logger.info(f"{product_filepath=}")

        logger.info(f"Marking as downloaded. {product_url=}")
        es_conn.mark_product_as_downloaded(product_url, job_id)

        logger.info(f"product_url_downloaded={product_url}")

        additional_metadata = {}
        try:
            additional_metadata['processing_mode'] = download['processing_mode']
        except:
            logger.warning("processing_mode not found in the slc_catalog ES index")

        if provider == "ASF":
            if download.get("intersects_north_america"):
                logger.info("adding additional dataset metadata (intersects_north_america)")
                additional_metadata["intersects_north_america"] = True

        dataset_dir = extract_one_to_one(product, settings_cfg, working_dir=Path.cwd(),
                                         extra_metadata=additional_metadata)

        update_pending_dataset_with_index_name(dataset_dir)

        logger.info("Downloading associated orbit file")

        try:
            logger.info(f"Querying for Precise Ephemeris Orbit (POEORB) file")
            stage_orbit_file_args = stage_orbit_file.get_parser().parse_args(
                [
                    f"--output-directory={str(dataset_dir)}",
                    "--orbit-type=POEORB",
                    f"--query-time-range={settings_cfg.get('POE_ORBIT_TIME_RANGE', stage_orbit_file.DEFAULT_POE_TIME_RANGE)}",
                    str(product_filepath)
                ]
            )
            stage_orbit_file.main(stage_orbit_file_args)
        except NoQueryResultsException:
            logger.warning("POEORB file could not be found, querying for Restituted Orbit (ROEORB) file")
            stage_orbit_file_args = stage_orbit_file.get_parser().parse_args(
                [
                    f"--output-directory={str(dataset_dir)}",
                    "--orbit-type=RESORB",
                    f"--query-time-range={settings_cfg.get('RES_ORBIT_TIME_RANGE', stage_orbit_file.DEFAULT_RES_TIME_RANGE)}",
                    str(product_filepath)
                ]
            )
            stage_orbit_file.main(stage_orbit_file_args)

        logger.info("Added orbit file to dataset")

        if additional_metadata.get("intersects_north_america", False) \
                and additional_metadata['processing_mode'] in ("historical", "reprocessing"):
            logger.info(f"Processing mode is {additional_metadata['processing_mode']}. Attempting to download ionosphere correction file.")
            try:
                output_ionosphere_filepath = ionosphere_download.download_ionosphere_correction_file(dataset_dir=dataset_dir, product_filepath=product_filepath)
                ionosphere_url = ionosphere_download.get_ionosphere_correction_file_url(dataset_dir=dataset_dir, product_filepath=product_filepath)

                # add ionosphere metadata to the dataset about to be ingested
                ionosphere_metadata = ionosphere_download.generate_ionosphere_metadata(output_ionosphere_filepath, ionosphere_url=ionosphere_url, s3_bucket="...", s3_key="...")
                update_pending_dataset_metadata_with_ionosphere_metadata(dataset_dir, ionosphere_metadata)
            except IonosphereFileNotFoundException:
                logger.warning("Ionosphere file not found remotely. Allowing job to continue.")
                pass

        logger.info(f"Removing {product_filepath}")
        product_filepath.unlink(missing_ok=True)

    logger.info(f"Removing directory tree. {downloads_dir}")
    shutil.rmtree(downloads_dir)


def update_pending_dataset_with_index_name(dataset_dir: PurePath):
    logger.info("Updating dataset's dataset.json with index name")

    with Path(dataset_dir / f"{dataset_dir.name}.dataset.json").open("r") as fp:
        dataset_json: dict = json.load(fp)

    with Path(dataset_dir / f"{dataset_dir.name}.met.json").open("r") as fp:
        met_dict: dict = json.load(fp)

    dataset_json.update({
        "index": {
            "suffix": ("{version}_{dataset}-{date}".format(
                version=met_dict["dataset_version"],
                dataset=met_dict["ProductType"],
                date=datetime.utcnow().strftime("%Y.%m.%d.%H%M%S")  # TODO chrisjrd: update with final suffix
            )).lower()  # suffix index name with `-YYYY.MM
        }
    })

    with Path(dataset_dir / f"{dataset_dir.name}.dataset.json").open("w") as fp:
        json.dump(dataset_json, fp)


def update_pending_dataset_metadata_with_ionosphere_metadata(dataset_dir: PurePath, ionosphere_metadata: dict):
    logger.info("Updating dataset's met.json with ionosphere metadata")

    with Path(dataset_dir / f"{dataset_dir.name}.met.json").open("r") as fp:
        met_json: dict = json.load(fp)

    met_json.update(ionosphere_metadata)

    with Path(dataset_dir / f"{dataset_dir.name}.met.json").open("w") as fp:
        json.dump(met_json, fp)


def download_granules(
        session: requests.Session,
        es_conn,
        granule_id_to_product_urls_map: dict[str, list[str]],
        args,
        token,
        job_id
):
    cfg = SettingsConf().cfg  # has metadata extractor config
    logger.info("Creating directories to process granules")
    # house all file downloads
    downloads_dir = Path("downloads")
    downloads_dir.mkdir(exist_ok=True)

    if args.dry_run:
        logger.info(f"{args.dry_run=}. Skipping downloads.")

    if args.smoke_run:
        granule_id_to_product_urls_map = dict(itertools.islice(granule_id_to_product_urls_map.items(), 1))

    for granule_id, product_urls in granule_id_to_product_urls_map.items():
        logger.info(f"Processing {granule_id=}")

        granule_download_dir = downloads_dir / granule_id
        granule_download_dir.mkdir(exist_ok=True)

        # download products in granule
        products = []
        product_urls_downloaded = []
        for product_url in product_urls:
            if args.dry_run:
                logger.debug(f"{args.dry_run=}. Skipping download.")
                break
            product_filepath = download_product(product_url, session, token, args, granule_download_dir)
            products.append(product_filepath)
            product_urls_downloaded.append(product_url)
        logger.info(f"{products=}")

        logger.info(f"Marking as downloaded. {granule_id=}")
        for product_url in product_urls_downloaded:
            es_conn.mark_product_as_downloaded(product_url, job_id)

        logger.info(f"{len(product_urls_downloaded)=}, {product_urls_downloaded=}")

        extract_many_to_one(products, granule_id, cfg)

        logger.info(f"Removing directory {granule_download_dir}")
        shutil.rmtree(granule_download_dir)

    logger.info(f"Removing directory tree. {downloads_dir}")
    shutil.rmtree(downloads_dir)


def download_product(product_url, session: requests.Session, token: str, args, target_dirpath: Path):
    if args.transfer_protocol.lower() == "https":
        product_filepath = download_product_using_https(
            product_url,
            session,
            token,
            target_dirpath=target_dirpath.resolve()
        )
    elif args.transfer_protocol.lower() == "s3":
        product_filepath = download_product_using_s3(
            product_url,
            token,
            target_dirpath=target_dirpath.resolve(),
            args=args
        )
    elif args.transfer_protocol.lower() == "auto":
        if product_url.startswith("s3"):
            product_filepath = download_product_using_s3(
                product_url,
                token,
                target_dirpath=target_dirpath.resolve(),
                args=args
            )
        else:
            product_filepath = download_product_using_https(
                product_url,
                session,
                token,
                target_dirpath=target_dirpath.resolve()
            )

    return product_filepath


def download_asf_product(product_url, token: str, target_dirpath: Path):
    logger.info(f"Requesting from {product_url}")

    asf_response = _handle_url_redirect(product_url, token)
    asf_response.raise_for_status()

    product_filename = PurePath(product_url).name
    product_download_path = target_dirpath / product_filename
    with open(product_download_path, "wb") as file:
        file.write(asf_response.content)
    return product_download_path.resolve()


def extract_many_to_one(products: list[Path], group_dataset_id, settings_cfg: dict):
    """Creates a dataset for each of the given products, merging them into 1 final dataset.

    :param products: the products to create datasets for.
    :param group_dataset_id: a unique identifier for the group of products.
    :param settings_cfg: the settings.yaml config as a dict.
    """
    # house all datasets / extracted metadata
    extracts_dir = Path("extracts")
    extracts_dir.mkdir(exist_ok=True)

    # create individual dataset dir for each product in the granule
    # (this also extracts the metadata to *.met.json files)
    product_extracts_dir = extracts_dir / group_dataset_id
    product_extracts_dir.mkdir(exist_ok=True)
    dataset_dirs = [
        extract_one_to_one(product, settings_cfg, working_dir=product_extracts_dir)
        for product in products
    ]
    logger.info(f"{dataset_dirs=}")

    # generate merge metadata from single-product datasets
    shared_met_entries_dict = {}  # this is updated, when merging, with metadata common to multiple input files
    total_product_file_sizes, merged_met_dict = \
        product2dataset.merge_dataset_met_json(
            str(product_extracts_dir.resolve()),
            extra_met=shared_met_entries_dict  # copy some common metadata from each product.
        )
    logger.debug(f"{merged_met_dict=}")

    logger.info("Creating target dataset directory")
    target_dataset_dir = Path(group_dataset_id)
    target_dataset_dir.mkdir(exist_ok=True)
    for product in products:
        shutil.copy(product, target_dataset_dir.resolve())
    logger.info("Copied input products to dataset directory")

    logger.info("update merged *.met.json with additional, top-level metadata")
    merged_met_dict.update(shared_met_entries_dict)
    merged_met_dict["FileSize"] = total_product_file_sizes
    merged_met_dict["FileName"] = group_dataset_id
    merged_met_dict["id"] = group_dataset_id
    logger.debug(f"{merged_met_dict=}")

    # write out merged *.met.json
    merged_met_json_filepath = target_dataset_dir.resolve() / f"{target_dataset_dir.name}.met.json"
    with open(merged_met_json_filepath, mode="w") as output_file:
        json.dump(merged_met_dict, output_file)
    logger.info(f"Wrote {merged_met_json_filepath=!s}")

    # write out basic *.dataset.json file (version + created_timestamp)
    dataset_json_dict = extractor.extract.create_dataset_json(
        product_metadata={"dataset_version": merged_met_dict["dataset_version"]},
        ds_met={},
        alt_ds_met={}
    )
    dataset_json_dict.update({
        "index": {
            "suffix": ("{version}_{dataset}-{date}".format(
                version=merged_met_dict["dataset_version"],
                dataset=merged_met_dict["ProductType"],
                date=datetime.utcnow().strftime("%Y.%m.%d.%H%M%S")  # TODO chrisjrd: update with final suffix
            )).lower()  # suffix index name with `-YYYY.MM
        }
    })
    granule_dataset_json_filepath = target_dataset_dir.resolve() / f"{group_dataset_id}.dataset.json"
    with open(granule_dataset_json_filepath, mode="w") as output_file:
        json.dump(dataset_json_dict, output_file)
    logger.info(f"Wrote {granule_dataset_json_filepath=!s}")

    shutil.rmtree(extracts_dir)


def extract_one_to_one(product: Path, settings_cfg: dict, working_dir: Path, extra_metadata=None) -> PurePath:
    """Creates a dataset for the given product.

    :param product: the product to create datasets for.
    :param settings_cfg: the settings.yaml config as a dict.
    :param working_dir: the working directory for the extract process. Serves as the output directory for the extraction.
    :param extra_metadata: extra metadata to add to the dataset.
    """
    # create dataset dir for product
    # (this also extracts the metadata to *.met.json file)
    logger.info("Creating dataset directory")
    dataset_dir = extractor.extract.extract(
        product=str(product),
        product_types=settings_cfg["PRODUCT_TYPES"],
        workspace=str(working_dir.resolve()),
        extra_met=extra_metadata
    )
    logger.info(f"{dataset_dir=}")
    return PurePath(dataset_dir)


def download_product_using_https(url, session: requests.Session, token, target_dirpath: Path, chunk_size=25600) -> Path:
    headers = {"Echo-Token": token}
    with session.get(url, headers=headers) as r:
        r.raise_for_status()

        file_name = PurePath(url).name
        product_download_path = target_dirpath / file_name
        with open(product_download_path, "wb") as output_file:
            output_file.write(r.content)
        return product_download_path.resolve()


def download_product_using_s3(url, token, target_dirpath: Path, args) -> Path:
    provider = PRODUCT_PROVIDER_MAP[args.collection] if hasattr(args, "collection") else args.provider
    aws_creds = _get_aws_creds(token, provider)
    logger.debug(f"{_get_aws_creds.cache_info()=}")

    s3 = boto3.Session(aws_access_key_id=aws_creds['accessKeyId'],
                       aws_secret_access_key=aws_creds['secretAccessKey'],
                       aws_session_token=aws_creds['sessionToken'],
                       region_name='us-west-2').client("s3")
    product_download_path = _s3_download(url, s3, str(target_dirpath))
    return product_download_path.resolve()


def _https_transfer(url, bucket_name, token, staging_area=""):
    file_name = PurePath(url).name
    bucket = bucket_name[len("s3://"):] if bucket_name.startswith("s3://") else bucket_name
    key = Path(staging_area, file_name).name

    upload_start_time = datetime.utcnow()

    try:
        logger.info(f"Requesting from {url}")
        r = _handle_url_redirect(url, token)
        if r.status_code != 200:
            r.raise_for_status()

        with open("https.tmp", "wb") as file:
            file.write(r.content)

        logger.info(f"Uploading {file_name} to {bucket=}, {key=}")
        with open("https.tmp", "rb") as file:
            s3 = boto3.client("s3")
            s3.upload_fileobj(file, bucket, key)

        upload_end_time = datetime.utcnow()
        upload_duration = upload_end_time - upload_start_time
        upload_stats = {"file_name": file_name,
                        "file_size (in bytes)": r.headers.get("Content-Length"),
                        "upload_duration (in seconds)": upload_duration.total_seconds(),
                        "upload_start_time": _convert_datetime(upload_start_time),
                        "upload_end_time": _convert_datetime(upload_end_time)}
        logger.debug(f"{upload_stats=}")

        return upload_stats
    except (Exception, ConnectionResetError, requests.exceptions.HTTPError) as e:
        logger.error(e)
        return {"failed_download": e}


def _handle_url_redirect(url, token):
    if not validators.url(url):
        raise Exception(f"Malformed URL: {url}")

    r = requests.get(url, allow_redirects=False)

    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}
    return requests.get(r.headers["Location"], headers=headers, allow_redirects=True)


def _convert_datetime(datetime_obj, strformat="%Y-%m-%dT%H:%M:%S.%fZ"):
    if isinstance(datetime_obj, datetime):
        return datetime_obj.strftime(strformat)
    return datetime.strptime(str(datetime_obj), strformat)


def _to_s3_url(dl_dict: dict[str, Any]) -> str:
    if dl_dict.get("s3_url"):
        return dl_dict["s3_url"]
    else:
        raise Exception(f"Couldn't find any URL in {dl_dict=}")


@ttl_cache(ttl=3300)  # 3300s == 55m. Refresh credentials before expiry. Note: validity period is 60 minutes
def _get_aws_creds(token, provider):
    logger.info("entry")

    if provider == "LPCLOUD":
        return _get_lp_aws_creds(token)
    else:
        return _get_asf_aws_creds(token)


def _get_lp_aws_creds(token):
    logger.info("entry")

    with requests.get("https://data.lpdaac.earthdatacloud.nasa.gov/s3credentials",
                      headers={'Authorization': f'Bearer {token}'}) as r:
        r.raise_for_status()

        return r.json()


def _get_asf_aws_creds(token):
    logger.info("entry")

    with requests.get("https://sentinel1.asf.alaska.edu/s3credentials",
                     headers={'Authorization': f'Bearer {token}'}) as r:
        r.raise_for_status()

        return r.json()


def _s3_transfer(url, bucket_name, s3, tmp_dir, staging_area=""):
    try:
        _s3_download(url, s3, tmp_dir, staging_area)
        target_key = _s3_upload(url, bucket_name, tmp_dir, staging_area)

        return {"successful_download": target_key}
    except Exception as e:
        return {"failed_download": e}


def _s3_download(url, s3, tmp_dir, staging_area=""):
    file_name = PurePath(url).name
    target_key = str(Path(staging_area, file_name))

    source = url[len("s3://"):].partition("/")
    source_bucket = source[0]
    source_key = source[2]

    s3.download_file(source_bucket, source_key, f"{tmp_dir}/{target_key}")

    return Path(f"{tmp_dir}/{target_key}")


def _s3_upload(url, bucket_name, tmp_dir, staging_area=""):
    file_name = PurePath(url).name
    target_key = str(Path(staging_area, file_name))
    target_bucket = bucket_name[len("s3://"):] if bucket_name.startswith("s3://") else bucket_name

    target_s3 = boto3.resource("s3")
    target_s3.Bucket(target_bucket).upload_file(f"{tmp_dir}/{target_key}", target_key)

    return target_key


def group_download_urls_by_granule_id(download_urls):
    granule_id_to_download_urls_map = defaultdict(list)
    for download_url in download_urls:
        # remove both suffixes to get granule ID (e.g. removes .Fmask.tif)
        granule_id = PurePath(download_url).with_suffix("").with_suffix("").name
        granule_id_to_download_urls_map[granule_id].append(download_url)
    return granule_id_to_download_urls_map
