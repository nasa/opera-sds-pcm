import copy
import logging
import os

from os.path import basename
from pathlib import PurePath, Path
import urllib.parse
from datetime import datetime, timezone
import boto3

from data_subscriber import ionosphere_download, es_conn_util
from data_subscriber.cmr import Collection
from data_subscriber.cslc.cslc_static_catalog import CSLCStaticProductCatalog
from data_subscriber.download import SessionWithHeaderRedirection
from data_subscriber.cslc_utils import parse_cslc_burst_id, build_cslc_static_native_ids, determine_k_cycle
from data_subscriber.asf_rtc_download import AsfDaacRtcDownload
from data_subscriber.cslc.cslc_static_query import CslcStaticCmrQuery
from data_subscriber.url import cslc_unique_id
from util.aws_util import concurrent_s3_client_try_upload_file
from util.conf_util import SettingsConf
from util.job_submitter import try_submit_mozart_job

from data_subscriber.cslc_utils import (localize_disp_frame_burst_hist, split_download_batch_id, get_prev_day_indices,
                                        get_bounding_box_for_frame, parse_cslc_native_id, build_ccslc_m_index)

logger = logging.getLogger(__name__)

_C_CSLC_ES_INDEX_PATTERNS = "grq_1_l2_cslc_s1_compressed*"

class AsfDaacCslcDownload(AsfDaacRtcDownload):

    def __init__(self, provider):
        super().__init__(provider)
        self.disp_burst_map, self.burst_to_frames, self.datetime_to_frames = localize_disp_frame_burst_hist()
        self.daac_s3_cred_settings_key = "CSLC_DOWNLOAD"

    async def run_download(self, args, token, es_conn, netloc, username, password, cmr, job_id, rm_downloads_dir=True):

        settings = SettingsConf().cfg
        product_id = "_".join([batch_id for batch_id in args.batch_ids])
        logger.info(f"{product_id=}")
        cslc_s3paths = []
        c_cslc_s3paths = []
        cslc_static_s3paths = []
        ionosphere_s3paths = []
        latest_acq_cycle_index = 0
        burst_id_set = set()

        new_args = copy.deepcopy(args)

        for batch_id in args.batch_ids:
            logger.info(f"Downloading CSLC files for batch {batch_id}")

            # Determine the highest acquisition cycle index here for later use in retrieving m compressed CSLCs
            _, acq_cycle_index = split_download_batch_id(batch_id)
            latest_acq_cycle_index = max(latest_acq_cycle_index, int(acq_cycle_index))

            new_args.batch_ids = [batch_id]
            granule_sizes = []

            # Download the files from ASF only if the transfer protocol is HTTPS
            if args.transfer_protocol == "https":
                cslc_products_to_filepaths: dict[str, set[Path]] = await super().run_download(
                    new_args, token, es_conn, netloc, username, password, cmr, job_id, rm_downloads_dir=False
                )
                logger.info(f"Uploading CSLC input files to S3")
                cslc_files_to_upload = [fp for fp_set in cslc_products_to_filepaths.values() for fp in fp_set]
                cslc_s3paths.extend(concurrent_s3_client_try_upload_file(bucket=settings["DATASET_BUCKET"],
                                                                         key_prefix=f"tmp/disp_s1/{batch_id}",
                                                                         files=cslc_files_to_upload))

                for granule_id, fp_set in cslc_products_to_filepaths.items():
                    filepath = list(fp_set)[0]
                    file_size = os.path.getsize(filepath)
                    granule_sizes.append((granule_id, file_size))

            # For s3 we can use the files directly so simply copy over the paths
            else: # s3 or auto
                logger.info("Skipping download CSLC bursts and instead using ASF S3 paths for direct SCIFLO PGE ingestion")
                downloads = self.get_downloads(args, es_conn)
                cslc_s3paths = [download["s3_url"] for download in downloads]
                if len(cslc_s3paths) == 0:
                    raise Exception(f"No s3_path found for {batch_id}. You probably should specify https transfer protocol.")

                for p in cslc_s3paths:
                    # Split the following into bucket name and key
                    # 's3://asf-cumulus-prod-opera-products/OPERA_L2_CSLC-S1/OPERA_L2_CSLC-S1_T122-260026-IW3_20231214T011435Z_20231215T075814Z_S1A_VV_v1.0/OPERA_L2_CSLC-S1_T122-260026-IW3_20231214T011435Z_20231215T075814Z_S1A_VV_v1.0.h5'
                    parsed_url = urllib.parse.urlparse(p)
                    bucket = parsed_url.netloc
                    key = parsed_url.path[1:]
                    granule_id = p.split("/")[-1]

                    try:
                        head_object = boto3.client("s3").head_object(Bucket=bucket, Key=key)
                    except Exception as e:
                        logger.error("Failed when accessing the S3 object:" + p)
                        raise e
                    file_size = int(head_object["ContentLength"])

                    granule_sizes.append((granule_id, file_size))

                cslc_files_to_upload = [Path(p) for p in cslc_s3paths] # Need this for querying static CSLCs

                cslc_products_to_filepaths = {} # Dummy when trying to delete files later in this function

            # Mark the CSLC files as downloaded in the CSLC ES with the file size
            # While at it also build up burst_id set for compressed CSLC query
            for granule_id, file_size in granule_sizes:
                native_id = granule_id.split(".h5")[0] # remove file extension and revision id
                burst_id, _, _, _ = parse_cslc_native_id(native_id, self.burst_to_frames, self.disp_burst_map)
                unique_id = cslc_unique_id(batch_id, burst_id)
                es_conn.mark_product_as_downloaded(unique_id, job_id, filesize=file_size)
                burst_id_set.add(burst_id)

            logger.info(f"Querying CSLC-S1 Static Layer products for {batch_id}")
            cslc_static_granules = await self.query_cslc_static_files_for_cslc_batch(
                cslc_files_to_upload, args, token, job_id, settings
            )

            # Download the files from ASF only if the transfer protocol is HTTPS
            if args.transfer_protocol == "https":
                logger.info(f"Downloading CSLC Static Layer products for {batch_id}")
                cslc_static_products_to_filepaths: dict[str, set[Path]] = await self.download_cslc_static_files_for_cslc_batch(
                    cslc_static_granules, args, token, netloc,
                    username, password, job_id
                )

                logger.info("Uploading CSLC Static input files to S3")
                cslc_static_files_to_upload = [fp for fp_set in cslc_static_products_to_filepaths.values() for fp in fp_set]
                cslc_static_s3paths.extend(concurrent_s3_client_try_upload_file(bucket=settings["DATASET_BUCKET"],
                                                                                key_prefix=f"tmp/disp_s1/{batch_id}",
                                                                                files=cslc_static_files_to_upload))
            # For s3 we can use the files directly so simply copy over the paths
            else:  # s3 or auto
                logger.info("Skipping download CSLC static files and instead using ASF S3 paths for direct SCIFLO PGE ingestion")

                cslc_static_products_to_filepaths = {} # Dummy when trying to delete files later in this function

                for cslc_static_granule in cslc_static_granules:
                    for url in cslc_static_granule["filtered_urls"]:
                        if url.startswith("s3"):
                            cslc_static_s3paths.append(url)

                if len(cslc_static_s3paths) == 0:
                    raise Exception(f"No s3_path found for static files for {batch_id}. You probably should specify https transfer protocol.")

            # Download all Ionosphere files corresponding to the dates covered by the input CSLC set
            # We always download ionosphere files, there is no direct S3 ingestion option
            logger.info(f"Downloading Ionosphere files for {batch_id}")
            ionosphere_paths = self.download_ionosphere_files_for_cslc_batch(cslc_files_to_upload,
                                                                             self.downloads_dir)

            logger.info(f"Uploading Ionosphere files to S3")
            ionosphere_s3paths.extend(concurrent_s3_client_try_upload_file(bucket=settings["DATASET_BUCKET"],
                                                                           key_prefix=f"tmp/disp_s1/{batch_id}",
                                                                           files=ionosphere_paths))

            # Delete the files from the file system after uploading to S3
            if rm_downloads_dir:
                logger.info("Removing downloaded files from local filesystem")
                for fp_set in cslc_products_to_filepaths.values():
                    for fp in fp_set:
                        os.remove(fp)

                for fp_set in cslc_static_products_to_filepaths.values():
                    for fp in fp_set:
                        os.remove(fp)

                for iono_file in ionosphere_paths:
                    os.remove(iono_file)

        # Determine M Compressed CSLCs by querying compressed cslc GRQ ES
        # Uses ccslc_m_index field which looks like T100-213459-IW3_417 (burst_id_acquisition-cycle-index)
        k, m = es_conn.get_k_and_m(args.batch_ids[0])
        logger.info(f"{k=}, {m=}")

        frame_id, _ = split_download_batch_id(args.batch_ids[0])

        # Search for all previous M compressed CSLCs
        prev_day_indices = await get_prev_day_indices(latest_acq_cycle_index, frame_id, self.disp_burst_map, args, token, cmr, settings)
        for mm in range(m-1): # m parameter is inclusive of the current frame at hand
            acq_cycle_index = prev_day_indices[len(prev_day_indices) - mm - 1]
            for burst_id in burst_id_set:
                ccslc_m_index = build_ccslc_m_index(burst_id, acq_cycle_index) #looks like t034_071112_iw3_461
                logger.info("Retrieving Compressed CSLCs for ccslc_m_index: %s", ccslc_m_index)
                ccslcs = es_conn.es.query(
                    index=_C_CSLC_ES_INDEX_PATTERNS,
                    body={"query": {  "bool": {  "must": [
                                    {"term": {"metadata.ccslc_m_index.keyword": ccslc_m_index}}]}}})

                # Should have exactly one compressed cslc per acq cycle per burst
                if len(ccslcs) != 1:
                    raise Exception(f"Expected 1 Compressed CSLC for {ccslc_m_index}, got {len(ccslcs)}")

                for ccslc in ccslcs:
                    c_cslc_s3paths.extend(ccslc["_source"]["metadata"]["product_s3_paths"])

        # Compute bounding box for frame. All batches should have the same frame_id so we pick the first one
        #TODO: Renable this when we get epsg, xmin/max, and ymin/max values for the frame.
        frame = self.disp_burst_map[int(frame_id)]
        #bounding_box = get_bounding_box_for_frame(frame)
        #print(f'{bounding_box=}')

        # TODO: This code differs from data_subscriber/rtc/rtc_job_submitter.py. Ideally both should be refactored into a common function
        # Now submit DISP-S1 SCIFLO job
        logger.info(f"Submitting DISP-S1 SCIFLO job")

        save_compressed_slcs = False
        if await determine_k_cycle(None, latest_acq_cycle_index, frame_id, self.disp_burst_map, k, args, token, cmr, settings) == 0:
            save_compressed_slcs = True
        logger.info(f"{save_compressed_slcs=}")

        product = {
            "_id": product_id,
            "_source": {
                "dataset": f"L3_DISP_S1-{product_id}",
                "metadata": {
                    "batch_id": product_id,
                    "frame_id": frame_id, # frame_id should be same for all download batches
                    "ProductReceivedTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "product_paths": {
                        "L2_CSLC_S1": cslc_s3paths,
                        "L2_CSLC_S1_COMPRESSED": c_cslc_s3paths,
                        "L2_CSLC_S1_STATIC": cslc_static_s3paths,
                        "IONOSPHERE_TEC": ionosphere_s3paths
                    },
                    "FileName": product_id,
                    "id": product_id,
                    #"bounding_box": bounding_box, TODO: enable this when available
                    "save_compressed_slcs": save_compressed_slcs,
                    "Files": [
                        {
                            "FileName": PurePath(s3path).name,
                            "FileSize": 1, #TODO? Get file size? It's also 1 in rtc_job_submitter.py
                            "FileLocation": os.path.dirname(s3path),
                            "id": PurePath(s3path).name,
                            "product_paths": "$.product_paths"
                        }
                        for s3path in cslc_s3paths + c_cslc_s3paths + cslc_static_s3paths + ionosphere_s3paths
                    ]
                }
            }
        }

        #print(f"{product=}")

        proc_mode_suffix = ""
        if "proc_mode" in args and args.proc_mode == "historical":
            proc_mode_suffix = "_hist"

        return try_submit_mozart_job(
            product=product,
            job_queue=f'opera-job_worker-sciflo-l3_disp_s1{proc_mode_suffix}',
            rule_name=f'trigger-SCIFLO_L3_DISP_S1{proc_mode_suffix}',
            params=self.create_job_params(product),
            job_spec=f'job-SCIFLO_L3_DISP_S1{proc_mode_suffix}:{settings["RELEASE_VERSION"]}',
            job_type=f'hysds-io-SCIFLO_L3_DISP_S1{proc_mode_suffix}:{settings["RELEASE_VERSION"]}',
            job_name=f'job-WF-SCIFLO_L3_DISP_S1{proc_mode_suffix}'
        )

    def get_downloads(self, args, es_conn):
        # For CSLC download, the batch_ids are globally unique so there's no need to query for dates
        all_downloads = []
        for batch_id in args.batch_ids:
            downloads = es_conn.get_download_granule_revision(batch_id)
            logger.info(f"Got {len(downloads)=} downloads for {batch_id=}")
            all_downloads.extend(downloads)

        return all_downloads

    async def query_cslc_static_files_for_cslc_batch(self, cslc_files, args, token, job_id, settings):
        cslc_query_args = copy.deepcopy(args)

        cmr = settings["DAAC_ENVIRONMENTS"][cslc_query_args.endpoint]["BASE_URL"]

        burst_ids = [parse_cslc_burst_id(cslc_file.stem)
                     for cslc_file in cslc_files]

        query_native_id = build_cslc_static_native_ids(burst_ids)

        cslc_query_args.native_id = query_native_id
        cslc_query_args.no_schedule_download = True
        cslc_query_args.collection = Collection.CSLC_S1_STATIC_V1.value
        cslc_query_args.bbox = "-180,-90,180,90"
        cslc_query_args.start_date = None
        cslc_query_args.end_date = None
        cslc_query_args.use_temporal = False
        cslc_query_args.max_revision = 1000
        cslc_query_args.proc_mode = "forward"  # CSLC static query does not care about
                                               # this, but a value must be provided
        es_conn = CSLCStaticProductCatalog(logging.getLogger(__name__))

        cmr_query = CslcStaticCmrQuery(cslc_query_args, token, es_conn, cmr, job_id, settings)

        result = await cmr_query.run_query(cslc_query_args, token, es_conn, cmr, job_id, settings)

        return result["download_granules"]

    async def download_cslc_static_files_for_cslc_batch(self, cslc_static_granules, args, token,
                                                        netloc, username, password, job_id):

        session = SessionWithHeaderRedirection(username, password, netloc)

        downloads = []
        es_conn = CSLCStaticProductCatalog(logging.getLogger(__name__))

        for cslc_static_granule in cslc_static_granules:
            download_dict = {
                'id': cslc_static_granule['granule_id'],
                'revision_id': cslc_static_granule.get('revision_id', 1)
            }

            urls = cslc_static_granule["filtered_urls"]

            for url in urls:
                if url.startswith("s3"):
                    download_dict["s3_url"] = url
                elif url.startswith("https"):
                    download_dict["https_url"] = url

            downloads.append(download_dict)

        product_to_product_filepaths_map = self.perform_download(
            session, es_conn, downloads, args, token, job_id
        )

        return product_to_product_filepaths_map


    def download_ionosphere_files_for_cslc_batch(self, cslc_files, download_dir):
        # Reduce the provided CSLC paths to just the filenames
        cslc_files = list(map(lambda path: basename(path), cslc_files))

        downloaded_ionosphere_dates = set()
        downloaded_ionosphere_files = set()

        for cslc_file in cslc_files:
            logger.info(f'Downloading Ionosphere file for CSLC granule {cslc_file}')

            cslc_file_tokens = cslc_file.split('_')
            acq_datetime = cslc_file_tokens[4]
            acq_date = acq_datetime.split('T')[0]

            logger.debug(f'{acq_date=}')

            if acq_date not in downloaded_ionosphere_dates:
                ionosphere_filepath = ionosphere_download.download_ionosphere_correction_file(
                    dataset_dir=download_dir, product_filepath=cslc_file
                )

                downloaded_ionosphere_dates.add(acq_date)
                downloaded_ionosphere_files.add(ionosphere_filepath)
            else:
                logger.info(f'Already downloaded Ionosphere file for date {acq_date}, skipping...')

        return downloaded_ionosphere_files

    def create_job_params(self, product):
        return [
            {
                "name": "dataset_type",
                "from": "value",
                "type": "text",
                "value": "L2_CSLC_S1"
            },
            {
                "name": "input_dataset_id",
                "type": "text",
                "from": "value",
                "value": product["_source"]["metadata"]["batch_id"]
            },
            {
                "name": "product_metadata",
                "from": "value",
                "type": "object",
                "value": product["_source"]
            }
        ]
