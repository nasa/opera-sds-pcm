"""Download of one DISP-NI k-set of NISAR GSLCs and submission of its SCIFLO_L3_DISP_NI job.

gslc_query_hist catalogs a k-set's GSLCs in gslc_catalog and submits one gslc_download job with all k
batch ids. This job gathers the k-set's GSLCs, the compressed GSLCs of the frame's preceding k-sets,
the GUNWs that chain through the k-set's dates (if there are any) and the frequency/polarization the
GSLCs share, then submits the PGE job.

As for DISP-S1, the inputs are only downloaded with --transfer-protocol=https, and re-uploaded to the
dataset bucket. Otherwise the PGE job localizes them straight from ASF's S3.
"""

import asyncio
import hashlib
import os
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import PurePath

import dateutil.parser

from data_subscriber.asf_rtc_download import AsfDaacRtcDownload
from data_subscriber.cmr import (CMR_TIME_FORMAT, COLLECTION_TO_PROVIDER_MAP, Collection, DateTimeRange,
                                 async_query_cmr_v2)
from data_subscriber.cslc_utils import split_download_batch_id
from data_subscriber.gslc.gslc_dependency import GSLCDependency
from data_subscriber.gslc.gslc_utils import (GSLC_DATASET_TYPE, gslc_frequency_polarization,
                                             localize_disp_ni_frame_db, parse_gslc_granule_id,
                                             process_disp_ni_frame_db)
from data_subscriber.gslc.gunw_utils import (GUNW_DATASET_TYPE, GUNW_MODE_POLARIZATION_SETTINGS_FIELD,
                                             build_gunw_native_id_patterns, parse_gunw_granule_id, select_gunws)
from util.aws_util import concurrent_s3_client_try_upload_file
from util.conf_util import SettingsConf
from util.job_submitter import try_submit_mozart_job

COMPRESSED_GSLC_DATASET_TYPE = "L2_GSLC_COMPRESSED"
SCIFLO_JOB_TYPE = "SCIFLO_L3_DISP_NI"
SCIFLO_JOB_QUEUE = "opera-job_worker-sciflo-l3_disp_ni"

# A GUNW's CMR temporal extent runs from its reference pass to its secondary pass, so a window just
# around the k-set's own passes finds every GUNW that starts or ends on one of them
GUNW_QUERY_MARGIN = timedelta(hours=1)


class AsfDaacGslcDownload(AsfDaacRtcDownload):

    def __init__(self, provider, disp_ni_frame_db_file=None):
        super().__init__(provider)
        if disp_ni_frame_db_file is None:
            self.frame_db = localize_disp_ni_frame_db()
        else:
            self.frame_db = process_disp_ni_frame_db(disp_ni_frame_db_file)

    def run_download(self, args, token, es_conn, netloc, username, password, cmr, job_id, rm_downloads_dir=True):
        settings = SettingsConf().cfg
        batch_ids = sorted(args.batch_ids, key=lambda batch_id: split_download_batch_id(batch_id)[1])
        product_id = "_".join(batch_ids)
        frame_id = split_download_batch_id(batch_ids[0])[0]

        docs = [doc for batch_id in batch_ids for doc in self.get_downloads(batch_id, es_conn)]
        position = self.k_set_position(frame_id, docs)

        polarization_codes = {doc["polarization"] for doc in docs}
        if len(polarization_codes) != 1:
            err = RuntimeError("GSLC k-set mixes polarizations")
            err.add_note(f"{product_id}: {sorted(polarization_codes)}")
            raise err
        frequency, polarization = gslc_frequency_polarization(polarization_codes.pop())

        gunw_granules = self.select_gunw_granules(frame_id, docs, token, cmr, settings)

        if args.transfer_protocol == "https":
            gslc_paths = self.download_and_upload([doc["https_url"] for doc in docs], token, product_id, settings,
                                                  rm_downloads_dir)
            gunw_paths = self.download_and_upload([g["https_url"] for g in gunw_granules], token, product_id,
                                                  settings, rm_downloads_dir)
        else:
            self.logger.info("Skipping GSLC download and instead using ASF S3 paths for direct SCIFLO PGE ingestion")
            gslc_paths = [doc.get("s3_url") for doc in docs]
            gunw_paths = [g["s3_url"] for g in gunw_granules]
            if not all(gslc_paths + gunw_paths):
                raise RuntimeError(f"No s3_url found for some inputs of {product_id}. Use the https transfer protocol.")

        # k and m are the k-set's own: the query job wrote the m the walk had ramped to for this k-set
        k, m = int(docs[0]["k"]), int(docs[0]["m"])
        compressed_gslcs = GSLCDependency(k, m, self.frame_db, self.logger).get_dependent_compressed_gslcs(
            frame_id, position, es_conn.es_util)
        if compressed_gslcs is None:
            err = RuntimeError("Compressed GSLC not found")
            err.add_note(f"frame_id={frame_id}: a compressed GSLC of a k-set before position {position} is missing")
            raise err

        compressed_gslc_paths = [path for hit in compressed_gslcs
                                 for path in hit["_source"]["metadata"]["product_s3_paths"] if path.endswith(".h5")]

        product = self.form_product(product_id, frame_id, docs, gslc_paths, compressed_gslc_paths, gunw_paths,
                                    frequency, polarization, args)

        release_version = args.release_version or settings["RELEASE_VERSION"]
        last_cycle = split_download_batch_id(batch_ids[-1])[1]
        submitted = try_submit_mozart_job(
            product=product,
            job_queue=SCIFLO_JOB_QUEUE,
            rule_name=f"trigger-{SCIFLO_JOB_TYPE}",
            params=self.create_job_params(product),
            job_spec=f"job-{SCIFLO_JOB_TYPE}:{release_version}",
            job_name=f"job-WF-{SCIFLO_JOB_TYPE}-frame-{frame_id}-latest_cycle-{last_cycle}",
            payload_hash=hashlib.md5("".join(sorted(gslc_paths)).encode()).hexdigest()
        )

        # Mark the GSLCs as downloaded only after the SCIFLO job has been submitted
        for doc in docs:
            es_conn.mark_product_as_downloaded(doc["unique_id"], job_id)

        return submitted

    def get_downloads(self, batch_id, es_conn):
        downloads = es_conn.get_download_granule_revision(batch_id)
        self.logger.info(f"Found {len(downloads)=} granules for {batch_id=}")
        if not downloads:
            raise RuntimeError(f"No GSLCs cataloged for {batch_id}")
        return downloads

    def k_set_position(self, frame_id, docs) -> int:
        """Index of the k-set's first date among the frame database's sensing dates."""

        frame = self.frame_db.get(frame_id)
        first_date = min(dateutil.parser.isoparse(str(doc["acquisition_ts"])).date() for doc in docs)
        frame_dates = [dt.date() for dt in frame.sensing_datetimes] if frame else []
        if first_date not in frame_dates:
            err = RuntimeError("k-set not in DISP-NI frame DB")
            err.add_note(f"frame_id={frame_id}: {first_date} is not a sensing date of the DISP-NI frame database")
            raise err

        return frame_dates.index(first_date)

    def select_gunw_granules(self, frame_id, docs, token, cmr, settings) -> list[dict]:
        """The CMR granules of the GUNWs the k-set uses, or none if it has no unbroken chain of them.

        Missing GUNWs never fail the job: the SAS then estimates the ionosphere from the GSLCs."""

        mode_polarization = f'{docs[0]["mode"]}_{docs[0]["polarization"]}'
        gunw_mode_polarizations = settings["DISP_NI"].get(GUNW_MODE_POLARIZATION_SETTINGS_FIELD, {}).get(
            mode_polarization, [])
        if not gunw_mode_polarizations:
            self.logger.warning("No GUNW mode/polarization is configured for %s GSLCs, so frame %d gets no GUNWs",
                                mode_polarization, frame_id)
            return []

        cycles_by_nisar_frame = defaultdict(list)
        for doc in docs:
            cycles_by_nisar_frame[doc["nisar_frame"]].append(int(doc["acquisition_cycle"]))

        sensing_starts = [parse_gslc_granule_id(doc["granule_id"]).sensing_start for doc in docs]
        timerange = DateTimeRange((min(sensing_starts) - GUNW_QUERY_MARGIN).strftime(CMR_TIME_FORMAT),
                                  (max(sensing_starts) + GUNW_QUERY_MARGIN).strftime(CMR_TIME_FORMAT))
        items = asyncio.run(async_query_cmr_v2(
            timerange=timerange, provider=COLLECTION_TO_PROVIDER_MAP[Collection.NISAR_GUNW],
            collection=Collection.NISAR_GUNW, token=token, cmr_hostname=cmr,
            native_id_patterns=build_gunw_native_id_patterns(cycles_by_nisar_frame)))

        granules = {}
        for item in items:
            granule = gunw_granule(item)
            if granule is not None:
                granules[granule["gunw"].granule_id] = granule

        selection = select_gunws([g["gunw"] for g in granules.values()], cycles_by_nisar_frame,
                                 gunw_mode_polarizations)
        if selection is None:
            self.logger.warning("No unbroken %s GUNW chain for frame %d cycles %s, so the SAS will estimate the "
                                "ionosphere from the GSLCs", gunw_mode_polarizations, frame_id,
                                dict(cycles_by_nisar_frame))
            return []

        gunw_mode_polarization, chain = selection
        self.logger.info("Using %d %s GUNWs for frame %d: %s", len(chain), gunw_mode_polarization, frame_id,
                         [gunw.granule_id for gunw in chain])
        return [granules[gunw.granule_id] for gunw in chain]

    def download_and_upload(self, https_urls, token, product_id, settings, rm_downloads_dir) -> list[str]:
        local_paths = [self.download_asf_product(url, token, self.downloads_dir, stream=True) for url in https_urls]

        self.logger.info("Uploading %d input files to S3", len(local_paths))
        s3_paths = concurrent_s3_client_try_upload_file(bucket=settings["DATASET_BUCKET"],
                                                        key_prefix=f"tmp/disp_ni/{product_id}",
                                                        files=local_paths)
        if rm_downloads_dir:
            for path in local_paths:
                os.remove(path)

        return s3_paths

    @staticmethod
    def frame_bounding_box(docs) -> list[float]:
        """[min_lon, min_lat, max_lon, max_lat] over the GSLCs' CMR footprints."""

        points = [point for doc in docs for point in (doc.get("bounding_box") or [])]
        lons = [point["lon"] for point in points]
        lats = [point["lat"] for point in points]
        return [min(lons), min(lats), max(lons), max(lats)]

    def form_product(self, product_id, frame_id, docs, gslc_paths, compressed_gslc_paths, gunw_paths, frequency,
                     polarization, args) -> dict:
        latest_cycle = max(int(doc["acquisition_cycle"]) for doc in docs)

        return {
            "_id": product_id,
            "_source": {
                "dataset": f"L3_DISP_NI-{product_id}",
                "metadata": {
                    "batch_id": product_id,
                    "frame_id": frame_id,
                    "ProductReceivedTime": datetime.now(timezone.utc).isoformat().replace("+00:00", "Z"),
                    "product_paths": {
                        GSLC_DATASET_TYPE: gslc_paths,
                        COMPRESSED_GSLC_DATASET_TYPE: compressed_gslc_paths,
                        GUNW_DATASET_TYPE: gunw_paths,
                    },
                    "FileName": product_id,
                    "id": product_id,
                    "bounding_box": self.frame_bounding_box(docs),
                    "frequency": frequency,
                    "polarization": polarization,
                    # Every historical k-set writes the compressed GSLC the next one needs (ICS 2.6.2)
                    "save_compressed_slc": True,
                    "processing_mode": args.proc_mode,
                    "acquisition_cycle": latest_cycle,
                    "Files": [
                        {
                            "FileName": PurePath(s3_path).name,
                            "FileSize": 1,
                            "FileLocation": os.path.dirname(s3_path),
                            "id": PurePath(s3_path).name,
                            "product_paths": "$.product_paths"
                        }
                        for s3_path in gslc_paths + compressed_gslc_paths + gunw_paths
                    ]
                }
            }
        }

    @staticmethod
    def create_job_params(product):
        return [
            {"name": "dataset_type", "from": "value", "type": "text", "value": GSLC_DATASET_TYPE},
            {"name": "input_dataset_id", "from": "value", "type": "text",
             "value": product["_source"]["metadata"]["batch_id"]},
            {"name": "product_metadata", "from": "value", "type": "object", "value": product["_source"]},
        ]


def gunw_granule(item: dict) -> dict | None:
    """A GUNW CMR item as {gunw, s3_url, https_url}, with the URLs of the GUNW itself and not of its
    QA_STATS.h5, which CMR lists next to it. None if the item is not a parseable GUNW with both URLs."""

    granule_id = item["umm"]["GranuleUR"]
    try:
        gunw = parse_gunw_granule_id(granule_id)
    except ValueError:
        return None

    urls = [url_item["URL"] for url_item in item["umm"].get("RelatedUrls", [])
            if PurePath(url_item["URL"]).name == f"{granule_id}.h5"]
    s3_url = next((url for url in urls if url.startswith("s3://")), None)
    https_url = next((url for url in urls if url.startswith("https://")), None)
    if s3_url is None or https_url is None:
        return None

    return {"gunw": gunw, "s3_url": s3_url, "https_url": https_url}
