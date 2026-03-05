"""CSLC-S1 Catalog Ingest (Path B).

Queries CMR for existing CSLC-S1 granules and creates metadata-only L2_CSLC_S1
datasets with DAAC S3 URLs.  HySDS post-processing publishes these datasets,
which then trigger the per-cycle evaluator (Stage 1).

This allows bootstrapping the evaluator pipeline from CMR inventory without
needing to re-run the CSLC PGE.
"""

import asyncio
import json
import logging
import os

from util.exec_util import exec_wrapper
from util.ctx_util import JobContext

from data_subscriber.cmr import Collection, get_cmr_token
from data_subscriber.cslc_utils import (
    localize_disp_frame_burst_hist,
    build_cslc_native_ids,
)
from tools.ops.cmr_audit.cmr_client import async_cmr_posts, paramss_to_request_body

logger = logging.getLogger(__name__)


class CslcCatalogIngest:
    """Queries CMR and creates metadata-only L2_CSLC_S1 datasets."""

    def __init__(self, settings):
        self.frame_to_bursts, self.burst_to_frames, _ = localize_disp_frame_burst_hist()
        self.settings = settings

    def ingest(self, frame_ids, start_date, end_date):
        """Query CMR for CSLC-S1 granules and create L2_CSLC_S1 datasets.

        Args:
            frame_ids: List of frame IDs (ints or strings).
            start_date: Start date (YYYY-MM-DDTHH:MM:SSZ).
            end_date: End date (YYYY-MM-DDTHH:MM:SSZ).
        """
        cmr_hostname, token, _, _, _ = get_cmr_token("OPS", self.settings)

        total_created = 0
        for frame_id in frame_ids:
            frame_id = int(frame_id)
            if frame_id not in self.frame_to_bursts:
                logger.warning(f"Frame {frame_id} not in constDB. Skipping.")
                continue

            items = self._query_cmr_for_frame(
                frame_id, start_date, end_date, cmr_hostname, token
            )
            logger.info(f"Frame {frame_id}: found {len(items)} granules in CMR")

            created = self._create_datasets(items)
            total_created += created
            logger.info(f"Frame {frame_id}: created {created} datasets")

        logger.info(f"Catalog ingest complete. Total datasets created: {total_created}")

    def _query_cmr_for_frame(self, frame_id, start_date, end_date,
                             cmr_hostname, token):
        """Query CMR for CSLC-S1 granules matching a frame's burst IDs."""
        count, native_id = build_cslc_native_ids(frame_id, self.frame_to_bursts)
        if count == 0:
            return []

        request_url = f"https://{cmr_hostname}/search/granules.umm_json"
        params = {
            "sort_key": "-start_date",
            "provider": "ASF",
            "ShortName[]": [Collection.CSLC_S1_V1],
            "token": token,
            "native-id[]": [native_id],
            "options[native-id][pattern]": "true",
            "temporal": f"{start_date},{end_date}",
        }

        items = asyncio.run(self._async_query(request_url, params))
        return items

    @staticmethod
    async def _async_query(request_url, params):
        """Run the CMR query and return raw UMM JSON items."""
        response_jsons = await async_cmr_posts(
            request_url, paramss_to_request_body([params])
        )
        return [
            item
            for rj in response_jsons
            for item in rj.get("items", [])
        ]

    @staticmethod
    def _create_datasets(items):
        """Create metadata-only L2_CSLC_S1 dataset dirs from CMR UMM items.

        Each dataset dir contains {id}/{id}.met.json and {id}/{id}.dataset.json.
        HySDS post-processing (publish_datasets_parallel) publishes them to ES,
        which then triggers the per-cycle evaluator via the trigger rule.
        """
        created = 0
        for item in items:
            granule_ur = item["umm"]["GranuleUR"]

            # VV-only — skip VH granules
            if "_VV_" not in granule_ur:
                continue

            # Extract S3 URLs for .h5 files
            s3_urls = [
                url_entry["URL"]
                for url_entry in item["umm"].get("RelatedUrls", [])
                if url_entry.get("URL", "").startswith("s3://")
                and url_entry["URL"].endswith(".h5")
            ]
            if not s3_urls:
                logger.warning(f"No S3 .h5 URLs for {granule_ur}. Skipping.")
                continue

            # Skip if already created in this run
            if os.path.isdir(granule_ur):
                continue

            # Extract temporal info
            temporal = item["umm"].get("TemporalExtent", {})
            if temporal.get("RangeDateTime"):
                start_time = temporal["RangeDateTime"]["BeginningDateTime"]
            else:
                start_time = temporal.get("SingleDateTime", "")

            os.makedirs(granule_ur)

            # .met.json — metadata that goes into _source.metadata in ES
            metadata = {
                "product_s3_paths": s3_urls,
                "catalog_ingest": True,
            }
            met_path = os.path.join(granule_ur, f"{granule_ur}.met.json")
            with open(met_path, "w") as f:
                json.dump(metadata, f, indent=2)

            # .dataset.json — HySDS dataset descriptor
            dataset_info = {
                "version": "1",
                "starttime": start_time,
            }
            ds_path = os.path.join(granule_ur, f"{granule_ur}.dataset.json")
            with open(ds_path, "w") as f:
                json.dump(dataset_info, f, indent=2)

            created += 1

        return created


@exec_wrapper
def ingest():
    """HySDS job entry point."""
    from util.conf_util import SettingsConf

    jc = JobContext("_context.json")
    job_context = jc.ctx

    frame_ids_str = job_context.get("frame_ids", "")
    start_date = job_context.get("start_date")
    end_date = job_context.get("end_date")

    # Parse frame_ids — comma-separated string or list
    if isinstance(frame_ids_str, str):
        frame_ids = [f.strip() for f in frame_ids_str.split(",") if f.strip()]
    else:
        frame_ids = frame_ids_str

    settings = SettingsConf().cfg
    ingester = CslcCatalogIngest(settings)
    ingester.ingest(frame_ids, start_date, end_date)


if __name__ == "__main__":
    ingest()
