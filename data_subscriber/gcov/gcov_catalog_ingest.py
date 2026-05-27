"""
GCOV Catalog Ingest

Queries CMR for existing NISAR GCOV granules and creates metadata-only L2_GCOV_NI
datasets with DAAC S3 URLs.  HySDS post-processing publishes these datasets,
which then trigger the dswx-ni evaluator.
"""

import asyncio
import json
import os
from datetime import datetime

from data_subscriber.cmr import Collection, get_cmr_token
from data_subscriber.gcov.gcov_granule_util import (extract_track_id, extract_frame_id, extract_cycle_number,
                                                    extract_polarization, extract_bandwidth_mode)
from data_subscriber.gcov_utils import load_mgrs_track_frame_db
from opera_commons.logger import get_logger
from tools.ops.cmr_audit.cmr_client import async_cmr_posts, paramss_to_request_body
from util.ctx_util import JobContext
from util.exec_util import exec_wrapper

logger = get_logger()


class GcovCatalogIngest:
    """Queries CMR and creates L2_GCOV_NI datasets."""

    def __init__(self, settings, es_conn=None):
        self.mgrs_db = load_mgrs_track_frame_db()
        self.settings = settings
        self.es_conn = es_conn

    def ingest(self, mgrs_sets, start_date, end_date, use_temporal):
        """Query CMR for CSLC-S1 granules and create L2_CSLC_S1 datasets.

        Args:
            mgrs_sets: List of MGRS set IDs. If empty or None do not apply filtering.
            start_date: Start date (YYYY-MM-DDTHH:MM:SSZ).
            end_date: End date (YYYY-MM-DDTHH:MM:SSZ).
            use_temporal: Query granules by temporal(acquisition) time rather than revision time.
        """
        cmr_hostname, token, _, _, _ = get_cmr_token("OPS", self.settings)

        if mgrs_sets is None:
            mgrs_sets = []

        items = self._query_cmr(set(mgrs_sets), start_date, end_date, cmr_hostname, token, use_temporal)

        created = self._create_datasets(items, self.es_conn)

        logger.info(f"Catalog ingest complete. Total datasets created: {created}")

    def _query_cmr(self, mgrs_sets, start_date, end_date,
                   cmr_hostname, token, use_temporal):
        request_url = f"https://{cmr_hostname}/search/granules.umm_json"
        all_items = []
        seen_ids = set()

        temporal_string = f"{start_date},{end_date}"

        params = {
            "sort_key": "start_date",
            "provider": "ASF",
            "ShortName[]": [Collection.NISAR_GCOV_BETA_V1],  # TODO: Update when out of beta
            "token": token,
        }

        if use_temporal:
            params['temporal'] = temporal_string
        else:
            params['revision_date'] = temporal_string

        logger.info(f'Querying CMR at {request_url} with params {json.dumps(params)}')
        items = asyncio.run(self._async_query(request_url, params))

        for item in items:
            granule_ur = item.get("umm", {}).get("GranuleUR", "")
            if mgrs_sets:
                mgrs_sets_for_granule = set(self.mgrs_db.frame_and_track_to_mgrs_sets(
                    {(extract_frame_id(granule_ur), extract_track_id(granule_ur))}
                ))

                if not mgrs_sets & mgrs_sets_for_granule:
                    continue

            if granule_ur not in seen_ids:
                seen_ids.add(granule_ur)
                all_items.append(item)

        return all_items

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
    def _create_datasets(items, es_conn=None):
        created = 0
        skipped = 0
        for item in items:
            granule_ur = item["umm"]["GranuleUR"]

            # Extract S3 URLs for .h5 files
            s3_urls = [
                url_entry["URL"]
                for url_entry in item["umm"].get("RelatedUrls", [])
                if url_entry.get("URL", "").startswith("s3://")
                and url_entry["URL"].endswith(".h5")
                and not url_entry["URL"].endswith("_QA_STATS.h5")
            ]

            # Extract S3 URLs for .h5 files
            https_urls = [
                url_entry["URL"]
                for url_entry in item["umm"].get("RelatedUrls", [])
                if url_entry.get("URL", "").startswith("https://")
                and url_entry["URL"].endswith(".h5")
                and not url_entry["URL"].endswith("_QA_STATS.h5")
            ]

            # Skip if already created in this run
            if os.path.isdir(granule_ur):
                continue

            # Skip if already published in ES (handles retries and historical overlap)
            if es_conn is not None:
                try:
                    result = es_conn.es.search(
                        index="grq_*_l2_gcov_ni-*",
                        body={"query": {"term": {"_id": granule_ur}}, "size": 0},
                    )
                    if result["hits"]["total"]["value"] > 0:
                        skipped += 1
                        logger.info(f'Skipping granule {granule_ur} as it has been ingested already')
                        continue
                except Exception as e:
                    logger.warning(f"ES check failed for {granule_ur}: {e}. Proceeding with creation.")

            # Extract temporal info
            temporal = item["umm"].get("TemporalExtent", {})
            if temporal.get("RangeDateTime"):
                start_time = temporal["RangeDateTime"]["BeginningDateTime"]
                end_time = temporal["RangeDateTime"]["EndingDateTime"]
            else:
                start_time = temporal.get("SingleDateTime", "")
                end_time = start_time

            os.makedirs(granule_ur)

            # .met.json — metadata that goes into _source.metadata in ES
            metadata = {
                "track": extract_track_id(granule_ur),
                "frame": extract_frame_id(granule_ur),
                "track_frame": f'{extract_track_id(granule_ur)}_{extract_frame_id(granule_ur)}',  # To simplify querying
                "polarization": extract_polarization(granule_ur),
                "bandwidth_mode": extract_bandwidth_mode(granule_ur),
                "acquisition_cycle": extract_cycle_number(granule_ur),
                "product_s3_paths": s3_urls,
                "product_https_paths": https_urls,
                "catalog_ingest": True,
            }
            met_path = os.path.join(granule_ur, f"{granule_ur}.met.json")
            with open(met_path, "w") as f:
                json.dump(metadata, f, indent=2)

            # .dataset.json — HySDS dataset descriptor
            dataset_info = {
                "version": "1",
                "starttime": start_time,
                "endtime": end_time,
                "index": {
                    "suffix": "1_l2_gcov_ni-{}".format(
                        datetime.utcnow().strftime("%Y.%m")
                    )
                },
            }
            ds_path = os.path.join(granule_ur, f"{granule_ur}.dataset.json")
            with open(ds_path, "w") as f:
                json.dump(dataset_info, f, indent=2)

            created += 1

        if skipped:
            logger.info(f"Skipped {skipped} datasets already in ES")
        return created


@exec_wrapper
def ingest():
    """HySDS job entry point."""
    from util.conf_util import SettingsConf
    from data_subscriber import es_conn_util

    jc = JobContext("_context.json")
    job_context = jc.ctx

    # Disable no-clobber for catalog ingest. Overlapping bursts between
    # frames can cause the same L2_GCOV_NI product to be published by
    # multiple catalog ingest jobs — this is expected and safe since
    # catalog ingest only writes metadata.
    jc.set('_force_ingest', True)
    jc.save()

    mgrs_sets_str = job_context.get("mgrs_sets", "")
    start_date = job_context.get("start_date")
    end_date = job_context.get("end_date")
    use_temporal = job_context.get("use_temporal", False)

    # Parse frame_ids — comma-separated string or list
    if isinstance(mgrs_sets_str, str):
        mgrs_sets = [f.strip() for f in mgrs_sets_str.split(",") if f.strip()]
    else:
        mgrs_sets = mgrs_sets_str

    settings = SettingsConf().cfg
    es_conn = es_conn_util.get_es_connection(logger)
    ingester = GcovCatalogIngest(settings, es_conn=es_conn)
    ingester.ingest(mgrs_sets, start_date, end_date, use_temporal)


if __name__ == "__main__":
    ingest()
