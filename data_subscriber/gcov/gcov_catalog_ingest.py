"""
GCOV Catalog Ingest

Queries CMR for existing NISAR GCOV granules and creates metadata-only L2_GCOV_NI
datasets with DAAC S3 URLs.  HySDS post-processing publishes these datasets,
which then trigger the dswx-ni evaluator.
"""

import asyncio
import json
import os
import re
import shutil
from datetime import datetime, timezone
from typing import Union, Optional, Sequence, Tuple, Set
from uuid import uuid4

from shapely import from_wkt, orient_polygons, to_geojson
from shapely.geometry import MultiPolygon, Polygon

from data_subscriber.cmr import Collection, get_cmr_token
from data_subscriber.gcov.gcov_granule_util import (extract_track_id, extract_frame_id, extract_cycle_number,
                                                    extract_polarization, extract_bandwidth_mode,
                                                    extract_orbit_direction, extract_crid)
from data_subscriber.gcov_utils import load_mgrs_track_frame_db
from opera_commons.constants import product_metadata as pm
from opera_commons.logger import get_logger
from tools.ops.cmr_audit.cmr_client import async_cmr_posts, paramss_to_request_body
from util.common_util import backoff_wrapper, convert_datetime
from util.ctx_util import JobContext
from util.datasets_json_util import DatasetsJson
from util.exec_util import exec_wrapper

logger = get_logger()


class GcovCatalogIngest:
    """Queries CMR and creates L2_GCOV_NI datasets."""

    def __init__(self, settings, dataset_pattern: re.Pattern, es_conn=None):
        self.mgrs_db = load_mgrs_track_frame_db()
        self.settings = settings
        self.dataset_pattern = dataset_pattern
        self.es_conn = es_conn

    def ingest(
            self,
            mgrs_sets: Sequence[str],
            start_date: str,
            end_date: str,
            use_temporal: bool,
            spatial: Optional[str] = None,
            native_id: Optional[str] = None,
            batch_publish: bool = True
    ):
        """Query CMR for CSLC-S1 granules and create L2_CSLC_S1 datasets.

        Args:
            mgrs_sets: List of MGRS set IDs. If empty or None do not apply filtering.
            start_date: Start date (YYYY-MM-DDTHH:MM:SSZ).
            end_date: End date (YYYY-MM-DDTHH:MM:SSZ).
            use_temporal: Query granules by temporal(acquisition) time rather than revision time.
            spatial: Spatial constraint for granule query. Either a 4-tuple of floats
                     (min_lon, min_lat, max_lon, max_lat) or a shapely Polygon or MultiPolygon.
            native_id: NISAR GCOV granule ID to query. If provided, spatial and temporal params will be ignored
            batch_publish: Publish an additional "batch" dataset containing all cataloged GCOVs
        """
        cmr_hostname, token, _, _, _ = get_cmr_token("OPS", self.settings)

        if mgrs_sets is None:
            mgrs_sets = []

        if native_id is not None and native_id != '':
            # If a native ID is provided, 1) validate it matches the GCOV file naming format and
            # b) strip spatiotemporal params
            if self.dataset_pattern.fullmatch(native_id) is not None:
                raise ValueError(
                    f'Native ID parameter {native_id} does not match expected pattern {self.dataset_pattern.pattern}')

            start_date, end_date, spatial = None, None, None

        if spatial is not None and spatial != '':
            errs = []
            valid = False

            try:
                min_lon, min_lat, max_lon, max_lat = [float(f) for f in spatial.split(',')]

                if min_lat >= max_lat:
                    errs.append(ValueError(f'Minimum latitude cannot be >= maximum latitude'))

                if min_lon >= max_lon:
                    errs.append(ValueError(f'Minimum longitude cannot be >= maximum longitude'))

                if any(c not in range(-180, 180) for c in {min_lon, max_lon}):
                    errs.append(ValueError(f'Longitudes must be between -180 and 180'))

                if any(c not in range(-90, 90) for c in {min_lat, max_lat}):
                    errs.append(ValueError(f'Latitudes must be between -90 and 90'))

                if len(errs) > 0:
                    raise ExceptionGroup('Failed to parse spatial constraint', errs)
                else:
                    spatial = (min_lon, min_lat, max_lon, max_lat)
                    valid = True
            except Exception as e:
                errs.append(ValueError(f'Could not parse {spatial} as bounding box'))
                errs[-1].__cause__ = e

            if not valid:
                try:
                    poly = from_wkt(spatial)

                    if not isinstance(poly, (Polygon, MultiPolygon)):
                        errs.append(TypeError('Spatial filter geometry must be Polygon or MultiPolygon'))
                    else:
                        spatial = poly
                        valid = True
                except Exception as e:
                    errs.append(ValueError(f'Could not parse {spatial} as polygon'))
                    errs[-1].__cause__ = e

            if not valid:
                raise ExceptionGroup('Failed to parse spatial constraint', errs)

        items = self._query_cmr(
            set(mgrs_sets),
            start_date,
            end_date,
            cmr_hostname,
            token,
            use_temporal,
            spatial,
            native_id
        )

        created = self._create_datasets(items, batch_publish, self.es_conn)

        logger.info(f"Catalog ingest complete. Total datasets created: {created}")

    def _query_cmr(
            self,
            mgrs_sets: Set[str],
            start_date: str,
            end_date: str,
            cmr_hostname: str,
            token: str,
            use_temporal: bool,
            spatial: Optional[Union[Tuple[float, float, float, float], Polygon, MultiPolygon]],
            native_id: Optional[str],
    ):
        request_url = f"https://{cmr_hostname}/search/granules.umm_json"
        all_items = []
        seen_ids = set()

        logger.info(f'TEMP: {mgrs_sets=} {start_date=} {end_date=} {use_temporal=} {spatial=} {native_id=}')

        temporal_string = f"{start_date},{end_date}"

        params = {
            "sort_key": "start_date",
            "provider": "ASF",
            "ShortName[]": [Collection.NISAR_GCOV],
            "token": token,
        }

        if start_date is not None or end_date is not None:
            if use_temporal:
                params['temporal'] = temporal_string
            else:
                params['revision_date'] = temporal_string

        if spatial is not None and spatial != '':
            if isinstance(spatial, tuple):
                min_lon, min_lat, max_lon, max_lat = spatial
                params['bounding_box'] = f'{min_lon},{min_lat},{max_lon},{max_lat}'
            elif isinstance(spatial, Polygon):
                spatial = orient_polygons(spatial, exterior_cw=False)
                params['polygon[]'] = ','.join([f'{lon},{lat}' for lon, lat in spatial.exterior.coords])
            elif isinstance(spatial, MultiPolygon):
                polygon_params = []

                for geom in spatial.geoms:
                    geom = orient_polygons(geom, exterior_cw=False)
                    polygon_params.append(','.join([f'{lon},{lat}' for lon, lat in geom.exterior.coords]))

                params['polygon[]'] = polygon_params
                params['options[polygon][or]'] = 'true'
            else:
                raise TypeError(type(spatial))

        if native_id is not None and native_id != '':
            track_number = extract_track_id(native_id)
            cycle_number = extract_cycle_number(native_id)
            orbit_direction = extract_orbit_direction(native_id)
            given_frame_number = extract_frame_id(native_id)

            frames = list(self.mgrs_db.track_and_frame_to_all_frames(track_number, given_frame_number))

            native_ids = [
                f'NISAR_L2_PR_GCOV_{cycle_number}_{track_number}_{orbit_direction}_{frame}_*' for frame in frames
            ]

            if not native_ids:
                raise Exception(
                    f"The supplied {native_id=} is not associated with any frame set"
                )

            params["options[native-id][pattern]"] = 'true'
            params["native-id[]"] = native_ids

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

    def _create_datasets(self, items, batch_publish, es_conn=None):
        """Create the GCOV and GCOV batch datasets from CMR items"""
        created = []
        skipped = 0
        now = datetime.now(timezone.utc).replace(tzinfo=None)

        for item in items:
            granule_ur = item["umm"]["GranuleUR"]

            if not self.dataset_pattern.fullmatch(granule_ur):
                logger.error(f'GCOV granule {granule_ur} does not match pattern {self.dataset_pattern.pattern} and '
                             f'will be dropped. THIS SHOULD NOT HAPPEN!')
                continue

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
                    result = backoff_wrapper(
                        es_conn.es.search,
                        index="grq_*_l2_gcov_ni-*",
                        body={"query": {"term": {"_id": granule_ur}}, "size": 0},
                    )
                    if result["hits"]["total"]["value"] > 0:
                        skipped += 1
                        logger.info(f'Skipping granule {granule_ur} as it has been ingested already')
                        continue
                except Exception as e:
                    logger.warning(f"GRQ check failed for {granule_ur}: {e}. Proceeding with creation. This may trigger "
                                   f"an extra state config evaluation, but this should have no negative impacts.")

            # Extract temporal info
            temporal = item["umm"].get("TemporalExtent", {})
            if temporal.get("RangeDateTime"):
                start_time = temporal["RangeDateTime"]["BeginningDateTime"]
                end_time = temporal["RangeDateTime"]["EndingDateTime"]
            else:
                start_time = temporal.get("SingleDateTime", "")
                end_time = start_time

            revision = item['meta']['revision-id']
            revision_date = item['meta']['revision-date']

            polygon = self._get_polygon_from_cmr_metadata(item)
            polygon_geojson = json.loads(to_geojson(polygon))

            os.makedirs(granule_ur)

            # .met.json — metadata that goes into _source.metadata in ES
            metadata = {
                "track": extract_track_id(granule_ur),
                "frame": extract_frame_id(granule_ur),
                "track_frame": f'{extract_track_id(granule_ur)}_{extract_frame_id(granule_ur)}',  # To simplify querying
                "polarization": extract_polarization(granule_ur),
                "bandwidth_mode": extract_bandwidth_mode(granule_ur),
                "acquisition_cycle": extract_cycle_number(granule_ur),
                "crid": extract_crid(granule_ur),
                "product_s3_paths": s3_urls,
                "product_https_paths": https_urls,
                "catalog_ingest": True,
                'revision_id': revision,
                'revision_date': revision_date,
            }
            met_path = os.path.join(granule_ur, f"{granule_ur}.met.json")
            with open(met_path, "w") as f:
                json.dump(metadata, f, indent=2)

            # .dataset.json — HySDS dataset descriptor
            dataset_info = {
                "version": "1",
                "creation_time": convert_datetime(now),
                pm.START_TIME: start_time,
                pm.END_TIME: end_time,
                pm.LOCATION: polygon_geojson,
                "index": {
                    "suffix": "1_l2_gcov_ni-{}".format(
                        now.strftime("%Y.%m")
                    )
                },
            }
            ds_path = os.path.join(granule_ur, f"{granule_ur}.dataset.json")
            with open(ds_path, "w") as f:
                json.dump(dataset_info, f, indent=2)

            created.append((granule_ur, metadata, start_time, end_time))

        if skipped:
            logger.info(f"Skipped {skipped} datasets already in ES")

        if batch_publish and len(created) > 0:
            batch_id = f'NISAR_GCOV_BATCH_{now.strftime("%Y%m%dT%H%M%S")}_{str(uuid4())}'

            if os.path.isdir(batch_id):
                shutil.rmtree(batch_id)

            os.makedirs(batch_id)

            start_times = []
            end_times = []
            metadata_list = []

            for gcov_id, gcov_metadata, gcov_start_time, gcov_end_time in created:
                start_times.append(gcov_start_time)
                end_times.append(gcov_end_time)
                granule_metadata = {'id': gcov_id}
                granule_metadata.update(gcov_metadata)
                metadata_list.append(granule_metadata)

            batch_metadata = {
                'count': len(metadata_list),
                'granules': metadata_list
            }

            batch_met_path = os.path.join(batch_id, f"{batch_id}.met.json")
            with open(batch_met_path, "w") as f:
                json.dump(batch_metadata, f, indent=2)

            # .dataset.json — HySDS dataset descriptor
            batch_dataset_info = {
                "version": "1",
                "creation_time": convert_datetime(now),
                pm.START_TIME: min(start_times),
                pm.END_TIME: max(end_times),
                "index": {
                    "suffix": "1_l2_gcov_ni_batch-{}".format(
                        now.strftime("%Y.%m")
                    )
                },
            }

            batch_ds_path = os.path.join(batch_id, f"{batch_id}.dataset.json")
            with open(batch_ds_path, "w") as f:
                json.dump(batch_dataset_info, f, indent=2)

        return len(created)

    @staticmethod
    def _get_polygon_from_cmr_metadata(cmr_metadata) -> Union[Polygon, MultiPolygon]:
        """Extract bounding polygon from CMR UMM-G metadata"""
        try:
            g_polygons = cmr_metadata["umm"]["SpatialExtent"]["HorizontalSpatialDomain"]["Geometry"]["GPolygons"]

            if len(g_polygons) == 1:
                points = g_polygons[0]["Boundary"]["Points"]
                poly = Polygon([(point['Longitude'], point['Latitude']) for point in points])
                return poly
            else:
                logger.info(f'GCOV {cmr_metadata["umm"]["GranuleUR"]} boundary is a multi-polygon '
                            f'({len(g_polygons)} sub-geoms)')

                polys = []

                for g_polygon in g_polygons:
                    points = g_polygon["Boundary"]["Points"]
                    polys.append(Polygon([(point['Longitude'], point['Latitude']) for point in points]))

                m_polygon = MultiPolygon(polys)
                return m_polygon
        except Exception as e:
            msg = (f'Failed to get bounding polygon for GCOV {cmr_metadata["umm"]["GranuleUR"]}. Please notify a PCM '
                   f'developer or open a PCM ticket')
            raise RuntimeError(msg) from e


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
    native_id = job_context.get("native_id")
    spatial = job_context.get("spatial")
    batch_publish = job_context.get("batch_publish", True)

    # Parse frame_ids — comma-separated string or list
    if isinstance(mgrs_sets_str, str):
        mgrs_sets = [f.strip() for f in mgrs_sets_str.split(",") if f.strip()]
    else:
        mgrs_sets = mgrs_sets_str

    ds = DatasetsJson()
    try:
        gcov_pattern = re.compile(ds.get('L2_GCOV_NI')['match_pattern'])
    except Exception as e:
        logger.warning(f'Cannot get gcov regex, using .* instead')
        gcov_pattern = re.compile(r'.*')

    settings = SettingsConf().cfg
    es_conn = es_conn_util.get_es_connection(logger)
    ingester = GcovCatalogIngest(settings, gcov_pattern, es_conn=es_conn)
    ingester.ingest(mgrs_sets, start_date, end_date, use_temporal, batch_publish, spatial, native_id)


if __name__ == "__main__":
    ingest()
