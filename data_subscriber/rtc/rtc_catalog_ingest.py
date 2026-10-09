"""
RTC Catalog Ingest

Queries CMR for existing OPERA RTC granules and creates a metadata-only L2_RTC_S1_BATCH
dataset pointing to the RTC granule IDs. HySDS post-processing publishes these datasets,
which then trigger the dswx-s1 evaluator.
"""

import asyncio
import json
import os
import re
from typing import Union, Optional, Iterable, Tuple, Set

from shapely import from_wkt, orient_polygons
from shapely.geometry import MultiPolygon, Polygon

from data_subscriber.cmr import Collection, get_cmr_token
from data_subscriber.rtc.mgrs_bursts_collection_db_client import (cached_load_mgrs_burst_db,
                                                                  get_reduced_rtc_native_id_patterns,
                                                                  burst_id_to_mgrs_set_ids)
from data_subscriber.rtc.rtc_batch import create_rtc_batch_dataset
from opera_commons.logger import get_logger
from tools.ops.cmr_audit.cmr_client import async_cmr_posts, paramss_to_request_body
from util.ctx_util import JobContext
from util.datasets_json_util import DatasetsJson
from util.exec_util import exec_wrapper
from util.job_util import supply_job_id

logger = get_logger()


class RTCCatalogIngest:
    """Queries CMR and create L2_RTC_S1_BATCH dataset."""

    def __init__(self, settings, dataset_pattern: re.Pattern, es_conn=None):
        self.mgrs_db = cached_load_mgrs_burst_db(filter_land=False)
        self.settings = settings
        self.dataset_pattern = dataset_pattern
        self.es_conn = es_conn

    def ingest(
            self,
            mgrs_sets: Iterable[str],
            start_date: str,
            end_date: str,
            use_temporal: bool,
            spatial: Optional[str] = None,
            native_id: Optional[str] = None,
    ):
        """Query CMR for RTC-S1 granules and create L2_RTC_S1_BATCH dataset.

        Args:
            mgrs_sets: List of MGRS set IDs. If empty or None do not apply filtering.
            start_date: Start date (YYYY-MM-DDTHH:MM:SSZ).
            end_date: End date (YYYY-MM-DDTHH:MM:SSZ).
            use_temporal: Query granules by temporal(acquisition) time rather than revision time.
            spatial: Spatial constraint for granule query. Either a 4-tuple of floats
                     (min_lon, min_lat, max_lon, max_lat) or a shapely Polygon or MultiPolygon.
            native_id: NISAR GCOV granule ID to query. If provided, spatial and temporal params will be ignored
        """
        cmr_hostname, token, _, _, _ = get_cmr_token("OPS", self.settings)

        if mgrs_sets is None:
            mgrs_sets = []

        if native_id is not None and native_id != '':
            # If a native ID is provided, 1) validate it matches the GCOV file naming format and
            # b) strip spatiotemporal params
            if self.dataset_pattern.fullmatch(native_id) is None:
                raise ValueError(
                    f'Native ID parameter "{native_id}" does not match '
                    f'expected pattern "{self.dataset_pattern.pattern}"'
                )

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

                if any(not (-180 <= c <= 180) for c in {min_lon, max_lon}):
                    errs.append(ValueError(f'Longitudes must be between -180 and 180'))

                if any(not (-90 <= c <= 90) for c in {min_lat, max_lat}):
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

        self._create_datasets(items, self.es_conn)

        logger.info("Catalog ingest complete.")

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

        params = {
            "sort_key": "start_date",
            "provider": "ASF",
            "ShortName[]": [Collection.RTC_S1_V1],
            "token": token,
        }

        if start_date is not None or end_date is not None:
            if start_date is None:
                start_date = ''
            if end_date is None:
                end_date = ''

            temporal_string = f"{start_date},{end_date}"

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
            # TODO: Validate native ID?

            burst_id = native_id.split('_')[3]
            native_ids = get_reduced_rtc_native_id_patterns(
                self.mgrs_db[ self.mgrs_db["bursts"].str.contains(burst_id)]
            )

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
            burst_id = granule_ur.split('_')[3]
            if mgrs_sets:
                mgrs_sets_for_granule = set(burst_id_to_mgrs_set_ids(self.mgrs_db, burst_id))

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

    def _create_datasets(self, items, es_conn=None):
        """Create the RTC batch dataset from CMR items"""
        rtc_ids = {}
        job_id = supply_job_id()

        for item in items:
            granule_ur = item["umm"]["GranuleUR"]

            if not self.dataset_pattern.fullmatch(granule_ur):
                logger.error(f'RTC granule {granule_ur} does not match pattern {self.dataset_pattern.pattern} and '
                             f'will be dropped. THIS SHOULD NOT HAPPEN!')
                continue

            granule_id = item["umm"]["GranuleUR"]
            sensor = granule_id.split("_")[6]

            rtc_ids.setdefault(sensor, []).append(granule_ur)

        for sensor, granule_ids in rtc_ids.items():
            create_rtc_batch_dataset(
                os.curdir,  # TODO: is this right? GCOV uses relative paths so I don't have an absolute reference
                rtc_ids=granule_ids,
                sensor=sensor,
                source=f'catalog-ingest-job:{job_id}'
            )


@exec_wrapper
def ingest():
    """HySDS job entry point."""
    from util.conf_util import SettingsConf
    from data_subscriber import es_conn_util

    jc = JobContext("_context.json")
    job_context = jc.ctx

    # Disable no-clobber for catalog ingest. Overlapping bursts between
    # frames can cause the same L2_RTC_S1_BATCH product to be published by
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

    if native_id == '':
        native_id = None

    if spatial == '':
        spatial = None

    # Parse frame_ids — comma-separated string or list
    if isinstance(mgrs_sets_str, str):
        mgrs_sets = [f.strip() for f in mgrs_sets_str.split(",") if f.strip()]
    else:
        mgrs_sets = mgrs_sets_str

    ds = DatasetsJson()
    try:
        gcov_pattern = re.compile(ds.get('L2_RTC_S1')['match_pattern'])
    except Exception as e:
        logger.warning(f'Cannot get rtc regex, using .* instead')
        gcov_pattern = re.compile(r'.*')

    settings = SettingsConf().cfg
    es_conn = es_conn_util.get_es_connection(logger)
    ingester = RTCCatalogIngest(settings, gcov_pattern, es_conn=es_conn)
    ingester.ingest(mgrs_sets, start_date, end_date, use_temporal, spatial, native_id)


if __name__ == "__main__":
    ingest()
