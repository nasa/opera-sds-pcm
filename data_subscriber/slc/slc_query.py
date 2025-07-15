
from data_subscriber.geojson_utils import localize_geojsons
from data_subscriber.query import BaseQuery
from data_subscriber.slc.slc_catalog import SLCSpatialProductCatalog
from geo.geo_util import _NORTH_AMERICA, does_bbox_intersect_north_america
from datetime import datetime
from collections import defaultdict


class SlcCmrQuery(BaseQuery):

    def __init__(self,  args, token, es_conn, cmr, job_id, settings):
        super().__init__(args, token, es_conn, cmr, job_id, settings)

        # For SLC downloads we need to mark whether the granule intersects with North America
        localize_geojsons([_NORTH_AMERICA])

    def update_granule_index(self, granule):
        spatial_catalog_conn = SLCSpatialProductCatalog(self.logger)
        spatial_catalog_conn.process_granule(granule)

    def prepare_additional_fields(self, granule, args, granule_id):
        additional_fields = super().prepare_additional_fields(granule, args, granule_id)
        if does_bbox_intersect_north_america(granule["bounding_box"]):
            additional_fields["intersects_north_america"] = True

        return additional_fields

    def update_url_index(
            self,
            es_conn,
            urls: list[str],
            granule: dict,
            job_id: str,
            query_dt: datetime,
            temporal_extent_beginning_dt: datetime,
            revision_date_dt: datetime,
            *args,
            **kwargs
    ):
        # group pairs of URLs (http and s3) by filename
        filename_to_urls_map = defaultdict(list)

        if self.settings.get('SLC_ALT_SRC', False):
            # TODO: May need to tweak filename. For ESA, each granule == 1 file. We have 2 URLs for each, one for the
            #  preferred compressed endpoint, the other for the raw endpoint. In the original version of this function
            #  that endpoint type was being parsed as the filename, leading to lots of collisions
            filename_to_urls_map[f"{granule['granule_id'][:-4]}.zip"] = urls

            for filename, filename_urls in filename_to_urls_map.items():
                es_conn.process_url(filename_urls, granule, job_id, query_dt, temporal_extent_beginning_dt,
                                    revision_date_dt, *args, filename=filename, provider='ESA', **kwargs)
        else:
            super().update_url_index(
                es_conn,
                urls,
                granule,
                job_id,
                query_dt,
                temporal_extent_beginning_dt,
                revision_date_dt,
                *args,
                **kwargs
            )

    def create_download_job_params(self, query_timerange, chunk_batch_ids):
        download_job_params = super().create_download_job_params(query_timerange, chunk_batch_ids)

        if self.settings.get('SLC_ALT_SRC', False):
            download_job_params.append({
                "name": "provider",
                "value": "--provider=DATASPACE",
                "from": "value"
            })

            self.logger.debug(f"new {download_job_params=}")

        return download_job_params

