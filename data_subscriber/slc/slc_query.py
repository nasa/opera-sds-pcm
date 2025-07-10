
from data_subscriber.geojson_utils import localize_geojsons
from data_subscriber.query import BaseQuery
from data_subscriber.slc.slc_catalog import SLCSpatialProductCatalog
from geo.geo_util import _NORTH_AMERICA, does_bbox_intersect_north_america


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
