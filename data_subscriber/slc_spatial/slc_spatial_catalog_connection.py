from data_subscriber.slc_spatial.slc_spatial_catalog import SLCSpatialProductCatalog


def get_slc_spatial_catalog_connection(logger):
    return SLCSpatialProductCatalog(logger=logger)
