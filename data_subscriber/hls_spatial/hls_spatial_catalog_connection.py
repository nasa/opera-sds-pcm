from data_subscriber.hls_spatial.hls_spatial_catalog import HLSSpatialProductCatalog


def get_hls_spatial_catalog_connection(logger):
    return HLSSpatialProductCatalog(logger=logger)
