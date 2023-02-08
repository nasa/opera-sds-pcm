from data_subscriber.hls.hls_catalog import HLSProductCatalog


def get_hls_catalog_connection(logger):
    return HLSProductCatalog(logger=logger)
