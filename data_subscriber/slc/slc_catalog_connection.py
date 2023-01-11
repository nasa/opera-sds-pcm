from data_subscriber.slc.slc_catalog import SLCProductCatalog


def get_slc_catalog_connection(logger):
    return SLCProductCatalog(logger=logger)
