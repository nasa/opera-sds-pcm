import logging

from data_subscriber.slc.slc_catalog_connection import get_slc_catalog_connection

logging.basicConfig(level=logging.INFO)  # Set up logging
LOGGER = logging.getLogger(__name__)

if __name__ == "__main__":
    data_subscriber_catalog = get_slc_catalog_connection(LOGGER)
    data_subscriber_catalog.delete_index()
