import logging

from data_subscriber.es_connection import get_data_subscriber_connection

logging.basicConfig(level=logging.INFO)  # Set up logging
LOGGER = logging.getLogger(__name__)

if __name__ == "__main__":
    observation_catalog = get_data_subscriber_connection(LOGGER)
    observation_catalog.create_index()
