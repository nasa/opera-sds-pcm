import argparse
import logging

from data_subscriber.slc.slc_catalog_connection import get_slc_catalog_connection

logging.basicConfig(level=logging.INFO)  # Set up logging
LOGGER = logging.getLogger(__name__)


def get_parser():
    """
    Get a parser for this application
    @return: parser to for this application
    """
    parser = argparse.ArgumentParser(description="Creates a catalog at a given endpoint.")
    parser.add_argument("--delete_old_catalog", action='store_true',
                        help="Indicate whether to delete the old Catalog if it exists.")
    return parser


if __name__ == "__main__":
    args = get_parser().parse_args()

    delete_old_catalog = False
    if args.delete_old_catalog:
        delete_old_catalog = True

    data_subscriber_catalog = get_slc_catalog_connection(LOGGER)
    data_subscriber_catalog.create_index(delete_old_index=delete_old_catalog)
