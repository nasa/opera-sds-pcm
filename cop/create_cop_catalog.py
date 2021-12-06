import logging
import argparse

from cop.es_connection import get_cop_connection

logging.basicConfig(level=logging.INFO)  # Set up logging
LOGGER = logging.getLogger(__name__)
TIURDROP_ES_INDEX = "tiurdrop_catalog"


def get_parser():
    """
    Get a parser for this application
    @return: parser to for this application
    """
    parser = argparse.ArgumentParser(
        description="Creates a COP catalog at a given endpoint."
    )
    parser.add_argument(
        "--delete_old_catalog",
        action="store_true",
        help="Indicate whether to delete the old COP Catalog if it exists.",
    )
    parser.add_argument(
        "--tiurdrop",
        action="store_true",
        help="Indicate whether its a tiurdrop index creation.",
    )
    parser.add_argument(
        "--delete_old_tiurdrop_catalog",
        action="store_true",
        help="Indicate whether to delete the old tiurdrop Catalog if it exists.",
    )

    return parser


if __name__ == "__main__":
    """
    Script to create the COP Catalog and its mappings. Default is to create the
    catalog in the GRQ ElasticSearch.
    """
    args = get_parser().parse_args()

    delete_old_catalog = False
    delete_old_tiurdrop_catalog = False
    is_tiurdrop = False
    if args.delete_old_catalog:
        delete_old_catalog = True
    if args.tiurdrop:
        is_tiurdrop = True
    if args.delete_old_tiurdrop_catalog:
        delete_old_tiurdrop_catalog = True

    cop_catalog = get_cop_connection(LOGGER)
    if is_tiurdrop:
        cop_catalog.create_index(index=TIURDROP_ES_INDEX, delete_old_index=delete_old_tiurdrop_catalog)
    else:
        cop_catalog.create_index(delete_old_index=delete_old_catalog)
