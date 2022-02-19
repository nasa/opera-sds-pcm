import logging
import argparse

from job_accountability.es_connection import get_job_accountability_connection


logging.basicConfig(level=logging.INFO)  # Set up logging
LOGGER = logging.getLogger(__name__)


def get_parser():
    """
    Get a parser for this application
    @return: parser to for this application
    """
    parser = argparse.ArgumentParser(description="Creates a Job Accountability catalog at a given endpoint.")
    parser.add_argument("--delete_old_catalog", action='store_true',
                        help="Indicate whether to delete the old COP Catalog if it exists.")
    return parser


if __name__ == "__main__":
    """
    Script to create the Pass Accountability Catalog and its mappings. Default is to create the
    catalog in the GRQ ElasticSearch.
    """
    args = get_parser().parse_args()

    delete_old_catalog = False
    if args.delete_old_catalog:
        delete_old_catalog = True

    job_catalog = get_job_accountability_connection(LOGGER)
    job_catalog.create_index(delete_old_index=delete_old_catalog)
