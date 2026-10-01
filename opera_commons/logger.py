import logging

from enum import Enum

import boto3

# set logger and custom filter to handle being run from sciflo
log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)

logger = logging.getLogger("opera_pcm")
"""
DEPRECATED. Public-facing logger for opera-pcm. Kept for backwards compatibility.

Newer code should instead create their own module-level logger via logging.getLogger(__name__)
rather than directly import this global variable,
as this module configures the root logger so that child loggers inherit its filters and configuration.

Older code accessing this constant directly should be updated to no longer do so.
"""

_logger = logging.getLogger(__name__)
"""module level logger. Renamed as to not shadow the existing (DEPRECATED) logger imported elsewhere."""


def init_pool_logger():
    handler = logging.StreamHandler()
    handler.setFormatter(
        logging.Formatter("[%(asctime)s: %(levelname)s/%(name)s] %(message)s")
    )
    logger.setLevel(logging.INFO)
    logger.addHandler(handler)


class LogLevels(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"

    @staticmethod
    def list():
        return list(map(lambda c: c.value, LogLevels))

    def __str__(self):
        return self.value

    @staticmethod
    def set_level(level):
        if level == LogLevels.DEBUG.value:
            logger.setLevel(logging.DEBUG)
        elif level == LogLevels.INFO.value:
            logger.setLevel(logging.INFO)
        elif level == LogLevels.WARNING.value:
            logger.setLevel(logging.WARNING)
        elif level == LogLevels.ERROR.value:
            logger.setLevel(logging.ERROR)
        else:
            raise RuntimeError(
                "{} is not a valid logging level. "
                "Should be one of the following: {}".format(level, LogLevels.list())
            )


class LogFilter(logging.Filter):
    def filter(self, record):
        if not hasattr(record, "id"):
            record.id = "--"
        return True


logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())

logger_initialized = False
def get_logger(verbose=False, quiet=False, log_format_override=None):
    """
    Configures the root logger. Returns the base opera-pcm logger.

    intentionally return base opera-pcm logger for backwards compatibility until legacy logger usage is updated.
    """
    global logger_initialized

    if not logger_initialized:

        if verbose:
            log_level = LogLevels.DEBUG.value
        elif quiet:
            log_level = LogLevels.WARNING.value
        else:
            log_level = LogLevels.INFO.value

        if verbose:
            log_format = '[%(asctime)s: %(levelname)s/%(module)s:%(funcName)s:%(lineno)d] %(message)s'
        else:
            log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
        if log_format_override:
            log_format = log_format_override

        logging.basicConfig(level=log_level, format=log_format, force=True)

        root_logger = logging.getLogger()  # configure root logger
        root_logger.addFilter(LogFilter())

        root_logger.addFilter(NoLogUtilsFilter())
        root_logger.info("Added logging filter for elasticsearch_utils/opensearch_utils")

        logger_initialized = True
        root_logger.info("Initial logging configuration complete")
        root_logger.info("Log level set to %s", log_level)

    return logger


class NoLogUtilsFilter(logging.Filter):
    """Filters out large JSON output of HySDS internals. Apply to any logger (typically __main__) or its
    handlers."""

    def filter(self, record):
        if not record.filename == "elasticsearch_utils.py":
            return True
        if not record.filename == "opensearch_utils.py":
            return True

        return record.funcName != "update_document"


class NoJobUtilsFilter(logging.Filter):
    """Filters out large JSON output of HySDS internals. Apply to the logger named "hysds_commons" or one of its
    handlers."""

    def filter(self, record):
        if not record.filename == "job_utils.py":
            return True

        return record.funcName not in (
            "resolve_mozart_job", "get_params_for_submission", "submit_mozart_job",
            "resolve_hysds_job", "submit_hysds_job"
        )


class NoBaseFilter(logging.Filter):
    """Filters out lower-level elasticsearch HTTP chatter. Apply to the logger named "elasticsearch" or to one of its
    handlers."""

    def filter(self, record):
        if not record.filename == "base.py":
            return True
        if not record.funcName == "log_request_success":
            return True

        # 'POST http://<es host>/<index pattern>/_search?... [status:200 request:0.013s]'
        # 'POST http://<es host>/<index>/_update/<product ID> [status:200 request:0.018s]'
        return "/job_specs/_doc/" not in record.getMessage() \
            and "/hysds_ios-grq/_doc/" not in record.getMessage() \
            and "/containers/_doc/" not in record.getMessage() \
            and "/_search?" not in record.getMessage() \
            and "/_update" not in record.getMessage() \
            and "/_search/scroll" not in record.getMessage()  # helpers.scan(...)


def configure_library_loggers():
    """
    Perform additional common logging configuration.
    """
    logger_hysds_commons = logging.getLogger("hysds_commons")
    logger_hysds_commons.addFilter(NoJobUtilsFilter())
    _logger.info("Added logging filter for hysds_commons")

    logger_elasticsearch = logging.getLogger("elasticsearch")
    logger_elasticsearch.addFilter(NoBaseFilter())
    _logger.info("Added logging filter for elasticsearch")

    logger_elasticsearch = logging.getLogger("opensearch")
    logger_elasticsearch.addFilter(NoBaseFilter())
    _logger.info("Added logging filter for opensearch")

    boto3.set_stream_logger(name='botocore.credentials', level=logging.ERROR)
    _logger.info("Configuring boto3 logger")

    import warnings
    from elasticsearch.exceptions import ElasticsearchWarning
    warnings.simplefilter('ignore', ElasticsearchWarning)
    _logger.info("Filtering (ignore) ElasticsearchWarning")
    from cryptography.utils import CryptographyDeprecationWarning
    warnings.simplefilter('ignore', CryptographyDeprecationWarning)
    _logger.info("Filtering (ignore) CryptographyDeprecationWarning")
