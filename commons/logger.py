import logging

from enum import Enum

import boto3

# set logger and custom filter to handle being run from sciflo
log_format = "[%(asctime)s: %(levelname)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)


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


logger = logging.getLogger("opera_pcm")
logger.setLevel(logging.INFO)
logger.addFilter(LogFilter())


logger_initialized = False
def get_logger(verbose=False, quiet=False):
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

        logging.basicConfig(level=log_level, format=log_format, force=True)

        logger.addFilter(NoLogUtilsFilter())

        logger_initialized = True
        logger.info("Initial logging configuration complete")
        logger.info("Log level set to %s", log_level)

    return logger


class NoLogUtilsFilter(logging.Filter):

    """Filters out large JSON output of HySDS internals. Apply to any logger (typically __main__) or its
    handlers."""
    def filter(self, record):
        if not record.filename == "elasticsearch_utils.py":
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
            and "/_update" not in record.getMessage()


def configure_library_loggers():
    logger_hysds_commons = logging.getLogger("hysds_commons")
    logger_hysds_commons.addFilter(NoJobUtilsFilter())

    logger_elasticsearch = logging.getLogger("elasticsearch")
    logger_elasticsearch.addFilter(NoBaseFilter())

    boto3.set_stream_logger(name='botocore.credentials', level=logging.ERROR)

    import warnings
    from elasticsearch.exceptions import ElasticsearchWarning
    warnings.simplefilter('ignore', ElasticsearchWarning)
    from cryptography.utils import CryptographyDeprecationWarning
    warnings.simplefilter('ignore', CryptographyDeprecationWarning)

