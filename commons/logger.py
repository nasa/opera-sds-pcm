import logging

from enum import Enum

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

        return "/job_specs/_doc/" not in record.getMessage() \
            and "/hysds_ios-grq/_doc/" not in record.getMessage() \
            and "/containers/_doc/" not in record.getMessage()
