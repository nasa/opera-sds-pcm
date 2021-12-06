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
