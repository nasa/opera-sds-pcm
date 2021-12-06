from enum import Enum


class ProcessingType(Enum):
    FORWARD = "forward"
    REPROCESSING = "reprocessing"
    URGENT = "urgent"

    @staticmethod
    def list():
        return list(map(lambda c: c.value, ProcessingType))

    def __str__(self):
        return self.value
