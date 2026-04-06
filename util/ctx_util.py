#!/usr/bin/env python
import os
import json
import logging

logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])


class JobContextError(Exception):
    """Exception class for JobContext class."""

    pass


class JobContext(object):
    """Job context class."""

    def __init__(self, file):
        """Construct JobContext instance."""

        logger.info("file: {}".format(file))
        self._file = file
        with open(self._file) as f:
            self._ctx = json.load(f)

    @property
    def file(self):
        return self._file

    @property
    def ctx(self):
        return self._ctx

    def get(self, key):
        try:
            return self._ctx[key]
        except KeyError:
            raise (
                JobContextError(
                    "Context '{}' doesn't exist in {}.".format(key, self._file)
                )
            )

    def set(self, key, val):
        self._ctx[key] = val

    def save(self):
        with open(self._file, "w") as f:
            json.dump(self._ctx, f, indent=2, sort_keys=True)


class DockerParamsError(Exception):
    """Exception class for DockerParams class."""

    pass


class DockerParams(object):
    """Job context class."""

    def __init__(self, file):
        """Construct DockerParams instance."""

        logger.info("file: {}".format(file))
        self._file = file
        with open(self._file) as f:
            self._params = json.load(f)

    @property
    def file(self):
        return self._file

    @property
    def params(self):
        return self._params

    def get(self, key):
        try:
            return self._params[key]
        except KeyError:
            raise (
                DockerParamsError(
                    "Docker params '{}' doesn't exist in {}.".format(key, self._file)
                )
            )


def job_param_by_name(context: dict, name: str):
    """
    Gets the job specification parameter from the _context.json file.
    :param context: the dict representation of _context.json.
    :param name: the name of the job specification parameter.
    """

    for param in context["job_specification"]["params"]:
        if param["name"] == name:
            return param["value"]

    raise Exception(f"param ({name}) not found in _context.json")
