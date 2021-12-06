#!/usr/bin/env python
import os
import logging

log_format = "[%(asctime)s: %(levelname)s/%(name)s/%(funcName)s] %(message)s"
logging.basicConfig(format=log_format, level=logging.INFO)
logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])


def norm_path(path):
    """Normalize path."""
    return os.path.abspath(os.path.normpath(path))
