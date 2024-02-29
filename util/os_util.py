#!/usr/bin/env python
import os
import logging

logger = logging.getLogger(os.path.splitext(os.path.basename(__file__))[0])


def norm_path(path):
    """Normalize path."""
    return os.path.abspath(os.path.normpath(path))
