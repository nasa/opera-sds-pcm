#!/usr/bin/env python
import os
import logging

logger = logging.getLogger(__name__)


def norm_path(path):
    """Normalize path."""
    return os.path.abspath(os.path.normpath(path))
