"""
The Core Metadata Extractor. This catalogs the minimum set
needed by SPDM.

@author: mcayanan
"""
from __future__ import print_function

from builtins import object
import sys
import os
import json
from datetime import datetime

ISO_DATETIME_PATTERN = "%Y-%m-%dT%H:%M:%S.%f"


class CoreMetExtractor(object):
    def get_core_metadata(self, product):
        metadata = {}
        time_now = datetime.utcnow()
        metadata["ProductReceivedTime"] = time_now.strftime(ISO_DATETIME_PATTERN) + "Z"
        metadata["ProductReceivedYear"] = time_now.year
        metadata["ProductReceivedMonth"] = "%02d" % time_now.month
        metadata["ProductReceivedDay"] = "%02d" % time_now.day
        metadata["FileLocation"] = os.path.dirname(product)
        metadata["FileSize"] = os.path.getsize(product)
        metadata["FileName"] = os.path.basename(product)

        return metadata


def main():
    """
    Main entry point
    """
    product = os.path.abspath(sys.argv[1])
    extractor = CoreMetExtractor()
    metadata = extractor.get_core_metadata(product)
    print(json.dumps(metadata))


if __name__ == "__main__":
    main()
